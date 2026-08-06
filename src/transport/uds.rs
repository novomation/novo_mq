use crate::{
    ReceiveError::{self, InternalBufferToSmall},
    SendError::{self},
    helpers::set_calling_thread_to_nrt,
    transport::{
        MAX_CONNECTIONS, TransportConnection, TransportError, TransportListener,
        triple_buffer::{self, required_size},
    },
};
use memmap2::MmapMut;
use nix::{
    errno::Errno::ENOMEM,
    sys::{
        epoll::{Epoll, EpollCreateFlags, EpollEvent, EpollFlags, EpollTimeout},
        eventfd::{EfdFlags, EventFd},
    },
};
use nix::{
    errno::Errno::{EAGAIN, ECONNRESET, ENOENT},
    sys::socket::{self, AddressFamily, Backlog, MsgFlags, RecvMsg, SockFlag, SockType, UnixAddr},
};
use std::{
    collections::HashMap,
    io::IoSliceMut,
    os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd},
    path::Path,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
};

pub fn set_nonblock(fd: nix::libc::c_int) -> Result<(), nix::errno::Errno> {
    unsafe {
        let flags = nix::libc::fcntl(fd, nix::libc::F_GETFL);
        let ret = nix::libc::fcntl(fd, nix::libc::F_SETFL, flags | nix::libc::O_NONBLOCK);
        match ret {
            -1 => Err(nix::errno::Errno::last()),
            _ => Ok(()),
        }
    }
}

pub mod shm_helpers {
    use crate::TransportError;
    use nix::cmsg_space;
    use nix::sys::memfd::{MFdFlags, memfd_create};
    use nix::sys::socket::{ControlMessage, ControlMessageOwned, MsgFlags, recvmsg, sendmsg};
    use nix::unistd::ftruncate;
    use std::io::{IoSlice, IoSliceMut};
    use std::os::fd::{AsFd, FromRawFd, OwnedFd, RawFd};

    #[derive(Debug, thiserror::Error)]
    pub enum FdPassError {
        #[error("nix error: {0}")]
        Nix(#[from] nix::Error),
        #[error("no file descriptor received")]
        NoFd,
        #[error("unexpected ancillary data")]
        UnexpectedAncillaryData,
    }

    pub fn send_memfd(sock: RawFd, fd: RawFd) -> Result<(), FdPassError> {
        let data = [0u8; 1];
        let iov = [IoSlice::new(&data)];
        let fds = [fd];
        let cmsg = [ControlMessage::ScmRights(&fds)];
        sendmsg::<()>(sock, &iov, &cmsg, MsgFlags::empty(), None)?;
        Ok(())
    }

    pub fn recv_memfd(sock: RawFd) -> Result<OwnedFd, FdPassError> {
        let mut data = [0u8; 1];
        let mut iov = [IoSliceMut::new(&mut data)];
        let mut cmsgspace = cmsg_space!([RawFd; 1]);
        let msg = recvmsg::<()>(sock, &mut iov, Some(&mut cmsgspace), MsgFlags::empty())?;
        for cmsg in msg.cmsgs()? {
            if let ControlMessageOwned::ScmRights(fds) = cmsg {
                if let Some(&fd) = fds.first() {
                    // SAFETY:
                    // The fd was received from SCM_RIGHTS and ownership
                    // is transferred to us.
                    return Ok(unsafe { OwnedFd::from_raw_fd(fd) });
                }
            }
        }
        Err(FdPassError::NoFd)
    }

    pub fn create_memfd(size: usize, name: &str) -> Result<OwnedFd, TransportError> {
        let fd = memfd_create(name, MFdFlags::MFD_CLOEXEC)?;
        ftruncate(fd.as_fd(), size as i64)?;
        Ok(fd)
    }
}

/// Hook for attaching connection-specific state/behaviour (e.g. a shared-memory
/// mapping negotiated over the socket) to a plain [`UnixDatagram`].
///
/// The default `()` implementation is a no-op, so existing code that doesn't
/// need any extra per-connection data keeps working unchanged.
pub trait BulkTransport: Sized + Send {
    type Config;
    /// Called right after the listener's `accept()` succeeds, before the
    /// connection is handed back to the caller.
    fn on_accept(fd: RawFd) -> Result<Self, TransportError>;
    /// Called right after `connect()` succeeds.
    fn on_connect(fd: RawFd, config: Self::Config) -> Result<Self, TransportError>;
    /// Called to send messages
    fn send_one_msg(&mut self, fd: RawFd, buf: &[u8], flags: MsgFlags) -> nix::Result<usize>;
    /// Called to receive messages
    fn recv_one_msg(
        &mut self,
        fd: RawFd,
        buf: &mut [u8],
        flags: MsgFlags,
    ) -> Result<usize, super::ReceiveError>;
}

/// Bidirectional triple-buffer transport.
///
/// Each side owns one writer (for its outgoing direction) and one reader (for
/// the incoming direction), backed by two separate memfd-based shared-memory
/// regions that are negotiated during `on_accept` / `on_connect`.
///
/// Internally, `(usize, T)` is stored so that the actual message length is
/// preserved alongside the fixed-size data buffer.
#[derive(Debug)]
pub struct TbTransport<T: Send> {
    writer: triple_buffer::Writer<(usize, T)>,
    reader: triple_buffer::Reader<(usize, T)>,
    /// Keeps the writer-side mmap alive.
    _writer_mmap: MmapMut,
    _writer_memfd: OwnedFd,
    /// Keeps the reader-side mmap alive.
    _reader_mmap: MmapMut,
    _reader_memfd: OwnedFd,
    conn_is_lost: Arc<AtomicBool>,
}

/// Handle used to hand a connection's fd off to the shared connection-loss
/// monitor thread. All connections (both `on_accept` and `on_connect`) are
/// watched by the same background thread via a single `epoll` instance;
/// `fd_queue` enqueues new fds to watch and `event_fd` wakes up the monitor's
/// blocking `epoll_wait` so it picks up the new fd promptly.
#[derive(Clone)]
pub struct TbTransportConfig {
    fd_queue: mpsc::Sender<(RawFd, Arc<AtomicBool>)>,
    event_fd: Arc<EventFd>,
}

impl TbTransportConfig {
    pub fn new() -> Self {
        connection_monitor().clone()
    }

    /// Registers `fd` with the monitor thread and returns the flag that will
    /// be set to `true` once the peer's connection is detected as lost.
    fn register(&self, fd: RawFd) -> Arc<AtomicBool> {
        let conn_is_lost = Arc::new(AtomicBool::new(false));
        if self.fd_queue.send((fd, conn_is_lost.clone())).is_err() {
            log::error!("Connection monitor thread is gone; not tracking fd {fd}");
            return conn_is_lost;
        }
        // Wake up epoll_wait so it registers the new fd promptly.
        let _ = self.event_fd.write(1);
        conn_is_lost
    }
}

impl Default for TbTransportConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Lazily starts the single background thread that detects connection loss
/// (via `epoll`) for every `TbTransport` connection, and returns a cloneable
/// handle to it.
fn connection_monitor() -> &'static TbTransportConfig {
    static MONITOR: OnceLock<TbTransportConfig> = OnceLock::new();
    MONITOR.get_or_init(|| {
        let (fd_queue, rx) = mpsc::channel();
        let event_fd =
            Arc::new(EventFd::from_flags(EfdFlags::EFD_NONBLOCK).expect("create eventfd"));
        let monitor_event_fd = event_fd.clone();
        std::thread::Builder::new()
            .name("novomq-conn-monitor".to_string())
            .spawn(move || {
                set_calling_thread_to_nrt();
                run_connection_monitor(rx, monitor_event_fd);
            })
            .expect("spawn connection monitor thread");
        TbTransportConfig { fd_queue, event_fd }
    })
}

/// Token used as the `epoll` event data for the wake-up eventfd, so it can be
/// told apart from a real connection fd (which is always a small, positive
/// integer).
const WAKE_TOKEN: u64 = u64::MAX;

/// Body of the shared connection-loss monitor thread. Waits on `event_fd`
/// (used to wake up when new connections are queued via `rx`) and on every
/// fd handed to it, then marks the corresponding `Arc<AtomicBool>` once the
/// peer's socket signals hang-up/error.
fn run_connection_monitor(rx: mpsc::Receiver<(RawFd, Arc<AtomicBool>)>, event_fd: Arc<EventFd>) {
    let epoll = match Epoll::new(EpollCreateFlags::EPOLL_CLOEXEC) {
        Ok(epoll) => epoll,
        Err(err) => {
            log::error!("Connection monitor: could not create epoll instance: {err}");
            return;
        }
    };

    let wake_event = EpollEvent::new(EpollFlags::EPOLLIN, WAKE_TOKEN);
    if let Err(err) = epoll.add(&*event_fd, wake_event) {
        log::error!("Connection monitor: could not register wake eventfd: {err}");
        return;
    }

    let mut connections: HashMap<RawFd, Arc<AtomicBool>> = HashMap::new();
    let mut events = vec![EpollEvent::empty(); 64];
    loop {
        let n = match epoll.wait(&mut events, EpollTimeout::NONE) {
            Ok(n) => n,
            Err(nix::errno::Errno::EINTR) => continue,
            Err(err) => {
                log::error!("Connection monitor: epoll_wait failed: {err}");
                break;
            }
        };

        for event in &events[..n] {
            let token = event.data();
            if token == WAKE_TOKEN {
                // Drain the eventfd counter, then register any newly-queued fds.
                let _ = event_fd.read();
                while let Ok((fd, conn_is_lost)) = rx.try_recv() {
                    let interest = EpollEvent::new(
                        EpollFlags::EPOLLIN
                            | EpollFlags::EPOLLRDHUP
                            | EpollFlags::EPOLLERR
                            | EpollFlags::EPOLLHUP,
                        fd as u64,
                    );
                    // Safety: `fd` is a valid, open descriptor owned elsewhere for
                    // at least the duration of this call; we only borrow it to
                    // register it with epoll.
                    let borrowed = unsafe { BorrowedFd::borrow_raw(fd) };
                    match epoll.add(borrowed, interest) {
                        Ok(()) => {
                            connections.insert(fd, conn_is_lost);
                        }
                        Err(err) => {
                            log::warn!("Connection monitor: could not watch fd {fd}: {err}");
                        }
                    }
                }
            } else {
                let fd = token as RawFd;
                if let Some(conn_is_lost) = connections.remove(&fd) {
                    conn_is_lost.store(true, Ordering::Relaxed);
                    // Safety: same as above; `fd` is still open at this point.
                    let borrowed = unsafe { BorrowedFd::borrow_raw(fd) };
                    let _ = epoll.delete(borrowed);
                    log::info!("Connection monitor: connection lost for fd {fd}");
                }
            }
        }
    }
}

impl<T: Send> BulkTransport for TbTransport<T>
where
    T: Default + AsMut<[u8]> + AsRef<[u8]> + Clone + Sized,
{
    type Config = TbTransportConfig;
    fn on_accept(fd: RawFd) -> Result<Self, TransportError> {
        let conn_is_lost = connection_monitor().register(fd);
        // Channel A: this side writes, peer reads.
        let writer_memfd = shm_helpers::create_memfd(
            triple_buffer::required_size::<(usize, T)>(),
            &uuid::Uuid::new_v4().to_string(),
        )
        .unwrap();
        let mut writer_mmap = unsafe { MmapMut::map_mut(&writer_memfd).unwrap() };
        let (writer, _) = unsafe { triple_buffer::triple_buffer(writer_mmap.as_mut_ptr()) };

        // Channel B: peer writes, this side reads.
        let reader_memfd = shm_helpers::create_memfd(
            triple_buffer::required_size::<(usize, T)>(),
            &uuid::Uuid::new_v4().to_string(),
        )
        .unwrap();
        let mut reader_mmap = unsafe { MmapMut::map_mut(&reader_memfd).unwrap() };
        let (_, reader) = unsafe { triple_buffer::triple_buffer(reader_mmap.as_mut_ptr()) };

        // Send channel A fd first, then channel B fd.
        shm_helpers::send_memfd(fd, writer_memfd.as_raw_fd()).unwrap();
        shm_helpers::send_memfd(fd, reader_memfd.as_raw_fd()).unwrap();

        log::info!(
            "Allocated 2*{} bytes for bidirectional shared-memory transport",
            triple_buffer::required_size::<(usize, T)>()
        );
        Ok(Self {
            writer,
            reader,
            _writer_mmap: writer_mmap,
            _writer_memfd: writer_memfd,
            _reader_mmap: reader_mmap,
            _reader_memfd: reader_memfd,
            conn_is_lost,
        })
    }

    fn on_connect(fd: RawFd, config: Self::Config) -> Result<Self, TransportError> {
        let recv = || -> Result<OwnedFd, TransportError> {
            loop {
                match shm_helpers::recv_memfd(fd.as_raw_fd()) {
                    Ok(owned) => return Ok(owned),
                    Err(shm_helpers::FdPassError::Nix(nix::errno::Errno::EAGAIN)) => {
                        std::thread::sleep(std::time::Duration::from_millis(10));
                    }
                    Err(e) => {
                        log::error!("Error receiving fd: {}", e);
                        return Err(TransportError::Os(nix::errno::Errno::EBADF));
                    }
                }
            }
        };

        // Channel A fd: peer's writer → our reader.
        let reader_memfd = recv()?;
        let mut reader_mmap = unsafe { MmapMut::map_mut(&reader_memfd).unwrap() };
        let reader = triple_buffer::Reader::new(
            reader_mmap.as_mut_ptr() as *mut triple_buffer::Shared<(usize, T)>
        );

        // Channel B fd: peer's reader → our writer.
        let writer_memfd = recv()?;
        let mut writer_mmap = unsafe { MmapMut::map_mut(&writer_memfd).unwrap() };
        let writer = triple_buffer::Writer::new(
            writer_mmap.as_mut_ptr() as *mut triple_buffer::Shared<(usize, T)>
        );

        log::info!(
            "Connected: bidirectional shared-memory transport, {} bytes per direction",
            triple_buffer::required_size::<(usize, T)>()
        );
        let conn_is_lost = config.register(fd);

        Ok(Self {
            writer,
            reader,
            _writer_mmap: writer_mmap,
            _writer_memfd: writer_memfd,
            _reader_mmap: reader_mmap,
            _reader_memfd: reader_memfd,
            conn_is_lost,
        })
    }

    fn send_one_msg(&mut self, _fd: RawFd, buf: &[u8], _flags: MsgFlags) -> nix::Result<usize> {
        if self.conn_is_lost.load(Ordering::Relaxed) {
            log::info!("Connection loss detected");
            return Err(ECONNRESET);
        }
        let size = std::mem::size_of::<T>();
        if buf.len() != size {
            log::error!(
                "Error: buf.len() ({}), does not match triple_buffer slot size ({}) set with T in TbTransport<T>",
                buf.len(),
                size
            );
            return Err(ENOMEM);
        }

        let copied = self.writer.write_bytes(buf);
        if copied < buf.len() {
            log::error!(
                "Error: buffer to small buf({}) > dst({})",
                buf.len(),
                copied
            );
            return Err(ENOMEM);
        }
        Ok(buf.len())
    }

    fn recv_one_msg(
        &mut self,
        _fd: RawFd,
        buf: &mut [u8],
        _flags: MsgFlags,
    ) -> Result<usize, super::ReceiveError> {
        if self.conn_is_lost.load(Ordering::Relaxed) {
            log::info!("Connection loss detected");
            return Ok(0);
        }
        // Copy directly out of the shared-memory slot instead of cloning the
        // full `(usize, T)` value onto the stack first.
        let size = std::mem::size_of::<T>();
        if buf.len() != size {
            log::error!(
                "Error: buf.len() ({}), does not match triple_buffer slot size ({}) set with T in TbTransport<T>",
                buf.len(),
                size
            );
            return Err(ReceiveError::ExternalBufferToSmall((buf.len(), size)));
        }
        match self.reader.read_bytes(buf) {
            Some(len) if len > buf.len() => {
                Err(ReceiveError::ExternalBufferToSmall((buf.len(), len)))
            }
            Some(len) => Ok(len),
            None => Err(ReceiveError::NoMessageAvailable),
        }
    }
}

impl BulkTransport for () {
    type Config = ();
    fn on_accept(_fd: RawFd) -> Result<Self, TransportError> {
        Ok(())
    }
    fn on_connect(_fd: RawFd, _cofig: Self::Config) -> Result<Self, TransportError> {
        #[cfg(test)]
        {
            use std::time::Duration;
            log::info!("In test case, so we sleep a little bit to test for race condition");
            std::thread::sleep(Duration::from_millis(100));
        }
        Ok(())
    }

    fn send_one_msg(&mut self, fd: RawFd, buf: &[u8], flags: MsgFlags) -> nix::Result<usize> {
        socket::send(fd, buf, flags)
    }

    fn recv_one_msg(
        &mut self,
        socket: RawFd,
        recv_buffer: &mut [u8],
        flags: MsgFlags,
    ) -> Result<usize, super::ReceiveError> {
        let recv_buffer_len = recv_buffer.len();
        let mut iov = [IoSliceMut::new(recv_buffer)];
        let mut cmsg_buffer = [0u8; 1];
        let res: Result<RecvMsg<()>, nix::errno::Errno> =
            socket::recvmsg(socket, &mut iov, Some(&mut cmsg_buffer), flags);
        match res {
            Ok(res) => {
                if res.flags.contains(MsgFlags::MSG_TRUNC) {
                    Err(ReceiveError::ExternalBufferToSmall((
                        recv_buffer_len,
                        res.bytes,
                    )))
                } else {
                    Ok(res.bytes)
                }
            }
            Err(nix::errno::Errno::EAGAIN) => Err(ReceiveError::NoMessageAvailable),
            Err(err) => Err(err.into()),
        }
    }
}

#[derive(Debug)]
pub struct UnixDatagramListener<E: BulkTransport> {
    socket: OwnedFd,
    local_addr: String,
    _marker: std::marker::PhantomData<E>,
}
impl<E: BulkTransport> Drop for UnixDatagramListener<E> {
    fn drop(&mut self) {
        let _ = nix::unistd::unlink(self.local_addr.as_str());
    }
}
impl<E: BulkTransport> UnixDatagramListener<E> {
    pub fn bind(addr: &str) -> Result<Self, TransportError> {
        // There should be only ENOMEM ENOBUFS  EMFILE ENFILE EACCES
        let fd = socket::socket(
            AddressFamily::Unix,
            #[cfg(target_os = "linux")]
            SockType::SeqPacket,
            #[cfg(target_os = "macos")]
            SockType::Stream,
            SockFlag::empty(),
            None,
        )?;
        set_nonblock(fd.as_raw_fd())?;
        let unix_addr = UnixAddr::new(addr)?;
        if Path::new(addr).exists() {
            // We ignore errors. This may happen when drop and so on is not in sync
            let _ = std::fs::remove_file(addr);
        }
        socket::bind(fd.as_raw_fd(), &unix_addr)?;
        log::info!("Bound to {addr}");
        socket::listen(&fd, Backlog::new(MAX_CONNECTIONS)?)?;

        Ok(UnixDatagramListener {
            socket: fd,
            local_addr: addr.to_string(),
            _marker: std::marker::PhantomData,
        })
    }
}
impl<E: BulkTransport> TransportListener<UnixDatagram<E>> for UnixDatagramListener<E> {
    fn accept(&mut self) -> Result<UnixDatagram<E>, TransportError> {
        let ret = socket::accept(self.socket.as_raw_fd());
        match ret {
            Ok(fd) => {
                let extra = E::on_accept(fd)?;
                Ok(UnixDatagram {
                    socket: unsafe { OwnedFd::from_raw_fd(fd) },
                    extra,
                })
            }
            Err(EAGAIN) => Err(TransportError::WouldBlock),
            Err(err) => Err(TransportError::Os(err)),
        }
    }

    fn local_addr(&self) -> Option<String> {
        let addr: UnixAddr = socket::getsockname(self.socket.as_raw_fd()).ok()?;
        Some(addr.to_string())
    }
}

#[derive(Debug)]
pub struct UnixDatagram<E: BulkTransport = ()> {
    socket: OwnedFd,
    pub extra: E,
}
impl<E: BulkTransport> Drop for UnixDatagram<E> {
    fn drop(&mut self) {
        if let Some(local_addr) = self.local_addr() {
            let _ = nix::unistd::unlink(local_addr.as_str());
        }
    }
}
impl<E: BulkTransport> UnixDatagram<E> {
    pub fn connect(addr: &str, config: E::Config) -> Result<Self, TransportError> {
        let fd = socket::socket(
            AddressFamily::Unix,
            #[cfg(target_os = "linux")]
            SockType::SeqPacket,
            #[cfg(target_os = "macos")]
            SockType::Stream,
            SockFlag::empty(),
            None,
        )?;
        set_nonblock(fd.as_raw_fd())?;
        let unix_addr = UnixAddr::new(addr)?;

        let ret = socket::connect(fd.as_raw_fd(), &unix_addr);
        match ret {
            Ok(_) => (),
            Err(ENOENT) => return Err(TransportError::PeerDoesNotExist),
            Err(err) => return Err(TransportError::Os(err)),
        }

        let extra = E::on_connect(fd.as_raw_fd(), config)?;
        let ud = UnixDatagram { socket: fd, extra };
        log::info!("UnixDatagram now connected to {:?}", ud.peer_addr());
        Ok(ud)
    }
}
impl<E: BulkTransport> TransportConnection for UnixDatagram<E> {
    fn local_addr(&self) -> Option<String> {
        let addr: UnixAddr = socket::getsockname(self.socket.as_raw_fd()).ok()?;
        Some(addr.to_string())
    }

    fn peer_addr(&self) -> Option<String> {
        socket::getpeername(self.socket.as_raw_fd())
            .ok()
            .map(|addr: UnixAddr| {
                addr.path()
                    .map(|addr| addr.to_str().unwrap_or_default().to_string())
                    .unwrap_or_default()
            })
    }

    fn send(&mut self, msg: &[u8]) -> Result<usize, super::SendError> {
        let ret = self
            .extra
            .send_one_msg(self.socket.as_raw_fd(), msg, MsgFlags::empty());
        match ret {
            Ok(len) => Ok(len),
            Err(ECONNRESET) => Err(SendError::ConnectionEnded),
            Err(err) => Err(SendError::OsError(err)),
        }
    }

    fn recv(&mut self, recv_buffer: &mut [u8]) -> Result<usize, super::ReceiveError> {
        let flags = MsgFlags::MSG_TRUNC | MsgFlags::MSG_DONTWAIT;
        self.extra
            .recv_one_msg(self.socket.as_raw_fd(), recv_buffer, flags)
    }
}
