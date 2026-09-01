pub use crate::transport::algos::triple_buffer::ByteInit;
use crate::{
    ReceiveError,
    helpers::set_calling_thread_to_nrt,
    transport::{BulkTransport, TransportError, algos::triple_buffer},
};
use memmap2::MmapMut;
use nix::errno::Errno::{EAGAIN, ECONNRESET, EINTR, ENOMEM};
use nix::sys::socket::{self, MsgFlags};
use std::collections::HashMap;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, Ordering},
    mpsc,
};

/// Number of bytes the connection-loss monitor writes into its wake-up pipe
/// to unblock the blocking `poll()`.
const WAKE_BYTE: u8 = 1;

/// Sends a `usize` over `fd`, looping until all bytes are queued.
///
/// On `SOCK_STREAM` (macOS) a single `send` may queue fewer bytes than
/// requested, so we loop; `EAGAIN` (non-blocking) just means "try again
/// shortly".
pub(crate) fn send_usize(fd: RawFd, value: usize) -> Result<(), TransportError> {
    let bytes = value.to_ne_bytes();
    let mut off = 0usize;
    while off < bytes.len() {
        match socket::send(fd, &bytes[off..], MsgFlags::empty()) {
            Ok(n) if n > 0 => off += n,
            Ok(_) => break,
            Err(EAGAIN) => std::thread::yield_now(),
            Err(e) => return Err(TransportError::Os(e)),
        }
    }
    Ok(())
}

/// Receives a `usize` sent via [`send_usize`], retrying on `EAGAIN` (the
/// socket is non-blocking) and, on `SOCK_STREAM` (macOS), looping until the
/// full `usize` has been read. Returns `ECONNRESET` if the peer vanishes
/// mid-handshake.
pub(crate) fn recv_usize(fd: RawFd) -> Result<usize, TransportError> {
    let mut buf = [0u8; std::mem::size_of::<usize>()];
    let mut off = 0usize;
    while off < buf.len() {
        match socket::recv(fd, &mut buf[off..], MsgFlags::empty()) {
            Ok(0) => return Err(TransportError::Os(nix::errno::Errno::ECONNRESET)),
            Ok(n) => off += n,
            Err(EAGAIN) => std::thread::sleep(std::time::Duration::from_millis(10)),
            Err(e) => return Err(TransportError::Os(e)),
        }
    }
    Ok(usize::from_ne_bytes(buf))
}

pub mod shm_helpers {
    use crate::TransportError;
    use nix::cmsg_space;
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
                    // SAFETY: The fd was received from `SCM_RIGHTS`; ownership
                    // is transferred to us.
                    return Ok(unsafe { OwnedFd::from_raw_fd(fd) });
                }
            }
        }
        Err(FdPassError::NoFd)
    }

    /// Allocates `size` bytes of **shared memory** backed by a file
    /// descriptor that can be passed over a Unix socket
    /// ([`send_memfd`](Self::send_memfd)) and re-mapped on the peer side.
    ///
    /// On Linux this is a `memfd`. macOS has no `memfd_create`, so we fall
    /// back to a named file in the OS temp directory that is extended to
    /// `size` bytes and shared via the identical `SCM_RIGHTS` mechanism; the
    /// peer still `mmap`s the same physical pages.
    ///
    /// The caller must keep the returned `OwnedFd` open for as long as either
    /// process has the region mapped.
    pub fn create_memfd(size: usize, name: &str) -> Result<OwnedFd, TransportError> {
        #[cfg(target_os = "linux")]
        {
            use nix::sys::memfd::{MFdFlags, memfd_create};
            let fd = memfd_create(name, MFdFlags::MFD_CLOEXEC)?;
            ftruncate(fd.as_fd(), size as i64)?;
            Ok(fd)
        }
        #[cfg(not(target_os = "linux"))]
        {
            use nix::fcntl::{OFlag, open};
            use nix::sys::stat::Mode;
            let path = std::env::temp_dir().join(name);
            let fd = open(
                &path,
                OFlag::O_RDWR | OFlag::O_CREAT | OFlag::O_EXCL,
                Mode::S_IRUSR | Mode::S_IWUSR,
            )?;
            ftruncate(fd.as_fd(), size as i64)?;
            Ok(fd)
        }
    }
}

/// Configuration handle for the shared-memory transport. A cloneable handle
/// into the process-wide connection-loss monitor shared by the triple-buffer,
/// SPSC and direct-shared-memory transports.
#[derive(Clone)]
pub struct TbTransportConfig {
    fd_queue: mpsc::Sender<(RawFd, Arc<AtomicBool>)>,
    /// Raw write-end fd of the monitor's wake-up pipe; `register` writes one
    /// byte here to unblock the monitor's blocking `poll()`.
    pipe_writer_raw: RawFd,
}

impl std::fmt::Debug for TbTransportConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TbTransportConfig")
            .field("fd_queue", &"?")
            .field("pipe_writer_raw", &"OwnedFd")
            .finish_non_exhaustive()
    }
}

impl TbTransportConfig {
    pub fn new() -> Self {
        connection_monitor().clone()
    }

    /// Registers `fd` with the shared monitor thread and returns the flag
    /// that is set to `true` once the peer's connection is detected as lost.
    pub(crate) fn register(&self, fd: RawFd) -> Arc<AtomicBool> {
        let conn_is_lost = Arc::new(AtomicBool::new(false));
        if self.fd_queue.send((fd, conn_is_lost.clone())).is_err() {
            log::error!("Connection monitor thread is gone; not tracking fd {fd}");
            return conn_is_lost;
        }
        // Unblock the monitor's blocking `poll()` so it registers the new fd.
        let wake = unsafe { BorrowedFd::borrow_raw(self.pipe_writer_raw) };
        let _ = nix::unistd::write(wake, &[WAKE_BYTE]);
        conn_is_lost
    }
}

impl Default for TbTransportConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Owned pipe ends kept alive for the whole process so that the raw fd numbers
/// (`pipe_writer_raw`) handed out through `TbTransportConfig` stay valid.
struct PipeEnds {
    _read: OwnedFd,
    _write: OwnedFd,
}

static MONITOR: OnceLock<PipeEnds> = OnceLock::new();
static CONFIG: OnceLock<TbTransportConfig> = OnceLock::new();

/// Lazily starts the single background thread that detects connection loss
/// (via `poll`) for every `TbTransport` connection, and returns a cloneable
/// handle to it.
pub(crate) fn connection_monitor() -> &'static TbTransportConfig {
    CONFIG.get_or_init(|| {
        let (fd_queue, rx) = mpsc::channel();
        let (pipe_read, pipe_write) = nix::unistd::pipe().expect("create wake-up pipe");
        // Make the read end non-blocking so the monitor's drain loop can tell
        // "empty" (EAGAIN) instead of blocking on a subsequent read.
        crate::transport::uds::set_nonblock(pipe_read.as_raw_fd())
            .expect("set wake pipe non-blocking");
        let pipe_read_raw = pipe_read.as_raw_fd();
        let pipe_writer_raw = pipe_write.as_raw_fd();

        // The thread only needs the raw number of the wake-up pipe's read end;
        // the `OwnedFd`s that actually keep the pipe open live in `MONITOR`.
        std::thread::Builder::new()
            .name("novomq-conn-monitor".to_string())
            .spawn(move || {
                set_calling_thread_to_nrt();
                run_connection_monitor(rx, pipe_read_raw);
            })
            .expect("spawn connection monitor thread");

        MONITOR
            .set(PipeEnds {
                _read: pipe_read,
                _write: pipe_write,
            })
            .ok();

        TbTransportConfig {
            fd_queue,
            pipe_writer_raw,
        }
    })
}

/// "peer hang-up / error / fd invalid" — any of these on a connected socket
/// means the peer closed (or the socket is in an error state).
///
/// `POLLIN` is deliberately **not** a loss signal: on macOS a live AF_UNIX
/// stream socket can report transient read-ready state, so keying off it would
/// produce spurious "connection lost" flags. `POLLHUP` reliably indicates
/// end-of-file / peer close on both Linux and macOS.
fn lost_events() -> nix::poll::PollFlags {
    use nix::poll::PollFlags;
    PollFlags::POLLHUP | PollFlags::POLLERR | PollFlags::POLLNVAL
}

/// Body of the shared connection-loss monitor thread. Polls over the wake-up
/// pipe plus every registered connection fd; any `LOST_EVENTS` on a
/// connection marks its owning transport's `Arc<AtomicBool>` as `true`.
///
/// `wake_raw` is the raw fd number of the pipe's read end; its `OwnedFd` is
/// kept alive by the `MONITOR` static, so borrowing it here is safe for the
/// whole thread's lifetime.
fn run_connection_monitor(rx: mpsc::Receiver<(RawFd, Arc<AtomicBool>)>, wake_raw: RawFd) {
    use nix::poll::{PollFd, poll};

    let mut connections: HashMap<RawFd, Arc<AtomicBool>> = HashMap::new();
    const MAX_WATCHED: usize = 4096;

    loop {
        // SAFETY: `wake_raw`'s `OwnedFd` is held by the `MONITOR` static for
        // the whole process, so the fd stays open for this thread's lifetime.
        let wake_pfd = PollFd::new(
            unsafe { BorrowedFd::borrow_raw(wake_raw) },
            nix::poll::PollFlags::POLLIN,
        );
        let mut poll_fds: Vec<PollFd> = Vec::with_capacity(connections.len().min(MAX_WATCHED) + 1);
        poll_fds.push(wake_pfd);
        for (i, (fd, _)) in connections.iter().enumerate() {
            if i >= MAX_WATCHED {
                break;
            }
            // SAFETY: `fd` is open while the connection owns it; `poll()` only
            // reads the raw number for this round.
            let borrowed = unsafe { BorrowedFd::borrow_raw(*fd) };
            poll_fds.push(PollFd::new(borrowed, lost_events()));
        }

        let n = match poll(&mut poll_fds, 10u16) {
            Ok(n) => n,
            Err(EINTR) => continue,
            Err(e) => {
                log::error!("Connection monitor: poll failed: {e}");
                break;
            }
        };
        if n == 0 {
            continue;
        }

        if let Some(revents) = poll_fds[0].revents()
            && revents.contains(nix::poll::PollFlags::POLLIN)
        {
            // The wake-up pipe is readable: drain it (multiple concurrent
            // `register()` calls may have queued several bytes), then pull any
            // newly-queued connections out of the mpsc queue.
            // SAFETY: the pipe-read fd is kept alive by the `MONITOR` static.
            let wake = unsafe { BorrowedFd::borrow_raw(wake_raw) };
            let mut buf = [0u8; 64];
            while nix::unistd::read(wake, &mut buf).is_ok_and(|n| n > 0) {}
            while let Ok((fd, conn_is_lost)) = rx.try_recv() {
                connections.insert(fd, conn_is_lost);
            }
        }

        for pfd in poll_fds.iter().skip(1) {
            if let Some(revents) = pfd.revents()
                && (revents & lost_events()) != nix::poll::PollFlags::empty()
            {
                let raw = pfd.as_fd().as_raw_fd();
                if let Some(conn_is_lost) = connections.remove(&raw) {
                    conn_is_lost.store(true, Ordering::Relaxed);
                    log::info!("Connection monitor: connection lost for fd {raw}");
                }
            }
        }
    }
}

/// Bidirectional triple-buffer transport.
///
/// Each side owns one writer (outgoing direction) and one reader (incoming),
/// backed by two shared-memory regions negotiated over the socket
/// (`memfd` / temp-file fd + `SCM_RIGHTS`). Both the mapping and the backing
/// fd are kept alive by the struct so the region stays valid for the
/// transport's lifetime.
///
/// Internally, `(usize, T)` is stored so the actual message length is
/// preserved alongside the fixed-size data buffer.
#[derive(Debug)]
pub struct TbTransport<T: Send> {
    writer: triple_buffer::Writer<(usize, T)>,
    reader: triple_buffer::Reader<(usize, T)>,
    /// Keeps the writer-side mmap alive.
    _writer_mmap: MmapMut,
    /// Keeps the writer-side backing object (memfd / file) open.
    _writer_memfd: OwnedFd,
    /// Keeps the reader-side mmap alive.
    _reader_mmap: MmapMut,
    /// Keeps the reader-side backing object (memfd / file) open.
    _reader_memfd: OwnedFd,
    conn_is_lost: Arc<AtomicBool>,
}

impl<T> BulkTransport for TbTransport<T>
where
    T: Send + ByteInit + AsMut<[u8]> + AsRef<[u8]> + Clone + Sized,
{
    type Config = TbTransportConfig;

    fn on_accept(fd: RawFd) -> Result<Self, TransportError> {
        // Handshake: both sides must agree on the payload size before any
        // shared-memory region is allocated.
        let our_size = size_of::<T>();
        send_usize(fd, our_size)?;
        let peer_size = recv_usize(fd)?;
        if peer_size != our_size {
            return Err(TransportError::BufferSizeMismatch {
                ours: our_size,
                peer: peer_size,
            });
        }

        let conn_is_lost = connection_monitor().register(fd);

        // Channel A: this side writes, peer reads.
        let writer_memfd = shm_helpers::create_memfd(
            triple_buffer::required_size::<(usize, T)>(),
            &uuid::Uuid::new_v4().to_string(),
        )?;
        let mut writer_mmap = unsafe { MmapMut::map_mut(writer_memfd.as_raw_fd()).unwrap() };
        let (writer, _) = unsafe { triple_buffer::triple_buffer(writer_mmap.as_mut_ptr()) };

        // Channel B: peer writes, this side reads.
        let reader_memfd = shm_helpers::create_memfd(
            triple_buffer::required_size::<(usize, T)>(),
            &uuid::Uuid::new_v4().to_string(),
        )?;
        let mut reader_mmap = unsafe { MmapMut::map_mut(reader_memfd.as_raw_fd()).unwrap() };
        let (_, reader) = unsafe { triple_buffer::triple_buffer(reader_mmap.as_mut_ptr()) };

        // Send channel A's fd first, then channel B's fd.
        shm_helpers::send_memfd(fd, writer_memfd.as_raw_fd()).map_err(|e| {
            log::error!("Error sending memfd: {e}");
            nix::errno::Errno::EBADF
        })?;
        shm_helpers::send_memfd(fd, reader_memfd.as_raw_fd()).map_err(|e| {
            log::error!("Error sending memfd: {e}");
            nix::errno::Errno::EBADF
        })?;

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
        let our_size = size_of::<T>();
        let peer_size = recv_usize(fd)?;
        send_usize(fd, our_size)?;
        if peer_size != our_size {
            return Err(TransportError::BufferSizeMismatch {
                ours: our_size,
                peer: peer_size,
            });
        }

        let conn_is_lost = config.register(fd);

        let recv = || -> Result<OwnedFd, TransportError> {
            for _ in 0..300 {
                match shm_helpers::recv_memfd(fd) {
                    Ok(owned) => return Ok(owned),
                    Err(shm_helpers::FdPassError::Nix(nix::errno::Errno::EAGAIN)) => {
                        std::thread::sleep(std::time::Duration::from_millis(10));
                    }
                    Err(shm_helpers::FdPassError::Nix(nix::errno::Errno::ECONNRESET)) => {
                        log::error!("Peer closed during fd handshake");
                        return Err(TransportError::PeerDoesNotExist);
                    }
                    Err(e) => {
                        log::error!("Error receiving fd: {e}");
                        return Err(TransportError::Os(nix::errno::Errno::EBADF));
                    }
                }
            }
            Err(TransportError::Os(nix::errno::Errno::EBADF))
        };

        // Channel A's fd: the peer's writer → our reader.
        let reader_memfd = recv()?;
        let mut reader_mmap = unsafe { MmapMut::map_mut(reader_memfd.as_raw_fd()).unwrap() };
        let reader = triple_buffer::Reader::new(
            reader_mmap.as_mut_ptr() as *mut triple_buffer::Shared<(usize, T)>
        );

        // Channel B's fd: the peer's reader → our writer.
        let writer_memfd = recv()?;
        let mut writer_mmap = unsafe { MmapMut::map_mut(writer_memfd.as_raw_fd()).unwrap() };
        let writer = triple_buffer::Writer::new(
            writer_mmap.as_mut_ptr() as *mut triple_buffer::Shared<(usize, T)>
        );

        log::info!(
            "Connected: bidirectional shared-memory transport, {} bytes per direction",
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

    fn send_one_msg(&mut self, _fd: RawFd, buf: &[u8], _flags: MsgFlags) -> nix::Result<usize> {
        if self.conn_is_lost.load(Ordering::Relaxed) {
            log::info!("Connection loss detected");
            return Err(ECONNRESET);
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
    ) -> Result<usize, ReceiveError> {
        if self.conn_is_lost.load(Ordering::Relaxed) {
            log::info!("Connection loss detected");
            return Ok(0);
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
