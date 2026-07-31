use crate::{
    ReceiveError,
    SendError::{self},
    transport::{
        MAX_CONNECTIONS, TransportConnection, TransportError, TransportListener, triple_buffer,
    },
};
use base64::engine::Config;
use memmap2::MmapMut;
use nix::sys::epoll::{
    EpollCreateFlags, EpollEvent, EpollFlags, EpollOp, epoll_create1, epoll_ctl, epoll_wait,
};
use nix::{
    errno::Errno::{EAGAIN, EALREADY, ECONNREFUSED, ECONNRESET, EINPROGRESS, ENOENT, EPIPE},
    sys::socket::{self, AddressFamily, Backlog, MsgFlags, RecvMsg, SockFlag, SockType, UnixAddr},
    sys::stat::{SFlag, lstat},
};
use std::{
    io::IoSliceMut,
    os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
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

fn create_unix_socket() -> Result<OwnedFd, TransportError> {
    Ok(socket::socket(
        AddressFamily::Unix,
        #[cfg(target_os = "linux")]
        SockType::SeqPacket,
        #[cfg(target_os = "macos")]
        SockType::Stream,
        SockFlag::empty(),
        None,
    )?)
}

fn prepare_bind_path(addr: &str) -> Result<(), TransportError> {
    match lstat(addr) {
        // First gate: only operate on an existing Unix socket path.
        // This prevents deleting unrelated filesystem entries.
        Ok(stat) => {
            let file_type = SFlag::from_bits_truncate(stat.st_mode);
            // Only remove stale Unix socket files. Keep any other existing files untouched.
            if !file_type.contains(SFlag::S_IFSOCK) {
                return Err(TransportError::AddressInUse);
            }
        }
        Err(ENOENT) => return Ok(()),
        Err(err) => return Err(err.into()),
    }

    let probe_fd = create_unix_socket()?;
    let unix_addr = UnixAddr::new(addr)?;
    // Second gate: distinguish active listeners from stale socket files.
    // Active endpoint => report EADDRINUSE. Refused connection => stale file, safe to unlink.
    match socket::connect(probe_fd.as_raw_fd(), &unix_addr) {
        Ok(_) | Err(EINPROGRESS) | Err(EAGAIN) | Err(EALREADY) => Err(TransportError::AddressInUse),
        Err(ECONNREFUSED) => {
            nix::unistd::unlink(addr)?;
            log::warn!("Removed stale Unix socket file at {addr}");
            Ok(())
        }
        Err(ENOENT) => Ok(()),
        Err(err) => Err(err.into()),
    }
}

/// Hook for attaching connection-specific state/behaviour (e.g. a shared-memory
/// mapping negotiated over the socket) to a plain [`UnixDatagram`].
///
/// The default `()` implementation is a no-op, so existing code that doesn't
/// need any extra per-connection data keeps working unchanged.
pub trait BulkTransport: Sized {
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
pub struct TbTransport<T> {
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

impl<T> BulkTransport for TbTransport<T>
where
    T: Default + AsMut<[u8]> + AsRef<[u8]> + Clone + Sized,
{
    type Config = ();
    fn on_accept(fd: RawFd) -> Result<Self, TransportError> {
        // Channel A: this side writes, peer reads.
        let writer_memfd = triple_buffer::create_memfd(
            triple_buffer::required_size::<(usize, T)>(),
            &uuid::Uuid::new_v4().to_string(),
        )
        .unwrap();
        let mut writer_mmap = unsafe { MmapMut::map_mut(&writer_memfd).unwrap() };
        let (writer, _) = unsafe { triple_buffer::triple_buffer(writer_mmap.as_mut_ptr()) };

        // Channel B: peer writes, this side reads.
        let reader_memfd = triple_buffer::create_memfd(
            triple_buffer::required_size::<(usize, T)>(),
            &uuid::Uuid::new_v4().to_string(),
        )
        .unwrap();
        let mut reader_mmap = unsafe { MmapMut::map_mut(&reader_memfd).unwrap() };
        let (_, reader) = unsafe { triple_buffer::triple_buffer(reader_mmap.as_mut_ptr()) };

        // Send channel A fd first, then channel B fd.
        triple_buffer::send_memfd(fd, writer_memfd.as_raw_fd()).unwrap();
        triple_buffer::send_memfd(fd, reader_memfd.as_raw_fd()).unwrap();

        log::info!(
            "Allocated 2×{} bytes for bidirectional shared-memory transport",
            triple_buffer::required_size::<(usize, T)>()
        );
        Ok(Self {
            writer,
            reader,
            _writer_mmap: writer_mmap,
            _writer_memfd: writer_memfd,
            _reader_mmap: reader_mmap,
            _reader_memfd: reader_memfd,
            conn_is_lost: Arc::new(AtomicBool::new(false)),
        })
    }

    fn on_connect(fd: RawFd, _cofig: Self::Config) -> Result<Self, TransportError> {
        let recv = || -> Result<OwnedFd, TransportError> {
            loop {
                match triple_buffer::recv_memfd(fd.as_raw_fd()) {
                    Ok(owned) => return Ok(owned),
                    Err(triple_buffer::FdPassError::Nix(nix::errno::Errno::EAGAIN)) => {
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
        let conn_is_lost = Arc::new(AtomicBool::new(false));
        std::thread::spawn({
            let conn_is_lost = conn_is_lost.clone();
            move || {
                let epoll_fd = epoll_create1(EpollCreateFlags::EPOLL_CLOEXEC).unwrap();
                let mut event = EpollEvent::new(
                    EpollFlags::EPOLLIN
                        | EpollFlags::EPOLLRDHUP
                        | EpollFlags::EPOLLERR
                        | EpollFlags::EPOLLHUP,
                    fd as u64,
                );

                epoll_ctl(epoll_fd, EpollOp::EpollCtlAdd, fd, Some(&mut event)).unwrap();
                let mut events = vec![EpollEvent::empty(); 64];
                let event_idx = epoll_wait(epoll_fd, &mut events, -1).unwrap();

                conn_is_lost.store(true, Ordering::Relaxed);
                log::info!("Connection lost: {}, {:?}.", event_idx, events[event_idx]);
            }
        });

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
        let mut data = T::default();
        let dst = data.as_mut();
        let len = buf.len().min(dst.len());
        dst[..len].copy_from_slice(&buf[..len]);
        self.writer.write((len, data));
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
        match self.reader.try_read() {
            Some((len, data)) => {
                let src = &data.as_ref()[..len];
                if len > buf.len() {
                    buf.copy_from_slice(&src[..buf.len()]);
                    return Err(ReceiveError::ExternalBufferToSmall((buf.len(), len)));
                }
                buf[..len].copy_from_slice(src);
                Ok(len)
            }
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
pub struct UnixDatagramListener<E: BulkTransport = ()> {
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
        prepare_bind_path(addr)?;
        // There should be only ENOMEM ENOBUFS  EMFILE ENFILE EACCES
        let fd = create_unix_socket()?;
        set_nonblock(fd.as_raw_fd())?;
        let unix_addr = UnixAddr::new(addr)?;
        socket::bind(fd.as_raw_fd(), &unix_addr)?;
        log::debug!("UnixDatagramListener bound to {addr}");
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
        let fd = create_unix_socket()?;
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
            // ECONNRESET means the peer has already torn the connection down.
            // EPIPE means this socket still looked connected, but this write is the first
            // operation that observed the closed peer and the kernel reports a broken pipe.
            // In both cases the connection is gone and callers should reconnect instead of
            // treating the send as an unexpected transport failure.
            Err(ECONNRESET) | Err(EPIPE) => Err(SendError::ConnectionEnded),
            Err(err) => Err(SendError::OsError(err)),
        }
    }

    fn recv(&mut self, recv_buffer: &mut [u8]) -> Result<usize, super::ReceiveError> {
        let flags = MsgFlags::MSG_TRUNC | MsgFlags::MSG_DONTWAIT;
        self.extra
            .recv_one_msg(self.socket.as_raw_fd(), recv_buffer, flags)
    }
}

#[cfg(test)]
mod tests {
    use std::{os::fd::AsRawFd, path::Path};

    #[cfg(target_os = "linux")]
    use nix::sys::socket::SockType;
    use nix::sys::socket::{self, AddressFamily, SockFlag, UnixAddr};

    use crate::transport::{TransportListener, uds::UnixDatagramListener};

    #[test]
    fn test_bind_reuses_dangling_socket_file() {
        let sock_addr = uuid::Uuid::new_v4().to_string();

        {
            let fd = socket::socket(
                AddressFamily::Unix,
                #[cfg(target_os = "linux")]
                SockType::SeqPacket,
                #[cfg(target_os = "macos")]
                SockType::Stream,
                SockFlag::empty(),
                None,
            )
            .unwrap();
            let unix_addr = UnixAddr::new(sock_addr.as_str()).unwrap();
            socket::bind(fd.as_raw_fd(), &unix_addr).unwrap();
            // Drop fd without unlinking path to simulate a stale socket file.
        }

        assert!(Path::new(&sock_addr).exists());
        let listener = UnixDatagramListener::<()>::bind(&sock_addr).unwrap();
        assert_eq!(listener.local_addr(), Some(sock_addr));
    }
}
