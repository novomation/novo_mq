use crate::{
    ReceiveError::{self},
    SendError::{self},
    transport::{
        MAX_CONNECTIONS, TransportConnection, TransportError, TransportListener,
        bulk_transport::BulkTransport,
    },
};
use nix::{
    errno::Errno::{EAGAIN, EALREADY, ECONNREFUSED, ECONNRESET, EINPROGRESS, ENOENT, EPIPE},
    sys::socket::{self, AddressFamily, Backlog, MsgFlags, RecvMsg, SockFlag, SockType, UnixAddr},
    sys::stat::{SFlag, lstat},
};
use std::{
    io::IoSliceMut,
    os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
    path::Path,
};

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
        Ok(stat) => {
            let file_type = SFlag::from_bits_truncate(stat.st_mode);
            if !file_type.contains(SFlag::S_IFSOCK) {
                return Err(TransportError::AddressInUse);
            }
        }
        Err(ENOENT) => return Ok(()),
        Err(err) => return Err(err.into()),
    }

    let probe_fd = create_unix_socket()?;
    let unix_addr = UnixAddr::new(addr)?;
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
                // Own the fd immediately so it's closed automatically if
                // `on_accept` returns an error (e.g. buffer size mismatch).
                let owned_fd = unsafe { OwnedFd::from_raw_fd(fd) };
                let extra = E::on_accept(owned_fd.as_raw_fd())?;
                Ok(UnixDatagram {
                    socket: owned_fd,
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
