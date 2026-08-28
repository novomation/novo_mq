use crate::{
    ReceiveError::{self},
    SendError::{self},
    transport::{
        MAX_CONNECTIONS, TransportConnection, TransportError, TransportListener,
        bulk_transport::BulkTransport,
    },
};
use nix::{
    errno::Errno::{EAGAIN, ECONNRESET, EPIPE, ENOENT},
    sys::socket::{
        self, AddressFamily, Backlog, MsgFlags, SockFlag, SockType, UnixAddr,
    },
};
#[cfg(not(target_os = "macos"))]
use nix::sys::socket::RecvMsg;
use std::{
    os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
    path::Path,
};
#[cfg(target_os = "macos")]
use std::vec::Vec;
#[cfg(not(target_os = "macos"))]
use std::io::IoSliceMut;

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
        #[cfg(not(target_os = "macos"))]
        {
            socket::send(fd, buf, flags)
        }
        #[cfg(target_os = "macos")]
        {
            // macOS has no SOCK_SEQPACKET, so the underlying SOCK_STREAM has no
            // message boundaries.  We emulate them by prepending an 8-byte
            // big-endian length on the wire.
            let mut out = Vec::with_capacity(8 + buf.len());
            out.extend_from_slice(&(buf.len() as u64).to_be_bytes());
            out.extend_from_slice(buf);
            let mut off = 0usize;
            // `SOCK_STREAM` has no message boundaries. Drain the whole framed
            // message so a send is all-or-nothing: this connection has exactly
            // one writer thread, so looping until the bytes are queued is safe.
            while off < out.len() {
                match socket::send(fd, &out[off..], flags) {
                    Ok(n) if n > 0 => off += n,
                    Ok(_) => break,
                    Err(EAGAIN) => std::thread::yield_now(),
                    Err(err) => return Err(err),
                }
            }
            Ok(buf.len())
        }
    }

    fn recv_one_msg(
        &mut self,
        socket: RawFd,
        recv_buffer: &mut [u8],
        _flags: MsgFlags,
    ) -> Result<usize, super::ReceiveError> {
        let recv_buffer_len = recv_buffer.len();

        #[cfg(not(target_os = "macos"))]
        {
            let mut iov = [IoSliceMut::new(recv_buffer)];
            let mut cmsg_buffer = [0u8; 1];
            let res: Result<RecvMsg<()>, nix::errno::Errno> = socket::recvmsg(
                socket,
                &mut iov,
                Some(&mut cmsg_buffer),
                _flags,
            );
            return match res {
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
            };
        }

        #[cfg(target_os = "macos")]
        return macos_try_recv_one(socket, recv_buffer, recv_buffer_len);
    }
}

#[cfg(target_os = "macos")]
fn macos_try_recv_one(
    fd: RawFd,
    recv_buffer: &mut [u8],
    recv_buffer_len: usize,
) -> Result<usize, super::ReceiveError> {
    // macOS has no SOCK_SEQPACKET, so the underlying SOCK_STREAM has no message
    // boundaries: we emulate them with an 8-byte big-endian length prefix.
    //
    // A recv must be *atomic*: report "no message" only when one has not even
    // started to arrive, and otherwise complete the whole framed message (spinning
    // on EAGAIN). This never leaves a half-consumed header on the stream — the old
    // best-effort reader returned NoMessageAvailable mid-body and permanently
    // corrupted the connection.
    const HDR: usize = 8;
    let mut header = [0u8; HDR];
    let mut off = 0usize;
    loop {
        match socket::recv(fd, &mut header[off..HDR], MsgFlags::empty()) {
            Ok(0) => {
                // Peer closed cleanly: if no message was in flight this mirrors
                // SEQPACKET clean shutdown and reports 0 received bytes.
                if off == 0 {
                    return Ok(0);
                }
                return Err(ReceiveError::OsError(nix::errno::Errno::ECONNRESET));
            }
            Ok(n) => {
                off += n;
            }
            Err(EAGAIN) => {
                if off == 0 {
                    return Err(ReceiveError::NoMessageAvailable);
                }
                std::thread::yield_now();
            }
            Err(e) => return Err(ReceiveError::OsError(e)),
        }
        if off == HDR {
            break;
        }
    }
    let msg_len = u64::from_be_bytes(header) as usize;
    if msg_len == 0 {
        return Ok(0);
    }
    let oversized = msg_len > recv_buffer_len;
    // Always drain the full body so the stream stays aligned; if the caller's
    // buffer is too small we drain into a temporary buffer.
    let mut tmp: std::vec::Vec<u8> = std::vec::Vec::new();
    let mut read_into: &mut [u8] = if oversized {
        tmp.resize(msg_len, 0);
        tmp.as_mut_slice()
    } else {
        &mut recv_buffer[..msg_len]
    };
    let mut boff = 0usize;
    loop {
        match socket::recv(fd, &mut read_into[boff..], MsgFlags::empty()) {
            Ok(0) => {
                return Err(ReceiveError::OsError(nix::errno::Errno::ECONNRESET));
            }
            Ok(n) => {
                boff += n;
            }
            Err(EAGAIN) => std::thread::yield_now(),
            Err(e) => return Err(ReceiveError::OsError(e)),
        }
        if boff == msg_len {
            break;
        }
    }
    if oversized {
        recv_buffer.copy_from_slice(&tmp[..recv_buffer_len]);
        Err(ReceiveError::ExternalBufferToSmall((recv_buffer_len, msg_len)))
    } else {
        Ok(msg_len)
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

/// Align the socket send/receive buffers with a message size so that
/// reasonably large payloads do not constantly trigger nonblocking
/// backpressure. macOS `SOCK_STREAM` defaults to a small (8 KB) buffer,
/// unlike Linux's 256 KB+.
pub fn set_socket_buffers(fd: nix::libc::c_int, size: i32) {
    unsafe {
        let _ = nix::libc::setsockopt(
            fd,
            nix::libc::SOL_SOCKET,
            nix::libc::SO_SNDBUF,
            &size as *const i32 as *const nix::libc::c_void,
            std::mem::size_of::<i32>() as u32,
        );
        let _ = nix::libc::setsockopt(
            fd,
            nix::libc::SOL_SOCKET,
            nix::libc::SO_RCVBUF,
            &size as *const i32 as *const nix::libc::c_void,
            std::mem::size_of::<i32>() as u32,
        );
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
        #[cfg(target_os = "macos")]
        set_socket_buffers(fd.as_raw_fd(), 1 << 20);
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
                // Own the fd immediately so it's closed automatically if
                // `on_accept` returns an error (e.g. buffer size mismatch).
                let owned_fd = unsafe { OwnedFd::from_raw_fd(fd) };
                #[cfg(target_os = "macos")]
                set_socket_buffers(owned_fd.as_raw_fd(), 1 << 20);
                let extra = E::on_accept(owned_fd.as_raw_fd())?;
                #[cfg(target_os = "macos")]
                let peer = peer_addr_of(&owned_fd);
                Ok(UnixDatagram {
                    socket: owned_fd,
                    extra,
                    #[cfg(target_os = "macos")]
                    peer,
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
    /// Cached peer path captured at connect()/accept() time.
    ///
    /// On macOS `SOCK_STREAM` `getpeername()` may no longer succeed once the
    /// peer has closed, so we keep the address we captured at connect/accept
    /// as a fallback.
    #[cfg(target_os = "macos")]
    peer: Option<String>,
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
        #[cfg(target_os = "macos")]
        set_socket_buffers(fd.as_raw_fd(), 1 << 20);
        let unix_addr = UnixAddr::new(addr)?;

        let ret = socket::connect(fd.as_raw_fd(), &unix_addr);
        match ret {
            Ok(_) => (),
            Err(ENOENT) => return Err(TransportError::PeerDoesNotExist),
            Err(err) => return Err(TransportError::Os(err)),
        }

        let extra = E::on_connect(fd.as_raw_fd(), config)?;
        #[cfg(target_os = "macos")]
        let peer = peer_addr_of(&fd);
        let ud = UnixDatagram {
            socket: fd,
            extra,
            #[cfg(target_os = "macos")]
            peer,
        };
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
        #[cfg(not(target_os = "macos"))]
        {
            socket::getpeername(self.socket.as_raw_fd())
                .ok()
                .map(|addr: UnixAddr| {
                    addr.path()
                        .map(|addr| addr.to_str().unwrap_or_default().to_string())
                        .unwrap_or_default()
                })
        }
        #[cfg(target_os = "macos")]
        {
            let live = socket::getpeername(self.socket.as_raw_fd())
                .ok()
                .and_then(|addr: UnixAddr| {
                    addr.path()
                        .map(|addr| addr.to_str().unwrap_or_default().to_string())
                        .filter(|s| !s.is_empty())
                });
            live.or_else(|| self.peer.clone())
        }
    }

    fn send(&mut self, msg: &[u8]) -> Result<usize, super::SendError> {
        let ret = self
            .extra
            .send_one_msg(self.socket.as_raw_fd(), msg, MsgFlags::empty());
        match ret {
            Ok(len) => Ok(len),
            Err(ECONNRESET) => Err(SendError::ConnectionEnded),
            #[cfg(target_os = "macos")]
            Err(EPIPE) => Err(SendError::ConnectionEnded),
            Err(err) => Err(SendError::OsError(err)),
        }
    }

    fn recv(&mut self, recv_buffer: &mut [u8]) -> Result<usize, super::ReceiveError> {
        let flags = MsgFlags::MSG_TRUNC | MsgFlags::MSG_DONTWAIT;
        self.extra
            .recv_one_msg(self.socket.as_raw_fd(), recv_buffer, flags)
    }
}

fn peer_addr_of(fd: &OwnedFd) -> Option<String> {
    socket::getpeername(fd.as_raw_fd())
        .ok()
        .map(|addr: UnixAddr| {
            addr.path()
                .map(|addr| addr.to_str().unwrap_or_default().to_string())
                .unwrap_or_default()
        })
}
