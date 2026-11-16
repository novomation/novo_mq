use crate::{
    ReceiveError, SendError,
    transport::{MAX_CONNECTIONS, TransportConnection, TransportError, TransportListener},
};
use nix::{
    errno::Errno::{
        self, EAGAIN, EALREADY, ECONNREFUSED, ECONNRESET, EINPROGRESS, EINTR, ENOENT, EPIPE,
    },
    sys::socket::{
        self, AddressFamily, Backlog, MsgFlags, RecvMsg, SockFlag, SockType, UnixAddr, sockopt,
    },
    sys::stat::{SFlag, lstat},
    sys::time::{TimeVal, TimeValLike},
};
use std::{
    io::IoSliceMut,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    time::{Duration, Instant},
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

/// Temporarily clears `O_NONBLOCK` so the following receive can block in the
/// OS. Only sockets that are non-blocking at rest (the accepted publisher
/// connections) need this; a connected socket is blocking at rest, so the
/// blocking receive path of a subscriber stays syscall-free.
struct BlockingModeGuard {
    fd: nix::libc::c_int,
    was_nonblocking: bool,
}
impl BlockingModeGuard {
    fn for_blocking(fd: nix::libc::c_int, nonblocking_at_rest: bool) -> Result<Self, Errno> {
        if !nonblocking_at_rest {
            return Ok(Self {
                fd,
                was_nonblocking: false,
            });
        }
        let saved_flags = unsafe { nix::libc::fcntl(fd, nix::libc::F_GETFL) };
        if saved_flags < 0 {
            return Err(Errno::last());
        }
        let ret = unsafe {
            nix::libc::fcntl(fd, nix::libc::F_SETFL, saved_flags & !nix::libc::O_NONBLOCK)
        };
        if ret < 0 {
            return Err(Errno::last());
        }
        Ok(Self {
            fd,
            was_nonblocking: true,
        })
    }
}
impl Drop for BlockingModeGuard {
    fn drop(&mut self) {
        if self.was_nonblocking
            && let Err(err) = set_nonblock(self.fd)
        {
            log::error!("Could not restore O_NONBLOCK on fd {}: {err}", self.fd);
        }
    }
}

/// Builds the `SO_RCVTIMEO` value for the blocking receive: the given
/// duration in microseconds, or zero (the OS disables the timeout) for an
/// unbounded wait.
fn receive_timeout_timeval(timeout: Option<Duration>) -> TimeVal {
    let micros = timeout
        .map(|timeout| i64::try_from(timeout.as_micros()).unwrap_or(i64::MAX))
        .unwrap_or_default();
    TimeVal::microseconds(micros)
}

/// Queued (partially unread) bytes on the socket, via `FIONREAD`.
#[cfg(not(target_os = "linux"))]
fn queued_bytes(fd: nix::libc::c_int) -> Result<usize, Errno> {
    let mut queued: nix::libc::c_int = 0;
    // SAFETY: `FIONREAD` writes one `c_int` into the passed pointer.
    let ret = unsafe { nix::libc::ioctl(fd, nix::libc::FIONREAD, &mut queued) };
    if ret < 0 {
        return Err(Errno::last());
    }
    Ok(usize::try_from(queued).unwrap_or(0))
}

#[cfg(not(target_os = "linux"))]
impl UnixDatagram {
    /// Consumes exactly `remaining` unread bytes (or fewer if the queue runs
    /// dry) with non-blocking receives, so that an oversized message is
    /// abandoned atomically like on the SEQPACKET transport.
    fn consume_queued_bytes(&self, remaining: usize) {
        let mut scratch = [0u8; 512];
        let mut consumed = 0usize;
        while consumed < remaining {
            let mut iov = [IoSliceMut::new(&mut scratch)];
            let Ok(res) = socket::recvmsg::<()>(
                self.socket.as_raw_fd(),
                &mut iov,
                None,
                MsgFlags::MSG_DONTWAIT,
            ) else {
                return;
            };
            if res.bytes == 0 {
                return;
            }
            consumed += res.bytes;
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

#[derive(Debug)]
pub struct UnixDatagramListener {
    socket: OwnedFd,
    local_addr: String,
}
impl Drop for UnixDatagramListener {
    fn drop(&mut self) {
        let _ = nix::unistd::unlink(self.local_addr.as_str());
    }
}
impl UnixDatagramListener {
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
        })
    }
}
impl TransportListener<UnixDatagram> for UnixDatagramListener {
    fn accept(&mut self) -> Result<UnixDatagram, TransportError> {
        let ret = socket::accept(self.socket.as_raw_fd());
        match ret {
            Ok(fd) => {
                // On Linux `accept()` does not inherit O_NONBLOCK from the
                // listening socket. The accepted socket carries the
                // publisher's traffic: keep it non-blocking so that
                // `send` never parks the publishing thread.
                let socket = unsafe { OwnedFd::from_raw_fd(fd) };
                set_nonblock(socket.as_raw_fd())?;
                Ok(UnixDatagram {
                    socket,
                    nonblocking_at_rest: true,
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
pub struct UnixDatagram {
    socket: OwnedFd,
    /// Blocking mode this handle is in between calls. Connected sockets are
    /// created blocking (the blocking receive parks the thread in the OS);
    /// accepted sockets are non-blocking because the publisher's send must
    /// never wait for a slow subscriber.
    nonblocking_at_rest: bool,
}
impl Drop for UnixDatagram {
    // Intentionally empty: a `UnixDatagram` is never bound. For a `connect`ed
    // socket the local address is anonymous, and for an `accept`ed socket it
    // is the *listener's* path. The bound path is owned by `UnixDatagramListener`,
    //  which unlinks it in its own `Drop`.
    fn drop(&mut self) {}
}
impl UnixDatagram {
    pub fn connect(addr: &str) -> Result<Self, TransportError> {
        let fd = create_unix_socket()?;
        // The receive socket is blocking at rest: `recv_blocking` parks the
        // calling thread in the OS. The non-blocking `recv` and `send` keep
        // their per-call semantics through `MSG_DONTWAIT`.
        let unix_addr = UnixAddr::new(addr)?;

        let ret = socket::connect(fd.as_raw_fd(), &unix_addr);
        match ret {
            Ok(_) => (),
            Err(ENOENT) => return Err(TransportError::PeerDoesNotExist),
            Err(err) => return Err(TransportError::Os(err)),
        }

        let ud = UnixDatagram {
            socket: fd,
            nonblocking_at_rest: false,
        };
        log::info!("UnixDatagram now connected to {:?}", ud.peer_addr());
        Ok(ud)
    }
}
impl TransportConnection for UnixDatagram {
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

    fn send(&self, msg: &[u8]) -> Result<usize, super::SendError> {
        let ret = socket::send(self.socket.as_raw_fd(), msg, MsgFlags::MSG_DONTWAIT);
        match ret {
            Ok(len) => Ok(len),
            // The peer receive buffer is full. `Publisher::try_send` counts it as a lost
            // message so that publishing must never block on a slow subscriber.
            Err(EAGAIN) => Err(SendError::OsBufferFull),
            // ECONNRESET means the peer has already torn the connection down.
            // EPIPE means this socket still looked connected, but this write is the first
            // operation that observed the closed peer and the kernel reports a broken pipe.
            // In both cases the connection is gone and callers should reconnect instead of
            // treating the send as an unexpected transport failure.
            Err(ECONNRESET) | Err(EPIPE) => Err(SendError::ConnectionEnded),
            Err(err) => Err(SendError::OsError(err)),
        }
    }

    fn recv(&self, recv_buffer: &mut [u8]) -> Result<usize, super::ReceiveError> {
        self.recv_message(recv_buffer, MsgFlags::MSG_TRUNC | MsgFlags::MSG_DONTWAIT)
    }

    fn recv_blocking(
        &self,
        recv_buffer: &mut [u8],
        timeout: Option<Duration>,
    ) -> Result<usize, super::ReceiveError> {
        // `SO_RCVTIMEO` cannot honor a timeout below one microsecond: zero
        // means an unbounded wait and anything else is rounded up to a
        // scheduler tick. Such a timeout is an immediate attempt instead, so
        // an empty queue reports a timeout and the caller's budget is never
        // exceeded (worst case: it misses a message queued in a tick).
        if timeout.is_some_and(|timeout| timeout < Duration::from_micros(1)) {
            return match self.recv(recv_buffer) {
                Err(ReceiveError::NoMessageAvailable) => Err(ReceiveError::Timeout),
                other => other,
            };
        }
        let _guard =
            BlockingModeGuard::for_blocking(self.socket.as_raw_fd(), self.nonblocking_at_rest)?;
        // An unbounded wait sends `SO_RCVTIMEO = 0` — only needed to *clear*
        // a timeout left by an earlier bounded receive. macOS/XNU quirk: a
        // socket whose peer connection was detached (peer closed without any
        // completion of a transfer) rejects even that harmless option write
        // with `EINVAL`. The immediately following receive finds the EOF in
        // that state — there is no waiting left to configure — so tolerate
        // the `EINVAL` instead of masking the disconnect behind an error.
        self.set_receive_timeout(timeout).or_else(|err| {
            if err == Errno::EINVAL && !cfg!(target_os = "linux") {
                return Ok(());
            }
            Err(err)
        })?;
        // `SO_RCVTIMEO` restarts with every syscall, so a signal-interrupted
        // receive re-arms the remaining time to still honor the full deadline.
        let deadline = timeout.map(|timeout| Instant::now() + timeout);
        loop {
            match self.recv_message(recv_buffer, MsgFlags::MSG_TRUNC) {
                // In blocking mode this "EAGAIN" (reported as
                // NoMessageAvailable, `EWOULDBLOCK` is an alias for it) can
                // only be produced by the receive timeout expiring. The
                // unfinished message stays queued for the next receive.
                Err(ReceiveError::NoMessageAvailable) => return Err(ReceiveError::Timeout),
                Err(ReceiveError::OsError(EINTR)) => {
                    if let Some(deadline) = deadline {
                        let remaining = deadline.saturating_duration_since(Instant::now());
                        // Re-arming with (sub)zero remaining would set
                        // `SO_RCVTIMEO=0`, which means an *unbounded* wait:
                        // report the expired budget instead.
                        if remaining < Duration::from_micros(1) {
                            return Err(ReceiveError::Timeout);
                        }
                        self.set_receive_timeout(Some(remaining))?;
                    }
                }
                result => return result,
            }
        }
    }
}

impl UnixDatagram {
    /// Core around `recvmsg`, shared by the non-blocking and the blocking
    /// receive. Maps `EAGAIN` of a non-blocking receive to
    /// [`ReceiveError::NoMessageAvailable`]; the blocking receive re-maps it
    /// (in blocking mode `EAGAIN` can only be produced by `SO_RCVTIMEO`).
    fn recv_message(&self, recv_buffer: &mut [u8], flags: MsgFlags) -> Result<usize, ReceiveError> {
        let recv_buffer_len = recv_buffer.len();
        let mut iov = [IoSliceMut::new(recv_buffer)];
        let mut cmsg_buffer = [0u8; 1];

        let res: Result<RecvMsg<()>, nix::errno::Errno> = socket::recvmsg(
            self.socket.as_raw_fd(),
            &mut iov,
            Some(&mut cmsg_buffer),
            flags,
        );

        match res {
            Ok(res) => {
                // Truncation semantics per transport:
                //
                // Linux speaks `SOCK_SEQPACKET`, where messages are atomic.
                // Linux honors `MSG_TRUNC` in the *input* flags by returning
                // the full datagram size and marking `msg_flags` with
                // `MSG_TRUNC`; a truncated datagram's remainder is discarded
                // by the kernel. That is the `ExternalBufferToSmall`
                // contract below: the oversized message is abandoned
                // atomically.
                //
                // macOS speaks `SOCK_STREAM` (XNU has no AF_UNIX
                // SOCK_SEQPACKET). There `msg_flags` merely *echoes the
                // input flags* and never reports truncation as an outcome
                // (verified: a truncated receive reports the echoed flags
                // only), so `MSG_TRUNC` cannot be tested here: since we pass
                // it, it would mark *every* receive as truncated and spin
                // every retry loop forever. Instead truncation is derived
                // from the byte count ("buffer fully filled") plus
                // `FIONREAD` ("and more is still queued"); the queued
                // remainder is consumed via follow-up receives to keep the
                // message-atomicity contract of `ExternalBufferToSmall`.
                #[cfg(target_os = "linux")]
                if res.flags.contains(MsgFlags::MSG_TRUNC) {
                    return Err(ReceiveError::ExternalBufferToSmall((
                        recv_buffer_len,
                        res.bytes,
                    )));
                }
                #[cfg(not(target_os = "linux"))]
                if res.bytes == recv_buffer_len
                    && let Ok(queued) = queued_bytes(self.socket.as_raw_fd())
                    && queued > 0
                {
                    // The reported full size is the received prefix plus
                    // what the kernel had queued; several queued messages
                    // cannot be told apart on the stream transport.
                    self.consume_queued_bytes(queued);
                    return Err(ReceiveError::ExternalBufferToSmall((
                        recv_buffer_len,
                        res.bytes + queued,
                    )));
                }
                Ok(res.bytes)
            }
            Err(nix::errno::Errno::EAGAIN) => Err(ReceiveError::NoMessageAvailable),
            // macOS quirk: XNU detaches a Unix stream socket's connection as
            // soon as the peer closes its socket. If nothing was ever queued
            // on the connection, a receive then fails with `EINVAL` instead
            // of reporting the traditional 0-byte EOF — the identical state
            // that Linux reports as EOF (verified: once data has flowed, the
            // receives report `0` bytes as usual). Map `EINVAL` to the
            // library's EOF semantics so the receiving side detects the
            // disconnect like on Linux.
            #[cfg(not(target_os = "linux"))]
            Err(nix::errno::Errno::EINVAL) => Ok(0),
            Err(err) => Err(err.into()),
        }
    }

    /// Sets `SO_RCVTIMEO` for the blocking receive. A timeout of zero
    /// disables the timeout (unbounded wait).
    fn set_receive_timeout(&self, timeout: Option<Duration>) -> Result<(), Errno> {
        socket::setsockopt(
            &self.socket,
            sockopt::ReceiveTimeout,
            &receive_timeout_timeval(timeout),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::{os::fd::AsRawFd, path::Path};

    #[cfg(any(target_os = "linux", target_os = "macos"))]
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
        let listener = UnixDatagramListener::bind(&sock_addr).unwrap();
        assert_eq!(listener.local_addr(), Some(sock_addr));
    }
}
