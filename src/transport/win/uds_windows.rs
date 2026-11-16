use crate::{
    ReceiveError, SendError,
    transport::{
        MAX_CONNECTIONS, TransportConnection, TransportError, TransportListener,
        win::{WindowsSocketError, WinsockError},
    },
};
use log::debug;
use std::{
    ffi::{CStr, CString},
    path::Path,
    str::FromStr,
    time::{Duration, Instant},
};
use windows::{
    Win32::Networking::WinSock::{
        ADDRESS_FAMILY, AF_UNIX, FIONBIO, SEND_RECV_FLAGS, SO_RCVTIMEO, SOCK_STREAM, SOCKADDR,
        SOCKADDR_UN, SOCKET, SOCKET_ERROR, SOL_SOCKET, WSADATA, WSAGetLastError, WSAStartup,
        accept, bind, closesocket, connect, getpeername, getsockname, ioctlsocket, listen, recv,
        send, setsockopt, socket,
    },
    core::HRESULT,
};

fn return_to_result(ret: i32) -> Result<(), WindowsSocketError> {
    if ret == SOCKET_ERROR {
        let err = unsafe { WSAGetLastError() };
        Err(WindowsSocketError::WinsockError(
            WinsockError::try_from(err).unwrap(),
        ))
    } else {
        Ok(())
    }
}

fn create_socket() -> Result<SOCKET, WindowsSocketError> {
    unsafe {
        // must init winsock once in your program
        let mut wsa_data = WSADATA::default();
        WSAStartup(2u16 << 8 | 2u16, &mut wsa_data);
    }

    let fd =
        unsafe { socket(AF_UNIX as i32, SOCK_STREAM, 0) }.map_err(WindowsSocketError::Windows)?;
    // Sockets are blocking at rest: the blocking receive parks the calling
    // thread in the OS. The non-blocking `recv` and `send` switch the mode for
    // the duration of the call (Winsock has no per-call flags).
    Ok(fd)
}
fn set_nonblocking(sock: SOCKET, nonblocking: bool) -> Result<(), WindowsSocketError> {
    let mut mode: u32 = if nonblocking { 1 } else { 0 };

    let ret = unsafe { ioctlsocket(sock, FIONBIO, &mut mode) };
    return_to_result(ret)
}

fn last_winsock_error() -> WindowsSocketError {
    WindowsSocketError::WinsockError(WinsockError::try_from(unsafe { WSAGetLastError() }).unwrap())
}

/// Builds the `SO_RCVTIMEO` value in milliseconds. Important: on Winsock a
/// timeout of `0` means *no timeout* (infinite), so a nonzero duration is
/// rounded up to at least one millisecond.
fn receive_timeout_ms(timeout: Duration) -> u32 {
    timeout.as_millis().try_into().unwrap_or(u32::MAX).max(1)
}

fn set_receive_timeout(
    socket: SOCKET,
    timeout: Option<Duration>,
) -> Result<(), WindowsSocketError> {
    // `None` (unbounded wait) sets the OS default: no receive timeout.
    let ms = timeout.map(receive_timeout_ms).unwrap_or(0);
    let ret = unsafe {
        setsockopt(
            socket,
            SOL_SOCKET,
            SO_RCVTIMEO,
            Some(ms.to_ne_bytes().as_slice()),
        )
    };
    return_to_result(ret)
}

/// Temporarily changes a socket's blocking mode and restores the handle's
/// at-rest mode on drop. Winsock exposes no way to query `FIONBIO`, so the
/// at-rest mode is recorded on `UnixDatagram`.
struct SocketModeGuard {
    socket: SOCKET,
    at_rest_nonblocking: bool,
    toggled: bool,
}
impl SocketModeGuard {
    fn enter(
        socket: SOCKET,
        target_nonblocking: bool,
        at_rest_nonblocking: bool,
    ) -> Result<Self, WindowsSocketError> {
        let toggled = target_nonblocking != at_rest_nonblocking;
        if toggled {
            set_nonblocking(socket, target_nonblocking)?;
        }
        Ok(Self {
            socket,
            at_rest_nonblocking,
            toggled,
        })
    }
}
impl Drop for SocketModeGuard {
    fn drop(&mut self) {
        if self.toggled
            && let Err(err) = set_nonblocking(self.socket, self.at_rest_nonblocking)
        {
            log::error!("Could not restore socket blocking mode: {err}");
        }
    }
}
fn sockaddr_un_new(addr: &str) -> Result<SOCKADDR_UN, WindowsSocketError> {
    let mut unix_addr = SOCKADDR_UN {
        sun_family: ADDRESS_FAMILY(AF_UNIX),
        sun_path: [0; _],
    };

    let c_addr = CString::from_str(addr)
        .map_err(|_| WindowsSocketError::WinsockError(WinsockError::InvalidParameter))?;
    // let min_len = addr.len().min(107);
    // // let src: &[i8] = unsafe { &*(&addr.as_bytes()[..min_len] as *const _ as *const [i8]) };
    if c_addr.as_bytes_with_nul().len() > unix_addr.sun_path.len() {
        return Err(WindowsSocketError::WinsockError(
            WinsockError::InvalidParameter,
        ));
    }

    unsafe {
        core::ptr::copy_nonoverlapping(
            c_addr.as_ptr(),
            unix_addr.sun_path.as_mut_ptr(),
            c_addr.as_bytes_with_nul().len(),
        )
    };

    Ok(unix_addr)
}

fn sockaddr_un_to_string(addr: &SOCKADDR_UN) -> Option<String> {
    debug!("to_string: len_w0={}", sockaddr_len(addr));
    let c_addr = CStr::from_bytes_until_nul(bytes_from_i8(&addr.sun_path)).ok()?;
    Some(c_addr.to_str().ok()?.to_string())
}

fn sockaddr_len(addr: &SOCKADDR_UN) -> usize {
    let c_addr = CStr::from_bytes_until_nul(bytes_from_i8(&addr.sun_path))
        .ok()
        .unwrap();
    c_addr.to_bytes_with_nul().len()
}

fn local_addr(socket: SOCKET) -> Option<String> {
    let mut addr = SOCKADDR_UN {
        ..Default::default()
    };
    let mut len = size_of_val(&addr) as i32;

    let ret = unsafe { getsockname(socket, &mut addr as *mut _ as *mut SOCKADDR, &mut len) };

    if return_to_result(ret).is_err() {
        return None;
    }
    sockaddr_un_to_string(&addr)
}

fn peer_addr(socket: SOCKET) -> Option<String> {
    let mut addr = SOCKADDR_UN {
        ..Default::default()
    };
    let mut len = size_of_val(&addr) as i32;

    let ret = unsafe { getpeername(socket, &mut addr as *mut _ as *mut SOCKADDR, &mut len) };

    if return_to_result(ret).is_err() {
        return None;
    }
    sockaddr_un_to_string(&addr)
}

fn bytes_from_i8(src: &[i8]) -> &[u8] {
    unsafe { &*(src as *const _ as *const [u8]) }
}

fn prepare_bind_path(addr: &str) -> Result<(), TransportError> {
    let addr_path = Path::new(addr);
    if !addr_path.exists() {
        return Ok(());
    }

    // Guard against deleting non-file paths such as directories.
    if !addr_path.is_file() {
        return Err(TransportError::AddressInUse);
    }

    let probe_socket = OwnedSocket(create_socket()?);
    let unix_addr = sockaddr_un_new(addr)?;
    let ret = unsafe {
        connect(
            probe_socket.0,
            &unix_addr as *const _ as *const SOCKADDR,
            size_of_val(&unix_addr) as i32,
        )
    };

    match return_to_result(ret) {
        Ok(_) => Err(TransportError::AddressInUse),
        Err(WindowsSocketError::WinsockError(WinsockError::Wouldblock))
        | Err(WindowsSocketError::WinsockError(WinsockError::Inprogress))
        | Err(WindowsSocketError::WinsockError(WinsockError::Already))
        | Err(WindowsSocketError::WinsockError(WinsockError::Isconn)) => {
            Err(TransportError::AddressInUse)
        }
        Err(WindowsSocketError::WinsockError(WinsockError::Connrefused)) => {
            std::fs::remove_file(addr).map_err(|_| TransportError::AddressInUse)?;
            log::warn!("Removed stale Unix socket file at {addr}");
            Ok(())
        }
        Err(err) => Err(TransportError::Os(err)),
    }
}

#[derive(Debug)]
pub struct OwnedSocket(SOCKET);
impl Drop for OwnedSocket {
    fn drop(&mut self) {
        let ret = unsafe { closesocket(self.0) };
        return_to_result(ret).unwrap();
    }
}

#[derive(Debug)]
pub struct UnixDatagramListener {
    socket: OwnedSocket,
    local_addr: String,
}
impl Drop for UnixDatagramListener {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.local_addr);
    }
}
impl UnixDatagramListener {
    pub fn bind(addr: &str) -> Result<Self, TransportError> {
        prepare_bind_path(addr)?;
        let fd = create_socket()?;
        // The listener must stay non-blocking: the management thread polls
        // `accept` against a cancelation token.
        set_nonblocking(fd, true)?;
        let unix_addr = sockaddr_un_new(addr)?;

        let ret = unsafe {
            bind(
                fd,
                &unix_addr as *const _ as *const SOCKADDR,
                size_of_val(&unix_addr) as i32,
            )
        };
        match return_to_result(ret) {
            Ok(_) => (),
            Err(WindowsSocketError::WinsockError(WinsockError::Addrinuse)) => {
                return Err(TransportError::AddressInUse);
            }
            Err(err) => return Err(TransportError::Os(err)),
        }
        log::debug!("UnixDatagramListener bound to {addr}");
        let ret = unsafe { listen(fd, MAX_CONNECTIONS) };
        return_to_result(ret)?;

        Ok(UnixDatagramListener {
            socket: OwnedSocket(fd),
            local_addr: addr.to_string(),
        })
    }
}
impl TransportListener<UnixDatagram> for UnixDatagramListener {
    fn accept(&mut self) -> Result<UnixDatagram, TransportError> {
        let socket = unsafe { accept(self.socket.0, None, None) }.map_err(|err| {
            if err.code() == HRESULT(-2147014861) {
                TransportError::WouldBlock
            } else {
                TransportError::Os(WindowsSocketError::Windows(err))
            }
        })?;
        // The accepted socket carries the publisher's traffic: keep it
        // non-blocking so that `send` never parks the publishing thread. Set
        // explicitly instead of relying on the listener-mode inheritance.
        set_nonblocking(socket, true).map_err(TransportError::Os)?;
        Ok(UnixDatagram {
            socket: OwnedSocket(socket),
            nonblocking_at_rest: true,
        })
    }

    fn local_addr(&self) -> Option<String> {
        local_addr(self.socket.0)
    }
}

#[derive(Debug)]
pub struct UnixDatagram {
    socket: OwnedSocket,
    /// Blocking mode this handle is in between calls. Connected sockets are
    /// created blocking (the blocking receive parks the thread in the OS);
    /// accepted sockets are non-blocking because the publisher's send must
    /// never wait for a slow subscriber. Winsock has no way to query the
    /// mode, so the temporary switches restore to this recorded value.
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
        let fd = create_socket()?;
        let unix_addr = sockaddr_un_new(addr)?;

        // The socket is blocking: the connect completes synchronously, either
        // connected, refused because there is no publisher for the topic, or
        // a real error.
        let ret = unsafe {
            connect(
                fd,
                &unix_addr as *const _ as *const SOCKADDR,
                size_of_val(&unix_addr) as i32,
            )
        };
        match return_to_result(ret) {
            Ok(_) => (),
            Err(WindowsSocketError::WinsockError(WinsockError::Connrefused)) => {
                return Err(TransportError::PeerDoesNotExist);
            }
            Err(err) => {
                return Err(TransportError::Os(err));
            }
        }

        let ud = UnixDatagram {
            socket: OwnedSocket(fd),
            nonblocking_at_rest: false,
        };
        log::info!("connect: Now connected to {:?}", ud.peer_addr());
        Ok(ud)
    }
}
impl TransportConnection for UnixDatagram {
    fn local_addr(&self) -> Option<String> {
        local_addr(self.socket.0)
    }
    fn peer_addr(&self) -> Option<String> {
        peer_addr(self.socket.0)
    }
    fn send(&self, msg: &[u8]) -> Result<usize, SendError> {
        // The publisher's send must never block on a slow subscriber. Winsock
        // has no per-call non-blocking flag: switch a socket that is blocking
        // at rest to non-blocking for this call (accepted sockets are already
        // non-blocking at rest).
        let _mode = SocketModeGuard::enter(self.socket.0, true, self.nonblocking_at_rest)
            .map_err(SendError::from)?;
        let ret = unsafe {
            send(
                self.socket.0,
                &(msg.len() as u16).to_le_bytes(),
                SEND_RECV_FLAGS(0),
            )
        };
        match return_to_result(ret) {
            Ok(_) => (),
            Err(WindowsSocketError::WinsockError(WinsockError::Connreset)) => {
                return Err(SendError::ConnectionEnded);
            }
            // The peer receive buffer is full: drop this send instead of
            // erroring (the accepted socket is non-blocking).
            Err(WindowsSocketError::WinsockError(WinsockError::Wouldblock)) => {
                return Err(SendError::OsBufferFull);
            }
            Err(err) => return Err(SendError::OsError(err)),
        }
        assert_eq!(ret, 2);
        debug!("send len={}", msg.len());

        let ret = unsafe { send(self.socket.0, msg, SEND_RECV_FLAGS(0)) };
        match return_to_result(ret) {
            Ok(_) => (),
            Err(WindowsSocketError::WinsockError(WinsockError::Connreset)) => {
                return Err(SendError::ConnectionEnded);
            }
            // The peer receive buffer is full: drop this send instead of
            // erroring (the accepted socket is non-blocking).
            Err(WindowsSocketError::WinsockError(WinsockError::Wouldblock)) => {
                return Err(SendError::OsBufferFull);
            }
            Err(err) => return Err(SendError::OsError(err)),
        }
        debug!("send msg={:?}", msg);
        assert_eq!(ret, msg.len() as i32);

        Ok(ret as usize)
    }

    fn recv(&self, recv_buffer: &mut [u8]) -> Result<usize, ReceiveError> {
        // Winsock has no per-call non-blocking flag: switch a socket that is
        // blocking at rest to non-blocking for this call and restore the
        // at-rest mode afterwards (also on the error paths).
        let _mode = SocketModeGuard::enter(self.socket.0, true, self.nonblocking_at_rest)
            .map_err(ReceiveError::from)?;
        let mut len = [0u8; 2];
        let ret = unsafe { recv(self.socket.0, &mut len, SEND_RECV_FLAGS(0)) };
        match return_to_result(ret) {
            Ok(_) => (),
            Err(WindowsSocketError::WinsockError(WinsockError::Wouldblock)) => {
                return Err(ReceiveError::NoMessageAvailable);
            }
            Err(err) => return Err(ReceiveError::OsError(err)),
        }
        if ret == 0 {
            return Ok(0);
        }
        assert_eq!(ret, 2);
        let len = u16::from_le_bytes(len);
        debug!("recv len={len}");
        loop {
            let len_min = (len as usize).min(recv_buffer.len());
            let ret = unsafe {
                recv(
                    self.socket.0,
                    &mut recv_buffer[0..len_min],
                    SEND_RECV_FLAGS(0),
                )
            };
            match return_to_result(ret) {
                Ok(_) => (),
                Err(WindowsSocketError::WinsockError(WinsockError::Wouldblock)) => {
                    continue;
                }
                Err(err) => return Err(ReceiveError::OsError(err)),
            }
            debug!("recv buf={:?} ret={ret}", &recv_buffer[0..ret as usize]);
            // EOF in the middle of the (already size-prefixed) message: the
            // connection is gone. Report it instead of spinning in the drain
            // loop, which would never see its remaining bytes.
            if ret == 0 {
                return Ok(0);
            }
            if ret < len as i32 {
                // clear pipe be drop bytes
                let mut buf = [0u8; 32];
                let mut len_left = len as i32 - ret;
                loop {
                    if len_left == 0 {
                        break;
                    }
                    let len_this_iteration = len_left.min(buf.len() as i32);
                    let ret2 = unsafe {
                        recv(
                            self.socket.0,
                            &mut buf[0..len_this_iteration as usize],
                            SEND_RECV_FLAGS(0),
                        )
                    };
                    match return_to_result(ret2) {
                        Ok(_) => (),
                        Err(WindowsSocketError::WinsockError(WinsockError::Wouldblock)) => {
                            continue;
                        }
                        Err(err) => return Err(ReceiveError::OsError(err)),
                    }
                    // EOF while draining an oversized message: connection
                    // gone, report it instead of spinning forever.
                    if ret2 == 0 {
                        return Ok(0);
                    }
                    len_left = len_left.saturating_sub(ret2);
                }

                return Err(ReceiveError::ExternalBufferToSmall((
                    recv_buffer.len(),
                    len as usize,
                )));
            }

            return Ok(ret as usize);
        }
    }

    fn recv_blocking(
        &self,
        recv_buffer: &mut [u8],
        timeout: Option<Duration>,
    ) -> Result<usize, ReceiveError> {
        // Winsock honors timeouts at millisecond granularity (0 ms means no
        // timeout, shorter durations are floored to 1 ms). A timeout below
        // one microsecond is an immediate attempt instead, so an empty queue
        // reports a timeout and the caller's budget is essentially kept.
        if timeout.is_some_and(|timeout| timeout < Duration::from_micros(1)) {
            return match self.recv(recv_buffer) {
                Err(ReceiveError::NoMessageAvailable) => Err(ReceiveError::Timeout),
                other => other,
            };
        }
        // Blocking mode: Winsock parks the calling thread until data is
        // available, the peer disconnects, the receive timeout expires or the
        // call is interrupted.
        let _mode = SocketModeGuard::enter(self.socket.0, false, self.nonblocking_at_rest)
            .map_err(ReceiveError::from)?;
        set_receive_timeout(self.socket.0, timeout).map_err(ReceiveError::from)?;
        // `SO_RCVTIMEO` restarts with every call, so an interrupted receive
        // re-arms the remaining time to still honor the full deadline.
        let deadline = timeout.map(|timeout| Instant::now() + timeout);

        // The message framing is a little-endian u16 length prefix.
        let mut prefix = [0u8; 2];
        let mut prefix_read = 0usize;
        while prefix_read < prefix.len() {
            let ret = unsafe {
                recv(
                    self.socket.0,
                    &mut prefix[prefix_read..],
                    SEND_RECV_FLAGS(0),
                )
            };
            if ret == SOCKET_ERROR {
                match last_winsock_error() {
                    // In blocking mode the timeout expiry is
                    // `WSAETIMEDOUT`; a spurious `WSAEWOULDBLOCK` (not
                    // legitimate in blocking mode) means the same thing.
                    WindowsSocketError::WinsockError(
                        WinsockError::Timedout | WinsockError::Wouldblock,
                    ) => return Err(ReceiveError::Timeout),
                    WindowsSocketError::WinsockError(WinsockError::Intr) => {
                        if let Some(deadline) = deadline {
                            let remaining = deadline.saturating_duration_since(Instant::now());
                            if remaining.is_zero() {
                                return Err(ReceiveError::Timeout);
                            }
                            set_receive_timeout(self.socket.0, Some(remaining))
                                .map_err(ReceiveError::from)?;
                        }
                    }
                    err => return Err(ReceiveError::OsError(err)),
                }
            } else if ret == 0 {
                // The peer performed an orderly shutdown: connection ended.
                return Ok(0);
            } else {
                prefix_read += ret as usize;
            }
        }
        let expected_len = u16::from_le_bytes(prefix) as usize;
        if expected_len == 0 {
            // Sending zero bytes is not allowed (`SendError::ZeroBytes`): a
            // zero length prefix can only stem from a broken connection.
            return Ok(0);
        }
        debug!("recv_blocking len={expected_len}");
        // Once the length prefix is read the payload phase runs without a
        // timeout: a mid-message WSAETIMEDOUT would leave the length-prefixed
        // stream desynchronized. If the peer dies instead, the receive
        // reports EOF.
        set_receive_timeout(self.socket.0, None).map_err(ReceiveError::from)?;
        Self::receive_blocking_payload(self.socket.0, recv_buffer, expected_len)
    }
}

impl UnixDatagram {
    /// Reads the payload after the length prefix was received, blocking in
    /// the OS. Returns EOF (0) if the peer disconnects mid-message, which
    /// makes the subscriber drop and replace the connection instead of
    /// desynchronizing the framing.
    fn receive_blocking_payload(
        socket: SOCKET,
        recv_buffer: &mut [u8],
        expected_len: usize,
    ) -> Result<usize, ReceiveError> {
        let mut received = 0usize;
        let len_min = expected_len.min(recv_buffer.len());
        while received < len_min {
            let ret = unsafe {
                recv(
                    socket,
                    &mut recv_buffer[received..len_min],
                    SEND_RECV_FLAGS(0),
                )
            };
            if ret == SOCKET_ERROR {
                match last_winsock_error() {
                    WindowsSocketError::WinsockError(WinsockError::Intr) => continue,
                    err => return Err(ReceiveError::OsError(err)),
                }
            } else if ret == 0 {
                return Ok(0);
            } else {
                received += ret as usize;
            }
        }
        if len_min < expected_len {
            // The message does not fit: read the rest into a scratch buffer
            // and report the too-small external buffer like the non-blocking
            // receive does.
            let mut scratch = [0u8; 32];
            let mut left = expected_len - len_min;
            while left > 0 {
                let chunk = left.min(scratch.len());
                let ret = unsafe { recv(socket, &mut scratch[..chunk], SEND_RECV_FLAGS(0)) };
                if ret == SOCKET_ERROR {
                    match last_winsock_error() {
                        WindowsSocketError::WinsockError(WinsockError::Intr) => continue,
                        err => return Err(ReceiveError::OsError(err)),
                    }
                } else if ret == 0 {
                    return Ok(0);
                }
                left = left.saturating_sub(ret as usize);
            }
            return Err(ReceiveError::ExternalBufferToSmall((
                recv_buffer.len(),
                expected_len,
            )));
        }
        Ok(len_min)
    }
}
