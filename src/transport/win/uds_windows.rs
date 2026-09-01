use crate::{
    ReceiveError, SendError, TransportError,
    transport::{
        MAX_CONNECTIONS, TransportConnection, TransportListener,
        win::{
            BulkTransport, OwnedSocket, WindowsSocketError, WinsockError, create_socket,
            local_addr, peer_addr, return_to_result, sockaddr_un_new,
        },
    },
};
use log::debug;
use std::path::Path;
use windows::{
    Win32::Networking::WinSock::{
        FD_SET, SEND_RECV_FLAGS, SOCKADDR, SOCKET, accept, bind, connect, listen, recv, select,
        send,
    },
    core::HRESULT,
};

impl BulkTransport for () {
    type Config = ();
    fn on_accept(_socket: SOCKET) -> Result<Self, TransportError> {
        Ok(())
    }
    fn on_connect(_socket: SOCKET, _config: Self::Config) -> Result<Self, TransportError> {
        Ok(())
    }

    fn send_one_msg(&mut self, socket: SOCKET, msg: &[u8]) -> Result<usize, SendError> {
        // 8-byte (usize) length prefix so payloads larger than 64 KiB (the
        // old u16 limit) are framed correctly. Matches `tb_windows::send_usize`.
        let len_bytes = msg.len().to_le_bytes();
        let ret = unsafe { send(socket, &len_bytes, SEND_RECV_FLAGS(0)) };
        match return_to_result(ret) {
            Ok(_) => (),
            Err(WindowsSocketError::WinsockError(WinsockError::Connreset)) => {
                return Err(SendError::ConnectionEnded);
            }
            Err(err) => return Err(SendError::OsError(err)),
        }
        assert_eq!(ret, len_bytes.len() as i32);
        debug!("send len={}", msg.len());

        let ret = unsafe { send(socket, msg, SEND_RECV_FLAGS(0)) };
        match return_to_result(ret) {
            Ok(_) => (),
            Err(WindowsSocketError::WinsockError(WinsockError::Connreset)) => {
                return Err(SendError::ConnectionEnded);
            }
            Err(err) => return Err(SendError::OsError(err)),
        }
        debug!("send msg={:?}", msg);
        assert_eq!(ret, msg.len() as i32);

        Ok(ret as usize)
    }

    fn recv_one_msg(
        &mut self,
        socket: SOCKET,
        recv_buffer: &mut [u8],
    ) -> Result<usize, ReceiveError> {
        let mut len = [0u8; size_of::<usize>()];
        let ret = unsafe { recv(socket, &mut len, SEND_RECV_FLAGS(0)) };
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
        assert_eq!(ret, size_of::<usize>() as i32);
        let len = usize::from_le_bytes(len);
        debug!("recv len={len}");

        // Reads `n` bytes fully (retrying on WSAEWOULDBLOCK); returns the peer's
        // zero-byte reset as `RecvError::OsError(...Connreset)`.
        let read_fully = |socket: SOCKET, dst: &mut [u8]| -> Result<(), ReceiveError> {
            let mut got = 0usize;
            while got < dst.len() {
                let ret = unsafe { recv(socket, &mut dst[got..], SEND_RECV_FLAGS(0)) };
                match return_to_result(ret) {
                    Ok(_) => (),
                    Err(WindowsSocketError::WinsockError(WinsockError::Wouldblock)) => {
                        std::thread::yield_now();
                        continue;
                    }
                    Err(err) => return Err(ReceiveError::OsError(err)),
                }
                let n = ret as usize;
                if n == 0 {
                    return Err(ReceiveError::OsError(WindowsSocketError::WinsockError(
                        WinsockError::Connreset,
                    )));
                }
                got += n;
            }
            Ok(())
        };

        if len == 0 {
            return Ok(0);
        }

        if recv_buffer.len() < len {
            // Caller's buffer is too small: drain the whole message so the
            // socket stream stays aligned for the next message.
            log::warn!("recv: buffer too small ({} < {len})", recv_buffer.len());
            let chunk = recv_buffer.len();
            if chunk > 0 {
                read_fully(socket, &mut recv_buffer[..chunk])?;
            }
            let mut tmp = vec![0u8; len.saturating_sub(chunk).min(65536).max(1)];
            let mut remaining = len - chunk.min(len);
            while remaining > 0 {
                let take = tmp.len().min(remaining);
                read_fully(socket, &mut tmp[..take])?;
                remaining = remaining.saturating_sub(take);
            }
            return Err(ReceiveError::ExternalBufferToSmall((recv_buffer.len(), len)));
        }

        read_fully(socket, &mut recv_buffer[..len])?;
        Ok(len)
    }
}

#[derive(Debug)]
pub struct UnixDatagramListener<E: BulkTransport = ()> {
    pub socket: OwnedSocket,
    local_addr: String,
    _marker: std::marker::PhantomData<E>,
}
impl<E: BulkTransport> Drop for UnixDatagramListener<E> {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.local_addr);
    }
}
impl<E: BulkTransport> UnixDatagramListener<E> {
    pub fn bind(addr: &str) -> Result<Self, TransportError> {
        let fd = create_socket()?;
        let addr_path = Path::new(addr);

        if addr_path.exists() {
            // We ignore errors. This may happen when drop and so on is not in sync
            let _ = std::fs::remove_file(addr);
        }
        let unix_addr = sockaddr_un_new(addr)?;

        let ret = unsafe {
            bind(
                fd,
                &unix_addr as *const _ as *const SOCKADDR,
                size_of_val(&unix_addr) as i32,
            )
        };
        return_to_result(ret)?;
        log::info!("Bound to {addr}");
        let ret = unsafe { listen(fd, MAX_CONNECTIONS) };
        return_to_result(ret)?;

        Ok(UnixDatagramListener {
            socket: OwnedSocket(fd),
            local_addr: addr.to_string(),
            _marker: std::marker::PhantomData,
        })
    }
}
impl<E: BulkTransport> TransportListener<UnixDatagram<E>> for UnixDatagramListener<E> {
    fn accept(&mut self) -> Result<UnixDatagram<E>, TransportError> {
        let socket = unsafe { accept(self.socket.0, None, None) }
            .map_err(|err| {
                if err.code() == HRESULT(-2147014861) {
                    TransportError::WouldBlock
                } else {
                    TransportError::Os(WindowsSocketError::Windows(err))
                }
            })
            .inspect_err(|e| log::debug!("Error accepting: {:?}", e))?;
        // Own the socket immediately so it is closed automatically if
        // `on_accept` returns an error (e.g. a buffer-size mismatch).
        let socket = OwnedSocket(socket);
        let extra = E::on_accept(socket.0)?;
        Ok(UnixDatagram { socket, extra })
    }

    fn local_addr(&self) -> Option<String> {
        local_addr(self.socket.0)
    }
}

#[derive(Debug)]
pub struct UnixDatagram<E: BulkTransport = ()> {
    socket: OwnedSocket,
    pub extra: E,
}
impl<E: BulkTransport> Drop for UnixDatagram<E> {
    fn drop(&mut self) {
        if let Some(addr) = local_addr(self.socket.0) {
            let _ = std::fs::remove_file(&addr);
        }
    }
}
impl<E: BulkTransport> UnixDatagram<E> {
    pub fn connect(addr: &str, config: E::Config) -> Result<Self, TransportError> {
        let fd = create_socket()?;
        let unix_addr = sockaddr_un_new(addr)?;

        let ret = unsafe {
            connect(
                fd,
                &unix_addr as *const _ as *const SOCKADDR,
                size_of_val(&unix_addr) as i32,
            )
        };
        match return_to_result(ret) {
            Ok(_) => {
                return Err(TransportError::Os(WindowsSocketError::WinsockError(
                    WinsockError::Fault,
                )));
            }
            Err(WindowsSocketError::WinsockError(WinsockError::Wouldblock)) => {
                // Do nothing here. We wait with select
            }
            Err(WindowsSocketError::WinsockError(WinsockError::Connrefused)) => {
                return Err(TransportError::PeerDoesNotExist);
            }
            Err(err) => {
                return Err(TransportError::Os(err));
            }
        }
        let mut writefds = FD_SET {
            fd_count: 1,
            fd_array: [fd; 64],
        };

        unsafe { select(0, None, Some(&mut writefds), None, None) };
        log::info!("Calling inner connection method");
        // Own the socket immediately: if `on_connect` fails (e.g. a buffer-size
        // mismatch detected in the handshake) we must close it so the peer's
        // blocking handshake read is unblocked and no half-dead AF_UNIX
        // connection lingers (parity with the Unix `OwnedFd`-on-error path).
        let socket = OwnedSocket(fd);
        let extra = match E::on_connect(socket.0, config) {
            Ok(extra) => extra,
            Err(e) => {
                log::info!("on_connect failed, dropping socket");
                return Err(e);
            }
        };
        let ud = UnixDatagram { socket, extra };
        log::info!("connect: Now connected to {:?}", ud.peer_addr());
        Ok(ud)
    }
}
impl<E: BulkTransport> TransportConnection for UnixDatagram<E> {
    fn local_addr(&self) -> Option<String> {
        local_addr(self.socket.0)
    }
    fn peer_addr(&self) -> Option<String> {
        peer_addr(self.socket.0)
    }
    fn send(&mut self, msg: &[u8]) -> Result<usize, SendError> {
        self.extra.send_one_msg(self.socket.0, msg)
    }

    fn recv(&mut self, recv_buffer: &mut [u8]) -> Result<usize, ReceiveError> {
        self.extra.recv_one_msg(self.socket.0, recv_buffer)
    }
}
