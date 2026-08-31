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
        let ret = unsafe {
            send(
                socket,
                &(msg.len() as u16).to_le_bytes(),
                SEND_RECV_FLAGS(0),
            )
        };
        match return_to_result(ret) {
            Ok(_) => (),
            Err(WindowsSocketError::WinsockError(WinsockError::Connreset)) => {
                return Err(SendError::ConnectionEnded);
            }
            Err(err) => return Err(SendError::OsError(err)),
        }
        assert_eq!(ret, 2);
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
        let mut len = [0u8; 2];
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
        assert_eq!(ret, 2);
        let len = u16::from_le_bytes(len);
        debug!("recv len={len}");
        loop {
            let len_min = (len as usize).min(recv_buffer.len());
            let ret = unsafe { recv(socket, &mut recv_buffer[0..len_min], SEND_RECV_FLAGS(0)) };
            match return_to_result(ret) {
                Ok(_) => (),
                Err(WindowsSocketError::WinsockError(WinsockError::Wouldblock)) => {
                    continue;
                }
                Err(err) => return Err(ReceiveError::OsError(err)),
            }
            debug!("recv buf={:?} ret={ret}", &recv_buffer[0..ret as usize]);
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
                            socket,
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
        prepare_bind_path(addr)?;
        let fd = create_socket()?;
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
        Ok(UnixDatagram {
            socket,
            extra,
        })
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
        let extra = E::on_connect(fd, config)?;
        let ud = UnixDatagram {
            socket: OwnedSocket(fd),
            extra,
        };
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
