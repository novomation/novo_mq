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
};
use windows::{
    Win32::Networking::WinSock::{
        ADDRESS_FAMILY, AF_UNIX, FD_SET, FIONBIO, SEND_RECV_FLAGS, SOCK_STREAM, SOCKADDR,
        SOCKADDR_UN, SOCKET, SOCKET_ERROR, WSADATA, WSAGetLastError, WSAStartup, accept, bind,
        closesocket, connect, getpeername, getsockname, ioctlsocket, listen, recv, select, send,
        socket,
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
    set_nonblocking(fd, true)?;
    Ok(fd)
}
fn set_nonblocking(sock: SOCKET, nonblocking: bool) -> Result<(), WindowsSocketError> {
    let mut mode: u32 = if nonblocking { 1 } else { 0 };

    let ret = unsafe { ioctlsocket(sock, FIONBIO, &mut mode) };
    return_to_result(ret)
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
        Ok(UnixDatagram {
            socket: OwnedSocket(socket),
        })
    }

    fn local_addr(&self) -> Option<String> {
        local_addr(self.socket.0)
    }
}

#[derive(Debug)]
pub struct UnixDatagram {
    socket: OwnedSocket,
}
impl Drop for UnixDatagram {
    fn drop(&mut self) {
        if let Some(addr) = local_addr(self.socket.0) {
            let _ = std::fs::remove_file(&addr);
        }
    }
}
impl UnixDatagram {
    pub fn connect(addr: &str) -> Result<Self, TransportError> {
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

        let ud = UnixDatagram {
            socket: OwnedSocket(fd),
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
    fn send(&mut self, msg: &[u8]) -> Result<usize, SendError> {
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
            Err(err) => return Err(SendError::OsError(err)),
        }
        debug!("send msg={:?}", msg);
        assert_eq!(ret, msg.len() as i32);

        Ok(ret as usize)
    }

    fn recv(&mut self, recv_buffer: &mut [u8]) -> Result<usize, ReceiveError> {
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
