use crate::{
    ReceiveError, SendError,
    transport::{
        TransportError,
        win::{WindowsSocketError, WinsockError},
    },
};
use log::debug;
use std::{
    ffi::{CStr, CString},
    str::FromStr,
};
use windows::Win32::Networking::WinSock::{
    ADDRESS_FAMILY, AF_UNIX, FIONBIO, SOCK_STREAM, SOCKADDR, SOCKADDR_UN, SOCKET, SOCKET_ERROR,
    WSADATA, WSAGetLastError, WSAStartup, closesocket, getpeername, getsockname, ioctlsocket,
    socket,
};

/// Hook for attaching connection-specific state/behaviour (e.g. a shared-memory
/// mapping negotiated over the socket) to a plain [`UnixDatagram`].
///
/// The default `()` implementation is a no-op, so existing code that doesn't
/// need any extra per-connection data keeps working unchanged.
pub trait BulkTransport: Sized + Send {
    type Config;
    /// Called right after the listener's `accept()` succeeds, before the
    /// connection is handed back to the caller.
    fn on_accept(socket: SOCKET) -> Result<Self, TransportError>;
    /// Called right after `connect()` succeeds.
    fn on_connect(socket: SOCKET, config: Self::Config) -> Result<Self, TransportError>;
    /// Called to send a message.
    fn send_one_msg(&mut self, socket: SOCKET, buf: &[u8]) -> Result<usize, SendError>;
    /// Called to receive a message.
    fn recv_one_msg(&mut self, socket: SOCKET, buf: &mut [u8]) -> Result<usize, ReceiveError>;
}

pub fn return_to_result(ret: i32) -> Result<(), WindowsSocketError> {
    if ret == SOCKET_ERROR {
        let err = unsafe { WSAGetLastError() };
        Err(WindowsSocketError::WinsockError(
            WinsockError::try_from(err).unwrap(),
        ))
    } else {
        Ok(())
    }
}

pub fn create_socket() -> Result<SOCKET, WindowsSocketError> {
    unsafe {
        // must init winsock once in your program
        let mut wsa_data = WSADATA::default();
        WSAStartup(2u16 << 8 | 2u16, &mut wsa_data);
    }

    let fd = unsafe { socket(AF_UNIX as i32, SOCK_STREAM, 0) }
        .map_err(WindowsSocketError::Windows)?;
    set_nonblocking(fd, true)?;
    Ok(fd)
}
pub fn set_nonblocking(sock: SOCKET, nonblocking: bool) -> Result<(), WindowsSocketError> {
    let mut mode: u32 = if nonblocking { 1 } else { 0 };

    let ret = unsafe { ioctlsocket(sock, FIONBIO, &mut mode) };
    return_to_result(ret)
}
pub fn sockaddr_un_new(addr: &str) -> Result<SOCKADDR_UN, WindowsSocketError> {
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

pub fn sockaddr_un_to_string(addr: &SOCKADDR_UN) -> Option<String> {
    debug!("to_string: len_w0={}", sockaddr_len(addr));
    let c_addr = CStr::from_bytes_until_nul(bytes_from_i8(&addr.sun_path)).ok()?;
    Some(c_addr.to_str().ok()?.to_string())
}

pub fn sockaddr_len(addr: &SOCKADDR_UN) -> usize {
    let c_addr = CStr::from_bytes_until_nul(bytes_from_i8(&addr.sun_path))
        .ok()
        .unwrap();
    c_addr.to_bytes_with_nul().len()
}

pub fn local_addr(socket: SOCKET) -> Option<String> {
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

pub fn peer_addr(socket: SOCKET) -> Option<String> {
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

pub fn bytes_from_i8(src: &[i8]) -> &[u8] {
    unsafe { &*(src as *const _ as *const [u8]) }
}

#[derive(Debug)]
pub struct OwnedSocket(pub SOCKET);
impl Drop for OwnedSocket {
    fn drop(&mut self) {
        let ret = unsafe { closesocket(self.0) };
        return_to_result(ret).unwrap();
    }
}
