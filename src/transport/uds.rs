use std::{
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    path::Path,
    sync::{Arc, RwLock},
};

use nix::sys::socket::{
    self, AddressFamily, Backlog, MsgFlags, SetSockOpt, SockFlag, SockType, UnixAddr, sockopt,
};

use super::{
    ManagedTransport, ReceiveError, TransportError, TransportManaging, TransportMessaging,
};

struct OwnedIpcAddress(Option<String>);
impl Drop for OwnedIpcAddress {
    fn drop(&mut self) {
        if let Some(addr) = self.0.clone() {
            let _ = std::fs::remove_file(addr);
        }
    }
}

pub struct Uds {
    fd: Arc<OwnedFd>,
}
impl Uds {
    pub(crate) fn new() -> Result<Self, TransportError> {
        // There should be only ENOMEM ENOBUFS  EMFILE ENFILE EACCES
        let fd = socket::socket(
            AddressFamily::Unix,
            SockType::Stream,
            SockFlag::empty(),
            None,
        )?;
        Ok(Self { fd: Arc::new(fd) })
    }
    pub(crate) fn from_fd(fd: OwnedFd) -> Result<Self, TransportError> {
        Ok(Self { fd: Arc::new(fd) })
    }
}
pub struct UdsManging {
    fd: Arc<OwnedFd>,
    connect_addr: Option<String>,
    bind_addr: Arc<RwLock<OwnedIpcAddress>>,
}
pub struct UdsMessaging {
    fd: Arc<OwnedFd>,
    // This field not used but required so that the socket is closed when both messaging and managing are out of scope.
    _bind_addr: Arc<RwLock<OwnedIpcAddress>>,
}

impl ManagedTransport<UdsManging, UdsMessaging> for Uds {
    fn into_managed(self) -> (UdsManging, UdsMessaging) {
        let shared_addr = Arc::new(RwLock::new(OwnedIpcAddress(None)));
        (
            UdsManging {
                fd: self.fd.clone(),
                bind_addr: shared_addr.clone(),
                connect_addr: None,
            },
            UdsMessaging {
                fd: self.fd.clone(),
                _bind_addr: shared_addr,
            },
        )
    }
}
impl TransportManaging for UdsManging {
    fn bind(&mut self, addr: &str) -> Result<(), TransportError> {
        self.bind_addr.write().unwrap().0 = Some(addr.to_string());
        let unix_addr = UnixAddr::new(addr)?;
        if Path::new(addr).exists() {
            std::fs::remove_file(addr)
                .or(Err(TransportError::OsError(nix::errno::Errno::from_raw(0))))?;
        }
        socket::bind(self.fd.as_raw_fd(), &unix_addr)?;
        // socket::setsockopt(self.fd.as_raw_fd(), sockopt::non, val)
        Ok(())
    }

    fn connect(&mut self, addr: &str) -> Result<(), TransportError> {
        self.connect_addr = Some(addr.to_string());
        let unix_addr = UnixAddr::new(addr)?;
        socket::connect(self.fd.as_raw_fd(), &unix_addr)?;
        Ok(())
    }

    fn get_peer_addr(&self) -> Option<String> {
        let addr: UnixAddr = socket::getpeername(self.fd.as_raw_fd()).ok()?;
        Some(addr.to_string())
    }
    fn addr(&self) -> Option<String> {
        let addr: UnixAddr = socket::getsockname(self.fd.as_raw_fd()).ok()?;
        Some(addr.to_string())
    }

    fn accept(&mut self) -> Result<impl TransportMessaging + 'static, TransportError> {
        socket::listen(&self.fd, Backlog::MAXALLOWABLE)?;
        let fd = socket::accept(self.fd.as_raw_fd())?;
        Ok(UdsMessaging {
            fd: Arc::new(unsafe { OwnedFd::from_raw_fd(fd) }),
            _bind_addr: Arc::new(RwLock::new(OwnedIpcAddress(None))),
        })
    }
}
unsafe impl Send for UdsManging {}
unsafe impl Sync for UdsManging {}

impl TransportMessaging for UdsMessaging {
    fn send(&self, msg: &[u8]) -> Result<usize, TransportError> {
        let len = socket::send(self.fd.as_raw_fd(), msg, MsgFlags::empty())?;
        Ok(len)
    }

    fn recv(&self, recv_buffer: &mut [u8]) -> Result<usize, ReceiveError> {
        let res = socket::recv(
            self.fd.as_raw_fd(),
            recv_buffer,
            MsgFlags::MSG_TRUNC | MsgFlags::MSG_DONTWAIT,
        );
        match res {
            Ok(len) => Ok(len),
            Err(nix::errno::Errno::EAGAIN) => Err(ReceiveError::NoMessageAvailable),
            Err(err) => Err(err.into()),
        }
    }

    fn recv_blocking(&self, recv_buffer: &mut [u8]) -> Result<usize, ReceiveError> {
        let len = socket::recv(self.fd.as_raw_fd(), recv_buffer, MsgFlags::MSG_TRUNC)?;
        Ok(len)
    }

    fn recvfrom(&self, recv_buffer: &mut [u8]) -> Result<usize, ReceiveError> {
        let (len, _) = socket::recvfrom::<UnixAddr>(self.fd.as_raw_fd(), recv_buffer)?;
        Ok(len)
    }
    fn raw_fd(&self) -> i32 {
        self.fd.as_raw_fd()
    }
    fn addr(&self) -> Option<String> {
        let addr: UnixAddr = socket::getsockname(self.fd.as_raw_fd()).ok()?;
        Some(addr.to_string())
    }
}

unsafe impl Send for UdsMessaging {}
unsafe impl Sync for UdsMessaging {}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::transport::{ManagedTransport, TransportManaging, TransportMessaging};

    #[test]
    fn smoke() {
        let res = Uds::new();
        assert!(res.is_ok());
        let (_m, _rxtx) = res.unwrap().into_managed();
    }

    #[test]
    fn bind() {
        let (mut sock, _) = Uds::new().unwrap().into_managed();
        let sock_addr = uuid::Uuid::new_v4().to_string();
        let res = sock.bind(&sock_addr);

        assert!(res.is_ok());
    }
    #[test]
    fn send() {
        let (mut sock, rxtx) = Uds::new().unwrap().into_managed();
        let sock_addr = uuid::Uuid::new_v4().to_string();
        sock.bind(&sock_addr).unwrap();
        let res = rxtx.send("msg".as_bytes());
        assert!(res.is_err());

        let (mut sock2, rxtx2) = Uds::new().unwrap().into_managed();
        let sock_addr2 = uuid::Uuid::new_v4().to_string();
        sock2.bind(&sock_addr2).unwrap();
        sock2.connect(&sock_addr).unwrap();
        let res = rxtx2.send("msg".as_bytes());
        assert!(res.is_ok());
    }

    #[test]
    fn recv() {
        let (mut sock, rxtx) = Uds::new().unwrap().into_managed();
        let sock_addr = uuid::Uuid::new_v4().to_string();
        sock.bind(&sock_addr).unwrap();

        let (mut sock2, rxtx2) = Uds::new().unwrap().into_managed();

        sock2.connect(&sock_addr).unwrap();

        let res = rxtx.send("msg".as_bytes());
        assert!(res.is_err());
        let res = rxtx2.send("msg".as_bytes());
        assert!(res.is_ok());

        let mut buf: [u8; 1024] = [0; 1024];
        let res = rxtx.recv(&mut buf);
        assert!(res.is_ok());
    }

    #[test]
    fn pub_sub() {
        let (mut publ, publ_tx) = Uds::new().unwrap().into_managed();
        let publ_sock_addr = uuid::Uuid::new_v4().to_string();
        publ.bind(&publ_sock_addr).unwrap();
        std::thread::spawn(move || {
            loop {
                publ.accept().unwrap();
            }
        });
        std::thread::sleep(Duration::from_millis(3));

        let (mut sub1, sub1_rx) = Uds::new().unwrap().into_managed();
        // sub1.bind(uuid::Uuid::new_v4().to_string().as_str())
        //     .unwrap();
        sub1.connect(&publ_sock_addr).unwrap();

        let (mut sub2, sub2_rx) = Uds::new().unwrap().into_managed();
        // sub2.bind(uuid::Uuid::new_v4().to_string().as_str())
        //     .unwrap();
        sub2.connect(&publ_sock_addr).unwrap();

        let res = publ_tx.send("msg1".as_bytes()).unwrap();
        // assert!(res.is_err());
        let res = publ_tx.send("msg2".as_bytes()).unwrap();
        // assert!(res.is_ok());

        let mut buf: [u8; 1024] = [0; 1024];
        let res = sub1_rx.recv(&mut buf);
        println!("{res:?}");
        let res = sub2_rx.recv(&mut buf);
        println!("{res:?}");

        assert!(res.is_ok());
    }

    #[test]
    fn manage() {
        let (mut sock2, rxtx2) = Uds::new().unwrap().into_managed();
        let sock_addr2 = uuid::Uuid::new_v4().to_string();
        sock2.bind(&sock_addr2).unwrap();

        let (mut sock, rxtx) = Uds::new().unwrap().into_managed();
        // sock.bind("addr").unwrap();
        sock.connect(&sock_addr2).unwrap();
        let sendstr = "msg78ho8dhgfds";
        std::thread::spawn(move || {
            rxtx.send(sendstr.as_bytes()).unwrap();
        })
        .join()
        .unwrap();

        let mut buf: [u8; 1024] = [0; 1024];
        let res = rxtx2.recv(&mut buf).unwrap();
        let recvstr = std::str::from_utf8(&buf[0..res]).unwrap();
        assert_eq!(sendstr, recvstr);
    }
}
