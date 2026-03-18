use super::{
    ManagedTransport, ReceiveError, TransportError, TransportManaging, TransportMessaging,
};
use crate::transport::SendError;
use log::debug;
use nix::sys::socket::{
    self, AddressFamily, Backlog, MsgFlags, RecvMsg, SockFlag, SockType, UnixAddr,
};
use std::{
    io::IoSliceMut,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    path::Path,
    sync::{Arc, RwLock},
};

struct OwnedIpcAddress(Option<String>);
impl Drop for OwnedIpcAddress {
    fn drop(&mut self) {
        if let Some(addr) = self.0.clone() {
            let _ = nix::unistd::unlink(addr.as_str());
            // let _ = std::fs::remove_file(&addr);
            debug!("IpcAddress {addr} dropped")
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
            SockType::SeqPacket,
            SockFlag::SOCK_NONBLOCK,
            None,
        )?;
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
impl UdsManging {
    fn peer_name(&self) -> Option<String> {
        socket::getpeername(self.fd.as_raw_fd())
            .ok()
            .map(|addr: UnixAddr| {
                addr.path()
                    .map(|addr| addr.to_str().unwrap_or_default().to_string())
                    .unwrap_or_default()
            })
    }
}
impl TransportManaging for UdsManging {
    fn bind(&mut self, addr: &str) -> Result<(), TransportError> {
        self.bind_addr.write().unwrap().0 = Some(addr.to_string());
        let unix_addr = UnixAddr::new(addr)?;
        if Path::new(addr).exists() {
            // We ignore errors. This may happen when drop and so on is not in sync
            let _ = std::fs::remove_file(addr);
        }
        socket::bind(self.fd.as_raw_fd(), &unix_addr)?;
        log::info!("Bound to {addr}");
        // socket::setsockopt(self.fd.as_raw_fd(), sockopt::non, val)
        Ok(())
    }

    fn connect(&mut self, addr: &str) -> Result<(), TransportError> {
        self.connect_addr = Some(addr.to_string());
        let unix_addr = UnixAddr::new(addr)?;
        socket::connect(self.fd.as_raw_fd(), &unix_addr)?;
        log::info!("connect: Now connected to {:?}", self.peer_name());
        Ok(())
    }

    fn accept(&mut self) -> Result<impl TransportMessaging + 'static, TransportError> {
        socket::listen(&self.fd, Backlog::MAXALLOWABLE)?;
        let fd = socket::accept4(self.fd.as_raw_fd(), SockFlag::SOCK_NONBLOCK)?;
        Ok(UdsMessaging {
            fd: Arc::new(unsafe { OwnedFd::from_raw_fd(fd) }),
            _bind_addr: Arc::new(RwLock::new(OwnedIpcAddress(None))),
        })
    }
}
unsafe impl Send for UdsManging {}
unsafe impl Sync for UdsManging {}

impl TransportMessaging for UdsMessaging {
    fn send(&self, msg: &[u8]) -> Result<usize, SendError> {
        let len = socket::send(self.fd.as_raw_fd(), msg, MsgFlags::empty())?;
        Ok(len)
    }

    fn recv(&self, recv_buffer: &mut [u8]) -> Result<usize, ReceiveError> {
        let recv_buffer_len = recv_buffer.len();
        let mut iov = [IoSliceMut::new(recv_buffer)];
        let flags = MsgFlags::MSG_TRUNC | MsgFlags::MSG_DONTWAIT;
        let mut cmsg_buffer = [0u8; 1];

        let res: Result<RecvMsg<()>, nix::errno::Errno> =
            socket::recvmsg(self.fd.as_raw_fd(), &mut iov, Some(&mut cmsg_buffer), flags);

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

    // fn recv_blocking(&self, recv_buffer: &mut [u8]) -> Result<usize, ReceiveError> {
    //     let len = socket::recv(self.fd.as_raw_fd(), recv_buffer, MsgFlags::MSG_TRUNC)?;
    //     Ok(len)
    // }

    // fn recvfrom(&self, recv_buffer: &mut [u8]) -> Result<usize, ReceiveError> {
    //     let (len, _) = socket::recvfrom::<UnixAddr>(self.fd.as_raw_fd(), recv_buffer)?;
    //     Ok(len)
    // }
    // fn raw_fd(&self) -> i32 {
    //     self.fd.as_raw_fd()
    // }
    fn addr(&self) -> Option<String> {
        let addr: UnixAddr = socket::getsockname(self.fd.as_raw_fd()).ok()?;
        Some(addr.to_string())
    }
}

unsafe impl Send for UdsMessaging {}
unsafe impl Sync for UdsMessaging {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        cancelation::CancelationToken,
        transport::{ManagedTransport, TransportManaging, TransportMessaging},
    };
    use log::trace;
    use nix::errno::Errno;
    use std::{
        sync::{atomic::AtomicBool, mpsc::Receiver},
        thread::JoinHandle,
        time::Duration,
    };
    use test_log::test;

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

    fn spawn_server() -> (
        UdsMessaging,
        String,
        Receiver<Box<dyn TransportMessaging + Send + 'static>>,
        CancelationToken,
        JoinHandle<()>,
    ) {
        let (mut server_mgd, server_msg) = Uds::new().unwrap().into_managed();
        let sock_addr = uuid::Uuid::new_v4().to_string();
        server_mgd.bind(&sock_addr).unwrap();
        let start_accepting = Arc::new(AtomicBool::new(false));
        let (tx, rx) = std::sync::mpsc::channel::<Box<dyn TransportMessaging + 'static + Send>>();
        let ct = CancelationToken::new();
        let join = std::thread::spawn({
            let start_accepting = start_accepting.clone();
            let ct = ct.clone();
            move || {
                start_accepting.store(true, std::sync::atomic::Ordering::Release);
                while !ct.is_cancelled() {
                    if let Ok(con) = server_mgd.accept() {
                        let _ = tx.send(Box::new(con));
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
                trace!("Exit mngmt")
            }
        });
        while !start_accepting.load(std::sync::atomic::Ordering::Acquire) {
            std::thread::sleep(Duration::from_millis(2));
        }
        (server_msg, sock_addr, rx, ct, join)
    }

    #[test]
    fn connect_on_closed_socket() {
        let (_mng, addr, _, ct, join) = spawn_server();
        let (mut client, _) = Uds::new().unwrap().into_managed();
        assert_eq!(client.connect(&addr), Ok(()));
        ct.cancel();
        join.join().unwrap();
    }
    #[test]
    fn accept_connect() {
        let (_server_msg, sock_addr, rx, ct, join) = spawn_server();
        let (mut client, _) = Uds::new().unwrap().into_managed();
        assert_eq!(client.connect(&sock_addr), Ok(()));
        let _ = rx.recv();
        // We can connect only once.
        assert_eq!(
            client.connect(&sock_addr),
            Err(TransportError(Errno::EISCONN))
        );
        ct.cancel();
        join.join().unwrap();
    }

    #[test]
    fn accept_connect_reconnect() {
        let (server_msg, sock_addr, _, ct, join) = spawn_server();
        let (mut client, client_recv) = Uds::new().unwrap().into_managed();
        assert_eq!(client.connect(&sock_addr), Ok(()));
        assert_eq!(
            client.connect(&sock_addr),
            Err(TransportError(Errno::EISCONN))
        );
        // We drop and delete the server fd here.
        drop(server_msg);
        std::thread::sleep(Duration::from_millis(500));
        // The client still has a peer. We conclude that the disconnection is only detected when using send/recv.
        assert_eq!(client.peer_name(), Some(sock_addr.clone()));
        // This returns a value of 0, which signals the end of the remote connections.
        let mut buf = [0u8; 12];
        assert_eq!(client_recv.recv(&mut buf), Ok(0));
        // The client still has a peer. We need to set up a new connection.
        assert_eq!(client.peer_name(), Some(sock_addr));
        ct.cancel();
        join.join().unwrap();
    }

    #[test]
    fn send_recv() {
        let (_, sock_addr, rx_serv_rxtx, ct, join) = spawn_server();
        let (mut client, client_rxtx) = Uds::new().unwrap().into_managed();
        assert_eq!(client.connect(&sock_addr), Ok(()));
        let serv_rxtx = rx_serv_rxtx.recv().unwrap();
        assert_eq!(client_rxtx.send("msg".as_bytes()), Ok(3));
        let mut buf = [0_u8, 2];
        assert_eq!(
            serv_rxtx.recv(&mut buf),
            Err(ReceiveError::ExternalBufferToSmall((2, 3)))
        );
        assert_eq!(
            serv_rxtx.recv(&mut buf),
            Err(ReceiveError::NoMessageAvailable)
        );
        ct.cancel();
        join.join().unwrap();
    }

    #[test]
    fn pub_sub() {
        let (_, publ_sock_addr, rx_serv_rxtx, ct, join) = spawn_server();

        let (mut sub1, sub1_rx) = Uds::new().unwrap().into_managed();
        sub1.connect(&publ_sock_addr).unwrap();
        let serv_tx1 = rx_serv_rxtx.recv().unwrap();

        let (mut sub2, sub2_rx) = Uds::new().unwrap().into_managed();
        sub2.connect(&publ_sock_addr).unwrap();
        let serv_tx2 = rx_serv_rxtx.recv().unwrap();

        assert_eq!(serv_tx1.send("msg1".as_bytes()), Ok(4));

        assert_eq!(serv_tx2.send("msg2".as_bytes()), Ok(4));

        let mut buf: [u8; 1024] = [0; 1024];
        assert_eq!(sub1_rx.recv(&mut buf), Ok(4));
        assert_eq!(
            String::from_utf8_lossy(&buf[0..4]).to_string(),
            "msg1".to_string()
        );

        assert_eq!(sub2_rx.recv(&mut buf), Ok(4));
        ct.cancel();
        join.join().unwrap();
    }

    // #[test]
    // fn manage() {
    //     let (mut serv, serv_rxtx) = Uds::new().unwrap().into_managed();
    //     let serv_addr = uuid::Uuid::new_v4().to_string();
    //     serv.bind(&serv_addr).unwrap();

    //     let (mut cli, cli_rxtx) = Uds::new().unwrap().into_managed();
    //     // sock.bind("addr").unwrap();
    //     cli.connect(&serv_addr).unwrap();
    //     let sendstr = "msg78ho8dhgfds";
    //     std::thread::spawn(move || {
    //         rxtx.send(sendstr.as_bytes()).unwrap();
    //     })
    //     .join()
    //     .unwrap();

    //     let mut buf: [u8; 1024] = [0; 1024];
    //     let res = rxtx2.recv(&mut buf).unwrap();
    //     let recvstr = std::str::from_utf8(&buf[0..res]).unwrap();
    //     assert_eq!(sendstr, recvstr);
    // }
}
