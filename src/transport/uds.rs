use log::debug;
use nix::sys::socket::{
    self, AddressFamily, Backlog, MsgFlags, RecvMsg, SockFlag, SockType, UnixAddr,
};

use crate::{
    ReceiveError,
    transport::{TransportConnection, TransportError, TransportListener},
};
use std::{
    io::IoSliceMut,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    path::Path,
};

#[derive(Debug)]
pub struct UnixDatagramListener {
    socket: OwnedFd,
    local_addr: String,
}
impl Drop for UnixDatagramListener {
    fn drop(&mut self) {
        let _ = nix::unistd::unlink(self.local_addr.as_str());
        debug!("IpcAddress {} dropped", self.local_addr)
    }
}
impl UnixDatagramListener {
    pub fn bind(addr: &str) -> Result<Self, TransportError> {
        // There should be only ENOMEM ENOBUFS  EMFILE ENFILE EACCES
        let fd = socket::socket(
            AddressFamily::Unix,
            SockType::SeqPacket,
            SockFlag::SOCK_NONBLOCK,
            None,
        )?;

        let unix_addr = UnixAddr::new(addr)?;
        if Path::new(addr).exists() {
            // We ignore errors. This may happen when drop and so on is not in sync
            let _ = std::fs::remove_file(addr);
        }
        socket::bind(fd.as_raw_fd(), &unix_addr)?;
        log::info!("Bound to {addr}");
        socket::listen(&fd, Backlog::MAXALLOWABLE)?;

        Ok(UnixDatagramListener {
            socket: fd,
            local_addr: addr.to_string(),
        })
    }
}
impl TransportListener<UnixDatagram> for UnixDatagramListener {
    fn accept(&mut self) -> Result<UnixDatagram, TransportError> {
        let fd = socket::accept4(self.socket.as_raw_fd(), SockFlag::SOCK_NONBLOCK)?;
        Ok(UnixDatagram {
            socket: unsafe { OwnedFd::from_raw_fd(fd) },
        })
    }
}

#[derive(Debug)]
pub struct UnixDatagram {
    socket: OwnedFd,
}
impl UnixDatagram {
    pub fn connect(addr: &str) -> Result<Self, TransportError> {
        let fd = socket::socket(
            AddressFamily::Unix,
            SockType::SeqPacket,
            SockFlag::SOCK_NONBLOCK,
            None,
        )?;

        let unix_addr = UnixAddr::new(addr)?;
        socket::connect(fd.as_raw_fd(), &unix_addr)?;
        let ud = UnixDatagram { socket: fd };
        log::info!("connect: Now connected to {:?}", ud.peer_addr());
        Ok(ud)
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
}
impl TransportConnection for UnixDatagram {
    fn local_addr(&self) -> Option<String> {
        let addr: UnixAddr = socket::getsockname(self.socket.as_raw_fd()).ok()?;
        Some(addr.to_string())
    }

    fn send(&self, msg: &[u8]) -> Result<usize, super::SendError> {
        let len = socket::send(self.socket.as_raw_fd(), msg, MsgFlags::empty())?;
        Ok(len)
    }

    fn recv(&self, recv_buffer: &mut [u8]) -> Result<usize, super::ReceiveError> {
        let recv_buffer_len = recv_buffer.len();
        let mut iov = [IoSliceMut::new(recv_buffer)];
        let flags = MsgFlags::MSG_TRUNC | MsgFlags::MSG_DONTWAIT;
        let mut cmsg_buffer = [0u8; 1];

        let res: Result<RecvMsg<()>, nix::errno::Errno> = socket::recvmsg(
            self.socket.as_raw_fd(),
            &mut iov,
            Some(&mut cmsg_buffer),
            flags,
        );

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cancelation::CancelationToken;
    use log::trace;
    use std::{
        sync::{Arc, atomic::AtomicBool, mpsc::Receiver},
        thread::JoinHandle,
        time::Duration,
    };
    use test_log::test;

    #[test]
    fn smoke() {
        let sock_addr = uuid::Uuid::new_v4().to_string();
        let mut listener = UnixDatagramListener::bind(&sock_addr).unwrap();
        let clinet_handle = std::thread::spawn(move || {
            let _client = UnixDatagram::connect(&sock_addr).unwrap();
        });
        let _connection = loop {
            if let Ok(connection) = listener.accept() {
                break connection;
            }
        };
        clinet_handle.join().unwrap();
    }

    fn spawn_server() -> (
        String,
        Receiver<Box<dyn TransportConnection + Send + 'static>>,
        CancelationToken,
        JoinHandle<()>,
    ) {
        let sock_addr = uuid::Uuid::new_v4().to_string();
        let mut server = UnixDatagramListener::bind(&sock_addr).unwrap();
        let start_accepting = Arc::new(AtomicBool::new(false));
        let (tx, rx) = std::sync::mpsc::channel::<Box<dyn TransportConnection + 'static + Send>>();
        let ct = CancelationToken::new();
        let join = std::thread::spawn({
            let start_accepting = start_accepting.clone();
            let ct = ct.clone();
            move || {
                start_accepting.store(true, std::sync::atomic::Ordering::Release);
                while !ct.is_cancelled() {
                    if let Ok(con) = server.accept() {
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
        (sock_addr, rx, ct, join)
    }

    #[test]
    fn connect_on_closed_socket() {
        let (addr, _, ct, join) = spawn_server();
        let _client = UnixDatagram::connect(&addr).unwrap();
        ct.cancel();
        join.join().unwrap();
        assert_eq!(
            UnixDatagram::connect(&addr).unwrap_err(),
            TransportError(nix::Error::ENOENT)
        );
    }
    #[test]
    fn accept_connect() {
        let (addr, rx_conn, ct, join) = spawn_server();
        let _client = UnixDatagram::connect(&addr).unwrap();

        assert!(rx_conn.recv().is_ok());

        ct.cancel();
        join.join().unwrap();
    }

    #[test]
    fn accept_connect_reconnect() {
        let (addr, rx_conn, ct, join) = spawn_server();
        let client = UnixDatagram::connect(&addr).unwrap();

        let c = rx_conn.recv().unwrap();
        ct.cancel();
        join.join().unwrap();

        // We drop and delete the server fd here.
        drop(c);
        std::thread::sleep(Duration::from_millis(500));
        // The client still has a peer. We conclude that the disconnection is only detected when using send/recv.
        assert_eq!(client.peer_addr(), Some(addr.clone()));
        // This returns a value of 0, which signals the end of the remote connections.
        let mut buf = [0u8; 12];
        assert_eq!(client.recv(&mut buf), Ok(0));
        // The client still has a peer. We need to set up a new connection.
        assert_eq!(client.peer_addr(), Some(addr));
    }

    #[test]
    fn send_recv() {
        let (addr, rx_conn, ct, join) = spawn_server();
        let client = UnixDatagram::connect(&addr).unwrap();

        let serv_conn = rx_conn.recv().unwrap();
        assert_eq!(client.send(b"msg"), Ok(3));
        let mut buf = [0_u8, 2];
        assert_eq!(
            serv_conn.recv(&mut buf),
            Err(ReceiveError::ExternalBufferToSmall((2, 3)))
        );
        assert_eq!(&buf, b"ms");
        assert_eq!(
            serv_conn.recv(&mut buf),
            Err(ReceiveError::NoMessageAvailable)
        );
        ct.cancel();
        join.join().unwrap();
    }

    #[test]
    fn pub_sub() {
        let (addr, rx_conn, ct, join) = spawn_server();

        let sub1 = UnixDatagram::connect(&addr).unwrap();
        let serv_tx1 = rx_conn.recv().unwrap();

        let sub2 = UnixDatagram::connect(&addr).unwrap();

        let serv_tx2 = rx_conn.recv().unwrap();

        assert_eq!(serv_tx1.send("msg1".as_bytes()), Ok(4));

        assert_eq!(serv_tx2.send("msg2".as_bytes()), Ok(4));

        let mut buf: [u8; 1024] = [0; 1024];
        assert_eq!(sub1.recv(&mut buf), Ok(4));
        assert_eq!(
            String::from_utf8_lossy(&buf[0..4]).to_string(),
            "msg1".to_string()
        );

        assert_eq!(sub2.recv(&mut buf), Ok(4));
        ct.cancel();
        join.join().unwrap();
    }
}
