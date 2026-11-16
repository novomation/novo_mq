use std::time::Duration;
use thiserror::Error;
#[cfg(target_family = "unix")]
pub mod uds;
#[cfg(target_family = "windows")]
pub mod win;
#[cfg(target_family = "windows")]
pub use win as uds;

#[cfg(target_family = "unix")]
pub type OsError = nix::Error;
#[cfg(target_family = "windows")]
pub type OsError = win::WindowsSocketError;

pub const MAX_CONNECTIONS: i32 = 25;

#[derive(Error, Debug, PartialEq)]

pub enum TransportError {
    #[error("Publisher address is already in use")]
    AddressInUse,
    #[error("Peer does not exist")]
    PeerDoesNotExist,
    #[error("Blocking operation is still pending")]
    WouldBlock,
    #[error("TransportError {0}")]
    Os(#[from] OsError),
}

#[derive(Error, Debug, PartialEq)]
pub enum ReceiveError {
    #[error("System Error {0}")]
    OsError(#[from] OsError),
    #[error("No Message")]
    NoMessageAvailable,
    #[error("The publisher has disconnected. We are trying to establish a new connection.")]
    DisconnectDetected,
    #[error("Waiting for a publisher. Not yet connected.")]
    NotConnectedToPublisher,
    #[error("Cannot connect to subscriber.")]
    ConnectImpossible,
    #[error("Internal buffer too small.")]
    InternalBufferToSmall,
    #[error("Your external buffer is too small (buffer_len, received_len): {0:?}")]
    ExternalBufferToSmall((usize, usize)),
    #[error("No message arrived within the waiting timeout")]
    Timeout,
}

#[derive(Error, Debug, PartialEq)]
pub enum SendError {
    #[error("Buffer of der underlying OS full")]
    OsBufferFull,
    #[error("ConnectionEnded")]
    ConnectionEnded,
    #[error("System Error {0}")]
    OsError(#[from] OsError),
    #[error("Sending zero bytes is not allowed")]
    ZeroBytes,
}

pub trait TransportListener<T> {
    fn accept(&mut self) -> Result<T, TransportError>
    where
        T: TransportConnection;
    /// Returns the address of this socket.
    fn local_addr(&self) -> Option<String>;
}

pub trait TransportConnection {
    /// Returns the address of this socket.
    fn local_addr(&self) -> Option<String>;
    fn peer_addr(&self) -> Option<String>;
    fn send(&self, msg: &[u8]) -> Result<usize, SendError>;
    fn recv(&self, recv_buffer: &mut [u8]) -> Result<usize, ReceiveError>;
    /// Blocks the calling thread in the OS until a message is received.
    fn recv_blocking(
        &self,
        recv_buffer: &mut [u8],
        timeout: Option<Duration>,
    ) -> Result<usize, ReceiveError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        cancelation::CancelationToken,
        transport::uds::{UnixDatagram, UnixDatagramListener},
    };
    use log::{error, info, warn};
    use std::{
        sync::{Arc, atomic::AtomicBool, mpsc::Receiver},
        thread::JoinHandle,
        time::{Duration, Instant},
    };
    use test_log::test;

    #[test]
    fn test_bind() {
        let sock_addr = uuid::Uuid::new_v4().to_string();
        let listener = UnixDatagramListener::bind(&sock_addr).unwrap();
        assert_eq!(listener.local_addr(), Some(sock_addr));
    }

    #[test]
    fn test_bind_twice_fails() {
        let sock_addr = uuid::Uuid::new_v4().to_string();
        let _listener = UnixDatagramListener::bind(&sock_addr).unwrap();

        assert_eq!(
            UnixDatagramListener::bind(&sock_addr).unwrap_err(),
            TransportError::AddressInUse
        );
    }

    #[test]
    fn test_connect_no_existing() {
        assert_eq!(
            UnixDatagram::connect("oiuzhtrtz").unwrap_err(),
            TransportError::PeerDoesNotExist
        );
    }

    #[test]
    fn test_would_block_on_accept() {
        let sock_addr = uuid::Uuid::new_v4().to_string();
        let mut listener = UnixDatagramListener::bind(&sock_addr).unwrap();
        assert_eq!(listener.accept().unwrap_err(), TransportError::WouldBlock);
    }

    #[test]
    fn smoke() {
        let sock_addr = uuid::Uuid::new_v4().to_string();

        let mut listener = UnixDatagramListener::bind(&sock_addr).unwrap();
        let clinet_handle = std::thread::spawn({
            let sock_addr = sock_addr.clone();
            move || {
                let client = UnixDatagram::connect(&sock_addr).unwrap();
                assert_eq!(client.peer_addr(), Some(sock_addr));
            }
        });
        let _connection = loop {
            if let Ok(connection) = listener.accept() {
                assert_eq!(connection.local_addr(), Some(sock_addr));
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
                    match server.accept() {
                        //
                        Ok(conn) => {
                            let _ = tx.send(Box::new(conn));
                        }
                        Err(TransportError::WouldBlock) => warn!("WB"),
                        Err(err) => error!("Accept error: {err}"),
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
                info!("Exit mngmt")
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
            TransportError::PeerDoesNotExist
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
        // Linux only *observes* the disconnect through send/recv: until then
        // the peer address keeps answering `getpeername`. XNU detaches the
        // connection as soon as the peer closes its socket, so
        // `getpeername` fails there (Err(EINVAL)) even though the client's
        // own receive still reaches the EOF below.
        #[cfg(target_os = "linux")]
        assert_eq!(client.peer_addr(), Some(addr.clone()));
        // This returns a value of 0, which signals the end of the remote connections.
        let mut buf = [0u8; 12];
        assert_eq!(client.recv(&mut buf), Ok(0));
        #[cfg(target_os = "linux")]
        {
            // The client still has a peer. We need to set up a new connection.
            assert_eq!(client.peer_addr(), Some(addr));
        }
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
    fn send_on_connectionreset() {
        let (addr, rx_conn, ct, join) = spawn_server();
        let client = UnixDatagram::connect(&addr).unwrap();
        let serv_conn = rx_conn.recv().unwrap();

        // Send first => OK
        assert_eq!(client.send(b"msg"), Ok(3));

        // Drop server
        drop(serv_conn);
        ct.cancel();
        join.join().unwrap();

        // Send second => Error
        assert_eq!(client.send(b"msg"), Err(SendError::ConnectionEnded));
    }

    #[test]
    fn recv_non_blocking() {
        let (addr, rx_conn, ct, join) = spawn_server();
        let _client = UnixDatagram::connect(&addr).unwrap();

        let serv_conn = rx_conn.recv().unwrap();

        let mut buf = [0_u8, 2];
        assert_eq!(
            serv_conn.recv(&mut buf),
            Err(ReceiveError::NoMessageAvailable)
        );

        ct.cancel();
        join.join().unwrap();
    }

    /// The blocking receive parks the thread in the OS: a message sent later
    /// by another thread wakes it up. The elapsed time proves that the call
    /// really blocked instead of missing the message.
    #[test]
    fn recv_blocking_waits_for_a_message() {
        let (addr, rx_conn, ct, join) = spawn_server();
        let client = UnixDatagram::connect(&addr).unwrap();
        let serv_conn = rx_conn.recv().unwrap();

        let sender = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(300));
            assert_eq!(serv_conn.send(b"msg"), Ok(3));
        });

        let start = Instant::now();
        let mut buf = [0u8; 12];
        assert_eq!(client.recv_blocking(&mut buf, None), Ok(3));
        assert_eq!(&buf[..3], b"msg");
        assert!(
            start.elapsed() >= Duration::from_millis(250),
            "the blocking receive returned too early ({:?}); it must wait until the message arrives",
            start.elapsed()
        );
        sender.join().unwrap();
        ct.cancel();
        join.join().unwrap();
    }

    #[test]
    fn recv_blocking_times_out() {
        let (addr, rx_conn, ct, join) = spawn_server();
        let client = UnixDatagram::connect(&addr).unwrap();
        let _serv_conn = rx_conn.recv().unwrap();

        let start = Instant::now();
        let mut buf = [0u8; 12];
        assert_eq!(
            client.recv_blocking(&mut buf, Some(Duration::from_millis(100))),
            Err(ReceiveError::Timeout)
        );
        assert!(
            start.elapsed() >= Duration::from_millis(90),
            "the receive timeout expired too early ({:?})",
            start.elapsed()
        );
        ct.cancel();
        join.join().unwrap();
    }

    /// A zero timeout is an immediate attempt: an empty queue is reported as
    /// a timeout instead of parking the thread forever (which a naive
    /// `SO_RCVTIMEO=0` implementation would do).
    #[test]
    fn recv_blocking_zero_timeout_is_a_nonblocking_attempt() {
        let (addr, rx_conn, ct, join) = spawn_server();
        let client = UnixDatagram::connect(&addr).unwrap();
        let _serv_conn = rx_conn.recv().unwrap();

        let start = Instant::now();
        let mut buf = [0u8; 12];
        assert_eq!(
            client.recv_blocking(&mut buf, Some(Duration::ZERO)),
            Err(ReceiveError::Timeout)
        );
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "a zero timeout must not block ({:?})",
            start.elapsed()
        );
        ct.cancel();
        join.join().unwrap();
    }

    /// Timeouts below a microsecond behave like zero: `SO_RCVTIMEO` cannot
    /// represent them (0 µs is configured as *no timeout*), so they use the
    /// immediate attempt and never park the thread.
    #[test]
    fn recv_blocking_sub_microsecond_timeout_is_an_immediate_attempt() {
        let (addr, rx_conn, ct, join) = spawn_server();
        let client = UnixDatagram::connect(&addr).unwrap();
        let _serv_conn = rx_conn.recv().unwrap();

        let start = Instant::now();
        let mut buf = [0u8; 12];
        assert_eq!(
            client.recv_blocking(&mut buf, Some(Duration::from_nanos(500))),
            Err(ReceiveError::Timeout)
        );
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "a sub-microsecond timeout must not block ({:?})",
            start.elapsed()
        );
        ct.cancel();
        join.join().unwrap();
    }

    /// A disconnect during the blocking receive wakes the thread up with the
    /// traditional end-of-file return value of 0.
    #[test]
    fn recv_blocking_returns_eof_when_peer_closes() {
        let (addr, rx_conn, ct, join) = spawn_server();
        let client = UnixDatagram::connect(&addr).unwrap();

        drop(rx_conn.recv().unwrap());

        let mut buf = [0u8; 12];
        assert_eq!(client.recv_blocking(&mut buf, None), Ok(0));
        ct.cancel();
        join.join().unwrap();
    }

    /// Accepted sockets are non-blocking at rest: their blocking receive
    /// temporarily clears the non-blocking flag and must restore it
    /// afterwards, so a subsequent non-blocking receive does not park in the
    /// kernel.
    #[test]
    fn recv_blocking_on_accepted_socket_restores_nonblocking() {
        let (addr, rx_conn, ct, join) = spawn_server();
        let client = UnixDatagram::connect(&addr).unwrap();
        let serv_conn = rx_conn.recv().unwrap();

        let sender = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            assert_eq!(client.send(b"msg"), Ok(3));
            // Keep the connection open for the non-blocking check below.
            std::thread::sleep(Duration::from_millis(500));
        });

        let mut buf = [0u8; 12];
        assert_eq!(serv_conn.recv_blocking(&mut buf, None), Ok(3));
        assert_eq!(&buf[..3], b"msg");
        // The non-blocking receive on the (again) non-blocking socket must
        // report "no message" instead of parking forever:
        assert_eq!(
            serv_conn.recv(&mut buf),
            Err(ReceiveError::NoMessageAvailable)
        );
        sender.join().unwrap();
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
