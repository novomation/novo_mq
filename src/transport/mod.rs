use thiserror::Error;
pub mod algos;
#[cfg(target_family = "unix")]
mod bulk_transport;
#[cfg(target_family = "unix")]
pub use bulk_transport::BulkTransport;
#[cfg(target_os = "linux")]
pub mod direct_shm;
#[cfg(target_os = "linux")]
pub mod spsc;
#[cfg(target_os = "linux")]
pub mod tb;

#[cfg(target_family = "unix")]
pub mod uds;
#[cfg(target_family = "windows")]
pub mod win;
#[cfg(target_family = "windows")]
pub use uds::BulkTransport;
#[cfg(target_family = "windows")]
pub use win as uds;
#[cfg(target_family = "windows")]
pub use win::direct_shm_windows as direct_shm;
#[cfg(target_family = "windows")]
pub use win::spsc_windows as spsc;
#[cfg(target_family = "windows")]
pub use win::tb;

#[cfg(target_family = "unix")]
pub type OsError = nix::Error;
#[cfg(target_family = "windows")]
pub type OsError = win::WindowsSocketError;

pub const MAX_CONNECTIONS: i32 = 25;

#[derive(Error, Debug, PartialEq)]
pub enum TransportError {
    #[error("Peer does not exist")]
    PeerDoesNotExist,
    #[error("Blocking operation is still pending")]
    WouldBlock,
    #[error("TransportError {0}")]
    Os(#[from] OsError),
    #[error("Configured buffer sizes do not match: ours={ours}, peer={peer}")]
    BufferSizeMismatch { ours: usize, peer: usize },
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
    fn send(&mut self, msg: &[u8]) -> Result<usize, SendError>;
    fn recv(&mut self, recv_buffer: &mut [u8]) -> Result<usize, ReceiveError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        cancelation::CancelationToken,
        transport::uds::{UnixDatagram, UnixDatagramListener},
    };
    use log::{debug, error, info};
    use std::{
        sync::{Arc, atomic::AtomicBool, mpsc::Receiver},
        thread::JoinHandle,
        time::Duration,
    };
    use test_log::test;

    #[test]
    fn test_bind() {
        let sock_addr = uuid::Uuid::new_v4().to_string();
        let listener = UnixDatagramListener::<()>::bind(&sock_addr).unwrap();
        assert_eq!(listener.local_addr(), Some(sock_addr));
    }

    #[test]
    fn test_connect_no_existing() {
        assert_eq!(
            UnixDatagram::<()>::connect("oiuzhtrtz", ()).unwrap_err(),
            TransportError::PeerDoesNotExist
        );
    }

    #[test]
    fn test_would_block_on_accept() {
        let sock_addr = uuid::Uuid::new_v4().to_string();
        let mut listener = UnixDatagramListener::<()>::bind(&sock_addr).unwrap();
        assert_eq!(listener.accept().unwrap_err(), TransportError::WouldBlock);
    }

    #[test]
    fn smoke() {
        let sock_addr = uuid::Uuid::new_v4().to_string();

        let mut listener = UnixDatagramListener::<()>::bind(&sock_addr).unwrap();
        let clinet_handle = std::thread::spawn({
            let sock_addr = sock_addr.clone();
            move || {
                let client = UnixDatagram::<()>::connect(&sock_addr, ()).unwrap();
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

    fn spawn_server<T: BulkTransport + Send + 'static>() -> (
        String,
        Receiver<Box<dyn TransportConnection + Send + 'static>>,
        CancelationToken,
        JoinHandle<()>,
    ) {
        let sock_addr = uuid::Uuid::new_v4().to_string();
        let mut server = UnixDatagramListener::<T>::bind(&sock_addr).unwrap();
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
                        Err(TransportError::WouldBlock) => debug!("WB"),
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
        let (addr, _, ct, join) = spawn_server::<()>();
        std::thread::sleep(Duration::from_millis(500));

        let _client = UnixDatagram::<()>::connect(&addr, ()).unwrap();

        ct.cancel();
        join.join().unwrap();
        assert_eq!(
            UnixDatagram::<()>::connect(&addr, ()).unwrap_err(),
            TransportError::PeerDoesNotExist
        );
    }

    #[test]
    fn accept_connect() {
        let (addr, rx_conn, ct, join) = spawn_server::<()>();
        let _client = UnixDatagram::<()>::connect(&addr, ()).unwrap();

        assert!(rx_conn.recv().is_ok());

        ct.cancel();
        join.join().unwrap();
    }

    #[test]
    fn accept_connect_reconnect() {
        let (addr, rx_conn, ct, join) = spawn_server::<()>();
        let mut client = UnixDatagram::<()>::connect(&addr, ()).unwrap();

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
        let (addr, rx_conn, ct, join) = spawn_server::<()>();
        let mut client = UnixDatagram::<()>::connect(&addr, ()).unwrap();

        let mut serv_conn = rx_conn.recv().unwrap();
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
        let (addr, rx_conn, ct, join) = spawn_server::<()>();
        let mut client = UnixDatagram::<()>::connect(&addr, ()).unwrap();
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
        let (addr, rx_conn, ct, join) = spawn_server::<()>();
        let _client = UnixDatagram::<()>::connect(&addr, ()).unwrap();

        let mut serv_conn = rx_conn.recv().unwrap();

        let mut buf = [0_u8, 2];
        assert_eq!(
            serv_conn.recv(&mut buf),
            Err(ReceiveError::NoMessageAvailable)
        );

        ct.cancel();
        join.join().unwrap();
    }

    #[test]
    fn pub_sub() {
        let (addr, rx_conn, ct, join) = spawn_server::<()>();

        let mut sub1 = UnixDatagram::<()>::connect(&addr, ()).unwrap();
        let mut serv_tx1 = rx_conn.recv().unwrap();

        let mut sub2 = UnixDatagram::<()>::connect(&addr, ()).unwrap();

        let mut serv_tx2 = rx_conn.recv().unwrap();

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

#[cfg(not(target_os = "macos"))]
#[cfg(test)]
mod tb_tests {
    use super::*;
    use crate::{
        cancelation::CancelationToken,
        transport::uds::{UnixDatagram, UnixDatagramListener},
    };
    use log::{debug, error, info};
    use std::{
        sync::{Arc, atomic::AtomicBool, mpsc::Receiver},
        thread::JoinHandle,
        time::Duration,
    };
    use test_log::test;
    type TbTransp = tb::TbTransport<[u8; 24]>;
    type TbTranspWrongSize = tb::TbTransport<[u8; 26]>;

    #[test]
    fn test_bind_tb() {
        let sock_addr = uuid::Uuid::new_v4().to_string();
        let listener = UnixDatagramListener::<TbTransp>::bind(&sock_addr).unwrap();
        assert_eq!(listener.local_addr(), Some(sock_addr));
    }

    #[test]
    fn test_connect_no_existing_tb() {
        assert_eq!(
            UnixDatagram::<TbTransp>::connect("oiuzhtrtz", tb::TbTransportConfig::new())
                .unwrap_err(),
            TransportError::PeerDoesNotExist
        );
    }

    #[test]
    fn test_would_block_on_accept_tb() {
        let sock_addr = uuid::Uuid::new_v4().to_string();
        let mut listener = UnixDatagramListener::<TbTransp>::bind(&sock_addr).unwrap();
        assert_eq!(listener.accept().unwrap_err(), TransportError::WouldBlock);
    }

    #[test]
    fn smoke_tb() {
        let sock_addr = uuid::Uuid::new_v4().to_string();

        let mut listener = UnixDatagramListener::<TbTransp>::bind(&sock_addr).unwrap();
        let clinet_handle = std::thread::spawn({
            let sock_addr = sock_addr.clone();
            move || {
                let client =
                    UnixDatagram::<TbTransp>::connect(&sock_addr, tb::TbTransportConfig::new())
                        .unwrap();
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

    // Windows AF_UNIX has known reconnect-teardown issues after a
    // failed `on_connect` that are not present on Unix; skip on Windows.
    // See: https://github.com/novomation/novo_mq/issues (Windows reconnect race)
    #[cfg(not(target_family = "windows"))]
    #[test]
    fn wrong_peer_buffer_size() {
        let sock_addr = uuid::Uuid::new_v4().to_string();

        let mut listener = UnixDatagramListener::<TbTransp>::bind(&sock_addr).unwrap();
        let clinet_handle = std::thread::spawn({
            let sock_addr = sock_addr.clone();
            move || {
                let ret = UnixDatagram::<TbTranspWrongSize>::connect(
                    &sock_addr,
                    tb::TbTransportConfig::new(),
                )
                .unwrap_err();
                assert_eq!(
                    ret,
                    TransportError::BufferSizeMismatch { ours: 26, peer: 24 }
                );

                let client =
                    UnixDatagram::<TbTransp>::connect(&sock_addr, tb::TbTransportConfig::new())
                        .unwrap();
                assert_eq!(client.peer_addr(), Some(sock_addr));
            }
        });
        let mut con_counter = 0u8;
        loop {
            match listener.accept() {
                Ok(connection) => {
                    con_counter += 1;
                    assert_eq!(connection.local_addr(), Some(sock_addr.clone()));
                    break;
                }
                Err(TransportError::WouldBlock) => {
                    std::thread::sleep(Duration::from_millis(200));
                }
                Err(e) => {
                    con_counter += 1;
                    log::warn!("Err: {:?}", e);
                }
            }
        }
        assert_eq!(con_counter, 2);
        clinet_handle.join().unwrap();
    }

    fn spawn_server<T: BulkTransport + Send + 'static>() -> (
        String,
        Receiver<Box<dyn TransportConnection + Send + 'static>>,
        CancelationToken,
        JoinHandle<()>,
    ) {
        let sock_addr = uuid::Uuid::new_v4().to_string();
        let mut server = UnixDatagramListener::<T>::bind(&sock_addr).unwrap();
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
                        Err(TransportError::WouldBlock) => debug!("WB"),
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
    fn connect_on_closed_socket_tb() {
        let (addr, con, ct, join) = spawn_server::<TbTransp>();
        std::thread::sleep(Duration::from_millis(500));

        let _client =
            UnixDatagram::<TbTransp>::connect(&addr, tb::TbTransportConfig::new()).unwrap();
        drop(con);
        ct.cancel();
        join.join().unwrap();
        assert_eq!(
            UnixDatagram::<TbTransp>::connect(&addr, tb::TbTransportConfig::new()).unwrap_err(),
            TransportError::PeerDoesNotExist
        );
    }

    #[test]
    fn accept_connect_tb() {
        let (addr, rx_conn, ct, join) = spawn_server::<TbTransp>();
        let _client =
            UnixDatagram::<TbTransp>::connect(&addr, tb::TbTransportConfig::new()).unwrap();

        assert!(rx_conn.recv().is_ok());

        ct.cancel();
        join.join().unwrap();
    }

    #[test]
    fn accept_connect_reconnect_tb() {
        let (addr, rx_conn, ct, join) = spawn_server::<TbTransp>();
        let mut client =
            UnixDatagram::<TbTransp>::connect(&addr, tb::TbTransportConfig::new()).unwrap();

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
    fn send_recv_tb() {
        let (addr, rx_conn, ct, join) = spawn_server::<TbTransp>();
        let mut client =
            UnixDatagram::<TbTransp>::connect(&addr, tb::TbTransportConfig::new()).unwrap();

        let mut serv_conn = rx_conn.recv().unwrap();
        assert_eq!(client.send(b"msg"), Ok(3));
        let mut buf = [0_u8, 2];
        assert_eq!(
            serv_conn.recv(&mut buf),
            Err(ReceiveError::ExternalBufferToSmall((2, 3)))
        );
        assert_eq!(&buf, b"ms");
        assert_eq!(
            serv_conn.recv(&mut buf),
            Err(ReceiveError::ExternalBufferToSmall((2, 3)))
        );
        ct.cancel();
        join.join().unwrap();
    }

    #[test]
    fn send_on_connectionreset_tb() {
        let (addr, rx_conn, ct, join) = spawn_server::<TbTransp>();
        let mut client =
            UnixDatagram::<TbTransp>::connect(&addr, tb::TbTransportConfig::new()).unwrap();
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
    fn recv_non_blocking_tb() {
        let (addr, rx_conn, ct, join) = spawn_server::<TbTransp>();
        let _client =
            UnixDatagram::<TbTransp>::connect(&addr, tb::TbTransportConfig::new()).unwrap();

        let mut serv_conn = rx_conn.recv().unwrap();

        let mut buf = [0_u8, 2];
        assert_eq!(
            serv_conn.recv(&mut buf),
            Err(ReceiveError::NoMessageAvailable)
        );

        ct.cancel();
        join.join().unwrap();
    }

    #[test]
    fn pub_sub_tb() {
        let (addr, rx_conn, ct, join) = spawn_server::<TbTransp>();

        let mut sub1 =
            UnixDatagram::<TbTransp>::connect(&addr, tb::TbTransportConfig::new()).unwrap();
        let mut serv_tx1 = rx_conn.recv().unwrap();

        let mut sub2 =
            UnixDatagram::<TbTransp>::connect(&addr, tb::TbTransportConfig::new()).unwrap();

        let mut serv_tx2 = rx_conn.recv().unwrap();

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

#[cfg(target_os = "linux")]
#[cfg(test)]
mod tests_spsc {
    use super::*;
    use crate::{
        cancelation::CancelationToken,
        transport::{
            spsc::{SpscTransport, SpscTransportConfig},
            uds::{UnixDatagram, UnixDatagramListener},
        },
    };
    use log::{debug, error, info};
    use std::{
        sync::{Arc, atomic::AtomicBool, mpsc::Receiver},
        thread::JoinHandle,
        time::Duration,
    };
    use test_log::test;

    type SpscTransp = SpscTransport<[u8; 24], 4>;
    type SpscTranspWrong = SpscTransport<[u8; 22], 4>;

    #[test]
    fn test_bind_spsc() {
        let sock_addr = uuid::Uuid::new_v4().to_string();
        let listener = UnixDatagramListener::<SpscTransp>::bind(&sock_addr).unwrap();
        assert_eq!(listener.local_addr(), Some(sock_addr));
    }

    #[test]
    fn test_connect_no_existing_spsc() {
        assert_eq!(
            UnixDatagram::<SpscTransp>::connect("spz-oiuzhtrtz", SpscTransportConfig::new())
                .unwrap_err(),
            TransportError::PeerDoesNotExist
        );
    }

    #[test]
    fn test_would_block_on_accept_spsc() {
        let sock_addr = uuid::Uuid::new_v4().to_string();
        let mut listener = UnixDatagramListener::<SpscTransp>::bind(&sock_addr).unwrap();
        assert_eq!(listener.accept().unwrap_err(), TransportError::WouldBlock);
    }

    #[test]
    fn smoke_spsc() {
        let sock_addr = uuid::Uuid::new_v4().to_string();

        let mut listener = UnixDatagramListener::<SpscTransp>::bind(&sock_addr).unwrap();
        let clinet_handle = std::thread::spawn({
            let sock_addr = sock_addr.clone();
            move || {
                let client =
                    UnixDatagram::<SpscTransp>::connect(&sock_addr, SpscTransportConfig::new())
                        .unwrap();
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

    #[test]
    fn wrong_peer_buffer_size_spsc() {
        let sock_addr = uuid::Uuid::new_v4().to_string();

        let mut listener = UnixDatagramListener::<SpscTransp>::bind(&sock_addr).unwrap();
        let clinet_handle = std::thread::spawn({
            let sock_addr = sock_addr.clone();
            move || {
                let ret = UnixDatagram::<SpscTranspWrong>::connect(
                    &sock_addr,
                    SpscTransportConfig::new(),
                )
                .unwrap_err();
                assert_eq!(
                    ret,
                    TransportError::BufferSizeMismatch { ours: 22, peer: 24 }
                );

                let client =
                    UnixDatagram::<SpscTransp>::connect(&sock_addr, SpscTransportConfig::new())
                        .unwrap();
                assert_eq!(client.peer_addr(), Some(sock_addr));
            }
        });
        let mut con_counter = 0u8;
        loop {
            match listener.accept() {
                Ok(connection) => {
                    con_counter += 1;
                    assert_eq!(connection.local_addr(), Some(sock_addr.clone()));
                    break;
                }
                Err(TransportError::WouldBlock) => {
                    std::thread::sleep(Duration::from_millis(200));
                }
                Err(e) => {
                    con_counter += 1;
                    log::warn!("Err: {:?}", e);
                }
            }
        }
        assert_eq!(con_counter, 2);
        clinet_handle.join().unwrap();
    }

    fn spawn_server<T: BulkTransport + Send + 'static>() -> (
        String,
        Receiver<Box<dyn TransportConnection + Send + 'static>>,
        CancelationToken,
        JoinHandle<()>,
    ) {
        let sock_addr = uuid::Uuid::new_v4().to_string();
        let mut server = UnixDatagramListener::<T>::bind(&sock_addr).unwrap();
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
                        Ok(conn) => {
                            let _ = tx.send(Box::new(conn));
                        }
                        Err(TransportError::WouldBlock) => debug!("WB"),
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
    fn connect_on_closed_socket_spsc() {
        let (addr, con, ct, join) = spawn_server::<SpscTransp>();
        std::thread::sleep(Duration::from_millis(500));

        let _client =
            UnixDatagram::<SpscTransp>::connect(&addr, SpscTransportConfig::new()).unwrap();
        drop(con);
        ct.cancel();
        join.join().unwrap();
        assert_eq!(
            UnixDatagram::<SpscTransp>::connect(&addr, SpscTransportConfig::new()).unwrap_err(),
            TransportError::PeerDoesNotExist
        );
    }

    #[test]
    fn accept_connect_spsc() {
        let (addr, rx_conn, ct, join) = spawn_server::<SpscTransp>();
        let _client =
            UnixDatagram::<SpscTransp>::connect(&addr, SpscTransportConfig::new()).unwrap();

        assert!(rx_conn.recv().is_ok());

        ct.cancel();
        join.join().unwrap();
    }

    #[test]
    fn accept_connect_reconnect_spsc() {
        let (addr, rx_conn, ct, join) = spawn_server::<SpscTransp>();
        let mut client =
            UnixDatagram::<SpscTransp>::connect(&addr, SpscTransportConfig::new()).unwrap();

        let c = rx_conn.recv().unwrap();
        ct.cancel();
        join.join().unwrap();

        // We drop and delete the server fd here.
        drop(c);
        std::thread::sleep(Duration::from_millis(500));
        // The client still has a peer; disconnection is detected on send/recv.
        assert_eq!(client.peer_addr(), Some(addr.clone()));
        // `Ok(0)` signals the end of the remote connection; wait for the
        // connection-loss monitor to flag it.
        let mut buf = [0u8; 12];
        let mut saw_eof = false;
        for _ in 0..200 {
            match client.recv(&mut buf) {
                Ok(0) => {
                    saw_eof = true;
                    break;
                }
                _ => std::thread::sleep(Duration::from_millis(10)),
            }
        }
        assert!(saw_eof, "expected an end-of-connection (0 bytes) signal");
        // The client still needs a fresh connection to resume.
        assert_eq!(client.peer_addr(), Some(addr));
    }

    #[test]
    fn send_recv_spsc() {
        let (addr, rx_conn, ct, join) = spawn_server::<SpscTransp>();
        let mut client =
            UnixDatagram::<SpscTransp>::connect(&addr, SpscTransportConfig::new()).unwrap();

        let mut serv_conn = rx_conn.recv().unwrap();
        assert_eq!(client.send(b"msg"), Ok(3));
        let mut buf = [0_u8; 2];
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
    fn send_on_connectionreset_spsc() {
        let (addr, rx_conn, ct, join) = spawn_server::<SpscTransp>();
        let mut client =
            UnixDatagram::<SpscTransp>::connect(&addr, SpscTransportConfig::new()).unwrap();
        let serv_conn = rx_conn.recv().unwrap();

        // Send first => OK
        assert_eq!(client.send(b"msg"), Ok(3));

        // Drop server
        drop(serv_conn);
        ct.cancel();
        join.join().unwrap();

        // Second send reports an error (the peer's socket is gone).
        assert!(matches!(
            client.send(b"msg"),
            Err(SendError::ConnectionEnded)
                | Err(SendError::OsError(_))
                | Err(SendError::OsBufferFull)
        ));
    }

    #[test]
    fn recv_non_blocking_spsc() {
        let (addr, rx_conn, ct, join) = spawn_server::<SpscTransp>();
        let _client =
            UnixDatagram::<SpscTransp>::connect(&addr, SpscTransportConfig::new()).unwrap();

        let mut serv_conn = rx_conn.recv().unwrap();

        let mut buf = [0_u8; 2];
        assert_eq!(
            serv_conn.recv(&mut buf),
            Err(ReceiveError::NoMessageAvailable)
        );

        ct.cancel();
        join.join().unwrap();
    }

    #[test]
    fn pub_sub_spsc() {
        let (addr, rx_conn, ct, join) = spawn_server::<SpscTransp>();

        let mut sub1 =
            UnixDatagram::<SpscTransp>::connect(&addr, SpscTransportConfig::new()).unwrap();
        let mut serv_tx1 = rx_conn.recv().unwrap();

        let mut sub2 =
            UnixDatagram::<SpscTransp>::connect(&addr, SpscTransportConfig::new()).unwrap();

        let mut serv_tx2 = rx_conn.recv().unwrap();

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
