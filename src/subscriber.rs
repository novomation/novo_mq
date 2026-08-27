use crate::{
    cancelation::CancelationToken,
    helpers::set_calling_thread_to_nrt,
    topic::socket_path,
    transport::{
        BulkTransport, ReceiveError, TransportConnection, TransportError, uds::UnixDatagram,
    },
};
use log::{debug, info, trace, warn};
use std::{
    fmt::Debug,
    marker::PhantomData,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use waitfree_sync::spsc;

pub struct Subscriber<T> {
    _cancel_token: CancelationToken, // Unused because we only need this to be dropped
    has_connection: Arc<AtomicBool>,
    socket: Option<Box<dyn TransportConnection + Send>>,
    rx_new_socket: spsc::Receiver<Box<dyn TransportConnection + Send>>,
    topic: String,
    _marker: PhantomData<T>,
}

impl<T: BulkTransport + 'static> Subscriber<T> {
    pub(crate) fn new(
        domain: impl AsRef<Path>,
        topic: impl AsRef<str>,
        config: T::Config,
    ) -> Result<Self, TransportError>
    where
        T::Config: Sync + Send + Clone + 'static,
    {
        let cancel_token = CancelationToken::new();
        let has_connection: Arc<AtomicBool> = Arc::new(false.into());
        let publ_path = socket_path(domain, &topic);

        let (mut tx_new_socket, rx_new_socket) =
            spsc::spsc::<Box<dyn TransportConnection + Send>>(64);

        std::thread::spawn({
            let cancel_token = cancel_token.clone();
            let has_connection = has_connection.clone();
            let topic = topic.as_ref().to_string();
            move || {
                set_calling_thread_to_nrt();
                debug!("Start subscriber `{topic}` mgmt thread and try to connect");
                while !cancel_token.is_cancelled() {
                    if !has_connection.load(Ordering::Acquire) {
                        match UnixDatagram::<T>::connect(&publ_path, config.clone()) {
                            Ok(connection) => {
                                has_connection.store(true, Ordering::Release);
                                let _ = tx_new_socket.try_send(Box::new(connection)).inspect_err(
                                    |_| warn!("Could not save subscriber socket -> Try again"),
                                );
                                info!("Now connected to {topic}");
                            }
                            Err(TransportError::PeerDoesNotExist) => (),
                            Err(err) => {
                                info!("Could not connect {err}");
                            }
                        }
                        std::thread::sleep(Duration::from_millis(100));
                    } else {
                        std::thread::sleep(Duration::from_secs(1));
                    }
                }
                // Ok(())
            }
        });
        Ok(Self {
            _cancel_token: cancel_token,
            has_connection,
            socket: None,
            rx_new_socket,
            topic: topic.as_ref().to_string(),
            _marker: PhantomData,
        })
    }

    // #[rtsan_standalone::nonblocking]
    #[cfg_attr(feature = "rtsan", rtsan_standalone::nonblocking)]
    pub fn try_recv(&mut self, message: &mut [u8]) -> Result<(usize, usize), ReceiveError> {
        if self.socket.is_none() {
            self.socket = self.rx_new_socket.try_recv();
        }
        if let Some(socket) = &mut self.socket {
            let bytes = socket.recv(message)?;
            let read_bytes = bytes.min(message.len());
            // When a stream socket peer has performed an orderly shutdown, the
            // return value will be 0 (the traditional "end-of-file" return).
            //
            // Datagram sockets in various domains (e.g., the UNIX and Internet
            // domains) permit zero-size datagrams.  When such a datagram is
            // received, the return value is 0.
            //
            // The value 0 may also be returned if the requested number of bytes
            // to receive from a stream socket was 0.
            // Here we hope thaht we only catch the first case. So sending zero must be avoided.
            if bytes == 0 {
                self.has_connection.store(false, Ordering::Release);
                // We drop the existing connection
                let _ = self.socket.take();
                Err(ReceiveError::DisconnectDetected)
            } else {
                Ok((read_bytes, bytes))
            }
        } else {
            Err(ReceiveError::NotConnectedToPublisher)
        }
    }
    pub fn has_connection(&self) -> bool {
        self.has_connection.load(Ordering::Relaxed)
    }
    pub fn wait_until_connected(&self) {
        while !self.has_connection() {
            std::thread::sleep(Duration::from_millis(200));
        }
    }
    pub fn get_topic(&self) -> &str {
        &self.topic
    }
}
impl<T> Drop for Subscriber<T> {
    fn drop(&mut self) {
        trace!(
            "Subscriber {:?} was dropped",
            self.socket.as_ref().map(|s| s.local_addr())
        );
    }
}
impl<T> Debug for Subscriber<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let socket_str = match &self.socket {
            Some(s) => match s.local_addr() {
                Some(addr) => addr,
                None => "?".to_string(),
            },
            None => "None".to_string(),
        };
        f.debug_struct("Subscriber")
            // .field("_cancel_token", &self._cancel_token)
            .field("has_connection", &self.has_connection)
            .field("socket", &socket_str)
            // .field("rx_new_socket", &self.rx_new_socket)
            .field("topic", &self.topic)
            .finish()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        publisher::Publisher,
        transport::tb::{TbTransport, TbTransportConfig},
    };
    use test_log::test;
    type TbTransp = TbTransport<[u8; 24]>;

    // TOOD: Error hang
    //     197 |     UnexpectedAncillaryData,
    //     |     ^^^^^^^^^^^^^^^^^^^^^^^
    //     |
    //     = note: `FdPassError` has a derived impl for the trait `Debug`, but this is intentionally ignored during dead code analysis
    //     = note: `#[warn(dead_code)]` (part of `#[warn(unused)]`) on by default

    // warning: `novomq` (lib test) generated 1 warning
    //     Finished `test` profile [unoptimized + debuginfo] target(s) in 0.08s
    //      Running unittests src/lib.rs (target/debug/deps/novomq-7c03daa4abc78247)

    // running 1 test
    // [INFO  novomq::transport::uds] Bound to cGV0ZXI
    // [INFO  novomq::transport::uds] In test case, so we sleep a little bit to test for race condition
    // [INFO  novomq::publisher] Got new subscriber for `peter`
    // [INFO  novomq::transport::uds] UnixDatagram now connected to Some("cGV0ZXI")
    // [INFO  novomq::subscriber] Now connected to peter

    // running 1 test
    // [INFO  novomq::transport::uds] Bound to cGV0ZXI
    // [INFO  novomq::transport::uds] In test case, so we sleep a little bit to test for race condition
    // [INFO  novomq::publisher] Got new subscriber for `peter`
    // [INFO  novomq::publisher] Notifying publisher.
    // [INFO  novomq::publisher] Got connection notification 1
    // [INFO  novomq::transport::uds] UnixDatagram now connected to Some("cGV0ZXI")
    // [INFO  novomq::subscriber] Now connected to peter
    // ^C *  Terminal will be reused by tasks, press any key to close it
    #[test]
    fn smoke() {
        let mut sub = Subscriber::<()>::new("", "peter", ()).unwrap();
        let mut publ = Publisher::<()>::new("", "peter").unwrap();
        // sub.wait_until_connected();

        publ.wait_for_subscribers(1);
        std::thread::sleep(Duration::from_millis(200));
        sub.wait_until_connected();

        publ.try_send("hans".as_bytes()).unwrap();

        let mut buf = [0u8; 1000];
        let (len, _) = sub.try_recv(&mut buf).unwrap();
        println!("{:?}", String::from_utf8_lossy(&buf[0..len]));
        drop(publ);
        let mut publ = Publisher::<()>::new("", "peter").unwrap();

        let mut buf = [0u8; 1000];
        assert_eq!(
            sub.try_recv(&mut buf),
            Err(ReceiveError::DisconnectDetected)
        );
        publ.wait_for_subscribers(1);
        sub.wait_until_connected();

        publ.try_send("hans".as_bytes()).unwrap();
        let (len, _) = sub.try_recv(&mut buf).unwrap();
        println!("{:?}", String::from_utf8_lossy(&buf[0..len]));
    }

    #[test]
    fn smoke_tb() {
        let mut sub = Subscriber::<TbTransp>::new("", "peter1", TbTransportConfig::new()).unwrap();
        let mut publ = Publisher::<TbTransp>::new("", "peter1").unwrap();
        publ.wait_for_subscribers(1);
        sub.wait_until_connected();
        publ.try_send("hans".as_bytes()).unwrap();

        let mut buf = [0u8; 1000];
        let (len, _) = sub.try_recv(&mut buf).unwrap();
        println!("{:?}", String::from_utf8_lossy(&buf[0..len]));
        drop(publ);
        let mut publ = Publisher::<TbTransp>::new("", "peter1").unwrap();

        let mut buf = [0u8; 1000];
        assert_eq!(
            sub.try_recv(&mut buf),
            Err(ReceiveError::DisconnectDetected)
        );
        publ.wait_for_subscribers(1);
        sub.wait_until_connected();
        publ.try_send("hans".as_bytes()).unwrap();
        let (len, _) = sub.try_recv(&mut buf).unwrap();
        println!("{:?}", String::from_utf8_lossy(&buf[0..len]));
    }
}
