use crate::{
    cancelation::CancelationToken,
    helpers::set_calling_thread_to_nrt,
    topic::socket_path,
    transport::{ReceiveError, TransportConnection, TransportError, uds::UnixDatagram},
};
use log::{debug, info, trace, warn};
use std::{
    fmt::Debug,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use waitfree_sync::spsc;

pub struct Subscriber {
    _cancel_token: CancelationToken, // Unused because we only need this to be dropped
    has_connection: Arc<AtomicBool>,
    socket: Option<Box<dyn TransportConnection + Send>>,
    rx_new_socket: spsc::Receiver<Box<dyn TransportConnection + Send>>,
    topic: String,
}

impl Subscriber {
    pub(crate) fn new(
        domain: impl AsRef<Path>,
        topic: impl AsRef<str>,
    ) -> Result<Self, TransportError> {
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
                        match UnixDatagram::connect(&publ_path) {
                            Ok(connection) => {
                                has_connection.store(true, Ordering::Release);
                                let _ = tx_new_socket.try_send(Box::new(connection)).inspect_err(
                                    |_| warn!("Could not save subscriber socket -> Try again"),
                                );
                                info!("Now connected to {topic}");
                            }
                            Err(TransportError::PeerDoesNotExist) => (),
                            Err(err) => {
                                debug!("Could not connect {err}");
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
        })
    }

    // #[rtsan_standalone::nonblocking]
    #[cfg_attr(feature = "rtsan", rtsan_standalone::nonblocking)]
    pub fn try_recv(&mut self, message: &mut [u8]) -> Result<(usize, usize), ReceiveError> {
        self.pick_up_new_socket();
        let Some(socket) = self.socket.as_ref() else {
            return Err(ReceiveError::NotConnectedToPublisher);
        };
        let bytes = socket.recv(message)?;
        self.handle_received(bytes, message.len())
    }

    /// Blocks the calling thread in the OS until a message arrives on this
    /// topic. The thread does not poll: the kernel parks it until a message
    /// arrives or the publisher disconnects, which reports
    /// [`ReceiveError::DisconnectDetected`].
    ///
    /// While no publisher is connected, this returns
    /// [`ReceiveError::NotConnectedToPublisher`] instead of waiting — a
    /// consumer typically calls [`Subscriber::wait_until_connected`] first.
    /// For a loop that consumes across a disconnect, back off between
    /// attempts (a bounded [`Subscriber::recv_with_timeout`] or a short
    /// sleep): a plain `loop { recv() }` spins on `NotConnectedToPublisher`
    /// until the management thread reconnects.
    pub fn recv(&mut self, message: &mut [u8]) -> Result<(usize, usize), ReceiveError> {
        self.recv_blocking(message, None)
    }

    /// Blocks like [`Subscriber::recv`], but reports
    /// [`ReceiveError::Timeout`] when no message arrived within `timeout`.
    /// A `timeout` of zero performs an immediate attempt.
    pub fn recv_with_timeout(
        &mut self,
        message: &mut [u8],
        timeout: Duration,
    ) -> Result<(usize, usize), ReceiveError> {
        self.recv_blocking(message, Some(timeout))
    }

    fn recv_blocking(
        &mut self,
        message: &mut [u8],
        timeout: Option<Duration>,
    ) -> Result<(usize, usize), ReceiveError> {
        self.pick_up_new_socket();
        let Some(socket) = self.socket.as_ref() else {
            return Err(ReceiveError::NotConnectedToPublisher);
        };
        let bytes = socket.recv_blocking(message, timeout)?;
        self.handle_received(bytes, message.len())
    }

    /// Connects a new socket if the management thread handed one over.
    fn pick_up_new_socket(&mut self) {
        if self.socket.is_none() {
            self.socket = self.rx_new_socket.try_recv().ok();
        }
    }

    /// Reports a received message and handles the disconnect case.
    ///
    /// When a stream socket peer has performed an orderly shutdown, the
    /// return value will be 0 (the traditional "end-of-file" return).
    ///
    /// Datagram sockets in various domains (e.g., the UNIX and Internet
    /// domains) permit zero-size datagrams. When such a datagram is received,
    /// the return value is 0. The value 0 may also be returned if the
    /// requested number of bytes to receive from a stream socket was 0. Here
    /// we hope that we only catch the first case, so sending zero must be
    /// avoided.
    fn handle_received(
        &mut self,
        bytes: usize,
        message_len: usize,
    ) -> Result<(usize, usize), ReceiveError> {
        if bytes == 0 {
            self.has_connection.store(false, Ordering::Release);
            // We drop the existing connection: the management thread
            // establishes a new one.
            let _ = self.socket.take();
            return Err(ReceiveError::DisconnectDetected);
        }
        Ok((bytes.min(message_len), bytes))
    }
    pub fn has_connection(&self) -> bool {
        self.has_connection.load(Ordering::Relaxed)
    }
    pub fn wait_until_connected(&self) {
        while !self.has_connection() {
            std::thread::sleep(Duration::from_millis(2));
        }
    }
    pub fn get_topic(&self) -> &str {
        &self.topic
    }
}
impl Drop for Subscriber {
    fn drop(&mut self) {
        trace!(
            "Subscriber {:?} was dropped",
            self.socket.as_ref().map(|s| s.local_addr())
        );
    }
}
impl Debug for Subscriber {
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
    use crate::publisher::Publisher;
    use std::time::Instant;

    use super::*;

    use test_log::test;

    #[test]
    fn smoke() {
        let mut sub = Subscriber::new("", "peter").unwrap();
        let mut publ = Publisher::new("", "peter").unwrap();
        publ.wait_for_subscribers(1);

        publ.try_send("hans".as_bytes()).unwrap();

        let mut buf = [0u8; 1000];
        let (len, _) = sub.try_recv(&mut buf).unwrap();
        println!("{:?}", String::from_utf8_lossy(&buf[0..len]));
        drop(publ);
        let mut publ = Publisher::new("", "peter").unwrap();

        let mut buf = [0u8; 1000];
        assert_eq!(
            sub.try_recv(&mut buf),
            Err(ReceiveError::DisconnectDetected)
        );
        publ.wait_for_subscribers(1);
        publ.try_send("hans".as_bytes()).unwrap();
        let (len, _) = sub.try_recv(&mut buf).unwrap();
        println!("{:?}", String::from_utf8_lossy(&buf[0..len]));
    }

    /// The blocking receive parks the thread in the OS until the publisher
    /// sends in a parallel thread.
    #[test]
    fn blocking_recv_receives_message() {
        let mut sub = Subscriber::new("", "blocking_recv").unwrap();
        let mut publ = Publisher::new("", "blocking_recv").unwrap();
        publ.wait_for_subscribers(1);

        let sender = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(200));
            publ.try_send("hans".as_bytes()).unwrap();
        });

        let start = Instant::now();
        let mut buf = [0u8; 1000];
        let (len, _) = sub.recv(&mut buf).unwrap();
        assert_eq!(
            String::from_utf8_lossy(&buf[0..len]).to_string(),
            "hans".to_string()
        );
        assert!(
            start.elapsed() >= Duration::from_millis(150),
            "the blocking receive returned too early ({:?})",
            start.elapsed()
        );
        sender.join().unwrap();
    }

    /// Without a publisher there is nothing to wait on: the bounded receive
    /// reports `NotConnectedToPublisher` immediately.
    #[test]
    fn recv_without_publisher_is_immediate() {
        let mut sub = Subscriber::new("", "no_publisher").unwrap();
        let mut buf = [0u8; 8];
        assert_eq!(
            sub.recv_with_timeout(&mut buf, Duration::from_millis(5)),
            Err(ReceiveError::NotConnectedToPublisher)
        );
    }

    #[test]
    fn recv_with_timeout_returns_timeout_on_silence() {
        let mut sub = Subscriber::new("", "silent_topic").unwrap();
        let publ = Publisher::new("", "silent_topic").unwrap();
        publ.wait_for_subscribers(1);

        let start = Instant::now();
        let mut buf = [0u8; 8];
        assert_eq!(
            sub.recv_with_timeout(&mut buf, Duration::from_millis(100)),
            Err(ReceiveError::Timeout)
        );
        assert!(
            start.elapsed() >= Duration::from_millis(90),
            "the receive timeout expired too early ({:?})",
            start.elapsed()
        );
    }

    /// A sub-microsecond timeout is an immediate attempt, even though the
    /// publisher is connected: on Unix `SO_RCVTIMEO` would otherwise be
    /// configured with zero microseconds, which means an unbounded wait.
    #[test]
    fn recv_with_timeout_sub_microsecond_is_immediate() {
        let mut sub = Subscriber::new("", "submicro_topic").unwrap();
        let publ = Publisher::new("", "submicro_topic").unwrap();
        publ.wait_for_subscribers(1);

        let mut buf = [0u8; 8];
        assert_eq!(
            sub.recv_with_timeout(&mut buf, Duration::from_nanos(500)),
            Err(ReceiveError::Timeout)
        );
    }

    /// A disconnect during the blocking receive wakes the thread up with
    /// `DisconnectDetected`. A new publisher reconnects and the next
    /// blocking receive delivers again.
    #[test]
    fn blocking_recv_sees_disconnect_and_recovers() {
        let mut sub = Subscriber::new("", "flaky_topic").unwrap();
        let publ = Publisher::new("", "flaky_topic").unwrap();
        publ.wait_for_subscribers(1);

        drop(publ);
        let mut buf = [0u8; 8];
        assert_eq!(sub.recv(&mut buf), Err(ReceiveError::DisconnectDetected));

        let mut publ = Publisher::new("", "flaky_topic").unwrap();
        publ.wait_for_subscribers(1);
        let sender = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            publ.try_send("again".as_bytes()).unwrap();
        });
        let (len, _) = sub.recv(&mut buf).unwrap();
        assert_eq!(
            String::from_utf8_lossy(&buf[0..len]).to_string(),
            "again".to_string()
        );
        sender.join().unwrap();
    }
}
