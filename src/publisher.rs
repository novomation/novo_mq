use crate::{
    cancelation::CancelationToken,
    helpers::set_calling_thread_to_nrt,
    topic::socket_path,
    transport::{
        SendError, TransportConnection, TransportError, TransportListener,
        uds::{UnixDatagram, UnixDatagramListener},
    },
};
use log::{debug, error, info};
use std::{
    fmt::Debug,
    path::Path,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};
use waitfree_sync::spsc;

pub struct Publisher {
    connections: Vec<(UnixDatagram, usize)>,
    rx_connection: spsc::Receiver<UnixDatagram>,
    cancel_token: CancelationToken,
    number_of_subscribers: Arc<AtomicUsize>,

    topic: String,
    socket_addr: String,
    condvar: Arc<(Mutex<usize>, Condvar)>,
    join_mgmt: Option<JoinHandle<()>>,
}
impl Publisher {
    pub(crate) fn new(
        domain: impl AsRef<Path>,
        topic: impl AsRef<str>,
    ) -> Result<Self, TransportError> {
        // Create publisher socket

        let number_of_subscribers = Arc::new(AtomicUsize::new(0));
        let socket_addr = socket_path(domain, &topic);
        // A concurrent drop of another datalayer handle may have removed the
        // domain directory, so make sure it exists before binding.
        if let Some(parent) = Path::new(&socket_addr).parent() {
            if !parent.as_os_str().is_empty() {
                let _ = std::fs::create_dir_all(parent);
            }
        }
        let mut listener = UnixDatagramListener::bind(&socket_addr)
            .inspect_err(|err| error!("Could not bind {err}"))?;

        let (mut tx_connection, rx_connection) = spsc::spsc::<UnixDatagram>(64);
        let cancel_token = CancelationToken::new();

        // The counter starts at zero, which is the true initial state (no
        // subscriber is connected yet). Waiting for a management-thread
        // "started" signal on this same condvar used to deadlock: an accept
        // landing before the creating thread reached that wait raised the
        // count, and the wait for it to return to zero could never be met.
        let condvar = Arc::new((Mutex::new(0), Condvar::new()));
        let join_mgmt = std::thread::spawn({
            let cancel_token = cancel_token.clone();
            let topic = topic.as_ref().to_string();
            let condvar = condvar.clone();
            let number_of_subscribers = number_of_subscribers.clone();
            move || {
                set_calling_thread_to_nrt();
                debug!(
                    "Start mgmt thread for publisher `{topic}` ({}) and waiting for subscribers",
                    listener.local_addr().unwrap_or_default()
                );
                while !cancel_token.is_cancelled() {
                    let res = listener.accept();
                    match res {
                        Ok(connection) => {
                            info!("Got new subscriber for `{topic}`");

                            let _ = tx_connection.try_send(connection).inspect_err(|_| {
                                error!("Could not register subscriber: Queue full")
                            });
                            number_of_subscribers.fetch_add(1, Ordering::AcqRel);

                            let (lock, cvar) = &*condvar;
                            if let Ok(mut started) = lock.lock() {
                                *started = number_of_subscribers.load(Ordering::Acquire);
                                // We notify the condvar that the value has changed.
                                cvar.notify_one();
                            }
                        }
                        Err(TransportError::WouldBlock) => {
                            // Ignorw
                        }
                        Err(err) => error!("Error in accept: {err}"),
                    }

                    std::thread::sleep(Duration::from_millis(200));
                }
            }
        });

        Ok(Self {
            connections: Vec::with_capacity(64),
            rx_connection,
            cancel_token,
            topic: topic.as_ref().to_string(),
            socket_addr,
            condvar,
            number_of_subscribers,
            join_mgmt: Some(join_mgmt),
        })
    }
    /// This blocks the current thread until `number_of_subscribers` connected to this publisher
    pub fn wait_for_atleast_subscribers(&self, number_of_subscribers: usize) {
        let (num, cvar) = &*self.condvar;
        let mut num = num.lock().unwrap();

        while *num < number_of_subscribers {
            num = cvar.wait(num).unwrap();
        }
        info!("Waited and now there are at least {number_of_subscribers} subscribers");
    }
    /// This blocks the current thread until `number_of_subscribers` connected to this publisher
    pub fn wait_for_subscribers(&self, number_of_subscribers: usize) {
        let (num, cvar) = &*self.condvar;
        let mut num = num.lock().unwrap();

        while *num != number_of_subscribers {
            num = cvar.wait(num).unwrap();
        }
        info!("Waited and now there are {number_of_subscribers} subscribers");
    }

    #[cfg_attr(feature = "rtsan", rtsan_standalone::nonblocking)]
    pub fn try_send(&mut self, message: &[u8]) -> Result<(), SendError> {
        if message.is_empty() {
            return Err(SendError::ZeroBytes);
        }
        while let Some(con) = self.rx_connection.try_recv() {
            self.connections.push((con, 0));
        }
        debug!(
            "Send on {} with {} subscribers",
            self.topic,
            self.connections.len(),
        );
        let mut connections_to_remove = heapless::Vec::<usize, 20>::new();
        for (i, (con, lost_msg)) in self.connections.iter_mut().enumerate() {
            match con.send(message) {
                Ok(_) => (),
                Err(SendError::OsBufferFull) => {
                    *lost_msg += 1;
                }
                Err(SendError::ConnectionEnded) => {
                    let _ = connections_to_remove.push(i);
                }
                Err(err) => {
                    error!("conn{i} {err}");
                    return Err(err);
                }
            }
        }
        for to_remove in connections_to_remove {
            self.connections.remove(to_remove);
            self.number_of_subscribers.fetch_sub(1, Ordering::AcqRel);
        }
        Ok(())
    }
}
impl Drop for Publisher {
    fn drop(&mut self) {
        self.cancel_token.cancel();
        // Wait for the managment thread to finish so that everything is dropped.
        if let Some(join_mgmt) = self.join_mgmt.take() {
            let _ = join_mgmt.join();
        }
        // The mgmt thread's drop unlinked the socket file. If no other
        // publisher socket is left, remove the now-empty domain directory.
        if let Some(parent) = Path::new(&self.socket_addr).parent() {
            if !parent.as_os_str().is_empty() {
                let _ = std::fs::remove_dir(parent);
            }
        }
    }
}
impl Debug for Publisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Publisher")
            .field("connections", &self.connections.len())
            .field("number_of_subscribers", &self.number_of_subscribers)
            .field("topic", &self.topic)
            .finish()
    }
}

#[cfg(test)]
mod test {
    use std::fs::create_dir_all;
    use std::path::Path;

    use super::*;
    use crate::subscriber::Subscriber;
    use test_log::test;

    #[test]
    fn smoke() {
        create_dir_all("publisher_smoke").unwrap();
        let mut publ = Publisher::new("publisher_smoke", "topic").unwrap();

        publ.try_send("message".as_bytes()).unwrap();

        let sub = UnixDatagram::connect(&socket_path("publisher_smoke", "topic")).unwrap();

        publ.wait_for_subscribers(1);
        publ.try_send("message".as_bytes()).unwrap();

        drop(sub);

        publ.wait_for_subscribers(1);

        publ.try_send("iufvghj".as_bytes()).unwrap();
        publ.try_send("iufvghj".as_bytes()).unwrap();
        // Drop the publisher first: its listener owns the socket file and
        // unlinks it on drop, and the empty domain is removed with it.
        drop(publ);
        assert!(!Path::new("publisher_smoke").exists());
    }

    #[test]
    fn test_publisher_on_same_topic() {
        create_dir_all("test_publisher_on_same_topic").unwrap();
        let _publ = Publisher::new("test_publisher_on_same_topic", "topic").unwrap();

        assert_eq!(
            Publisher::new("test_publisher_on_same_topic", "topic").unwrap_err(),
            TransportError::AddressInUse
        );
        drop(_publ);
        assert!(!Path::new("test_publisher_on_same_topic").exists());
    }

    #[test]
    fn test_returning_subscriber_after_drop() {
        let domain = "reconnecting_subscriber";
        let _ = std::fs::remove_dir_all(domain);
        create_dir_all(domain).unwrap();
        let mut publ = Publisher::new(domain, "topic").unwrap();

        let sub = Subscriber::new(domain, "topic").unwrap();
        sub.wait_until_connected();
        publ.wait_for_subscribers(1);
        drop(sub);

        // Flush the dead connection: this is what prunes it (and used to
        // delete the listener file).
        for _ in 0..20 {
            let _ = publ.try_send(b"flush");
            if !Path::new(&socket_path(domain, "topic")).exists() {
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        assert!(
            Path::new(&socket_path(domain, "topic")).exists(),
            "the publisher listener file must survive the subscriber drop"
        );

        // A new subscriber must be able to connect again and receive.
        let mut sub = Subscriber::new(domain, "topic").unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !sub.has_connection() {
            assert!(
                std::time::Instant::now() < deadline,
                "returning subscriber never reconnected"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        let mut buf = [0u8; 64];
        loop {
            publ.try_send(b"hello again").unwrap();
            if let Ok((len, _)) = sub.try_recv(&mut buf) {
                assert_eq!(&buf[..len], b"hello again");
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "returning subscriber never received a message"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        drop(sub);
        drop(publ);
        assert!(
            !Path::new(domain).exists(),
            "empty domain must be cleaned up when the last publisher is dropped"
        );
    }

    #[test]
    fn subscriber_connecting_before_publisher_is_ready() {
        let domain = "early_subscriber";
        let _ = std::fs::remove_dir_all(domain);
        create_dir_all(domain).unwrap();
        // The subscriber keeps retrying until the listener exists:
        let mut sub = Subscriber::new(domain, "topic").unwrap();
        let mut publ = Publisher::new(domain, "topic").unwrap();

        sub.wait_until_connected();
        // The accept may have happened while `Publisher::new` was still
        // setting up; the wait must observe it either way:
        publ.wait_for_subscribers(1);
        publ.try_send(b"hello").unwrap();
        let mut buf = [0u8; 64];
        let (len, _) = sub.try_recv(&mut buf).unwrap();
        assert_eq!(&buf[..len], b"hello");
        drop(sub);
        drop(publ);
        assert!(!Path::new(domain).exists());
    }
}
