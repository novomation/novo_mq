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
        let mut listener = UnixDatagramListener::bind(&socket_addr)
            .inspect_err(|err| error!("Could not bind {err}"))?;

        let (mut tx_connection, rx_connection) = spsc::spsc::<UnixDatagram>(64);
        let cancel_token = CancelationToken::new();

        let condvar = Arc::new((Mutex::new(usize::MAX), Condvar::new()));
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
                if let Ok(mut c) = condvar.0.lock() {
                    *c = 0;
                }
                condvar.1.notify_one();
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

        let (num, cvar) = &*condvar.clone();
        let mut num = num.lock().unwrap();

        while *num != 0_usize {
            num = cvar.wait(num).unwrap();
        }

        Ok(Self {
            connections: Vec::with_capacity(64),
            rx_connection,
            cancel_token,
            topic: topic.as_ref().to_string(),
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
                // Err(SendError::OsError(Errno::ECONNRESET))
                // | Err(SendError::OsError(Errno::EPIPE)) => {
                //     let _ = connections_to_remove.push(i);
                // }
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
    }
}
impl Debug for Publisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Publisher")
            .field("connections", &self.connections.len())
            // .field("rx_connection", &self.rx_connection)
            // .field("cancel_token", &self.cancel_token)
            .field("number_of_subscribers", &self.number_of_subscribers)
            .field("topic", &self.topic)
            // .field("condvar", &self.condvar)
            // .field("join_mgmt", &self.join_mgmt)
            .finish()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::topic::encode;
    use test_log::test;

    #[test]
    fn smoke() {
        let mut publ = Publisher::new("", "topic").unwrap();

        publ.try_send("message".as_bytes()).unwrap();

        let sub = UnixDatagram::<()>::connect(encode(&publ.topic).as_str(), ()).unwrap();

        publ.wait_for_subscribers(1);
        publ.try_send("message".as_bytes()).unwrap();

        drop(sub);

        publ.wait_for_subscribers(1);

        publ.try_send("iufvghj".as_bytes()).unwrap();
        publ.try_send("iufvghj".as_bytes()).unwrap();
    }
}
