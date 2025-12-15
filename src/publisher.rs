use log::{debug, error, info};
use nix::errno::Errno;
use std::{
    path::Path,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};
use waitfree_sync::spsc;

use crate::{
    topic::{encode, socket_path},
    transport::{
        self, ManagedTransport, SendError, TransportError, TransportManaging, TransportMessaging,
    },
};

pub struct Publisher {
    connections: Vec<(Box<dyn TransportMessaging + Send>, usize)>,
    rx_connection: spsc::Receiver<Box<dyn TransportMessaging + Send>>,
    local_stop_flag: Arc<AtomicBool>,
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
        let (mut uds, _) = transport::uds::Uds::new()
            .inspect_err(|err| error!("Could not create uds {err}"))?
            .into_managed();
        let number_of_subscribers = Arc::new(AtomicUsize::new(0));

        let socket_addr = socket_path(domain, &topic);
        uds.bind(&socket_addr)
            .inspect_err(|err| error!("Could not bind {err}"))?;
        let (mut tx_connection, rx_connection) =
            spsc::spsc::<Box<dyn TransportMessaging + Send>>(64);
        let local_stop_flag: Arc<AtomicBool> = Arc::new(false.into());

        let condvar = Arc::new((Mutex::new(usize::MAX), Condvar::new()));
        let join_mgmt = std::thread::spawn({
            let local_stop_flag = local_stop_flag.clone();
            let topic = topic.as_ref().to_string();
            let condvar = condvar.clone();
            let number_of_subscribers = number_of_subscribers.clone();

            move || {
                debug!("Start publisher `{topic}` mgmt thread and waiting for subscribers");
                if let Ok(mut c) = condvar.0.lock() {
                    *c = 0;
                }
                condvar.1.notify_one();
                while !local_stop_flag.load(Ordering::Relaxed) {
                    let res = uds.accept().inspect_err(|err| match err {
                        TransportError(Errno::EAGAIN) => (), // We ignore eagain. It's not an error. It just signals that nothing happend in non blocking mode.
                        TransportError(_) => error!("Error in accept: {err}"),
                        // TransportError::IoError(_) => error!("Error in accept: {err}"),
                    });
                    if let Ok(connection) = res {
                        info!("Got new subscriber for `{topic}`");
                        let _ = tx_connection
                            .try_send(Box::new(connection))
                            .inspect_err(|_| error!("Could not register subscriber: Queue full"));
                        number_of_subscribers.fetch_add(1, Ordering::AcqRel);

                        let (lock, cvar) = &*condvar;
                        if let Ok(mut started) = lock.lock() {
                            *started = number_of_subscribers.load(Ordering::Acquire);
                            // We notify the condvar that the value has changed.
                            cvar.notify_one();
                        }
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
            local_stop_flag,
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
            self.connections.len()
        );
        let mut connections_to_remove = heapless::Vec::<usize, 20>::new();
        for (i, (con, lost_msg)) in self.connections.iter_mut().enumerate() {
            match con.send(message) {
                Ok(_) => (),
                Err(SendError::OsError(Errno::EAGAIN)) => {
                    *lost_msg += 1;
                }
                Err(SendError::OsError(Errno::ECONNRESET)) => {
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
        self.local_stop_flag.store(true, Ordering::Relaxed);
        if let Some(join_mgmt) = self.join_mgmt.take() {
            let _ = join_mgmt.join();
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use test_log::test;

    #[test]
    fn smoke() {
        let mut publ = Publisher::new("", "topic").unwrap();

        publ.try_send("message".as_bytes()).unwrap();

        let (mut test_sub, sub) = transport::uds::Uds::new().unwrap().into_managed();
        test_sub.connect(encode(&publ.topic).as_str()).unwrap();
        publ.wait_for_subscribers(1);
        publ.try_send("message".as_bytes()).unwrap();

        drop(sub);
        drop(test_sub);
        publ.wait_for_subscribers(1);

        publ.try_send("iufvghj".as_bytes()).unwrap();
        publ.try_send("iufvghj".as_bytes()).unwrap();
    }
}
