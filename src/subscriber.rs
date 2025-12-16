use crate::{
    topic::{encode, socket_path},
    transport::{
        self, ManagedTransport, ReceiveError, TransportError, TransportManaging, TransportMessaging,
    },
};
use log::{debug, error, info, trace, warn};
use nix::errno::Errno;
use std::{
    array::from_ref, path::Path, sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    }, time::Duration
};
use waitfree_sync::spsc;

pub struct Subscriber {
    local_stop_flag: Arc<AtomicBool>,
    has_connection: Arc<AtomicBool>,
    has_error: Arc<AtomicBool>,
    socket: Option<Box<dyn TransportMessaging + Send>>,
    rx_new_socket: spsc::Receiver<Box<dyn TransportMessaging + Send>>,
    topic: String,
}
impl Subscriber {
    pub(crate) fn new(
        domain: impl AsRef<Path>,
        topic: impl AsRef<str>,
        cpu_set: linux_rt::CpuSet,
    ) -> Result<Self, TransportError> {
        if let Err(e) = linux_rt::sched::set_affinity(linux_rt::sched::Pid::this(), cpu_set.clone()) {
            error!("Error setting affinity of managment to {:?}. Errno: {}", cpu_set, e);
        }
        if let Err(e) = linux_rt::sched::set_other(linux_rt::sched::Pid::this(), 0) {
            error!("Error scheduling policy of managment to sched_other. Errno: {}", e);
        }
        let effective_affinity = linux_rt::sched::get_affinity(linux_rt::sched::Pid::this()).unwrap();
        info!("Publisher management affinity: {:?}", effective_affinity);
        let local_stop_flag: Arc<AtomicBool> = Arc::new(false.into());
        let has_connection: Arc<AtomicBool> = Arc::new(false.into());
        let has_error: Arc<AtomicBool> = Arc::new(false.into());
        let publ_path = socket_path(domain, &topic);

        let (mut tx_new_socket, rx_new_socket) =
            spsc::spsc::<Box<dyn TransportMessaging + Send>>(64);

        std::thread::spawn({
            let local_stop_flag = local_stop_flag.clone();
            let has_connection = has_connection.clone();
            let has_error = has_error.clone();

            let topic = topic.as_ref().to_string();
            move || {
                debug!("Start subscriber `{topic}` mgmt thread and try to connect");
                trace!(
                    "local_stop_flag={}",
                    local_stop_flag.load(Ordering::Relaxed)
                );
                while !local_stop_flag.load(Ordering::Relaxed) {
                    if !has_connection.load(Ordering::Acquire) {
                        // info!("Try to connect subscriber to `{topic}`");
                        let transport = match transport::uds::Uds::new() {
                            Ok(transport) => transport,
                            Err(err) => {
                                error!("Could not create subscriber socket {err}");
                                has_error.store(true, Ordering::Relaxed);
                                return Err(err);
                            }
                        };
                        let (mut uds, uds_tx) = transport.into_managed();
                        match uds.connect(&publ_path) {
                            Ok(_) => {
                                has_connection.store(true, Ordering::Release);
                                let _ =
                                    tx_new_socket.try_send(Box::new(uds_tx)).inspect_err(|_| {
                                        warn!("Could not save subscriber socket -> Try again")
                                    });
                                info!("Now connected to {topic}");
                            }
                            Err(TransportError(Errno::ENOENT)) => (),
                            Err(err) => {
                                error!("Could not connect {err}");
                            }
                        }
                    } else {
                        std::thread::sleep(Duration::from_secs(1));
                    }
                }
                Ok(())
            }
        });
        Ok(Self {
            local_stop_flag,
            has_connection,
            has_error,
            socket: None,
            rx_new_socket,
            topic: topic.as_ref().to_string()
        })
    }
    pub fn try_recv(&mut self, message: &mut [u8]) -> Result<(usize, usize), ReceiveError> {
        if self.socket.is_none() {
            self.socket = self.rx_new_socket.try_recv();
        }
        if let Some(socket) = &self.socket {
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
            self.socket.as_ref().map(|s| s.addr())
        );
        self.local_stop_flag.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod test {
    use crate::publisher::Publisher;

    use super::*;
    use std::time::Duration;
    use test_log::test;

    #[test]
    fn smoke() {
        let cpu_set = linux_rt::sched::get_affinity(linux_rt::sched::Pid::this()).unwrap();
        let mut sub = Subscriber::new("", "peter", cpu_set.clone()).unwrap();
        let mut publ = Publisher::new("", "peter", cpu_set.clone()).unwrap();
        publ.wait_for_subscribers(1);

        publ.try_send("hans".as_bytes()).unwrap();

        let mut buf = [0u8; 1000];
        let (len, all) = sub.try_recv(&mut buf).unwrap();
        println!("{:?}", String::from_utf8_lossy(&buf[0..len]));
        drop(publ);
        let mut publ = Publisher::new("", "peter", cpu_set).unwrap();

        let mut buf = [0u8; 1000];
        assert_eq!(
            sub.try_recv(&mut buf),
            Err(ReceiveError::DisconnectDetected)
        );
        publ.wait_for_subscribers(1);
        publ.try_send("hans".as_bytes()).unwrap();
        let (len, all) = sub.try_recv(&mut buf).unwrap();
        println!("{:?}", String::from_utf8_lossy(&buf[0..len]));
    }
}
