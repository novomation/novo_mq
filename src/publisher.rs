use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use base64::Engine;
use log::{debug, error, info};
use waitfree_sync::spsc;

use crate::{
    topic::encode,
    transport::{self, ManagedTransport, TransportError, TransportManaging, TransportMessaging},
};

pub struct Publisher {
    connections: Vec<Box<dyn TransportMessaging + Send>>,
    rx_connection: spsc::Receiver<Box<dyn TransportMessaging + Send>>,
    local_stop_flag: Arc<AtomicBool>,
    topic: String,
}
impl Publisher {
    pub fn new(topic: impl AsRef<str>) -> Result<Self, TransportError> {
        let (mut uds, _) = transport::uds::Uds::new()?.into_managed();

        let socket_addr = encode(topic.as_ref());
        uds.bind(&socket_addr)?;
        let (mut tx_connection, rx_connection) =
            spsc::spsc::<Box<dyn TransportMessaging + Send>>(64);
        let local_stop_flag: Arc<AtomicBool> = Arc::new(false.into());
        let (tx_sig, rx_sig) = std::sync::mpsc::channel();
        std::thread::spawn({
            let local_stop_flag = local_stop_flag.clone();
            let topic = topic.as_ref().to_string();
            move || {
                debug!("Start publisher `{topic}` mgmt thread");
                let _ = tx_sig.send(());
                while !local_stop_flag.load(Ordering::Relaxed) {
                    let res = uds.accept();
                    if let Ok(connection) = res {
                        info!("Got new subscriber for `{topic}`");
                        let _ = tx_connection
                            .try_send(Box::new(connection))
                            .inspect_err(|_| error!("Could not register subscriber: Queue full"));
                    }
                }
            }
        });
        let _ = rx_sig.recv();
        Ok(Self {
            connections: Vec::with_capacity(64),
            rx_connection,
            local_stop_flag,
            topic: topic.as_ref().to_string(),
        })
    }
    pub fn try_send(&mut self, message: &[u8]) -> Result<(), TransportError> {
        while let Some(con) = self.rx_connection.try_recv() {
            self.connections.push(con);
        }
        debug!(
            "Send on {} with {} subscribers",
            self.topic,
            self.connections.len()
        );
        for con in &self.connections {
            con.send(message)?;
        }
        Ok(())
    }
}
impl Drop for Publisher {
    fn drop(&mut self) {
        self.local_stop_flag.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::Duration;
    use test_log::test;

    #[test]
    fn smoke() {
        let mut publ = Publisher::new("topic").unwrap();

        publ.try_send("message".as_bytes()).unwrap();

        let (mut test_sub, sub) = transport::uds::Uds::new().unwrap().into_managed();
        test_sub.connect(encode(&publ.topic).as_str()).unwrap();
        publ.try_send("message".as_bytes()).unwrap();
    }
}
