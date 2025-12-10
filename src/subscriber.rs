use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use waitfree_sync::spsc::spsc;

use crate::{
    topic::encode,
    transport::{self, ManagedTransport, TransportError, TransportManaging},
};

pub struct Subscriber {
    local_stop_flag: Arc<AtomicBool>,
    has_connection: Arc<AtomicBool>,
}
impl Subscriber {
    pub fn new(topic: impl AsRef<str>) -> Result<Self, TransportError> {
        let (mut uds, uds_tx) = transport::uds::Uds::new()?.into_managed();
        let local_stop_flag: Arc<AtomicBool> = Arc::new(false.into());
        let has_connection: Arc<AtomicBool> = Arc::new(false.into());

        let publ_addr = encode(topic.as_ref());

        std::thread::spawn({
            let local_stop_flag = local_stop_flag.clone();
            let has_connection = has_connection.clone();
            move || {
                while !local_stop_flag.load(Ordering::Relaxed) {
                    if !has_connection.load(Ordering::Acquire) {
                        if let Ok(()) = uds.connect(&publ_addr) {
                            has_connection.store(true, Ordering::Release);
                        }
                    } else {
                        std::thread::sleep(Duration::from_secs(1));
                    }
                }
            }
        });
        Ok(Self {
            local_stop_flag,
            has_connection,
        })
    }
    pub fn try_recv() {}
}
impl Drop for Subscriber {
    fn drop(&mut self) {
        self.local_stop_flag.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::Duration;
    use test_log::test;

    #[test]
    fn smoke() {
        let sub = Subscriber::new("peter").unwrap();
    }
}
