pub use crate::transport::BulkTransport;
use crate::{TransportError, publisher::Publisher, subscriber::Subscriber};
use log::{debug, warn};
use std::path::{Path, PathBuf};

pub struct Datalayer<T: BulkTransport + 'static = ()> {
    domain_path: PathBuf,
    _phantom: std::marker::PhantomData<T>,
}
impl<T: BulkTransport + 'static> Datalayer<T> {
    /// Opens a new datalayer. You can have multiple datalayers. So we have to define the domain. This is basically a file system path
    pub fn open(domain: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        std::fs::create_dir_all(&domain)?;

        Ok(Self {
            domain_path: domain.as_ref().to_path_buf(),
            _phantom: std::marker::PhantomData,
        })
    }

    pub fn subscriber(
        &self,
        topic: impl AsRef<str>,
        config: T::Config,
    ) -> Result<Subscriber<T>, TransportError>
    where
        T::Config: Sync + Send + Clone + 'static,
    {
        Subscriber::<T>::new(&self.domain_path, topic, config)
    }

    pub fn publisher(&self, topic: impl AsRef<str>) -> Result<Publisher<T>, TransportError> {
        Publisher::<T>::new(&self.domain_path, topic)
    }
}

impl<T: BulkTransport + 'static> Drop for Datalayer<T> {
    fn drop(&mut self) {
        debug!("Try remove {:?}", self.domain_path);
        // Try to remove directory only if it's empty
        match std::fs::remove_dir(&self.domain_path) {
            Ok(_) => (),
            Err(e) => {
                // Ignore "not empty" errors, log others if needed
                if e.kind() != std::io::ErrorKind::DirectoryNotEmpty {
                    warn!(
                        "Failed to cleanup domain directory `{:?}``: {}",
                        self.domain_path, e
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use test_log::test;

    #[test]
    fn it_works() {
        let tx = std::thread::spawn(|| {
            let dl: Datalayer = Datalayer::open("test_domain").unwrap();
            let mut pl = dl.publisher("my_topic").unwrap();
            pl.wait_for_subscribers(1);
            pl.try_send("my_message".as_bytes()).unwrap();
        });
        let rx = std::thread::spawn(|| {
            let dl: Datalayer = Datalayer::open("test_domain").unwrap();

            let mut sub = dl.subscriber("my_topic", ()).unwrap();
            sub.wait_until_connected();
            std::thread::sleep(Duration::from_millis(1000));

            let mut buf = [0u8; 500];
            loop {
                if let Ok((len, _)) = sub.try_recv(&mut buf) {
                    println!("{:?}", String::from_utf8_lossy(&buf[0..len]));
                    break;
                } else {
                    println!("Waiting for message.");
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        });
        tx.join().unwrap();
        rx.join().unwrap();
        // Check drop cleaned up!
        assert!(!Path::new("test_domain").exists());
    }
}

#[cfg(not(target_os = "macos"))]
#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    use crate::transport::tb::{ByteInit, TbTransport, TbTransportConfig};

    use super::*;
    use test_log::test;

    #[test]
    fn it_works_tb() {
        let wait = Arc::new(AtomicBool::new(true));
        let tx = std::thread::spawn({
            let wait = wait.clone();
            move || {
                let dl: Datalayer<TbTransport<[u8; 30]>> = Datalayer::open("test_domain1").unwrap();
                let mut pl = dl.publisher("my_topic").unwrap();
                pl.wait_for_subscribers(1);
                let ret = pl.try_send("my_message".as_bytes());
                log::info!("Send worked: ${:?}", ret);
                while wait.load(Ordering::Relaxed) {
                    std::thread::sleep(Duration::from_millis(1000));
                }
            }
        });
        let rx = std::thread::spawn(move || {
            let dl: Datalayer<TbTransport<[u8; 30]>> = Datalayer::open("test_domain1").unwrap();
            let mut sub = dl.subscriber("my_topic", TbTransportConfig::new()).unwrap();
            std::thread::sleep(Duration::from_millis(1000));
            sub.wait_until_connected();
            let mut buf = [0u8; 500];
            loop {
                if let Ok((len, _)) = sub.try_recv(&mut buf) {
                    println!("{:?}", String::from_utf8_lossy(&buf[0..len]));
                    wait.store(false, Ordering::Relaxed);
                    break;
                } else {
                    println!("Waiting for message.");
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        });
        tx.join().unwrap();
        rx.join().unwrap();
        // Check drop cleaned up!
        assert!(!Path::new("test_domain1").exists());
    }
}
