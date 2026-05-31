use crate::{publisher::Publisher, subscriber::Subscriber, transport::TransportError};
use log::{debug, warn};
use std::path::{Path, PathBuf};

pub struct Datalayer {
    domain_path: PathBuf,
}
impl Datalayer {
    /// Opens a new datalayer. You can have multiple datalayers. So we have to define the domain. This is basically a file system path
    pub fn open(domain: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        std::fs::create_dir_all(&domain)?;

        Ok(Self {
            domain_path: domain.as_ref().to_path_buf(),
        })
    }

    pub fn subscriber(&self, topic: impl AsRef<str>) -> Result<Subscriber, TransportError> {
        Subscriber::new(&self.domain_path, topic)
    }

    pub fn publisher(&self, topic: impl AsRef<str>) -> Result<Publisher, TransportError> {
        Publisher::new(&self.domain_path, topic)
    }
}

impl Drop for Datalayer {
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
    use super::*;
    use test_log::test;

    #[test]
    fn it_works() {
        let tx = std::thread::spawn(|| {
            let dl = Datalayer::open("test_domain").unwrap();
            let mut pl = dl.publisher("my_topic").unwrap();
            pl.wait_for_subscribers(1);
            pl.try_send("my_message".as_bytes()).unwrap();
        });
        let rx = std::thread::spawn(|| {
            let dl = Datalayer::open("test_domain").unwrap();
            let mut sub = dl.subscriber("my_topic").unwrap();
            sub.wait_until_connected();
            let mut buf = [0u8; 500];
            loop {
                if let Ok((len, _)) = sub.try_recv(&mut buf) {
                    println!("{:?}", String::from_utf8_lossy(&buf[0..len]));
                    break;
                }
            }
        });
        tx.join().unwrap();
        rx.join().unwrap();
        // Check drop cleaned up!
        assert!(!Path::new("test_domain").exists());
    }
}
