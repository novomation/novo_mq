use crate::{publisher::Publisher, subscriber::Subscriber, transport::TransportError};
use log::debug;
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
        // Best-effort cleanup for every handle. `remove_dir` only removes
        // *empty* directories, so a domain that still contains a publisher's
        // socket file is never touched, no matter which handle is dropped.
        // Whichever drop runs after the last socket file was unlinked removes
        // the then-empty directory, so no domain is left behind.
        if std::fs::remove_dir(&self.domain_path).is_ok() {
            debug!("Removed domain directory {:?}", self.domain_path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn it_works() {
        let _ = std::fs::remove_dir_all("test_domain");
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

    /// Dropping datalayer handles must never remove a domain that still holds
    /// a publisher's socket file, and once the last publisher is dropped the
    /// empty domain directory is cleaned up by the next drop.
    #[test]
    fn shared_domain_survives_second_datalayer_drop() {
        let domain = "shared_domain_test";
        let _ = std::fs::remove_dir_all(domain);
        let creator = Datalayer::open(domain).unwrap();
        let other = Datalayer::open(domain).unwrap();
        let publ = other.publisher("topic").unwrap();

        // A live publisher's domain survives dropping every datalayer handle:
        drop(creator);
        assert!(
            Path::new(domain).exists(),
            "domain of a live publisher must survive handle drops"
        );
        drop(other);
        assert!(Path::new(domain).exists());

        // Dropping the last publisher cleans up the empty domain:
        drop(publ);
        assert!(
            !Path::new(domain).exists(),
            "empty domain must be cleaned up when the last publisher is dropped"
        );
    }

    /// A publisher created on a datalayer whose (then empty) domain directory
    /// was already removed by a concurrent cleanup must still work:
    /// `Publisher::new` recreates the domain directory before binding.
    #[test]
    fn publisher_recreates_cleaned_up_domain() {
        let domain = "recreate_domain_test";
        let _ = std::fs::remove_dir_all(domain);
        let dl = Datalayer::open(domain).unwrap();
        let first = dl.publisher("first").unwrap();
        drop(first);
        assert!(!Path::new(domain).exists());
        // A new publisher must work even though the domain directory is gone:
        let second = dl.publisher("second").unwrap();
        drop(second);
        let _ = std::fs::remove_dir_all(domain);
    }
}
