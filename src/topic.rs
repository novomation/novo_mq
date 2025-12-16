//! The pub/sub connections are matched via topcis. The sockets for the pub/sub
//! connections are placed hierarchical in the file system.
//! The [Topic] struct transforms a topic string into a hierachical path and can create
//! the correspoding folders a names. We use base64 encoding to represent utf8 topic path parts
//! in the file system.
use base64::Engine;
use std::{
    path::{Path, PathBuf},
    str::FromStr,
};
use thiserror::Error;

pub fn encode(topic: &str) -> String {
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    b64.encode(topic)
}

pub fn socket_path(domain: impl AsRef<Path>, topic: impl AsRef<str>) -> String {
    domain
        .as_ref()
        .to_path_buf()
        .join(encode(topic.as_ref()))
        .to_str()
        .unwrap_or_default()
        .to_string()
}

#[derive(Error, Debug, PartialEq)]
#[error("Topci part is empty")]
pub struct InvalidTopic {}

#[derive(Debug, PartialEq)]
pub struct Topic {
    parts: Vec<String>,
}
impl Topic {
    pub fn new(topic: impl AsRef<str>) -> Result<Self, InvalidTopic> {
        let splitted = topic
            .as_ref()
            .split("/")
            .map(|part| part.to_string())
            .collect::<Vec<_>>();
        println!("{splitted:?}");
        if splitted.iter().find(|part| part.is_empty()).is_some() {
            return Err(InvalidTopic {});
        }
        Ok(Topic { parts: splitted })
    }

    fn encoded(&self) -> Vec<String> {
        self.parts.iter().map(|part| encode(part)).collect()
    }

    pub fn create_dirs(&self) -> Result<(), std::io::Error> {
        let mut path_parts = self.encoded();
        path_parts.pop();
        std::fs::create_dir_all(path_parts.join("/"))
    }

    pub fn to_socket_name(&self) -> Option<String> {
        self.encoded().last().map(|e| e.to_string())
    }

    pub fn to_path(&self) -> PathBuf {
        PathBuf::from_str(self.encoded().join("/").as_str()).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use crate::topic::{InvalidTopic, Topic, encode};
    use std::{path::PathBuf, str::FromStr};

    #[test]
    fn test_topic() {
        assert_eq!(
            Topic::new("marc"),
            Ok(Topic {
                parts: vec!["marc".to_string()]
            })
        );

        assert_eq!(Topic::new("marc/"), Err(InvalidTopic {}));

        assert_eq!(
            Topic::new("marc/0987hj)(="),
            Ok(Topic {
                parts: vec!["marc".to_string(), "0987hj)(=".to_string()]
            })
        );
        let topic = Topic::new("marc/0987hj)(=").unwrap();
        topic.create_dirs().unwrap();
        assert_eq!(topic.to_socket_name().unwrap(), encode("0987hj)(="));
        assert_eq!(
            topic.to_path(),
            PathBuf::from_str(&format!("{}/{}", encode("marc"), encode("0987hj)(="))).unwrap()
        );
    }
}
