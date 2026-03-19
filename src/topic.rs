//! The pub/sub connections are matched via topcis. The sockets for the pub/sub
//! connections are placed hierarchical in the file system.
//! The [Topic] struct transforms a topic string into a hierachical path and can create
//! the correspoding folders a names. We use base64 encoding to represent utf8 topic path parts
//! in the file system.
use base64::Engine;
use std::path::Path;

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
