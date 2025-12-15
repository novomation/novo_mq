use std::path::Path;

use base64::Engine;

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
