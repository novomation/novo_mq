use base64::Engine;

pub fn encode(topic: &str) -> String {
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    b64.encode(topic)
}
