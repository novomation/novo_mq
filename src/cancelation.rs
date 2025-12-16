use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

#[derive(Debug, Clone)]
pub struct CancelationToken {
    token: Arc<AtomicBool>,
}

impl CancelationToken {
    pub fn new() -> Self {
        Self {
            token: Arc::new(AtomicBool::new(false)),
        }
    }
    pub fn cancel(&self) {
        self.token.store(true, Ordering::Relaxed);
    }
    pub fn is_cancelled(&self) -> bool {
        self.token.load(Ordering::Relaxed)
    }
}
impl Drop for CancelationToken {
    fn drop(&mut self) {
        self.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smoke() {
        let ct = CancelationToken::new();
        assert!(!ct.is_cancelled());
        ct.cancel();
        assert!(ct.is_cancelled());
    }
    #[test]
    fn test_threads() {
        let ct = CancelationToken::new();
        std::thread::spawn({
            let ct = ct.clone();
            move || {
                ct.cancel();
            }
        })
        .join()
        .unwrap();
        assert!(ct.is_cancelled());
    }
    #[test]
    fn test_drop() {
        let ct = CancelationToken::new();
        std::thread::spawn({
            let ct = ct.clone();
            move || {
                assert!(!ct.is_cancelled());
            }
        })
        .join()
        .unwrap();
        assert!(ct.is_cancelled());
    }
}
