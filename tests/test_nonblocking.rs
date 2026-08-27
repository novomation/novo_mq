use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use novomq::datalayer::Datalayer;

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[test]
fn test_nonblocking() {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();
    let tx = std::thread::spawn(|| {
        let dl: Datalayer = Datalayer::open("test_domain_nonblocking").unwrap();
        let mut pl = dl.publisher("my_topic").unwrap();
        pl.wait_for_subscribers(1);
        pl.try_send("my_message".as_bytes()).unwrap();
    });
    let rx = std::thread::spawn(|| {
        let dl: Datalayer = Datalayer::open("test_domain_nonblocking").unwrap();
        let mut sub = dl.subscriber("my_topic", ()).unwrap();
        std::thread::sleep(Duration::from_millis(100));
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
}

#[cfg(not(target_os = "macos"))]
#[test]
fn test_nonblocking_tb() {
    use novomq::datalayer::{Datalayer, TbTransport, TbTransportConfig};
    let wait = Arc::new(AtomicBool::new(true));
    let tx = std::thread::spawn({
        let wait = wait.clone();
        move || {
            let dl: Datalayer<TbTransport<[u8; 16]>> =
                Datalayer::open("test_domain_nonblocking1").unwrap();
            let mut pl = dl.publisher("my_topic").unwrap();
            pl.wait_for_subscribers(1);
            pl.try_send("my_message".as_bytes()).unwrap();
            while wait.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    });
    let rx = std::thread::spawn(move || {
        let dl: Datalayer<TbTransport<[u8; 16]>> =
            Datalayer::open("test_domain_nonblocking1").unwrap();
        let mut sub = dl.subscriber("my_topic", TbTransportConfig::new()).unwrap();
        std::thread::sleep(Duration::from_millis(100));
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
}
