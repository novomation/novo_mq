use novomq::ReceiveError;
use novomq::datalayer::Datalayer;
use std::time::Duration;
use test_log::test;

/// The blocking receive parks the subscriber thread in the OS until a
/// message arrives: the publisher sends on a parallel thread.
#[test]
fn blocking_recv_of_one_message() {
    let tx = std::thread::spawn(|| {
        let dl = Datalayer::open("test_blocking").unwrap();
        let mut pl = dl.publisher("my_topic").unwrap();
        pl.wait_for_subscribers(1);
        pl.try_send("my_message".as_bytes()).unwrap();
        // Keep the publisher alive until the subscriber read the message, so
        // the blocking receive observes the message and not a disconnect.
        std::thread::sleep(Duration::from_millis(300));
    });
    let rx = std::thread::spawn(|| {
        let dl = Datalayer::open("test_blocking").unwrap();
        let mut sub = dl.subscriber("my_topic").unwrap();
        sub.wait_until_connected();
        let mut buf = [0u8; 500];
        let (len, _) = sub.recv(&mut buf).unwrap();
        assert_eq!(
            String::from_utf8_lossy(&buf[0..len]).to_string(),
            "my_message".to_string()
        );
    });
    tx.join().unwrap();
    rx.join().unwrap();
}

/// The bounded blocking receive reports a timeout while the connected
/// publisher stays silent.
#[test]
fn recv_with_timeout_times_out_without_message() {
    let tx = std::thread::spawn(|| {
        let dl = Datalayer::open("test_blocking_timeout").unwrap();
        let pl = dl.publisher("my_topic").unwrap();
        pl.wait_for_subscribers(1);
        std::thread::sleep(Duration::from_secs(1));
    });
    let rx = std::thread::spawn(|| {
        let dl = Datalayer::open("test_blocking_timeout").unwrap();
        let mut sub = dl.subscriber("my_topic").unwrap();
        sub.wait_until_connected();
        let mut buf = [0u8; 500];
        assert_eq!(
            sub.recv_with_timeout(&mut buf, Duration::from_millis(100)),
            Err(ReceiveError::Timeout)
        );
    });
    tx.join().unwrap();
    rx.join().unwrap();
}
