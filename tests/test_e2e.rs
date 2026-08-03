use novomq::datalayer::Datalayer;
use std::time::Duration;
use test_log::test;

#[test]
fn test_reconnect_of_subscriber() {
    let tx = std::thread::spawn(|| {
        let dl: Datalayer = Datalayer::open("test_reconnect_of_subscriber").unwrap();
        let mut pl = dl.publisher("my_topic").unwrap();
        pl.wait_for_subscribers(1);
        for _i in 0..10 {
            pl.try_send("my_message".as_bytes()).unwrap();
            std::thread::sleep(Duration::from_secs(1));
        }
    });
    let rx = std::thread::spawn(|| {
        let dl: Datalayer = Datalayer::open("test_reconnect_of_subscriber").unwrap();
        {
            let mut sub = dl.subscriber("my_topic", ()).unwrap();
            sub.wait_until_connected();
            let mut buf = [0u8; 500];
            loop {
                if let Ok((len, _)) = sub.try_recv(&mut buf) {
                    println!("{:?}", String::from_utf8_lossy(&buf[0..len]));
                    break;
                }
            }
        }
        {
            let mut sub = dl.subscriber("my_topic", ()).unwrap();
            sub.wait_until_connected();
            let mut buf = [0u8; 500];
            loop {
                if let Ok((len, _)) = sub.try_recv(&mut buf) {
                    println!("{:?}", String::from_utf8_lossy(&buf[0..len]));
                    break;
                }
            }
        }
    });
    tx.join().unwrap();
    rx.join().unwrap();
}
