use rt_datalayer::datalayer::Datalayer;

#[test]
fn test_nonblocking() {
    let tx = std::thread::spawn(|| {
        let dl = Datalayer::open("test_domain_nonblocking").unwrap();
        let mut pl = dl.publisher("my_topic").unwrap();
        pl.wait_for_subscribers(1);
        pl.try_send("my_message".as_bytes()).unwrap();
    });
    let rx = std::thread::spawn(|| {
        let dl = Datalayer::open("test_domain_nonblocking").unwrap();
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
}
