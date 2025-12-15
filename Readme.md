# rt-datalayer

This is a minimalist, real-time-capable data layer. The publish/subscribe pattern is currently supported.
We open a datalayer domain by creating a folder. The endpoints then create sockets inside this folder and connect to them.
The connection between the endpoints is automatically managed in the background.


## Use

```toml
# Cargo.toml
rt-datalayer = { version = "0.1.0", git = "ssh://git@github.com/novomation/rt-datalayer.git" }
```

## Example

```rust
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
```

