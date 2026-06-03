# NovoMQ - Simple real-time messaging 

[![CI](https://github.com/novomation/novo_mq/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/novomation/novo_mq/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT_OR_Apache--2.0-blue.svg)](
https://github.com/novomation/novo_mq#license)

NovoMQ is a minimalistic, real-time-capable messaging solution designed for inter-process communication (IPC) scenarios.
The publish/subscribe pattern is currently supported. The idea behind NovoMQ is to prioritise IPC and leverage many operating system (OS) features to streamline management.

For communication, a domain is opened within which publishers and subscribers are accessible to all participants.
Publishers and subscribers are organized into hierarchical topics, and only absolute topic paths are supported.
Subscribers can connect to only one topic, and filtering and wildcards for topic paths are not supported.

## Use

```toml
# Cargo.toml
novomq = { version = "...", git = "ssh://git@github.com/novomation/novomq.git" }
```

## Rust example

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

## License

SPDX-License-Identifier: Apache-2.0 OR MIT