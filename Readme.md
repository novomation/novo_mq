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
use novomq::datalayer::Datalayer;

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

## Transports

A **domain** is the entry point (`Datalayer::open`). Everything inside it runs over a
Unix-socket connection, and the *behaviour* of that connection — how a message
travels from publisher to subscriber — is chosen by the **transport** type you
parameterise the `Datalayer` with:

```rust
let dl: Datalayer<MyTransport> = Datalayer::open("domain").unwrap();
```

`publisher(topic)` / `subscriber(topic, config)` are otherwise identical across
all transports. The type parameter (`T`) must be a fixed-size, `Send + Sync`
byte buffer (e.g. `[u8; N]` or a `repr(C, packed)` struct). Both ends **must
agree on the same payload type** — a size mismatch is detected during the
connect handshake and surfaced as `TransportError::BufferSizeMismatch`.

| Transport | What it is | When to use |
|-----------|------------|-------------|
| `()` *(UDS)* | Plain Unix-socket datagrams | Small messages, simplicity, any OS |
| `TbTransport<T>` | Latest-value **triple buffer** in shared memory | "Latest value only", highest throughput |
| `SpscTransport<T, CAP>` | Bounded **single producer / single consumer** ring buffer | Order-preserving messages with back-pressure |
| `DirectShmTransport<T>` | One shared region with an embedded **mutex**, direct read/write | Many threads mutating one record in place |

`Datalayer` defaults to `T = ()`, so the "Rust example" above (with implicit `Datalayer`)
uses the UDS transport. The other three are only available on Linux (they rely on
`memfd` + `SCM_RIGHTS` to share kernel memory).

### 1. `()` — Unix-socket transport

Messages are sent straight over a `SOCK_SEQPACKET` (macOS: `SOCK_STREAM`)
Unix-socket. No shared memory, no configuration. Good for small payloads and the
simplest mental model.

```rust
use novomq::datalayer::Datalayer;

let dl: Datalayer = Datalayer::open("domain").unwrap();        // T = ()
let mut pl = dl.publisher("my_topic").unwrap();
pl.wait_for_subscribers(1);
pl.try_send("my_message".as_bytes()).unwrap();

let mut sub = dl.subscriber("my_topic", ()).unwrap();          // config == ()
sub.wait_until_connected();
let mut buf = [0u8; 256];
assert_eq!(sub.try_recv(&mut buf).unwrap().0, "my_message".len());
```

### 2. `TbTransport<T>` — latest-value triple buffer

Data is written into a **triple buffer** that lives in shared memory, so the
producer and consumer never block each other: the reader always sees the most
recently published value, and older, unread values may be dropped. Use it when
you only care about the *current* state (telemetry, a control snapshot, …) and
want the highest sustainable throughput.

```rust
use novomq::datalayer::{Datalayer, TbTransport, TbTransportConfig};

type Trans = TbTransport<[u8; 100]>;   // fixed 100-byte payload

// Publisher …
let dl: Datalayer<Trans> = Datalayer::open("domain").unwrap();
let mut pl = dl.publisher("topic").unwrap();
pl.wait_for_subscribers(1);
let snap = [7u8; 100];
pl.try_send(snap.as_bytes()).unwrap();

// … Subscriber (a different thread / process):
let dl: Datalayer<Trans> = Datalayer::open("domain").unwrap();
let mut sub = dl.subscriber("topic", TbTransportConfig::new()).unwrap();
sub.wait_until_connected();
let mut buf = [0u8; 100];
sub.try_recv(&mut buf).unwrap();
```

### 3. `SpscTransport<T, CAP>` — bounded SPSC ring buffer

Data is enqueued into a **bounded ring buffer** (capacity `CAP` messages) in
shared memory. Unlike the triple buffer it *keeps* every message (FIFO order)
until the consumer reads it — but it is strictly **single producer / single
consumer**: at most one publisher and one subscriber per topic. When the queue
is full (the publisher is running ahead), `try_send` reports the buffer as
full rather than overwriting.

```rust
use novomq::datalayer::{Datalayer, SpscTransport, SpscTransportConfig};

type Trans = SpscTransport<[u8; 100], 64>;   // 100-byte messages, queue of 64

let dl: Datalayer<Trans> = Datalayer::open("domain").unwrap();
let mut pl = dl.publisher("topic").unwrap();
pl.wait_for_subscribers(1);
pl.try_send(b"msg-1").unwrap();
pl.try_send(b"msg-2").unwrap();

let dl: Datalayer<Trans> = Datalayer::open("domain").unwrap();
let mut sub = dl.subscriber("topic", SpscTransportConfig::new()).unwrap();
sub.wait_until_connected();
let mut buf = [0u8; 100];
let (len, _) = sub.try_recv(&mut buf).unwrap();
assert_eq!(&buf[..len], b"msg-1");   // delivered in order
```

### 4. `DirectShmTransport<T>` — direct shared-memory region

The "no message queue" transport. Instead of sending discrete messages, both
ends map **one shared region** whose layout is `[ mutex ][ payload ]`. A
`pthread_mutex_t` is embedded *inside the shared memory itself*, so mutual
exclusion is real between any number of threads — even in different processes
that obtain the region through the UDS + `memfd` handshake. Access it directly
with `with_read` / `with_write`; every call runs while that embedded mutex is
held, e.g. the classic "several threads increment one counter" case.

```rust
use novomq::shared_memory::SharedDomain;
use std::sync::Arc;

// `SharedDomain` is the direct-access analogue of `Datalayer` (no topics):
let domain = SharedDomain::open("domain").unwrap();
type Region = [u8; 64];                     // a 64-byte shared record
let region: Arc<novomq::shared_memory::SharedMemory<Region>> =
    Arc::new(domain.create("counter"));     // first 8 bytes = a u64 counter

// Share across threads — every read/write is gated by the embedded mutex:
std::thread::scope(|s| {
    for _ in 0..8 {
        let region = region.clone();
        s.spawn(move || {
            region.with_write(|b| {
                let v = u64::from_le_bytes(b[..8].try_into().unwrap());
                b[..8].copy_from_slice(&(v + 1).to_le_bytes());   // atomic read-modify-write
            });
        });
    }
});
let v: u64 = region.with_read(|b| u64::from_le_bytes(b[..8].try_into().unwrap()));
assert_eq!(v, 8);
```

> **Note:** `DirectShmTransport<T>` requires `T: ByteInit + AsRef<[u8]> +
> AsMut<[u8]> + Send + Sync`. Plain byte arrays (`[u8; N]`) satisfy this out of
> the box; richer structs need those traits implemented (see the `Payload`
> examples in `benches/`). The high-level `novomq::shared_memory` module
> (`SharedDomain` / `SharedMemory`) wraps exactly this primitive.

## License

SPDX-License-Identifier: Apache-2.0 OR MIT