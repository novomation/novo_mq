# macOS Test Fixes — What We Learned & How

## Problem

`cargo test` on macOS (Apple Silicon / arm64) hung indefinitely and several
transport-level tests failed after the hang was avoided. The root causes were
**not** in the `TbTransport` (which remains intentionally unimplemented on
macOS and is excluded from the test suite via
`#[cfg(not(target_os = "macos"))]`).

## Root causes (in order they were discovered)

### 1. `set_calling_thread_to_nrt()` panicked (the "hang")

**Location:** `src/helpers.rs::allowed_cpus()`

The code read `/proc/<pid>/status` to discover the allowed CPU mask
(`Cpus_allowed`). That file does not exist on macOS, so the `.unwrap()`
panicked inside the publisher/subscriber mgmt threads. The mgmt threads
never finished their start-up step, so `Publisher::wait_for_subscribers`
and `Subscriber::wait_until_connected` blocked forever → every end-to-end
test hung.

**Symptom:** `Running unittests ... running 20 tests` then silence. In
earlier runs: `panicked at src/helpers.rs:20:73: ... Os { code: 2, ...
NotFound }`.

**Fix:** split the function:

* `target_os = "linux"` → unchanged `/proc` read (identical behaviour).
* `target_os = "macos"` (new) → ask the OS for the allowed parallelism
  (`std::thread::available_parallelism()`) and build a low-bits mask of
  that width. This is best-effort, but correct for the test use case.
  `rt_tunex::sched::set_affinity` is called on that mask.

### 2. `MSG_TRUNC` always "triggers" on macOS `SOCK_STREAM`

**Location:** `src/transport/uds.rs::BulkTransport for ()::recv_one_msg`

macOS has **no `SOCK_SEQPACKET`**, so the socket is opened with
`SOCK_STREAM`. On Darwin, a `recvmsg()` on a stream socket reports the
`MSG_TRUNC` flag even when the message fit the buffer completely
(unlike Linux `SOCK_SEQPACKET` / Windows datagram sockets).

The code mapped *any* `MSG_TRUNC` → `ExternalBufferToSmall`, so the very
first successful send/recv returned an error and the loop in the test
never terminated.

**Fix:** on macOS, decide "too small" from `bytes_received` vs
`buffer_len` rather than trusting the flag.

### 3. macOS `SOCK_STREAM` has no message boundaries

**Symptom:** `pub_sub` / `send_recv` failed with `left: Ok(2)` (two
consecutive 4-byte datagrams merged into one 8-byte stream read).

**Fix:** on macOS we **length-prefix** every message:
* `send_one_msg` writes `u64::to_be_bytes(len) || payload`.
* `recv_one_msg` reads the 8-byte BE length, then `read_full`s exactly
  `len` bytes.

The `ExternalBufferToSmall` case is preserved: if `len > user_buf.len()`
we buffer the oversized payload separately, copy as much as we can into
the caller's buffer, and return the tuple
`(user_buf_len, len)` — exactly what the Linux/Windows path does.

### 4. `getpeername()` is unreliable after the peer closes

**Symptom:** `accept_connect_reconnect` asserted
`client.peer_addr() == Some(addr)` but got `None` after the server side
was dropped.

**Fix:** cache the peer path at `connect` / `accept` time in a
macOS-only `UnixDatagram::peer: Option<String>` field; `peer_addr()`
falls back to it when the live `getpeername()` call returns nothing.

### 5. `send()` on a closed stream yields `EPIPE`, not `ECONNRESET`

**Symptom:** `send_on_connectionreset` and `publisher::test::smoke`
expected `SendError::ConnectionEnded` but got `Err(OsError(EPIPE))`.

**Fix:** in `UnixDatagram::send`, map `EPIPE` → `ConnectionEnded` for
macOS only.

## Files touched (macOS paths only)

* `src/helpers.rs` — new `#[cfg(target_os = "macos")] fn allowed_cpus()`;
  Linux function untouched.
* `src/transport/uds.rs`:
  * `BulkTransport for ()::{on_accept, on_connect}` unchanged.
  * `BulkTransport for ()::send_one_msg` / `recv_one_msg` — Linux
    branches unchanged, new macOS length-prefixed branches.
  * `UnixDatagram` — new macOS-only `peer` field.
  * `UnixDatagram::connect` / `UnixDatagramListener::accept` — capture
    the peer on macOS.
  * `TransportConnection::peer_addr` — live query on Linux, live+cache
    fallback on macOS.
  * `TransportConnection::send` — `EPIPE → ConnectionEnded` on macOS.

## What was intentionally NOT changed

* `src/transport/tb.rs` — remains Linux-only (`#[cfg(target_os = "linux")]`).
* The `_tb` test modules in `transport/mod.rs`, `publisher.rs`,
  `subscriber.rs`, `datalayer.rs`, `tests/test_nonblocking.rs` — still
  gated by `#[cfg(not(target_os = "macos"))]`.
* The `SchedAttr`, `CpuSet`, `triple_buffer` modules in `rt_tunex` —
  untouched.
* All Linux/Windows code — byte-for-byte identical.

## Verification

On macOS (Apple Silicon):

```
$ cargo test
...
test result: ok. 20 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
test result: ok.  1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
test result: ok.  0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

(Tests run single-threaded (`--test-threads=1`) for the lib suite.)

## Re-running the suite

```bash
cargo test
# or, if you want to see the per-test stdout:
cargo test --lib -- --test-threads=1 --nocapture
cargo test --test test_nonblocking
```
