# Direct shared-memory transport — findings & design

## Goal
A *second* layer beside the existing datalayer that lets **multiple threads** share
memory with **direct access** (no send/recv message copies). It must reuse the
existing **UDS-based connection logic** (the unix-socket + `accept`/`connect` +
memfd-passing handshake that `TbTransport`/`SpscTransport` already use), but at the
end expose a method to **read/write the shared memory directly**, gated behind a
**mutex**.

## Why the current scheme is a poor fit
The whole send/recv stack assumes *discrete messages*:
- `BulkTransport::send_one_msg` / `recv_one_msg` (`src/transport/bulk_transport.rs:18-25`)
- `Subscriber`/`Publisher` (`src/subscriber.rs`, `src/publisher.rs`) wrap a
  `Box<dyn TransportConnection>` and only expose `recv_into()`/`try_send()`.
- `TbTransport` (latest-value triple buffer) and `SpscTransport` (fixed-cap queue)
  both *queue* payloads into a slot and force a copy in and out.
For "just let me poke the same memory from several threads" that copy + message
framing is exactly the wrong abstraction. What we want is a **region of shared
memory** plus mutual exclusion.

## Reusable building blocks (already in the tree)
- **UDS connection engine**: `UnixDatagramListener<E::BulkTransport>` / `UnixDatagram<E>`
  in `src/transport/uds.rs`. It does `bind`/`accept`/`connect` over a
  `SOCK_SEQPACKET` (macOS: `SOCK_STREAM`) unix socket and hands the raw `fd` to
  `E::on_accept` / `E::on_connect`. `conn.extra: E` is **`pub`** (`uds.rs:301`),
  so a transport struct can carry **extra public methods** (our `lock()`).
- **Socket handshake helpers** in `src/transport/tb.rs`:
  - `send_usize`/`recv_usize` (tb.rs:33-49) — exchange config values
  - `shm_helpers::create_memfd` (tb.rs:95) — make an anonymous memfd of a size
  - `shm_helpers::send_memfd` / `recv_memfd` (tb.rs:68-93) — pass the fd via `SCM_RIGHTS`
  - `TbTransportConfig` / `connection_monitor` (tb.rs:129-178) — a shared background
    epoll thread that flags connection loss (an `Arc<AtomicBool>` per conn).
    `SpscTransport` **already** reuses this from `tb` (`spsc.rs:73-89`) — we will too.
- **Shared-memory layout helpers** in `src/transport/algos/spsc.rs` show the exact
  pattern to place a struct in mmap'd memory and `unsafe` wrap it on each side
  (`spsc_in_memory` / `spsc_wrap`, spsc.rs:107-125).

## Key constraint discovered
`memmap2::MmapMut` is **`Send` but NOT `Sync`** (memmap2 only impls
`StableDeref`, `Deref → [u8]`, `AsRef/AsMut<[u8]>`, `Debug` — no `Sync`).
That is precisely *why* shared mutable memory must be exposed behind a mutex:
at most one caller may touch the region at a time. `MmapMut::as_mut_ptr` is
ambiguous with `[u8]::as_mut_ptr` (the `Deref` target, `&mut self`), so the base
pointer is taken via `as_ptr().cast_mut()` and `.add()` is wrapped in `unsafe`.

## Design
New module `src/transport/direct_shm.rs`, Linux-only (memfd + SCM_RIGHTS, like
`tb`/`spsc`). Two type aliases / structs:

- `DirectShmTransport<const N: usize>` — holds `Arc<Shared>` + the `MmapMut`
  mapping + `conn_lost: Arc<AtomicBool>`.
  - `Shared { mutex: Mutex<()> }` — **shared across all connection ends that
    reference this connection** so the lock is real mutual exclusion between
    threads, not just local.
  - `lock(&self) -> DirectShmAccess<'_>` — take the mutex, return a handle
    that derefs to the shared `&[u8; N]` (length `N`). Handle is `Send`+`Sync`,
    so the guard (and the memory access it unlocks) can be handed to another
    thread and the mutex stays released on drop — exactly the requested
    "direct access … gated behind a mutex".
- `DirectShmTransportConfig` — `Clone`; wraps `TbTransportConfig` for the
  connection-loss monitor, mirroring `SpscTransportConfig`.

`BulkTransport` impl (so the UDS/`accept`/`connect` machinery works unchanged):
1. Handshake: `send_usize(N)` / `recv_usize()` both sides; mismatch →
   `TransportError::BufferSizeMismatch`.
2. `on_accept`: `create_memfd(N)` → map (`MmapMut::map_mut`) → `send_memfd` to peer.
3. `on_connect`: `recv_memfd()` → map.
4. Keep our `OwnedFd` + `MmapMut` alive in the struct (backing the mapping).
5. `send_one_msg`/`recv_one_msg`: best-effort copy (see note below).

**send/recv note:** the transport is meant for *direct* access via `lock()`.
`send_one_msg`/`recv_one_msg` are provided only so the type still satisfies
`BulkTransport` and can be used with the generic `UnixDatagram` plumbing; they
copy the whole payload into/from the region at offset 0 (latest-value, no
queuing). Real use should prefer `lock()`.

## Why a fresh transport and not an `algos` module?
`algos` (triple_buffer.rs, spsc.rs) are the *algorithms* that `BulkTransport`
wraps (they only implement `send_one_msg`/`recv_one_msg` and must be `Send`).
Direct access needs a *public* `lock()` handle with a reference tied to the
connection's lifetime, which `BulkTransport`'s `send`/`recv` surface can't
express. A new transport that *reuses* `tb`'s handshake helpers is the cleanest
seam: we keep the "uds-based connection logic" and override only the payload
surface.

## Public surface
- `pub use` from `lib.rs` and expose under `crate::datalayer` (like
  `TbTransport`/`SpscTransport`) so `Datalayer<DirectShmTransport<N>>::…` works.
- `UnixDatagram<DirectShmTransport<N>>::connect(...)` → `.extra.lock()` →
  mutate/read `&[u8; N]`.

## Verification
- Unit tests: handshake ok / size mismatch; direct write→read round-trip;
  **multi-thread** read/write under the mutex (guard is `Send`, so one thread
  holds the memory while another waits); guard released on drop; peer-side
  sees the same memory.
- Bench `benches/direct_shm.rs`: spin loop measuring lock + memcpy + unlock on
  the direct region, comparable to `mem_test.rs`.
- Run: `cargo test --lib`, `cargo test --benches` (feature-gated as needed),
  `cargo clippy --all-targets`, `cargo fmt --check`.

## Implementation note (final API)
The payload sits at [`payload_offset`], which is the `pthread_mutex_t` size
rounded up to `align_of::<T>()`. Access is through the RAII
`DirectShmGuard` returned by `DirectShmTransport::lock()`:
- `as_bytes(&self) -> &[u8]` — direct immutable view
- `as_bytes_mut(&mut self) -> &mut [u8]` — direct mutable view (the guard is the
  sole holder; clippy's `mut_from_ref` is satisfied by the `&mut self`)
`send_one_msg`/`recv_one_msg` also route through the same lock. All of it is
sound because the embedded `pthread_mutex_t` is the *only* gate on the
(non-`Sync`) raw-pointer payload — mirroring `std::sync::Mutex`. `unsafe impl Send`
and `unsafe impl Sync` on the transport are justified by this single-keeper
invariant.

## Conventions followed (from the existing `tb`/`spsc` transports)
- `T: Send + ByteInit + AsMut<[u8]> + AsRef<[u8]> + Clone + Sync` on the
  `BulkTransport` impl (the same bound the other shared-memory transports use).
- Both sides *always* send-and-recv their `size_of::<T>()` before checking the
  match, so the error path is symmetric (no 0-size race) — see
  `transport::direct_shm::on_accept` / `on_connect`.

## Verification (this session)
- `cargo test --lib` : **67 passed, 0 failed** (5 new direct_shm tests)
- `cargo clippy --lib --benches --tests` : clean for the new code
- `cargo fmt` : applied
- `benches/direct_shm.rs` : compiles & runs end-to-end (bind + memfd + mutex +
  cross-thread write/read), comparable output to `long_term_tb`

## Public surface
- `novomq::datalayer::{DirectShmTransport, DirectShmTransportConfig,
  DirectShmGuard, read_shared, write_shared, required_size}`
- `novomq::datalayer::{UnixDatagram, UnixDatagramListener, TransportListener,
  socket_path}` (re-exported so custom code can drive the UDS handshake directly)
- `Datalayer<DirectShmTransport<T>>::publisher(..)` /
  `..subscriber(.., DirectShmTransportConfig::new())`
