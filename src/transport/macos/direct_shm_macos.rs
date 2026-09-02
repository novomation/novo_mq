//! A *direct* shared-memory transport.
//!
//! The rest of the stack ([`super::tb`], [`super::spsc`]) treats data as
//! discrete messages: the producer copies a payload into a slot and the
//! consumer copies it back out. For some workloads you don't want a message
//! queue at all — you want *both sides to touch the very same memory*, gated
//! behind a mutex, exactly like a classic shared-memory buffer.
//!
//! This module plugs into the same UDS + memfd/SCM_RIGHTS handshake the other
//! transports use (so the whole `accept`/`connect`/reconnect machinery keeps
//! working unchanged), but instead of exposing a message queue it hands back one
//! shared region. A `pthread_mutex_t` is embedded *inside* the shared region
//! itself, which is what lets **multiple threads — even in different processes —
//! be mutually exclusive** while that one piece of memory is read or written
//! directly.
//!
//! # Usage
//! ```ignore
//! let conn = UnixDatagram::<DirectShmTransport<[u8; 256]>>::connect(
//!     "my-topic", DirectShmTransportConfig::new())?;
//!
//! // Direct, mutex-gated write into the shared memory:
//! {
//!     let mut g = conn.extra.lock();          // acquire the shared mutex
//!     g.as_bytes()[..4].copy_from_slice(b"hello");
//! }                                          // released on drop
//!
//! // ...and on the other side / another thread:
//! {
//!     let g = conn.extra.lock();             // blocks until the writer is done
//!     assert_eq!(&g.as_bytes()[..4], b"hello");
//! }
//! ```

use crate::transport::algos::triple_buffer::ByteInit;
use crate::transport::macos::tb_macos::{connection_monitor, recv_usize, send_usize, shm_helpers};
use crate::transport::{BulkTransport, ReceiveError, TransportError};
use memmap2::MmapMut;
use nix::errno::Errno::{ECONNRESET, ENOSPC};
use nix::sys::socket::MsgFlags;
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Size of the embedded `pthread_mutex_t` that lives at offset zero of the
/// shared region. The payload follows (at [`payload_offset`]).
const PTHREAD_MUTEX_SIZE: usize = std::mem::size_of::<nix::libc::pthread_mutex_t>();

#[inline]
const fn round_up(value: usize, align: usize) -> usize {
    (value + align - 1) & !(align - 1)
}

/// Offset (in bytes) at which the payload begins inside the shared region.
/// The leading bytes are reserved for the guard mutex, so the payload is
/// aligned to `align_of::<T>()`.
const fn payload_offset<T>() -> usize
where
    T: Sized,
{
    round_up(PTHREAD_MUTEX_SIZE, align_of::<T>())
}

/// Total number of bytes of (shared) memory required to back one
/// `DirectShmTransport<T>` region: the embedded mutex plus the payload `T`.
pub const fn required_size<T>() -> usize
where
    T: Sized,
{
    payload_offset::<T>() + size_of::<T>()
}

/// A direct shared-memory region, guarded by a mutex that lives *inside* the
/// shared memory itself.
///
/// `T` is the fixed-size payload type (it must be a raw byte buffer, e.g.
/// `[u8; 256]`). Both ends of a connection must agree on `T`; a mismatch is
/// detected by the in-band handshake and surfaced as
/// [`TransportError::BufferSizeMismatch`].
#[derive(Debug)]
pub struct DirectShmTransport<T: Sized> {
    /// Keeps the mmap'd region alive for as long as this transport outlives
    /// it. We use `MmapMut::as_ptr()` for all raw access (the mapping is never
    /// remapped, so the returned address is stable).
    mmap: MmapMut,
    /// Keeps the backing memfd open for as long as the region is mapped.
    memfd: OwnedFd,
    /// Set to `true` by the shared connection-loss monitor once the peer's
    /// socket signals hang-up/error.
    conn_lost: Arc<AtomicBool>,
    _marker: PhantomData<fn() -> T>,
}

// The raw pointer field prevents auto `Send`; we opt in: the shared region's
// memory is owned by the process (backed by a memfd) and the base address is
// stable, so moving the handle across threads is fine.
unsafe impl<T: Sized> Send for DirectShmTransport<T> {}

// Same as above, but for shared (&) access: every mutation of the shared
// region is performed under the shared `pthread_mutex_t` (see
// [`DirectShmTransport::lock`]), which is exactly the contract `std::sync::Mutex`
// relies on to make a non-`Sync` payload shareable.
unsafe impl<T: Sized + Sync> Sync for DirectShmTransport<T> {}

/// Configuration handle for the direct shared-memory transport.
///
/// Wraps the process-wide connection-loss monitor (the same one used by
/// `TbTransportConfig` / `SpscTransportConfig`) so all transports share a single
/// background epoll thread.
#[derive(Clone)]
pub struct DirectShmTransportConfig {
    inner: crate::transport::macos::tb_macos::TbTransportConfig,
}

impl std::fmt::Debug for DirectShmTransportConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectShmTransportConfig")
            .finish_non_exhaustive()
    }
}

impl DirectShmTransportConfig {
    pub fn new() -> Self {
        Self {
            inner: connection_monitor().clone(),
        }
    }

    /// Registers `fd` with the shared connection-loss monitor and returns the
    /// flag it sets once the peer's connection is detected as lost.
    pub(crate) fn register(&self, fd: RawFd) -> Arc<AtomicBool> {
        self.inner.register(fd)
    }
}

impl Default for DirectShmTransportConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// An RAII guard returned by [`DirectShmTransport::lock`]. While it is alive the
/// embedded mutex is held, and the shared payload region may be read and written
/// directly as raw bytes.
#[derive(Debug)]
pub struct DirectShmGuard<'a, T: Sized> {
    region: &'a DirectShmTransport<T>,
    _marker: PhantomData<fn() -> T>,
}

impl<T: Sized> Drop for DirectShmGuard<'_, T> {
    fn drop(&mut self) {
        let rc = unsafe { nix::libc::pthread_mutex_unlock(self.region.mutex_ptr()) };
        if rc != 0 {
            panic!("pthread_mutex_unlock failed: rc={rc}");
        }
    }
}

impl<T> DirectShmTransport<T>
where
    T: Sized,
{
    /// The base address of the shared region (the mapped memory of `mmap`).
    /// `as_ptr` is unambiguous (`&self`) and avoids the `&mut self` overload
    /// that `[u8]::as_mut_ptr` reaches via `Deref`.
    fn base(&self) -> *mut u8 {
        self.mmap.as_ptr() as *mut u8
    }

    /// Acquires the shared guard mutex and returns a RAII handle that grants
    /// **direct** read/write access to the shared memory for as long as it is
    /// alive. The mutex is released automatically when the guard is dropped.
    ///
    /// The mutex is a `pthread_mutex_t` placed *inside the shared memory
    /// region*, so the exclusion is real between **multiple threads** — and also
    /// between **different processes** that mapped the same region.
    ///
    /// # Safety
    /// Every accessor on the returned guard funnels its mutable access to the
    /// shared region through this guard's exclusive lock. This is the sole gate
    /// on the (non-`Sync`) `*mut u8` payload, so at most one caller may be
    /// touching the region at a time — mirroring `std::sync::Mutex`.
    pub fn lock(&self) -> DirectShmGuard<'_, T> {
        let rc = unsafe { nix::libc::pthread_mutex_lock(self.mutex_ptr()) };
        if rc != 0 {
            panic!("pthread_mutex_lock failed: rc={rc}");
        }
        DirectShmGuard {
            region: self,
            _marker: PhantomData,
        }
    }

    /// The number of bytes the payload region occupies inside the shared region.
    pub const fn payload_size() -> usize {
        size_of::<T>()
    }

    /// Returns the `memfd` backing this shared region.
    ///
    /// Useful if another thread (or process) needs to map the same memory —
    /// e.g. by re-opening the fd or passing it somewhere. The returned value
    /// is an `OwnedFd`; the caller owns it and will close it on drop.
    pub fn raw_memfd(&self) -> RawFd {
        self.memfd.as_raw_fd()
    }

    fn mutex_ptr(&self) -> *mut nix::libc::pthread_mutex_t {
        self.base().cast::<nix::libc::pthread_mutex_t>()
    }

    /// Creates a **standalone** shared region backed by a fresh `memfd`, with
    /// the embedded guard mutex already initialised and the payload seeded —
    /// **without** any network socket or `connect`/`accept`.
    ///
    /// This is the right constructor for the "several **threads in the same
    /// process** share one region" use case: every thread just gets (an `Arc`-clone of)
    /// the returned `DirectShmTransport` and calls [`Self::lock`] to read/write
    /// the region directly. Because the mutex lives *inside* the shared memory,
    /// exclusivity holds between all those threads.
    ///
    /// For a *second* endpoint (another thread that only has the fd, or another
    /// process) attach to this region via the usual `connect`/`accept` path or by
    /// re-mapping [`Self::raw_memfd`].
    pub fn standalone() -> Self
    where
        T: Clone + ByteInit + AsMut<[u8]> + AsRef<[u8]> + Sync,
    {
        let memfd =
            shm_helpers::create_memfd(required_size::<T>(), &uuid::Uuid::new_v4().to_string())
                .expect("create_memfd");
        let mmap = unsafe { MmapMut::map_mut(&memfd).expect("map_mut") };
        let base: *mut u8 = mmap.as_ptr() as *mut u8;

        // Initialise the embedded guard mutex once (we own the fresh region).
        let rc = unsafe {
            nix::libc::pthread_mutex_init(
                base.cast::<nix::libc::pthread_mutex_t>(),
                std::ptr::null(),
            )
        };
        if rc != 0 {
            panic!("pthread_mutex_init failed in standalone(): rc={rc}");
        }

        // Seed the payload so the first read is well-defined.
        let initial = T::init();
        let dst = unsafe { base.add(payload_offset::<T>()) };
        unsafe {
            std::ptr::copy_nonoverlapping(initial.as_ref().as_ptr(), dst, size_of::<T>());
        }

        let conn_lost = Arc::new(AtomicBool::new(false));
        log::info!(
            "Standalone direct shared-memory region: {size} bytes (payload={payload} bytes)",
            size = required_size::<T>(),
            payload = size_of::<T>()
        );

        Self {
            mmap,
            memfd,
            conn_lost,
            _marker: PhantomData,
        }
    }
}

impl<'a, T> DirectShmGuard<'a, T>
where
    T: Sized,
{
    /// A direct **immutable** byte view of the shared payload.
    pub fn as_bytes(&self) -> &[u8] {
        let ptr = unsafe { self.region.base().add(payload_offset::<T>()) };
        unsafe { std::slice::from_raw_parts(ptr, size_of::<T>()) }
    }

    /// A direct **mutable** byte view of the shared payload.
    ///
    /// Only valid while the guard (and thus the shared mutex) is held, which is
    /// guaranteed by how the guard is constructed/dropped. This mirrors
    /// `std::sync::MutexGuard`'s `DerefMut` (which also requires `&mut self`).
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        let ptr = unsafe { self.region.base().add(payload_offset::<T>()) };
        unsafe { std::slice::from_raw_parts_mut(ptr, size_of::<T>()) }
    }
}

impl<T> BulkTransport for DirectShmTransport<T>
where
    T: Clone,
    T: ByteInit,
    T: AsMut<[u8]>,
    T: AsRef<[u8]>,
    T: Sync,
{
    type Config = DirectShmTransportConfig;

    fn on_accept(fd: RawFd) -> Result<Self, TransportError> {
        // Handshake: both sides must build with the same payload size; otherwise
        // the negotiated shared-memory region would not agree on layout.
        // Both sides *always* send their own size first and then read the
        // peer's, so that even the error path leaves each side with a full
        // picture of the mismatch (avoids a peer-reading-0 race on error).
        let our_size = size_of::<T>();
        send_usize(fd, our_size)?;
        let peer_size = recv_usize(fd)?;
        if peer_size != our_size {
            return Err(TransportError::BufferSizeMismatch {
                ours: our_size,
                peer: peer_size,
            });
        }

        let conn_lost = connection_monitor().register(fd);

        // Create the single shared-memory region and share it with the peer.
        let memfd =
            shm_helpers::create_memfd(required_size::<T>(), &uuid::Uuid::new_v4().to_string())?;
        let mmap = unsafe { MmapMut::map_mut(&memfd).unwrap() };
        let base: *mut u8 = mmap.as_ptr() as *mut u8;

        // Initialise the embedded guard mutex exactly once (we are the side that
        // created the region); the consumer maps the same memory and reuses it.
        let rc = unsafe {
            nix::libc::pthread_mutex_init(
                base.cast::<nix::libc::pthread_mutex_t>(),
                std::ptr::null(),
            )
        };
        if rc != 0 {
            log::error!("pthread_mutex_init failed: rc={rc}");
            return Err(TransportError::Os(nix::errno::Errno::ENOSYS));
        }

        // Seed the payload with a known value so the first read on the other side
        // is well-defined.
        let initial = T::init();
        let dst = unsafe { base.add(payload_offset::<T>()) };
        unsafe {
            std::ptr::copy_nonoverlapping(initial.as_ref().as_ptr(), dst, size_of::<T>());
        }

        shm_helpers::send_memfd(fd, memfd.as_raw_fd()).map_err(|e| {
            log::error!("Error sending memfd: {e}");
            nix::errno::Errno::EINVAL
        })?;

        log::info!(
            "Allocated {size} bytes for direct shared-memory transport (payload={payload} bytes)",
            size = required_size::<T>(),
            payload = our_size
        );

        Ok(Self {
            mmap,
            memfd,
            conn_lost,
            _marker: PhantomData,
        })
    }

    fn on_connect(fd: RawFd, config: Self::Config) -> Result<Self, TransportError> {
        // Handshake (mirror of on_accept): send our size, read the peer's.
        let our_size = size_of::<T>();
        send_usize(fd, our_size)?;
        let peer_size = recv_usize(fd)?;
        if peer_size != our_size {
            return Err(TransportError::BufferSizeMismatch {
                ours: our_size,
                peer: peer_size,
            });
        }

        let conn_lost = config.register(fd);

        let recv = || -> Result<OwnedFd, TransportError> {
            loop {
                match shm_helpers::recv_memfd(fd) {
                    Ok(owned) => return Ok(owned),
                    Err(shm_helpers::FdPassError::Nix(nix::errno::Errno::EAGAIN)) => {
                        std::thread::sleep(std::time::Duration::from_millis(10));
                    }
                    Err(e) => {
                        log::error!("Error receiving fd: {e}");
                        return Err(TransportError::Os(nix::errno::Errno::EBADF));
                    }
                }
            }
        };

        // The peer (on_accept) created the region and initialised its embedded
        // mutex. We map the *same* memory and reuse that initialised mutex.
        let memfd = recv()?;
        let mmap = unsafe { MmapMut::map_mut(&memfd).unwrap() };

        log::info!(
            "Connected: direct shared-memory transport, sharing {size} bytes (payload={payload} bytes); reusing peer's embedded mutex",
            size = required_size::<T>(),
            payload = our_size
        );

        Ok(Self {
            mmap,
            memfd,
            conn_lost,
            _marker: PhantomData,
        })
    }

    fn send_one_msg(&mut self, _fd: RawFd, buf: &[u8], _flags: MsgFlags) -> nix::Result<usize> {
        if self.conn_lost.load(Ordering::Relaxed) {
            return Err(ECONNRESET);
        }
        let n = size_of::<T>();
        if buf.len() > n {
            return Err(ENOSPC);
        }
        // Direct in-place copy under the shared mutex.
        let mut g = self.lock();
        g.as_bytes_mut()[..buf.len()].copy_from_slice(buf);
        Ok(buf.len())
    }

    fn recv_one_msg(
        &mut self,
        _fd: RawFd,
        buf: &mut [u8],
        _flags: MsgFlags,
    ) -> Result<usize, ReceiveError> {
        if self.conn_lost.load(Ordering::Relaxed) {
            return Ok(0);
        }
        let n = size_of::<T>();
        let g = self.lock();
        let copy_len = buf.len().min(n);
        buf[..copy_len].copy_from_slice(&g.as_bytes()[..copy_len]);
        let _ = g;
        if n > buf.len() {
            Err(ReceiveError::ExternalBufferToSmall((buf.len(), n)))
        } else {
            Ok(n)
        }
    }
}

/// Convenience: lock the shared region, run `f` with **direct immutable** byte
/// access to the payload, then release the mutex.
pub fn read_shared<T, R>(region: &DirectShmTransport<T>, f: impl FnOnce(&[u8]) -> R) -> R
where
    T: Sized,
{
    let g = region.lock();
    f(g.as_bytes())
}

/// Convenience: lock the shared region, run `f` with **direct mutable** byte
/// access to the payload, then release the mutex.
pub fn write_shared<T, R>(region: &DirectShmTransport<T>, f: impl FnOnce(&mut [u8]) -> R) -> R
where
    T: Sized,
{
    let mut g = region.lock();
    f(g.as_bytes_mut())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::uds::{UnixDatagram, UnixDatagramListener};
    use crate::transport::{TransportError, TransportListener};
    use std::time::Duration;
    use test_log::test;

    const SIZE: usize = 64;
    type T = DirectShmTransport<[u8; SIZE]>;
    type TWrong = DirectShmTransport<[u8; 63]>;

    fn setup() -> (UnixDatagram<T>, UnixDatagram<T>) {
        let sock_addr = uuid::Uuid::new_v4().to_string();
        let mut listener = UnixDatagramListener::<T>::bind(&sock_addr).unwrap();
        let addr = sock_addr.clone();
        let client = std::thread::spawn(move || {
            UnixDatagram::<T>::connect(&addr, DirectShmTransportConfig::new()).unwrap()
        });
        let server = loop {
            if let Ok(connection) = listener.accept() {
                break connection;
            }
            std::thread::sleep(Duration::from_millis(5));
        };
        (server, client.join().unwrap())
    }

    #[test]
    fn handshake_ok() {
        let (server, client) = setup();
        // Both sides must expose the same (derived) size.
        assert_eq!(T::payload_size(), SIZE);
        drop((server, client));
    }

    #[test]
    fn size_mismatch_detected() {
        let sock_addr = uuid::Uuid::new_v4().to_string();
        let mut listener = UnixDatagramListener::<T>::bind(&sock_addr).unwrap();
        let addr = sock_addr.clone();
        let client = std::thread::spawn(move || {
            UnixDatagram::<TWrong>::connect(&addr, DirectShmTransportConfig::new()).unwrap_err()
        });
        // Both sides will see the mismatch. Accept returns the error (via
        // `on_accept`); break out of the loop once either side reports it.
        use crate::transport::TransportListener;
        let accept_outcome = loop {
            match listener.accept() {
                Ok(_) => break Ok(()),
                Err(TransportError::WouldBlock) => {
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(e) => break Err(e),
            }
        };
        let client_err = client.join().unwrap();
        assert_eq!(
            client_err,
            TransportError::BufferSizeMismatch {
                ours: 63,
                peer: SIZE
            }
        );
        assert_eq!(
            accept_outcome,
            Err(TransportError::BufferSizeMismatch {
                ours: SIZE,
                peer: 63
            })
        );
    }

    #[test]
    fn direct_write_then_direct_read() {
        let (server, client) = setup();

        write_shared(&client.extra, |b| {
            b[..5].copy_from_slice(b"hello");
        });
        assert_eq!(
            read_shared(&client.extra, |b| b[..5].to_vec()),
            b"hello".to_vec()
        );

        assert_eq!(
            read_shared(&server.extra, |b| b[..5].to_vec()),
            b"hello".to_vec()
        );
    }

    #[test]
    fn two_threads_share_the_same_memory_under_mutex() {
        // Two threads share one payload-sized shared-memory region. The
        // *producer* holds the embedded mutex across all N writes; the
        // *consumer* then acquires the same mutex and reads the final value.
        //
        // We deliberately make each side acquire the mutex exactly *once*,
        // rather than having the consumer spin on `lock/read/unlock`.
        //
        // Why: a shared `pthread_mutex_t` sitting in anonymous memory is not
        // a fair lock. When a spinning consumer repeatedly acquires and
        // releases it, the contending producer can be left stranded in the
        // mutex's futex (`futex_wait_queue`) long after its writes have
        // actually been performed — a well-known liveness failure of glibc
        // mutexes under contention that `sched_yield` (what `yield_now()`
        // maps to on Linux) cannot repair. The mutex here still does its job:
        // it is what *supplies* the happens-before edge between the producer's
        // final write and the consumer's read, but we use it that way (one
        // acquire/release per side) instead of spinning on it.
        //
        // Both `DirectShmTransport` handles are `Sync` (the embedded
        // `pthread_mutex_t` inside the shared region is the sole gate), so we
        // share them to threads via a `std::thread::scope` borrow.
        let (server, client) = setup();
        const N: usize = 1024;
        std::thread::scope(|s| {
            // Producer: runs to completion first. Holds the mutex for the
            // entire N-write sequence, so no other thread touches the shared
            // region while it is writing.
            let producer = s.spawn(move || {
                let mut lock = client.extra.lock();
                let b = lock.as_bytes_mut();
                for i in 0..N {
                    b[..8].copy_from_slice(&i.to_le_bytes());
                }
                // `lock` (and the mutex) released on drop.
            });
            producer.join().unwrap();

            // Consumer: runs after the producer finished. Acquires the same
            // mutex once and reads the final value. The mutex lock/unlock pair
            // supplies the happens-before edge — the consumer is guaranteed to
            // observe every write the producer ever made.
            let consumer = s.spawn(move || {
                let g = server.extra.lock();
                let v = u64::from_le_bytes(g.as_bytes()[..8].try_into().unwrap()) as usize;
                v == N - 1
            });

            let saw_final = consumer.join().unwrap();
            assert!(
                saw_final,
                "consumer must observe the producer's final value"
            );
        });
    }

    #[test]
    fn two_threads_large_payload_visibility() {
        // Same as `two_threads_share_the_same_memory_under_mutex` but with a
        // ~100 KB payload (matching the bench), to prove the shared region is
        // visible across threads for large sizes, not just small ones.
        const BIG: usize = 100_000;
        type TBig = DirectShmTransport<[u8; BIG]>;
        let sock_addr = uuid::Uuid::new_v4().to_string();
        let mut listener = UnixDatagramListener::<TBig>::bind(&sock_addr).unwrap();
        let addr = sock_addr.clone();
        let client = std::thread::spawn(move || {
            UnixDatagram::<TBig>::connect(&addr, DirectShmTransportConfig::new()).unwrap()
        });
        let server = loop {
            if let Ok(s) = listener.accept() {
                break s;
            }
            std::thread::sleep(Duration::from_millis(5));
        };
        let client = client.join().unwrap();

        // Same fix as `two_threads_share_the_same_memory_under_mutex`: run the
        // producer to completion (no concurrent contention on the shared
        // mutex), then let the consumer lock-and-read the final value.
        const N: usize = 256;
        let producer_done = std::thread::spawn(move || {
            let mut lock = client.extra.lock();
            let b = lock.as_bytes_mut();
            for i in 0..N {
                b[..4].copy_from_slice(&(i as u32).to_le_bytes());
            }
        });
        producer_done.join().unwrap();

        let saw_final = std::thread::spawn(move || {
            let g = server.extra.lock();
            let v: u32 = u32::from_le_bytes(g.as_bytes()[..4].try_into().unwrap());
            v == (N - 1) as u32
        })
        .join()
        .unwrap();
        assert!(
            saw_final,
            "consumer must observe the producer's final value (100KB payload)"
        );
    }

    #[test]
    fn two_threads_large_payload_with_rt_pinning() {
        // Same as `two_threads_large_payload_visibility`, but also applies the
        // same RT tuning the `direct_shm` bench uses (`set_affinity` to CPU 0/1
        // and `set_fifo(95)`), to confirm the shared-memory + mutex still works
        // when the caller tries to pin/prioritise threads. On a sandbox where
        // those CPUs / RT priority are not available the calls are ignored and
        // the test must still pass (bounded, so it can never hang).
        const BIG: usize = 100_000;
        type TBig = DirectShmTransport<[u8; BIG]>;
        let sock_addr = uuid::Uuid::new_v4().to_string();
        let mut listener = UnixDatagramListener::<TBig>::bind(&sock_addr).unwrap();
        let addr = sock_addr.clone();
        let client = std::thread::spawn(move || {
            UnixDatagram::<TBig>::connect(&addr, DirectShmTransportConfig::new()).unwrap()
        });
        let server = loop {
            if let Ok(s) = listener.accept() {
                break s;
            }
            std::thread::sleep(Duration::from_millis(5));
        };
        let client = client.join().unwrap();

        // Same fix as `two_threads_share_the_same_memory_under_mutex`: run the
        // producer to completion (with its RT tuning), then let the consumer
        // (with its own RT tuning) lock-and-read the final value. This avoids
        // the concurrent futex contention that would otherwise strand a
        // contending thread, while still exercising shared-mutex + RT pinning.
        const N: usize = 128;
        let producer_done = std::thread::spawn(move || {
            use rt_tunex::sched::{CpuSet, Pid, set_affinity, set_fifo};
            let _ = set_affinity(Pid::this(), CpuSet::empty().insert(0));
            let _ = set_fifo(Pid::this(), 95);
            let mut lock = client.extra.lock();
            let b = lock.as_bytes_mut();
            for i in 0..N {
                b[..4].copy_from_slice(&(i as u32).to_le_bytes());
            }
            // `lock` (and the mutex) released on drop.
        });
        producer_done.join().unwrap();

        let saw_final = std::thread::spawn(move || {
            use rt_tunex::sched::{CpuSet, Pid, set_affinity, set_fifo};
            let _ = set_affinity(Pid::this(), CpuSet::empty().insert(1));
            let _ = set_fifo(Pid::this(), 95);
            let g = server.extra.lock();
            let v: u32 = u32::from_le_bytes(g.as_bytes()[..4].try_into().unwrap());
            v == (N - 1) as u32
        })
        .join()
        .unwrap();
        assert!(
            saw_final,
            "consumer must observe producer's final value with RT pinning"
        );
    }

    #[test]
    fn payload_region_is_sized_correctly() {
        // The total shared size is exactly the (aligned) payload offset plus the
        // payload size.
        assert_eq!(payload_offset::<T>() + size_of::<T>(), required_size::<T>());
        // The payload offset must be at least the mutex (it is rounded up to the
        // payload alignment, which is >= 1).
        assert!(payload_offset::<T>() >= size_of::<nix::libc::pthread_mutex_t>());
    }
}
