//! A high-level **direct shared-memory** layer, mirroring
//! [`crate::datalayer`] for the "touch the same memory directly" use case
//! rather than pub/sub messaging.
//!
//! Instead of a producer `try_send`-ing bytes into a shared region and a
//! consumer `try_recv`-ing them back out (as
//! [`crate::datalayer::Datalayer`] does), **both ends read and write the same
//! memory directly**, serialised by the `pthread_mutex_t` embedded *inside*
//! that region (see [`crate::transport::direct_shm::DirectShmTransport`]).
//! No copies, no message framing — just direct, mutex-gated access to the
//! bytes, exactly like a classic shared-memory buffer.
//!
//! # Multi-thread direct-access example
//! ```ignore
//! use novomq::shared_memory::SharedDomain;
//! use std::sync::Arc;
//!
//! let domain = SharedDomain::open("domain").unwrap();
//! let region: Arc<SharedMemory<[u8; 4096]>> = Arc::new(domain.create("my-region"));
//!
//! // Share with any number of threads — `SharedMemory` is `Send + Sync`:
//! let handles: Vec<_> = (0..8).map(|_| region.clone()).collect();
//! let joins: Vec<_> = handles
//!     .into_iter()
//!     .enumerate()
//!     .map(|(i, h)| std::thread::spawn(move || h.with_write(|b| b[i].wrapping_add(1))))
//!     .collect();
//! for j in joins { j.join().unwrap(); }
//! // Direct, mutex-gated read from any other thread:
//! region.with_read(|b| { assert!(b[0] <= 1) });
//! ```
//!
//! `create` and `get` both hand out handles to the **same physical region**
//! (the one `create` allocated) — two handles in one process over the same
//! `(name, T)` always see each other's bytes, which is the aliasing semantics
//! a cross-thread shared-memory layer needs. Cross-*process* sharing still
//! uses the `DirectShmTransport` UDS + memfd handshake (the same plumbing
//! this module reuses internally).
use crate::transport::algos::triple_buffer::ByteInit;
use crate::transport::direct_shm::DirectShmTransport;
use crate::transport::TransportError;
use std::any::Any;
use std::collections::HashMap;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};

/// The payload/view type a [`SharedMemory`] region is built from.
///
/// It must be a fixed-size buffer with direct byte access (e.g. `[u8; 4096]`)
/// and shareable across threads (`Send + Sync`). The blanket impl below turns
/// the bound into a marker you can just write as `T: SharedPayload`.
pub trait SharedPayload:
    ByteInit + Clone + AsMut<[u8]> + AsRef<[u8]> + Send + Sync + 'static
{
}
impl<T> SharedPayload
    for T where
    T: ByteInit + Clone + AsMut<[u8]> + AsRef<[u8]> + Send + Sync + 'static,
{
}

/// A `Send + Sync` handle to **one shared memory region**.
///
/// `Clone` is cheap (it just bumps the `Arc`'s strong count). Every direct
/// [`Self::with_read`] / [`Self::with_write`] goes through the mutex embedded
/// *inside* the shared memory, so two `SharedMemory` handles pointing at the
/// same physical region — which is always the case if they came from the same
/// [`SharedDomain`] with the same name and payload type — give real mutual
/// exclusion between all threads that hold one of them.
#[derive(Clone)]
pub struct SharedMemory<T: SharedPayload = [u8; 0]> {
    /// The concrete region we share. Stored behind `Arc<dyn Any + Send + Sync>`
    /// so the domain's registry can erase over `T` while still keeping the
    /// concrete type reachable via `downcast_ref`.
    region: Arc<dyn Any + Send + Sync>,
    _marker: std::marker::PhantomData<T>,
}

impl<T: SharedPayload> std::fmt::Debug for SharedMemory<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedMemory")
            .field("size", &self.size())
            .finish()
    }
}

impl<T: SharedPayload> SharedMemory<T> {
    /// The concrete transport behind the erased handle.
    ///
    /// `downcast_ref` is a one-`vtable-lookup` indirection; it is guaranteed
    /// to succeed because the registry only stores regions that were actually
    /// created with this `T` (verified at the `create`/`get` boundary).
    fn inner(&self) -> &DirectShmTransport<T> {
        self.region
            .downcast_ref::<DirectShmTransport<T>>()
            .expect("registry is keyed on the concrete T")
    }

    /// Number of bytes in the payload region (== `size_of::<T>()`).
    pub fn size(&self) -> usize {
        size_of::<T>()
    }

    /// Direct, mutex-gated **read** access to the whole region.
    ///
    /// The closure receives `&[u8]` of length [`Self::size`] while the region's
    /// embedded mutex is held; returns whatever `f` returns.
    pub fn with_read<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R {
        let g = self.inner().lock();
        f(g.as_bytes())
    }

    /// Direct, mutex-gated **write** access to the whole region.
    ///
    /// The closure receives `&mut [u8]` of length [`Self::size`] while the
    /// region's embedded mutex is held — exactly like `std::sync::MutexGuard`
    /// yields `&mut` data to a closure.
    pub fn with_write<R>(&self, f: impl FnOnce(&mut [u8]) -> R) -> R {
        let mut g = self.inner().lock();
        f(g.as_bytes_mut())
    }

    /// Copy the current contents of the region out as an owned `T`.
    ///
    /// The region's embedded mutex is held for the whole read, so the snapshot
    /// is consistent against a concurrent writer.
    pub fn snapshot(&self) -> T
    where
        T: for<'a> TryFrom<&'a [u8]>,
        for<'a> <T as TryFrom<&'a [u8]>>::Error: std::fmt::Debug,
    {
        self.with_read(|bytes| T::try_from(bytes).expect("size matches"))
    }

    /// Write an owned `T` into the region (replacing its contents).
    ///
    /// The region's embedded mutex is held for the whole write.
    pub fn store(&self, value: &T)
    where
        T: AsRef<[u8]>,
    {
        let value = value.as_ref();
        self.with_write(|dst| {
            let n = dst.len().min(value.len());
            dst[..n].copy_from_slice(&value[..n]);
        });
    }
}

/// Top-level entry point for direct shared memory (the analogue of
/// [`Datalayer`](crate::datalayer::Datalayer)).
///
/// A domain is (like the datalayer) just a filesystem directory under which
/// named regions may live. Multiple domains may coexist in one process.
/// Type-erased registry entry: the payload size a region was registered at,
/// plus a `Weak` reference to the concrete `DirectShmTransport<T>` (held
/// behind `dyn Any` so multiple different `T`s can coexist in one domain).
///
/// The `Weak` keeps the memory alive only while at least one [`SharedMemory`]
/// handle to it still exists; when the last handle drops, the concrete
/// transport (and with it the memfd-backed mmap) is released.
type RegionRegistry = Mutex<HashMap<String, (usize, Weak<dyn Any + Send + Sync>)>>;

pub struct SharedDomain {
    domain_path: PathBuf,
    registry: RegionRegistry,
}

impl SharedDomain {
    /// Opens a new shared-memory domain, creating the directory if it does not
    /// already exist (mirror of
    /// [`Datalayer::open`](crate::datalayer::Datalayer::open)).
    pub fn open(domain: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        let domain_path = domain.as_ref().to_path_buf();
        std::fs::create_dir_all(&domain_path)?;
        Ok(Self {
            domain_path,
            registry: Mutex::new(HashMap::new()),
        })
    }

    /// The filesystem path of this domain.
    pub fn path(&self) -> &Path {
        &self.domain_path
    }

    /// Create (own) a named shared region of payload type `T` in this domain.
    ///
    /// Allocates `size_of::<T>()` bytes of shared memory, embeds the guard
    /// mutex, and registers the region under `name` for [`Self::get`] lookups.
    /// Returns a [`SharedMemory<T>`] handle ready for `with_read` /
    /// `with_write` from any number of threads in this process.
    ///
    /// The memory is released when the last `SharedMemory` handle that holds
    /// an `Arc` to it is dropped (the registry itself only keeps a `Weak`).
    pub fn create<T>(&self, name: impl AsRef<str>) -> SharedMemory<T>
    where
        T: SharedPayload,
    {
        let region: Arc<dyn Any + Send + Sync> = Arc::new(DirectShmTransport::<T>::standalone());
        let size = size_of::<T>();
        let weak = Arc::downgrade(&region);
        self.registry
            .lock()
            .unwrap()
            .insert(name.as_ref().to_string(), (size, weak));
        SharedMemory {
            region,
            _marker: std::marker::PhantomData,
        }
    }

    /// Attach to an existing named shared region of payload type `T` in this
    /// domain (the counterpart of [`Self::create`]).
    ///
    /// Both endpoints must agree on `T`; a size mismatch returns
    /// [`TransportError::BufferSizeMismatch`].
    ///
    /// **Aliasing semantics:** on a second call with the same `T`, the
    /// returned handle points at the **same physical region** that
    /// [`Self::create`] allocated (it is the same `Arc`), so all handles in
    /// this process over the same `(name, T)` pair see each other's writes.
    pub fn get<T>(&self, name: impl AsRef<str>) -> Result<SharedMemory<T>, TransportError>
    where
        T: SharedPayload,
    {
        let name = name.as_ref().to_string();
        let (size, weak) = self
            .registry
            .lock()
            .unwrap()
            .get(&name)
            .cloned()
            .ok_or(TransportError::PeerDoesNotExist)?;
        if size != size_of::<T>() {
            return Err(TransportError::BufferSizeMismatch {
                ours: size_of::<T>(),
                peer: size,
            });
        }
        let region = weak.upgrade().ok_or(TransportError::PeerDoesNotExist)?;
        Ok(SharedMemory {
            region,
            _marker: std::marker::PhantomData,
        })
    }
}

impl Drop for SharedDomain {
    fn drop(&mut self) {
        // Best-effort cleanup, mirroring `Datalayer::drop`.
        let _ = std::fs::remove_dir(&self.domain_path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    type Region = [u8; 4096];

    fn temp_domain(label: &str) -> SharedDomain {
        let dir = std::env::temp_dir().join(format!(
            "novomq-sm-{label}-{}",
            uuid::Uuid::new_v4()
        ));
        SharedDomain::open(&dir).unwrap()
    }

    #[test]
    fn create_and_direct_read_write_in_one_process() {
        let domain = temp_domain("local");
        let shared = domain.create::<Region>("r");
        shared.with_write(|b| {
            b[..5].copy_from_slice(b"hello");
        });
        let seen = shared.with_read(|b| b[..5].to_vec());
        assert_eq!(seen, b"hello".to_vec());
    }

    #[test]
    fn get_returns_same_region_as_create() {
        let domain = temp_domain("same");
        let a = domain.create::<Region>("r");
        let b = domain.get::<Region>("r").unwrap();
        a.with_write(|x| {
            x[..8].copy_from_slice(&0xDEAD_BEEF_CAFE_F00Du64.to_le_bytes());
        });
        let v: u64 = b.with_read(|x| u64::from_le_bytes(x[..8].try_into().unwrap()));
        assert_eq!(v, 0xDEAD_BEEF_CAFE_F00D);
    }

    #[test]
    fn multi_thread_direct_access_is_consistent() {
        let domain = temp_domain("multi");
        let shared = Arc::new(domain.create::<Region>("r"));

        std::thread::scope(|s| {
            let writer = shared.clone();
            s.spawn(move || {
                for i in 0..500u32 {
                    writer.with_write(|b| b[..4].copy_from_slice(&i.to_le_bytes()));
                }
            });
            let reader = shared.clone();
            s.spawn(move || {
                let mut max_seen = 0u32;
                for _ in 0..1_000_000 {
                    let v: u32 =
                        reader.with_read(|b| u32::from_le_bytes(b[..4].try_into().unwrap()));
                    if v > max_seen {
                        max_seen = v;
                    }
                    if max_seen >= 499 {
                        break;
                    }
                    std::thread::yield_now();
                }
                max_seen
            });
        });
        assert_eq!(shared.size(), size_of::<Region>());
    }

    #[test]
    fn size_matches_payload() {
        let domain = temp_domain("size");
        let shared = domain.create::<[u8; 1234]>("r");
        assert_eq!(shared.size(), 1234);
    }

    #[test]
    fn unknown_name_is_reported() {
        let domain = temp_domain("unknown");
        let err = domain.get::<Region>("missing").unwrap_err();
        assert!(matches!(err, TransportError::PeerDoesNotExist));
    }

    #[test]
    fn size_mismatch_between_endpoints_is_detected() {
        let domain = temp_domain("mismatch");
        let _ = domain.create::<[u8; 4096]>("r");
        let err = domain.get::<[u8; 1024]>("r").unwrap_err();
        assert!(
            matches!(err, TransportError::BufferSizeMismatch { .. }),
            "expected BufferSizeMismatch, got {err:?}"
        );
    }
}
