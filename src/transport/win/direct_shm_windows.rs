//! A *direct* shared-memory transport (Windows).
//!
//! Mirrors [`crate::transport::direct_shm::DirectShmTransport`]: a single
//! shared region is negotiated over the socket. A `CRITICAL_SECTION` is
//! embedded *inside* the shared region itself, which is what lets **multiple
//! threads — even in different processes — be mutually exclusive** while
//! that one piece of memory is read or written directly.
//!
//! # Usage
//! ```ignore
//! let conn = UnixDatagram::<DirectShmTransport<[u8; 256]>>::connect(
//!     "my-topic", DirectShmTransportConfig::new())?;
//!
//! { let mut g = conn.extra.lock(); g.as_bytes_mut()[..5].copy_from_slice(b"hello"); }
//! { let g = conn.extra.lock(); assert_eq!(&g.as_bytes()[..5], b"hello"); }
//! ```
use crate::transport::BulkTransport;
use crate::transport::ReceiveError;
use crate::transport::SendError;
use crate::transport::TransportError;
use crate::transport::algos::triple_buffer::ByteInit;
use crate::transport::win::tb::{
    MappedView, OwnedMapping, create_mapping, open_mapping, new_name, recv_name, recv_usize,
    send_name, send_usize,
};
use crate::transport::win::tb::TbTransportConfig;
use std::marker::PhantomData;
use std::mem::size_of;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use windows::Win32::Networking::WinSock::SOCKET;
use windows::Win32::System::Threading::{
    CRITICAL_SECTION, EnterCriticalSection, InitializeCriticalSection, LeaveCriticalSection,
};

#[inline]
const fn round_up(value: usize, align: usize) -> usize {
    (value + align - 1) & !(align - 1)
}

/// Size of the embedded `CRITICAL_SECTION` that lives at offset zero of the
/// shared region. The payload follows (at [`payload_offset`]).
const CRIT_SECTION_SIZE: usize = std::mem::size_of::<CRITICAL_SECTION>();

/// Offset (in bytes) at which the payload begins inside the shared region.
/// The leading bytes are reserved for the guard mutex, so the payload is
/// aligned to `align_of::<T>()`.
pub const fn payload_offset<T>() -> usize
where
    T: Sized,
{
    round_up(CRIT_SECTION_SIZE, std::mem::align_of::<T>())
}

/// Total number of bytes of (shared) memory required to back one
/// `DirectShmTransport<T>` region: the embedded mutex plus the payload `T`.
pub const fn required_size<T>() -> usize
where
    T: Sized,
{
    payload_offset::<T>() + size_of::<T>()
}

/// A direct shared-memory region, guarded by a `CRITICAL_SECTION` that
/// lives *inside* the shared memory itself.
#[derive(Debug)]
pub struct DirectShmTransport<T: Sized> {
    /// Keeps the named file mapping handle alive.
    _mapping: OwnedMapping,
    /// Keeps the view of the shared region mapped.
    _view: MappedView,
    /// Set to `true` by the shared connection-loss monitor once the peer's
    /// socket signals hang-up/error.
    conn_lost: Arc<AtomicBool>,
    _marker: PhantomData<fn() -> T>,
}

unsafe impl<T: Sized> Send for DirectShmTransport<T> {}
unsafe impl<T: Sized> Sync for DirectShmTransport<T> {}

/// Configuration handle for the direct shared-memory transport (Windows).
///
/// Wraps the shared connection-loss monitor (same as `TbTransportConfig`).
#[derive(Clone)]
pub struct DirectShmTransportConfig {
    inner: TbTransportConfig,
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
            inner: TbTransportConfig::new(),
        }
    }

    /// Registers the socket with the shared connection-loss monitor and
    /// returns the flag it sets once the peer's connection is detected as
    /// lost.
    fn register(&self, socket: SOCKET) -> Arc<AtomicBool> {
        self.inner.register(socket)
    }
}

impl Default for DirectShmTransportConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// An RAII guard returned by [`DirectShmTransport::lock`]. While it is alive
/// the shared `CRITICAL_SECTION` is held, and the payload region may be
/// read and written directly as raw bytes.
#[derive(Debug)]
pub struct DirectShmGuard<'a, T: Sized> {
    region: &'a DirectShmTransport<T>,
    _marker: PhantomData<fn() -> T>,
}

impl<T: Sized> Drop for DirectShmGuard<'_, T> {
    fn drop(&mut self) {
        unsafe { LeaveCriticalSection(self.region.crit_ptr()) }
    }
}

impl<T> DirectShmTransport<T>
where
    T: Sized,
{
    /// The base address of the shared region.
    fn base(&self) -> *mut u8 {
        self._view.ptr()
    }

    /// Acquires the shared `CRITICAL_SECTION` and returns a RAII handle that
    /// grants **direct** read/write access to the payload region for as long
    /// as it is alive. The mutex is released automatically when the guard is
    /// dropped.
    pub fn lock(&self) -> DirectShmGuard<'_, T> {
        unsafe { EnterCriticalSection(self.crit_ptr()) }
        DirectShmGuard {
            region: self,
            _marker: PhantomData,
        }
    }

    /// The number of bytes the payload region occupies inside the shared
    /// region.
    pub const fn payload_size() -> usize {
        size_of::<T>()
    }

    fn crit_ptr(&self) -> *mut CRITICAL_SECTION {
        self.base().cast::<CRITICAL_SECTION>()
    }

    /// Creates a **standalone** shared region backed by a fresh named file
    /// mapping, with the embedded guard mutex already initialised and the
    /// payload seeded — **without** any network socket or `connect`/`accept`.
    pub fn standalone() -> Self
    where
        T: Clone + ByteInit + AsMut<[u8]> + AsRef<[u8]> + Sync,
    {
        let name = new_name();
        let size: u32 = required_size::<T>()
            .try_into()
            .expect("region size fits in u32");
        let (mapping, view) = create_mapping(&name, size).expect("create_mapping");
        let base: *mut u8 = view.ptr();

        // Initialise the embedded CRITICAL_SECTION once (we own the fresh region).
        unsafe { InitializeCriticalSection(base.cast::<CRITICAL_SECTION>()) };

        // Seed the payload with a known value so the first read is well-defined.
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
            _mapping: mapping,
            _view: view,
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
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        let ptr = unsafe { self.region.base().add(payload_offset::<T>()) };
        unsafe { std::slice::from_raw_parts_mut(ptr, size_of::<T>()) }
    }
}

impl<T> BulkTransport for DirectShmTransport<T>
where
    T: Clone + ByteInit + AsMut<[u8]> + AsRef<[u8]> + Sync,
{
    type Config = DirectShmTransportConfig;

    fn on_accept(socket: SOCKET) -> Result<Self, TransportError> {
        let our_size = size_of::<T>();
        send_usize(socket, our_size)?;
        let peer_size = recv_usize(socket)?;
        if peer_size != our_size {
            return Err(TransportError::BufferSizeMismatch {
                ours: our_size,
                peer: peer_size,
            });
        }

        let conn_lost = DirectShmTransportConfig::new().register(socket);

        let name = new_name();
        let size: u32 = required_size::<T>()
            .try_into()
            .expect("region size fits in u32");
        let (mapping, view) = create_mapping(&name, size)?;
        let base: *mut u8 = view.ptr();

        // Initialise the embedded CRITICAL_SECTION (we own the fresh region);
        // the peer maps the same memory and reuses it.
        unsafe { InitializeCriticalSection(base.cast::<CRITICAL_SECTION>()) };

        let initial = T::init();
        let dst = unsafe { base.add(payload_offset::<T>()) };
        unsafe {
            std::ptr::copy_nonoverlapping(initial.as_ref().as_ptr(), dst, size_of::<T>());
        }

        send_name(socket, &name)?;

        log::info!(
            "Allocated {size} bytes for direct shared-memory transport (payload={payload} bytes)",
            size = required_size::<T>(),
            payload = our_size
        );

        Ok(Self {
            _mapping: mapping,
            _view: view,
            conn_lost,
            _marker: PhantomData,
        })
    }

    fn on_connect(socket: SOCKET, config: Self::Config) -> Result<Self, TransportError> {
        let our_size = size_of::<T>();
        send_usize(socket, our_size)?;
        let peer_size = recv_usize(socket)?;
        if peer_size != our_size {
            return Err(TransportError::BufferSizeMismatch {
                ours: our_size,
                peer: peer_size,
            });
        }

        let conn_lost = config.register(socket);

        let name = recv_name(socket)?;
        let size: u32 = required_size::<T>()
            .try_into()
            .expect("region size fits in u32");
        let (mapping, view) = open_mapping(&name, size)?;

        log::info!(
            "Connected: direct shared-memory transport, {size} bytes shared (payload={payload} bytes); reusing peer's embedded mutex",
            size = required_size::<T>(),
            payload = our_size
        );

        Ok(Self {
            _mapping: mapping,
            _view: view,
            conn_lost,
            _marker: PhantomData,
        })
    }

    fn send_one_msg(&mut self, _socket: SOCKET, buf: &[u8]) -> Result<usize, SendError> {
        if self.conn_lost.load(Ordering::Relaxed) {
            log::info!("Connection loss detected");
            return Err(SendError::ConnectionEnded);
        }
        let n = size_of::<T>();
        if buf.len() > n {
            log::error!(
                "DirectShm transport: buffer too small: buf.len()={} > size_of<T>()={n}",
                buf.len()
            );
            return Err(SendError::OsBufferFull);
        }
        let mut g = self.lock();
        g.as_bytes_mut()[..buf.len()].copy_from_slice(buf);
        Ok(buf.len())
    }

    fn recv_one_msg(&mut self, _socket: SOCKET, buf: &mut [u8]) -> Result<usize, ReceiveError> {
        if self.conn_lost.load(Ordering::Relaxed) {
            log::info!("Connection loss detected");
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

/// Convenience: lock the shared region, run `f` with direct immutable byte
/// access to the payload, then release the mutex.
pub fn read_shared<T, R>(region: &DirectShmTransport<T>, f: impl FnOnce(&[u8]) -> R) -> R
where
    T: Sized,
{
    let g = region.lock();
    f(g.as_bytes())
}

/// Convenience: lock the shared region, run `f` with direct mutable byte
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
    use crate::transport::TransportListener;
    use std::time::Duration;

    const SIZE: usize = 64;
    type T = DirectShmTransport<[u8; SIZE]>;

    fn setup() -> (UnixDatagram<T>, UnixDatagram<T>) {
        let sock_addr = uuid::Uuid::new_v4().to_string();
        let mut listener = UnixDatagramListener::<T>::bind(&sock_addr).unwrap();
        let addr = sock_addr.clone();
        let client_handle = std::thread::spawn(move || {
            UnixDatagram::<T>::connect(&addr, DirectShmTransportConfig::new()).unwrap()
        });
        let server = loop {
            if let Ok(connection) = listener.accept() {
                break connection;
            }
            std::thread::sleep(Duration::from_millis(5));
        };
        (server, client_handle.join().unwrap())
    }

    #[test]
    fn handshake_ok() {
        let (server, client) = setup();
        assert_eq!(T::payload_size(), SIZE);
        drop((server, client));
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
    fn size_mismatch_detected() {
        type TWrong = DirectShmTransport<[u8; 63]>;
        let sock_addr = uuid::Uuid::new_v4().to_string();
        let mut listener = UnixDatagramListener::<T>::bind(&sock_addr).unwrap();
        let addr = sock_addr.clone();
        let client_handle = std::thread::spawn(move || {
            UnixDatagram::<TWrong>::connect(&addr, DirectShmTransportConfig::new()).unwrap_err()
        });
        // Both sides will see the mismatch; the server's accept() should
        // return the mismatch error, and the client must observe it too.
        let accept_outcome = loop {
            match listener.accept() {
                Ok(_) => break Ok(()),
                Err(TransportError::WouldBlock) => {
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(e) => break Err(e),
            }
        };
        let client_err = client_handle.join().unwrap();
        assert_eq!(
            client_err,
            TransportError::BufferSizeMismatch {
                ours: 63,
                peer: SIZE,
            }
        );
        assert_eq!(
            accept_outcome,
            Err(TransportError::BufferSizeMismatch {
                ours: SIZE,
                peer: 63,
            })
        );
    }
}
