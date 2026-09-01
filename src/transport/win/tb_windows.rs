pub use crate::transport::algos::triple_buffer::ByteInit;
use crate::{
    ReceiveError, SendError,
    transport::{
        TransportError,
        algos::triple_buffer,
        win::{BulkTransport, WindowsSocketError, WinsockError, return_to_result},
    },
};
use std::{
    ffi::{CStr, CString},
    mem::size_of,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use windows::{
    Win32::{
        Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE},
        Networking::WinSock::{
            SEND_RECV_FLAGS, SOCKET, SOCKET_ERROR, WSA_IO_PENDING, WSABUF, WSAGetLastError,
            WSARecv, recv, send,
        },
        System::{
            IO::{CreateIoCompletionPort, GetQueuedCompletionStatus, OVERLAPPED},
            Memory::{
                CreateFileMappingA, FILE_MAP_ALL_ACCESS, MEMORY_MAPPED_VIEW_ADDRESS, MapViewOfFile,
                OpenFileMappingA, PAGE_READWRITE, UnmapViewOfFile,
            },
        },
    },
    core::{PCSTR, PSTR},
};

/// RAII wrapper closing a named file-mapping `HANDLE` on drop.
#[derive(Debug)]
pub(crate) struct OwnedMapping(HANDLE);
// Safety: `HANDLE` is just an opaque kernel object identifier; it has no
// thread-affinity and closing it from any thread is well-defined.
unsafe impl Send for OwnedMapping {}
impl Drop for OwnedMapping {
    fn drop(&mut self) {
        let _ = unsafe { CloseHandle(self.0) };
    }
}

/// RAII wrapper unmapping a view created by `MapViewOfFile` on drop.
#[derive(Debug)]
pub(crate) struct MappedView(MEMORY_MAPPED_VIEW_ADDRESS);
// Safety: the mapped memory is only ever accessed through the triple-buffer's
// own synchronization; the raw pointer itself has no thread-affinity.
unsafe impl Send for MappedView {}
impl Drop for MappedView {
    fn drop(&mut self) {
        let _ = unsafe { UnmapViewOfFile(self.0) };
    }
}
impl MappedView {
    /// The base address of the mapped view.
    pub(crate) fn ptr(&self) -> *mut u8 {
        self.0.Value as *mut u8
    }
}

/// Creates a new named file mapping of `size` bytes and maps it into this
/// process's address space.
pub(crate) fn create_mapping(
    name: &CStr,
    size: u32,
) -> Result<(OwnedMapping, MappedView), TransportError> {
    let mapping = unsafe {
        CreateFileMappingA(
            INVALID_HANDLE_VALUE,
            None,
            PAGE_READWRITE,
            0,
            size,
            PCSTR(name.as_ptr() as *const u8),
        )
    }
    .map_err(|err| TransportError::Os(WindowsSocketError::Windows(err)))?;
    let mapping = OwnedMapping(mapping);

    let view = unsafe { MapViewOfFile(mapping.0, FILE_MAP_ALL_ACCESS, 0, 0, size as usize) };
    if view.Value.is_null() {
        return Err(TransportError::Os(WindowsSocketError::WinsockError(
            WinsockError::Fault,
        )));
    }

    Ok((mapping, MappedView(view)))
}

/// Opens an existing named file mapping (created by the peer via
/// [`create_mapping`]) and maps it into this process's address space.
pub(crate) fn open_mapping(
    name: &CStr,
    size: u32,
) -> Result<(OwnedMapping, MappedView), TransportError> {
    let mapping = unsafe {
        OpenFileMappingA(
            FILE_MAP_ALL_ACCESS.0,
            false,
            PCSTR(name.as_ptr() as *const u8),
        )
    }
    .map_err(|err| TransportError::Os(WindowsSocketError::Windows(err)))?;
    let mapping = OwnedMapping(mapping);

    let view = unsafe { MapViewOfFile(mapping.0, FILE_MAP_ALL_ACCESS, 0, 0, size as usize) };
    if view.Value.is_null() {
        return Err(TransportError::Os(WindowsSocketError::WinsockError(
            WinsockError::Fault,
        )));
    }

    Ok((mapping, MappedView(view)))
}

/// Builds a fresh unique NUL-terminated name for a named file mapping.
pub(crate) fn new_name() -> CString {
    CString::new(uuid::Uuid::new_v4().to_string()).expect("uuid is a valid cstring")
}

/// Sends the full contents of `buf`, retrying on `WOULDBLOCK`.
fn send_all(socket: SOCKET, mut buf: &[u8]) -> Result<(), TransportError> {
    while !buf.is_empty() {
        let ret = unsafe { send(socket, buf, SEND_RECV_FLAGS(0)) };
        match return_to_result(ret) {
            Ok(_) => buf = &buf[ret as usize..],
            Err(WindowsSocketError::WinsockError(WinsockError::Wouldblock)) => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(err) => return Err(TransportError::Os(err)),
        }
    }
    Ok(())
}

/// Fills `buf` completely, retrying on `WOULDBLOCK`.
fn recv_all(socket: SOCKET, mut buf: &mut [u8]) -> Result<(), TransportError> {
    while !buf.is_empty() {
        let ret = unsafe { recv(socket, buf, SEND_RECV_FLAGS(0)) };
        match return_to_result(ret) {
            Ok(_) if ret == 0 => {
                return Err(TransportError::Os(WindowsSocketError::WinsockError(
                    WinsockError::Connreset,
                )));
            }
            Ok(_) => buf = &mut buf[ret as usize..],
            Err(WindowsSocketError::WinsockError(WinsockError::Wouldblock)) => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(err) => return Err(TransportError::Os(err)),
        }
    }
    Ok(())
}

/// Sends a NUL-terminated name, prefixed with its length, over `socket`.
pub(crate) fn send_name(socket: SOCKET, name: &CStr) -> Result<(), TransportError> {
    let bytes = name.to_bytes_with_nul();
    send_all(socket, &(bytes.len() as u16).to_le_bytes())?;
    send_all(socket, bytes)
}

/// Receives a NUL-terminated name (as sent by [`send_name`]) from `socket`.
pub(crate) fn recv_name(socket: SOCKET) -> Result<CString, TransportError> {
    let mut len_buf = [0u8; 2];
    recv_all(socket, &mut len_buf)?;
    let len = u16::from_le_bytes(len_buf) as usize;

    let mut buf = vec![0u8; len];
    recv_all(socket, &mut buf)?;
    CStr::from_bytes_with_nul(&buf)
        .map(|s| s.to_owned())
        .map_err(|_| TransportError::Os(WindowsSocketError::WinsockError(WinsockError::Fault)))
}

/// Sends a `usize` over `socket`. Used to exchange configuration values
/// (payload size, queue capacity) during the shared-memory handshake.
pub(crate) fn send_usize(socket: SOCKET, value: usize) -> Result<(), TransportError> {
    send_all(socket, &value.to_le_bytes())
}

/// Receives a `usize` sent via [`send_usize`].
pub(crate) fn recv_usize(socket: SOCKET) -> Result<usize, TransportError> {
    let mut buf = [0u8; std::mem::size_of::<usize>()];
    recv_all(socket, &mut buf)?;
    Ok(usize::from_le_bytes(buf))
}

/// Bidirectional triple-buffer transport.
///
/// Mirrors the Unix `memfd` + `triple_buffer` based shared-memory transport
/// (see `transport::uds::TbTransport` on the Unix side): instead of passing
/// file descriptors over the socket, each side creates a named file mapping
/// and sends its name to the peer, which then opens the same mapping by name.
#[derive(Debug)]
pub struct TbTransport<T: Send> {
    writer: triple_buffer::Writer<(usize, T)>,
    reader: triple_buffer::Reader<(usize, T)>,
    /// Keeps the writer-side mapping/view alive.
    _writer_mapping: OwnedMapping,
    _writer_view: MappedView,
    /// Keeps the reader-side mapping/view alive.
    _reader_mapping: OwnedMapping,
    _reader_view: MappedView,
    conn_is_lost: Arc<AtomicBool>,
}

/// RAII wrapper closing an I/O completion port `HANDLE` on drop.
struct OwnedIocp(HANDLE);
// Safety: `HANDLE` is just an opaque kernel object identifier; it has no
// thread-affinity and closing it (or using it to post/retrieve completions)
// from any thread is well-defined.
unsafe impl Send for OwnedIocp {}
unsafe impl Sync for OwnedIocp {}
impl Drop for OwnedIocp {
    fn drop(&mut self) {
        let _ = unsafe { CloseHandle(self.0) };
    }
}

/// Handle used to hand a connection's socket off to the shared
/// connection-loss monitor thread. All connections (both `on_accept` and
/// `on_connect`) are watched by the same background thread via a single I/O
/// Completion Port (the Windows analog of Unix's `epoll`-based monitor).
#[derive(Clone)]
pub struct TbTransportConfig {
    iocp: Arc<OwnedIocp>,
}

impl TbTransportConfig {
    pub fn new() -> Self {
        connection_monitor().clone()
    }

    /// Registers `socket` with the monitor thread's I/O completion port and
    /// returns the flag that will be set to `true` once the peer's
    /// connection is detected as lost.
    ///
    /// Since no application data ever flows over `socket` again once the
    /// shared-memory handshake completes, any completion on it (whether a
    /// successful zero-byte read, an error, or a hang-up) is treated
    /// uniformly as "connection lost", mirroring the Unix implementation's
    /// use of `EPOLLIN | EPOLLRDHUP | EPOLLERR | EPOLLHUP`.
    pub(crate) fn register(&self, socket: SOCKET) -> Arc<AtomicBool> {
        let conn_is_lost = Arc::new(AtomicBool::new(false));
        let key = Arc::into_raw(conn_is_lost.clone()) as usize;

        let handle = HANDLE(socket.0 as *mut core::ffi::c_void);
        if let Err(err) = unsafe { CreateIoCompletionPort(handle, Some(self.iocp.0), key, 0) } {
            log::warn!("Connection monitor: could not associate socket with IOCP: {err}");
            // Reclaim and drop the Arc we leaked above; it was never handed
            // off to the monitor thread.
            let _ = unsafe { Arc::from_raw(key as *const AtomicBool) };
            return conn_is_lost;
        }

        let overlapped = Box::into_raw(Box::new(OVERLAPPED::default()));
        let wsabuf = WSABUF {
            len: 0,
            buf: PSTR(std::ptr::null_mut()),
        };
        let mut flags = 0u32;
        let ret = unsafe {
            WSARecv(
                socket,
                std::slice::from_ref(&wsabuf),
                None,
                &mut flags,
                Some(overlapped),
                None,
            )
        };
        if ret == SOCKET_ERROR {
            let err = unsafe { WSAGetLastError() };
            if err != WSA_IO_PENDING {
                log::warn!(
                    "Connection monitor: could not watch socket {}: {err:?}",
                    socket.0
                );
                // The completion will never arrive; reclaim both allocations.
                let _ = unsafe { Box::from_raw(overlapped) };
                let _ = unsafe { Arc::from_raw(key as *const AtomicBool) };
            }
        }

        conn_is_lost
    }
}

impl Default for TbTransportConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Lazily starts the single background thread that detects connection loss
/// (via an I/O Completion Port) for every `TbTransport` connection, and
/// returns a cloneable handle to it.
fn connection_monitor() -> &'static TbTransportConfig {
    static MONITOR: OnceLock<TbTransportConfig> = OnceLock::new();
    MONITOR.get_or_init(|| {
        let iocp = unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, None, 0, 0) }
            .expect("create IO completion port");
        let iocp = Arc::new(OwnedIocp(iocp));
        let monitor_iocp = iocp.clone();
        std::thread::Builder::new()
            .name("novomq-conn-monitor".to_string())
            .spawn(move || run_connection_monitor(monitor_iocp))
            .expect("spawn connection monitor thread");
        TbTransportConfig { iocp }
    })
}

/// Body of the shared connection-loss monitor thread. Blocks on
/// `GetQueuedCompletionStatus`; every completion (successful or not) means
/// the associated socket produced activity - a hang-up, an error, or
/// (unexpectedly) incoming data - which is treated uniformly as connection
/// loss, since no application data flows over the socket after the
/// handshake.
fn run_connection_monitor(iocp: Arc<OwnedIocp>) {
    loop {
        let mut bytes = 0u32;
        let mut key = 0usize;
        let mut overlapped_ptr: *mut OVERLAPPED = std::ptr::null_mut();
        let result = unsafe {
            GetQueuedCompletionStatus(iocp.0, &mut bytes, &mut key, &mut overlapped_ptr, u32::MAX)
        };

        if overlapped_ptr.is_null() {
            // No I/O actually completed; this is a genuine wait/API failure
            // (e.g. bad handle). Log it and keep waiting.
            if let Err(err) = result {
                log::error!("Connection monitor: GetQueuedCompletionStatus failed: {err}");
            }
            continue;
        }

        // Reclaim and drop the `OVERLAPPED` allocation made in `register`.
        let _ = unsafe { Box::from_raw(overlapped_ptr) };

        // Reclaim the `Arc<AtomicBool>` smuggled through the completion key.
        let conn_is_lost = unsafe { Arc::from_raw(key as *const AtomicBool) };
        conn_is_lost.store(true, Ordering::Relaxed);
        log::info!("Connection monitor: connection lost");
    }
}

impl<T> BulkTransport for TbTransport<T>
where
    T: Send + ByteInit + AsMut<[u8]> + AsRef<[u8]> + Clone + Sized,
{
    type Config = TbTransportConfig;

    fn on_accept(socket: SOCKET) -> Result<Self, TransportError> {
        // Size check: both sides must be built with the same payload type `T`,
        // otherwise the negotiated shared-memory regions would not agree on
        // layout. Both sides always send their own size and read the peer's
        // (in opposite orders), so the error path leaves each side with a full
        // picture of the mismatch.
        let our_size = size_of::<T>();
        send_usize(socket, our_size)?;
        let peer_size = recv_usize(socket)?;
        if peer_size != our_size {
            return Err(TransportError::BufferSizeMismatch {
                ours: our_size,
                peer: peer_size,
            });
        }

        let size: u32 = triple_buffer::required_size::<(usize, T)>()
            .try_into()
            .unwrap();

        // Channel A: this side writes, peer reads.
        let writer_name = CString::new(uuid::Uuid::new_v4().to_string()).unwrap();
        let (writer_mapping, writer_view) = create_mapping(&writer_name, size)?;
        let (writer, _) = unsafe { triple_buffer::triple_buffer(writer_view.0.Value as *mut u8) };

        // Channel B: peer writes, this side reads.
        let reader_name = CString::new(uuid::Uuid::new_v4().to_string()).unwrap();
        let (reader_mapping, reader_view) = create_mapping(&reader_name, size)?;
        let (_, reader) = unsafe { triple_buffer::triple_buffer(reader_view.0.Value as *mut u8) };

        // Send channel A name first, then channel B name.
        send_name(socket, &writer_name)?;
        send_name(socket, &reader_name)?;

        log::info!("Allocated 2*{size} bytes for bidirectional shared-memory transport");

        // Only register with the connection monitor once the handshake is
        // fully done: registering earlier would post an overlapped recv that
        // races with the plain synchronous socket I/O used above, causing
        // handshake traffic to be misdetected as connection loss.
        let conn_is_lost = connection_monitor().register(socket);

        Ok(Self {
            writer,
            reader,
            _writer_mapping: writer_mapping,
            _writer_view: writer_view,
            _reader_mapping: reader_mapping,
            _reader_view: reader_view,
            conn_is_lost,
        })
    }

    fn on_connect(socket: SOCKET, config: Self::Config) -> Result<Self, TransportError> {
        // Size check (mirror of `on_accept`): read our peer's size first, then
        // send our own. See `on_accept` for the rationale.
        let our_size = size_of::<T>();
        let peer_size = recv_usize(socket)?;
        send_usize(socket, our_size)?;
        if peer_size != our_size {
            return Err(TransportError::BufferSizeMismatch {
                ours: our_size,
                peer: peer_size,
            });
        }

        let size: u32 = triple_buffer::required_size::<(usize, T)>()
            .try_into()
            .unwrap();
        log::info!("Initializing subscriber side.");

        // Channel A name: peer's writer -> our reader.
        let writer_name = recv_name(socket)?;
        log::info!("Opening writer mapping");
        let (reader_mapping, reader_view) = open_mapping(&writer_name, size)?;
        let reader = triple_buffer::Reader::new(
            reader_view.0.Value as *mut triple_buffer::Shared<(usize, T)>,
        );

        // Channel B name: peer's reader -> our writer.
        let reader_name = recv_name(socket)?;
        log::info!("Opening reader mapping");
        let (writer_mapping, writer_view) = open_mapping(&reader_name, size)?;
        let writer = triple_buffer::Writer::new(
            writer_view.0.Value as *mut triple_buffer::Shared<(usize, T)>,
        );

        log::info!("Connected: bidirectional shared-memory transport, {size} bytes per direction");

        // Only register with the connection monitor once the handshake is
        // fully done: registering earlier would post an overlapped recv that
        // races with the plain synchronous socket I/O used above, causing
        // handshake traffic to be misdetected as connection loss.
        let conn_is_lost = config.register(socket);

        Ok(Self {
            writer,
            reader,
            _writer_mapping: writer_mapping,
            _writer_view: writer_view,
            _reader_mapping: reader_mapping,
            _reader_view: reader_view,
            conn_is_lost,
        })
    }

    fn send_one_msg(&mut self, _socket: SOCKET, buf: &[u8]) -> Result<usize, SendError> {
        if self.conn_is_lost.load(Ordering::Relaxed) {
            log::info!("Connection loss detected");
            return Err(SendError::ConnectionEnded);
        }
        self.writer.write_bytes(buf);
        Ok(buf.len())
    }

    fn recv_one_msg(&mut self, _socket: SOCKET, buf: &mut [u8]) -> Result<usize, ReceiveError> {
        if self.conn_is_lost.load(Ordering::Relaxed) {
            log::info!("Connection loss detected");
            return Ok(0);
        }
        // Copy directly out of the shared-memory slot instead of cloning the
        // full `(usize, T)` value onto the stack first.
        match self.reader.read_bytes(buf) {
            Some(len) if len > buf.len() => {
                Err(ReceiveError::ExternalBufferToSmall((buf.len(), len)))
            }
            Some(len) => Ok(len),
            None => Err(ReceiveError::NoMessageAvailable),
        }
    }
}
