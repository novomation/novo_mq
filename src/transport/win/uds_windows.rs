use crate::{
    ReceiveError, SendError,
    transport::{
        MAX_CONNECTIONS, TransportConnection, TransportError, TransportListener, triple_buffer,
        win::{WindowsSocketError, WinsockError},
    },
};
use log::debug;
use std::{
    ffi::{CStr, CString},
    path::Path,
    str::FromStr,
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
            ADDRESS_FAMILY, AF_UNIX, FD_SET, FIONBIO, SEND_RECV_FLAGS, SOCK_STREAM, SOCKADDR,
            SOCKADDR_UN, SOCKET, SOCKET_ERROR, WSA_IO_PENDING, WSABUF, WSADATA, WSAGetLastError,
            WSARecv, WSAStartup, accept, bind, closesocket, connect, getpeername, getsockname,
            ioctlsocket, listen, recv, select, send, socket,
        },
        System::{
            IO::{CreateIoCompletionPort, GetQueuedCompletionStatus, OVERLAPPED},
            Memory::{
                CreateFileMappingA, FILE_MAP_ALL_ACCESS, MEMORY_MAPPED_VIEW_ADDRESS, MapViewOfFile,
                OpenFileMappingA, PAGE_READWRITE, UnmapViewOfFile,
            },
        },
    },
    core::{HRESULT, PCSTR, PSTR},
};

fn return_to_result(ret: i32) -> Result<(), WindowsSocketError> {
    if ret == SOCKET_ERROR {
        let err = unsafe { WSAGetLastError() };
        Err(WindowsSocketError::WinsockError(
            WinsockError::try_from(err).unwrap(),
        ))
    } else {
        Ok(())
    }
}

fn create_socket() -> Result<SOCKET, WindowsSocketError> {
    unsafe {
        // must init winsock once in your program
        let mut wsa_data = WSADATA::default();
        WSAStartup(2u16 << 8 | 2u16, &mut wsa_data);
    }

    let fd =
        unsafe { socket(AF_UNIX as i32, SOCK_STREAM, 0) }.map_err(WindowsSocketError::Windows)?;
    set_nonblocking(fd, true)?;
    Ok(fd)
}
fn set_nonblocking(sock: SOCKET, nonblocking: bool) -> Result<(), WindowsSocketError> {
    let mut mode: u32 = if nonblocking { 1 } else { 0 };

    let ret = unsafe { ioctlsocket(sock, FIONBIO, &mut mode) };
    return_to_result(ret)
}
fn sockaddr_un_new(addr: &str) -> Result<SOCKADDR_UN, WindowsSocketError> {
    let mut unix_addr = SOCKADDR_UN {
        sun_family: ADDRESS_FAMILY(AF_UNIX),
        sun_path: [0; _],
    };

    let c_addr = CString::from_str(addr)
        .map_err(|_| WindowsSocketError::WinsockError(WinsockError::InvalidParameter))?;
    // let min_len = addr.len().min(107);
    // // let src: &[i8] = unsafe { &*(&addr.as_bytes()[..min_len] as *const _ as *const [i8]) };
    if c_addr.as_bytes_with_nul().len() > unix_addr.sun_path.len() {
        return Err(WindowsSocketError::WinsockError(
            WinsockError::InvalidParameter,
        ));
    }

    unsafe {
        core::ptr::copy_nonoverlapping(
            c_addr.as_ptr(),
            unix_addr.sun_path.as_mut_ptr(),
            c_addr.as_bytes_with_nul().len(),
        )
    };

    Ok(unix_addr)
}

fn sockaddr_un_to_string(addr: &SOCKADDR_UN) -> Option<String> {
    debug!("to_string: len_w0={}", sockaddr_len(addr));
    let c_addr = CStr::from_bytes_until_nul(bytes_from_i8(&addr.sun_path)).ok()?;
    Some(c_addr.to_str().ok()?.to_string())
}

fn sockaddr_len(addr: &SOCKADDR_UN) -> usize {
    let c_addr = CStr::from_bytes_until_nul(bytes_from_i8(&addr.sun_path))
        .ok()
        .unwrap();
    c_addr.to_bytes_with_nul().len()
}

fn local_addr(socket: SOCKET) -> Option<String> {
    let mut addr = SOCKADDR_UN {
        ..Default::default()
    };
    let mut len = size_of_val(&addr) as i32;

    let ret = unsafe { getsockname(socket, &mut addr as *mut _ as *mut SOCKADDR, &mut len) };

    if return_to_result(ret).is_err() {
        return None;
    }
    sockaddr_un_to_string(&addr)
}

fn peer_addr(socket: SOCKET) -> Option<String> {
    let mut addr = SOCKADDR_UN {
        ..Default::default()
    };
    let mut len = size_of_val(&addr) as i32;

    let ret = unsafe { getpeername(socket, &mut addr as *mut _ as *mut SOCKADDR, &mut len) };

    if return_to_result(ret).is_err() {
        return None;
    }
    sockaddr_un_to_string(&addr)
}

fn bytes_from_i8(src: &[i8]) -> &[u8] {
    unsafe { &*(src as *const _ as *const [u8]) }
}

fn prepare_bind_path(addr: &str) -> Result<(), TransportError> {
    let addr_path = Path::new(addr);
    if !addr_path.exists() {
        return Ok(());
    }

    // Guard against deleting non-file paths such as directories.
    if !addr_path.is_file() {
        return Err(TransportError::AddressInUse);
    }

    let probe_socket = OwnedSocket(create_socket()?);
    let unix_addr = sockaddr_un_new(addr)?;
    let ret = unsafe {
        connect(
            probe_socket.0,
            &unix_addr as *const _ as *const SOCKADDR,
            size_of_val(&unix_addr) as i32,
        )
    };

    match return_to_result(ret) {
        Ok(_) => Err(TransportError::AddressInUse),
        Err(WindowsSocketError::WinsockError(WinsockError::Wouldblock))
        | Err(WindowsSocketError::WinsockError(WinsockError::Inprogress))
        | Err(WindowsSocketError::WinsockError(WinsockError::Already))
        | Err(WindowsSocketError::WinsockError(WinsockError::Isconn)) => {
            Err(TransportError::AddressInUse)
        }
        Err(WindowsSocketError::WinsockError(WinsockError::Connrefused)) => {
            std::fs::remove_file(addr).map_err(|_| TransportError::AddressInUse)?;
            log::warn!("Removed stale Unix socket file at {addr}");
            Ok(())
        }
        Err(err) => Err(TransportError::Os(err)),
    }
}

#[derive(Debug)]
pub struct OwnedSocket(SOCKET);
impl Drop for OwnedSocket {
    fn drop(&mut self) {
        let ret = unsafe { closesocket(self.0) };
        return_to_result(ret).unwrap();
    }
}

/// Hook for attaching connection-specific state/behaviour (e.g. a shared-memory
/// mapping negotiated over the socket) to a plain [`UnixDatagram`].
///
/// The default `()` implementation is a no-op, so existing code that doesn't
/// need any extra per-connection data keeps working unchanged.
pub trait BulkTransport: Sized + Send {
    type Config;
    /// Called right after the listener's `accept()` succeeds, before the
    /// connection is handed back to the caller.
    fn on_accept(socket: SOCKET) -> Result<Self, TransportError>;
    /// Called right after `connect()` succeeds.
    fn on_connect(socket: SOCKET, config: Self::Config) -> Result<Self, TransportError>;
    /// Called to send a message.
    fn send_one_msg(&mut self, socket: SOCKET, buf: &[u8]) -> Result<usize, SendError>;
    /// Called to receive a message.
    fn recv_one_msg(&mut self, socket: SOCKET, buf: &mut [u8]) -> Result<usize, ReceiveError>;
}

/// RAII wrapper closing a named file-mapping `HANDLE` on drop.
#[derive(Debug)]
struct OwnedMapping(HANDLE);
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
struct MappedView(MEMORY_MAPPED_VIEW_ADDRESS);
// Safety: the mapped memory is only ever accessed through the triple-buffer's
// own synchronization; the raw pointer itself has no thread-affinity.
unsafe impl Send for MappedView {}
impl Drop for MappedView {
    fn drop(&mut self) {
        let _ = unsafe { UnmapViewOfFile(self.0) };
    }
}

/// Creates a new named file mapping of `size` bytes and maps it into this
/// process's address space.
fn create_mapping(name: &CStr, size: u32) -> Result<(OwnedMapping, MappedView), TransportError> {
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
fn open_mapping(name: &CStr, size: u32) -> Result<(OwnedMapping, MappedView), TransportError> {
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
fn send_name(socket: SOCKET, name: &CStr) -> Result<(), TransportError> {
    let bytes = name.to_bytes_with_nul();
    send_all(socket, &(bytes.len() as u16).to_le_bytes())?;
    send_all(socket, bytes)
}

/// Receives a NUL-terminated name (as sent by [`send_name`]) from `socket`.
fn recv_name(socket: SOCKET) -> Result<CString, TransportError> {
    let mut len_buf = [0u8; 2];
    recv_all(socket, &mut len_buf)?;
    let len = u16::from_le_bytes(len_buf) as usize;

    let mut buf = vec![0u8; len];
    recv_all(socket, &mut buf)?;
    CStr::from_bytes_with_nul(&buf)
        .map(|s| s.to_owned())
        .map_err(|_| TransportError::Os(WindowsSocketError::WinsockError(WinsockError::Fault)))
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
    fn register(&self, socket: SOCKET) -> Arc<AtomicBool> {
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

impl<T: Send> BulkTransport for TbTransport<T>
where
    T: Default + AsMut<[u8]> + AsRef<[u8]> + Clone + Sized,
{
    type Config = TbTransportConfig;

    fn on_accept(socket: SOCKET) -> Result<Self, TransportError> {
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

impl BulkTransport for () {
    type Config = ();
    fn on_accept(_socket: SOCKET) -> Result<Self, TransportError> {
        Ok(())
    }
    fn on_connect(_socket: SOCKET, _config: Self::Config) -> Result<Self, TransportError> {
        Ok(())
    }

    fn send_one_msg(&mut self, socket: SOCKET, msg: &[u8]) -> Result<usize, SendError> {
        let ret = unsafe {
            send(
                socket,
                &(msg.len() as u16).to_le_bytes(),
                SEND_RECV_FLAGS(0),
            )
        };
        match return_to_result(ret) {
            Ok(_) => (),
            Err(WindowsSocketError::WinsockError(WinsockError::Connreset)) => {
                return Err(SendError::ConnectionEnded);
            }
            Err(err) => return Err(SendError::OsError(err)),
        }
        assert_eq!(ret, 2);
        debug!("send len={}", msg.len());

        let ret = unsafe { send(socket, msg, SEND_RECV_FLAGS(0)) };
        match return_to_result(ret) {
            Ok(_) => (),
            Err(WindowsSocketError::WinsockError(WinsockError::Connreset)) => {
                return Err(SendError::ConnectionEnded);
            }
            Err(err) => return Err(SendError::OsError(err)),
        }
        debug!("send msg={:?}", msg);
        assert_eq!(ret, msg.len() as i32);

        Ok(ret as usize)
    }

    fn recv_one_msg(
        &mut self,
        socket: SOCKET,
        recv_buffer: &mut [u8],
    ) -> Result<usize, ReceiveError> {
        let mut len = [0u8; 2];
        let ret = unsafe { recv(socket, &mut len, SEND_RECV_FLAGS(0)) };
        match return_to_result(ret) {
            Ok(_) => (),
            Err(WindowsSocketError::WinsockError(WinsockError::Wouldblock)) => {
                return Err(ReceiveError::NoMessageAvailable);
            }
            Err(err) => return Err(ReceiveError::OsError(err)),
        }
        if ret == 0 {
            return Ok(0);
        }
        assert_eq!(ret, 2);
        let len = u16::from_le_bytes(len);
        debug!("recv len={len}");
        loop {
            let len_min = (len as usize).min(recv_buffer.len());
            let ret = unsafe { recv(socket, &mut recv_buffer[0..len_min], SEND_RECV_FLAGS(0)) };
            match return_to_result(ret) {
                Ok(_) => (),
                Err(WindowsSocketError::WinsockError(WinsockError::Wouldblock)) => {
                    continue;
                }
                Err(err) => return Err(ReceiveError::OsError(err)),
            }
            debug!("recv buf={:?} ret={ret}", &recv_buffer[0..ret as usize]);
            if ret < len as i32 {
                // clear pipe be drop bytes
                let mut buf = [0u8; 32];
                let mut len_left = len as i32 - ret;
                loop {
                    if len_left == 0 {
                        break;
                    }
                    let len_this_iteration = len_left.min(buf.len() as i32);
                    let ret2 = unsafe {
                        recv(
                            socket,
                            &mut buf[0..len_this_iteration as usize],
                            SEND_RECV_FLAGS(0),
                        )
                    };
                    match return_to_result(ret2) {
                        Ok(_) => (),
                        Err(WindowsSocketError::WinsockError(WinsockError::Wouldblock)) => {
                            continue;
                        }
                        Err(err) => return Err(ReceiveError::OsError(err)),
                    }
                    len_left = len_left.saturating_sub(ret2);
                }

                return Err(ReceiveError::ExternalBufferToSmall((
                    recv_buffer.len(),
                    len as usize,
                )));
            }

            return Ok(ret as usize);
        }
    }
}

#[derive(Debug)]
pub struct UnixDatagramListener<E: BulkTransport = ()> {
    socket: OwnedSocket,
    local_addr: String,
    _marker: std::marker::PhantomData<E>,
}
impl<E: BulkTransport> Drop for UnixDatagramListener<E> {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.local_addr);
    }
}
impl<E: BulkTransport> UnixDatagramListener<E> {
    pub fn bind(addr: &str) -> Result<Self, TransportError> {
        prepare_bind_path(addr)?;
        let fd = create_socket()?;
        let unix_addr = sockaddr_un_new(addr)?;

        let ret = unsafe {
            bind(
                fd,
                &unix_addr as *const _ as *const SOCKADDR,
                size_of_val(&unix_addr) as i32,
            )
        };
        match return_to_result(ret) {
            Ok(_) => (),
            Err(WindowsSocketError::WinsockError(WinsockError::Addrinuse)) => {
                return Err(TransportError::AddressInUse);
            }
            Err(err) => return Err(TransportError::Os(err)),
        }
        log::debug!("UnixDatagramListener bound to {addr}");
        let ret = unsafe { listen(fd, MAX_CONNECTIONS) };
        return_to_result(ret)?;

        Ok(UnixDatagramListener {
            socket: OwnedSocket(fd),
            local_addr: addr.to_string(),
            _marker: std::marker::PhantomData,
        })
    }
}
impl<E: BulkTransport> TransportListener<UnixDatagram<E>> for UnixDatagramListener<E> {
    fn accept(&mut self) -> Result<UnixDatagram<E>, TransportError> {
        let socket = unsafe { accept(self.socket.0, None, None) }
            .map_err(|err| {
                if err.code() == HRESULT(-2147014861) {
                    TransportError::WouldBlock
                } else {
                    TransportError::Os(WindowsSocketError::Windows(err))
                }
            })
            .inspect_err(|e| log::debug!("Error accepting: {:?}", e))?;
        let extra = E::on_accept(socket)?;
        Ok(UnixDatagram {
            socket: OwnedSocket(socket),
            extra,
        })
    }

    fn local_addr(&self) -> Option<String> {
        local_addr(self.socket.0)
    }
}

#[derive(Debug)]
pub struct UnixDatagram<E: BulkTransport = ()> {
    socket: OwnedSocket,
    pub extra: E,
}
impl<E: BulkTransport> Drop for UnixDatagram<E> {
    fn drop(&mut self) {
        if let Some(addr) = local_addr(self.socket.0) {
            let _ = std::fs::remove_file(&addr);
        }
    }
}
impl<E: BulkTransport> UnixDatagram<E> {
    pub fn connect(addr: &str, config: E::Config) -> Result<Self, TransportError> {
        let fd = create_socket()?;
        let unix_addr = sockaddr_un_new(addr)?;

        let ret = unsafe {
            connect(
                fd,
                &unix_addr as *const _ as *const SOCKADDR,
                size_of_val(&unix_addr) as i32,
            )
        };
        match return_to_result(ret) {
            Ok(_) => {
                return Err(TransportError::Os(WindowsSocketError::WinsockError(
                    WinsockError::Fault,
                )));
            }
            Err(WindowsSocketError::WinsockError(WinsockError::Wouldblock)) => {
                // Do nothing here. We wait with select
            }
            Err(WindowsSocketError::WinsockError(WinsockError::Connrefused)) => {
                return Err(TransportError::PeerDoesNotExist);
            }
            Err(err) => {
                return Err(TransportError::Os(err));
            }
        }
        let mut writefds = FD_SET {
            fd_count: 1,
            fd_array: [fd; 64],
        };

        unsafe { select(0, None, Some(&mut writefds), None, None) };
        log::info!("Calling inner connection method");
        let extra = E::on_connect(fd, config)?;
        let ud = UnixDatagram {
            socket: OwnedSocket(fd),
            extra,
        };
        log::info!("connect: Now connected to {:?}", ud.peer_addr());
        Ok(ud)
    }
}
impl<E: BulkTransport> TransportConnection for UnixDatagram<E> {
    fn local_addr(&self) -> Option<String> {
        local_addr(self.socket.0)
    }
    fn peer_addr(&self) -> Option<String> {
        peer_addr(self.socket.0)
    }
    fn send(&mut self, msg: &[u8]) -> Result<usize, SendError> {
        self.extra.send_one_msg(self.socket.0, msg)
    }

    fn recv(&mut self, recv_buffer: &mut [u8]) -> Result<usize, ReceiveError> {
        self.extra.recv_one_msg(self.socket.0, recv_buffer)
    }
}
