//! A bidirectional wait-free SPSC-queue shared-memory transport.
//!
//! This is the SPSC counterpart to [`tb::TbTransport`]:
//! both negotiate two per-direction shared-memory regions over a Unix socket
//! (memfd + `SCM_RIGHTS`), but this one uses the wait-free SPSC queue instead
//! of a latest-value triple buffer, so it can hold up to `CAP` messages per
//! direction in flight rather than only the newest one.
//!
//! Like the triple-buffer transport it plugs into the standard
//! [`BulkTransport`] machinery (`on_accept` / `on_connect`), so it can be used
//! with `Datalayer<T>` / `Publisher<T>` / `Subscriber<T>` exactly like
//! `TbTransport`.
//!
//! # Message size and capacity
//!
//! `T: AsMut<[u8]> + AsRef<[u8]> + ByteInit` is the payload type (it must be
//! a fixed-size byte buffer, e.g. `[u8; 32]`) and `CAP` is the number of
//! messages that may be queued per direction (a power of two). Both are fixed
//! at compile time; both ends of a connection must agree on both, which is
//! verified by a handshake over the socket (same mechanism as the
//! triple-buffer transport).
//!
//! # Backpressure
//!
//! One connection is a 1:1 pair (one publisher's send side to one
//! subscriber's receive side). A publisher fans out to many subscribers; if
//! one subscriber's inbound queue is full the publisher's `send` to it
//! returns [`SendError::OsBufferFull`], which the publisher already treats as
//! "back pressure / move on to the next subscriber".
use crate::ReceiveError;
use crate::transport::BulkTransport;
use crate::transport::TransportError;
use crate::transport::algos::spsc;
use crate::transport::algos::triple_buffer::ByteInit;
use crate::transport::macos::tb_macos::{
    TbTransportConfig, connection_monitor, recv_usize, send_usize, shm_helpers,
};
use memmap2::MmapMut;
use nix::errno::Errno::{ECONNRESET, ENOMEM, ENOSPC};
use nix::sys::socket::MsgFlags;
use std::mem::size_of;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// A bidirectional wait-free SPSC-queue shared-memory transport.
///
/// `T: AsMut<[u8]> + AsRef<[u8]> + ByteInit` is the fixed-size payload type
/// (e.g. `[u8; 32]`) and `CAP` is the per-direction queue capacity (a power of
/// two). Both ends of a connection must agree on both — the mismatch is
/// detected by an in-band handshake over the connection socket.
#[derive(Debug)]
pub struct SpscTransport<T, const CAP: usize> {
    /// This side produces into this queue (the peer consumes).
    tx: spsc::Sender<(usize, T), CAP>,
    /// The peer produces into this queue; this side consumes.
    rx: spsc::Receiver<(usize, T), CAP>,
    /// Keeps the tx-side mapping and its backing memfd alive.
    _tx_mmap: MmapMut,
    _tx_memfd: OwnedFd,
    /// Keeps the rx-side mapping and its backing memfd alive.
    _rx_mmap: MmapMut,
    _rx_memfd: OwnedFd,
    /// Set to `true` by the shared connection-loss monitor thread once the
    /// peer's socket signals hang-up/error.
    conn_is_lost: Arc<AtomicBool>,
}

/// Configuration handle for the SPSC transport.
///
/// Wraps the process-wide connection-loss monitor (the same one used by
/// `TbTransportConfig`) so both transports benefit from a single background
/// epoll thread watching every connection.
#[derive(Clone)]
pub struct SpscTransportConfig {
    inner: TbTransportConfig,
}

impl SpscTransportConfig {
    pub fn new() -> Self {
        Self {
            inner: connection_monitor().clone(),
        }
    }

    /// Registers `fd` with the shared connection-loss monitor thread and
    /// returns the flag that will be set once the peer's connection is lost.
    pub fn register(&self, fd: RawFd) -> Arc<AtomicBool> {
        self.inner.register(fd)
    }
}

impl Default for SpscTransportConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, const CAP: usize> BulkTransport for SpscTransport<T, CAP>
where
    T: Send + ByteInit + AsMut<[u8]> + AsRef<[u8]> + Sized,
{
    type Config = SpscTransportConfig;

    fn on_accept(fd: RawFd) -> Result<Self, TransportError> {
        // Before passing any file descriptors, make sure both sides were
        // built with the same configured payload size (`T`) and capacity
        // (`CAP`); otherwise the negotiated memfd-backed regions would not
        // agree on layout.
        let (ours, our_cap) = (size_of::<T>(), CAP);
        send_usize(fd, ours)?;
        send_usize(fd, our_cap)?;
        let peer_size = recv_usize(fd)?;
        let peer_cap = recv_usize(fd)?;
        if (peer_size, peer_cap) != (ours, our_cap) {
            return Err(TransportError::BufferSizeMismatch {
                ours,
                peer: peer_size,
            });
        }

        let conn_is_lost = connection_monitor().register(fd);

        // Queue A: this side writes, peer reads (our tx / peer rx).
        let writer_memfd = shm_helpers::create_memfd(
            spsc::required_size::<(usize, T), CAP>(),
            &uuid::Uuid::new_v4().to_string(),
        )?;
        let mut writer_mmap = unsafe { MmapMut::map_mut(&writer_memfd).unwrap() };
        let (writer, _) = unsafe { spsc::spsc_in_memory(writer_mmap.as_mut_ptr()) };

        // Queue B: peer writes, this side reads (our rx / peer tx).
        let reader_memfd = shm_helpers::create_memfd(
            spsc::required_size::<(usize, T), CAP>(),
            &uuid::Uuid::new_v4().to_string(),
        )?;
        let mut reader_mmap = unsafe { MmapMut::map_mut(&reader_memfd).unwrap() };
        let (_, reader) = unsafe { spsc::spsc_in_memory(reader_mmap.as_mut_ptr()) };

        // Send queue A's fd first, then queue B's fd.
        shm_helpers::send_memfd(fd, writer_memfd.as_raw_fd()).map_err(|e| {
            log::error!("Error sending memfd: {e}");
            nix::errno::Errno::EINVAL
        })?;
        shm_helpers::send_memfd(fd, reader_memfd.as_raw_fd()).map_err(|e| {
            log::error!("Error sending memfd: {e}");
            nix::errno::Errno::EINVAL
        })?;

        log::info!(
            "Allocated 2*{} bytes for bidirectional SPSC shared-memory transport (size={}, cap={})",
            spsc::required_size::<(usize, T), CAP>(),
            size_of::<T>(),
            CAP
        );
        Ok(Self {
            tx: writer,
            rx: reader,
            _tx_mmap: writer_mmap,
            _tx_memfd: writer_memfd,
            _rx_mmap: reader_mmap,
            _rx_memfd: reader_memfd,
            conn_is_lost,
        })
    }

    fn on_connect(fd: RawFd, config: Self::Config) -> Result<Self, TransportError> {
        let (ours, our_cap) = (size_of::<T>(), CAP);
        send_usize(fd, ours)?;
        send_usize(fd, our_cap)?;
        let peer_size = recv_usize(fd)?;
        let peer_cap = recv_usize(fd)?;
        if (peer_size, peer_cap) != (ours, our_cap) {
            return Err(TransportError::BufferSizeMismatch {
                ours,
                peer: peer_size,
            });
        }

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

        // Queue A's fd first: this is the peer's tx → our rx.
        let reader_memfd = recv()?;
        let mut reader_mmap = unsafe { MmapMut::map_mut(&reader_memfd).unwrap() };
        let (_, reader) = unsafe { spsc::spsc_wrap(reader_mmap.as_mut_ptr()) };

        // Queue B's fd second: this is the peer's rx → our tx.
        let writer_memfd = recv()?;
        let mut writer_mmap = unsafe { MmapMut::map_mut(&writer_memfd).unwrap() };
        let (writer, _) = unsafe { spsc::spsc_wrap(writer_mmap.as_mut_ptr()) };

        let conn_is_lost = config.register(fd);

        log::info!(
            "Connected: bidirectional SPSC shared-memory transport, {} bytes per direction",
            spsc::required_size::<(usize, T), CAP>()
        );
        Ok(Self {
            tx: writer,
            rx: reader,
            _tx_mmap: writer_mmap,
            _tx_memfd: writer_memfd,
            _rx_mmap: reader_mmap,
            _rx_memfd: reader_memfd,
            conn_is_lost,
        })
    }

    fn send_one_msg(&mut self, _fd: RawFd, buf: &[u8], _flags: MsgFlags) -> nix::Result<usize> {
        if self.conn_is_lost.load(Ordering::Relaxed) {
            log::info!("Connection loss detected");
            return Err(ECONNRESET);
        }
        if buf.is_empty() {
            return Err(ENOMEM);
        }
        if buf.len() > size_of::<T>() {
            log::error!(
                "Message too large for the SPSC transport: {} > {}",
                buf.len(),
                size_of::<T>()
            );
            return Err(ENOMEM);
        }
        // Build the payload directly in a stack copy (fixed size), then hand it
        // to the queue with its byte length, mirroring the triple buffer's
        // `(usize, T)` layout.
        let mut payload = T::init();
        payload.as_mut()[..buf.len()].copy_from_slice(buf);
        match self.tx.try_send((buf.len(), payload)) {
            Ok(()) => Ok(buf.len()),
            Err(spsc::SendError::NoSpaceLeft(_)) => {
                // The other end's queue is full → back pressure. The publisher
                // treats `ENOSPC` as "OS buffer full / drop and move on".
                Err(ENOSPC)
            }
        }
    }

    fn recv_one_msg(
        &mut self,
        _fd: RawFd,
        buf: &mut [u8],
        _flags: MsgFlags,
    ) -> Result<usize, ReceiveError> {
        if self.conn_is_lost.load(Ordering::Relaxed) {
            log::info!("Connection loss detected");
            return Ok(0);
        }
        match self.rx.try_recv() {
            Ok((msg_len, payload)) => {
                let src: &[u8] = payload.as_ref();
                let copy_len = src.len().min(msg_len).min(buf.len());
                buf[..copy_len].copy_from_slice(&src[..copy_len]);
                if msg_len > buf.len() {
                    Err(ReceiveError::ExternalBufferToSmall((buf.len(), msg_len)))
                } else {
                    Ok(msg_len)
                }
            }
            Err(spsc::TryRecvError::Empty) => Err(ReceiveError::NoMessageAvailable),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::uds::{UnixDatagram, UnixDatagramListener};
    use crate::transport::{
        ReceiveError, SendError, TransportConnection, TransportError, TransportListener,
    };
    use std::time::Duration;
    use test_log::test;

    type SpscT = SpscTransport<[u8; 32], 4>;

    fn accept_one(listener: &mut UnixDatagramListener<SpscT>) -> UnixDatagram<SpscT> {
        loop {
            if let Ok(connection) = listener.accept() {
                return connection;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn test_bind() {
        let sock_addr = uuid::Uuid::new_v4().to_string();
        let listener = UnixDatagramListener::<SpscT>::bind(&sock_addr).unwrap();
        assert_eq!(listener.local_addr(), Some(sock_addr));
    }

    #[test]
    fn test_connect_no_existing() {
        assert_eq!(
            UnixDatagram::<SpscT>::connect("spsc-nothing", SpscTransportConfig::new()).unwrap_err(),
            TransportError::PeerDoesNotExist
        );
    }

    #[test]
    fn test_would_block_on_accept() {
        let sock_addr = uuid::Uuid::new_v4().to_string();
        let mut listener = UnixDatagramListener::<SpscT>::bind(&sock_addr).unwrap();
        assert_eq!(listener.accept().unwrap_err(), TransportError::WouldBlock);
    }

    #[test]
    fn smoke_handshake() {
        let sock_addr = uuid::Uuid::new_v4().to_string();
        let mut listener = UnixDatagramListener::<SpscT>::bind(&sock_addr).unwrap();
        let client_addr = sock_addr.clone();
        let client_handle = std::thread::spawn(move || {
            UnixDatagram::<SpscT>::connect(&client_addr, SpscTransportConfig::new()).unwrap()
        });
        let connection = accept_one(&mut listener);
        let client = client_handle.join().unwrap();
        assert_eq!(connection.local_addr(), Some(sock_addr.clone()));
        assert_eq!(client.peer_addr(), Some(sock_addr));
        drop(client);
        drop(connection);
    }

    #[test]
    fn send_recv() {
        let sock_addr = uuid::Uuid::new_v4().to_string();
        let mut listener = UnixDatagramListener::<SpscT>::bind(&sock_addr).unwrap();
        let client_addr = sock_addr.clone();
        let client_handle = std::thread::spawn(move || {
            UnixDatagram::<SpscT>::connect(&client_addr, SpscTransportConfig::new()).unwrap()
        });
        let mut server = accept_one(&mut listener);
        let mut client = client_handle.join().unwrap();

        assert_eq!(client.send(b"hello"), Ok(5));
        let mut buf = [0u8; 64];
        assert_eq!(server.recv(&mut buf), Ok(5));
        assert_eq!(&buf[..5], b"hello");
        assert_eq!(server.recv(&mut buf), Err(ReceiveError::NoMessageAvailable));
    }

    #[test]
    fn queues_multiple_and_delivers_in_order() {
        let sock_addr = uuid::Uuid::new_v4().to_string();
        let mut listener = UnixDatagramListener::<SpscT>::bind(&sock_addr).unwrap();
        let client_addr = sock_addr.clone();
        let client_handle = std::thread::spawn(move || {
            UnixDatagram::<SpscT>::connect(&client_addr, SpscTransportConfig::new()).unwrap()
        });
        let mut server = accept_one(&mut listener);
        let mut client = client_handle.join().unwrap();

        // Send CAP messages back-to-back without receiving; the SPSC queue
        // (unlike the latest-value triple buffer) must retain them all in order.
        for i in 0..4usize {
            client.send(format!("msg{i}").as_bytes()).unwrap();
        }
        for i in 0..4usize {
            let mut buf = [0u8; 64];
            let len = server.recv(&mut buf).unwrap();
            assert_eq!(&buf[..len], format!("msg{i}").as_bytes());
        }
    }

    #[test]
    fn backpressure_when_queue_full() {
        let sock_addr = uuid::Uuid::new_v4().to_string();
        let mut listener = UnixDatagramListener::<SpscT>::bind(&sock_addr).unwrap();
        let client_addr = sock_addr.clone();
        let client_handle = std::thread::spawn(move || {
            UnixDatagram::<SpscT>::connect(&client_addr, SpscTransportConfig::new()).unwrap()
        });
        let mut server = accept_one(&mut listener);
        let mut client = client_handle.join().unwrap();

        // Fill the 4-slot inbound queue on the server.
        for _ in 0..4 {
            assert_eq!(client.send(b"msg"), Ok(3));
        }
        // 5th send: queue full → publisher-visible backpressure error.
        assert_eq!(client.send(b"msg"), Err(SendError::OsBufferFull));

        // Draining one message frees a slot; the next send succeeds again.
        let mut buf = [0u8; 64];
        assert_eq!(server.recv(&mut buf), Ok(3));
        assert_eq!(client.send(b"msg"), Ok(3));
    }
}
