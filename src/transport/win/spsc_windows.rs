//! A bidirectional wait-free SPSC-queue shared-memory transport for Windows.
//!
//! Mirrors [`crate::transport::spsc::SpscTransport`]: two per-direction
//! shared-memory regions are negotiated, but via **named file mappings**
//! instead of memfds — each side creates a mapping and sends its name over
//! the socket; the peer then opens the same mapping by name. The queue uses
//! the cross-platform wait-free SPSC queue in
//! [`crate::transport::algos::spsc`], so it can hold up to `CAP` messages per
//! direction in flight (not just the newest one).
//!
//! Like `TbTransport` on this platform, it plugs into the standard
//! [`BulkTransport`] machinery (`on_accept` / `on_connect`).
//!
//! # Backpressure
//!
//! If the peer's inbound queue is full, [`send_one_msg`](BulkTransport::send_one_msg)
//! returns [`SendError::OsBufferFull`], which the publisher treats as
//! "back pressure / drop and move on to the next subscriber".
use crate::transport::BulkTransport;
use crate::transport::ReceiveError;
use crate::transport::SendError;
use crate::transport::TransportError;
use crate::transport::algos::spsc;
use crate::transport::algos::triple_buffer::ByteInit;
use crate::transport::win::tb::{
    MappedView, OwnedMapping, create_mapping, open_mapping, new_name, recv_name, recv_usize,
    send_name, send_usize,
};
use crate::transport::win::tb::TbTransportConfig;
use std::mem::size_of;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use windows::Win32::Networking::WinSock::SOCKET;

/// A bidirectional wait-free SPSC-queue shared-memory transport (Windows).
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
    /// Keeps the tx-side mapping/view alive.
    _tx_mapping: OwnedMapping,
    _tx_view: MappedView,
    /// Keeps the rx-side mapping/view alive.
    _rx_mapping: OwnedMapping,
    _rx_view: MappedView,
    /// Set to `true` by the shared connection-loss monitor thread once the
    /// peer's connection is detected as lost.
    conn_is_lost: Arc<AtomicBool>,
}

/// Configuration handle for the SPSC transport (Windows).
///
/// Wraps the shared connection-loss monitor (same as `TbTransportConfig`).
#[derive(Clone)]
pub struct SpscTransportConfig {
    inner: TbTransportConfig,
}

impl SpscTransportConfig {
    pub fn new() -> Self {
        Self {
            inner: TbTransportConfig::new(),
        }
    }

    /// Registers the socket with the shared connection-loss monitor and
    /// returns the flag it sets once the peer's connection is detected as lost.
    fn register(&self, socket: SOCKET) -> Arc<AtomicBool> {
        self.inner.register(socket)
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

    fn on_accept(socket: SOCKET) -> Result<Self, TransportError> {
        // Handshake: exchange our (T size, CAP) and the peer's.
        let (ours, our_cap) = (size_of::<T>(), CAP);
        send_usize(socket, ours)?;
        send_usize(socket, our_cap)?;
        let peer_size = recv_usize(socket)?;
        let peer_cap = recv_usize(socket)?;
        if (peer_size, peer_cap) != (ours, our_cap) {
            return Err(TransportError::BufferSizeMismatch {
                ours: size_of::<T>(),
                peer: peer_size,
            });
        }

        let conn_is_lost = TbTransportConfig::new().register(socket);

        // Queue A: this side writes, peer reads.
        let tx_name = new_name();
        let size: u32 = spsc::required_size::<(usize, T), CAP>()
            .try_into()
            .unwrap();
        let (tx_mapping, tx_view) = create_mapping(&tx_name, size)?;
        let (tx, _) = unsafe { spsc::spsc_in_memory(tx_view.ptr()) };

        // Queue B: peer writes, this side reads.
        let rx_name = new_name();
        let (rx_mapping, rx_view) = create_mapping(&rx_name, size)?;
        let (_, rx) = unsafe { spsc::spsc_in_memory(rx_view.ptr()) };

        send_name(socket, &tx_name)?;
        send_name(socket, &rx_name)?;

        log::info!(
            "Allocated 2*{size} bytes for bidirectional SPSC shared-memory transport (size={}, cap={})",
            size_of::<T>(),
            CAP
        );

        Ok(Self {
            tx,
            rx,
            _tx_mapping: tx_mapping,
            _tx_view: tx_view,
            _rx_mapping: rx_mapping,
            _rx_view: rx_view,
            conn_is_lost,
        })
    }

    fn on_connect(socket: SOCKET, config: Self::Config) -> Result<Self, TransportError> {
        let (ours, our_cap) = (size_of::<T>(), CAP);
        send_usize(socket, ours)?;
        send_usize(socket, our_cap)?;
        let peer_size = recv_usize(socket)?;
        let peer_cap = recv_usize(socket)?;
        if (peer_size, peer_cap) != (ours, our_cap) {
            return Err(TransportError::BufferSizeMismatch {
                ours: size_of::<T>(),
                peer: peer_size,
            });
        }

        let size: u32 = spsc::required_size::<(usize, T), CAP>()
            .try_into()
            .unwrap();

        // Queue A name: peer's tx → our rx.
        let rx_name = recv_name(socket)?;
        let (rx_mapping, rx_view) = open_mapping(&rx_name, size)?;
        let rx = unsafe {
            spsc::Receiver::new(rx_view.ptr() as *mut spsc::Shared<(usize, T), CAP>)
        };

        // Queue B name: peer's rx → our tx.
        let tx_name = recv_name(socket)?;
        let (tx_mapping, tx_view) = open_mapping(&tx_name, size)?;
        let tx = unsafe {
            spsc::Sender::new(tx_view.ptr() as *mut spsc::Shared<(usize, T), CAP>)
        };

        let conn_is_lost = config.register(socket);

        log::info!(
            "Connected: bidirectional SPSC shared-memory transport, {size} bytes per direction (cap={})",
            CAP
        );

        Ok(Self {
            tx,
            rx,
            _tx_mapping: tx_mapping,
            _tx_view: tx_view,
            _rx_mapping: rx_mapping,
            _rx_view: rx_view,
            conn_is_lost,
        })
    }

    fn send_one_msg(&mut self, _socket: SOCKET, buf: &[u8]) -> Result<usize, SendError> {
        if self.conn_is_lost.load(Ordering::Relaxed) {
            log::info!("Connection loss detected");
            return Err(SendError::ConnectionEnded);
        }
        if buf.is_empty() {
            log::error!("SPSC transport: sending zero bytes is not allowed");
            return Err(SendError::OsBufferFull);
        }
        if buf.len() > size_of::<T>() {
            log::error!(
                "SPSC transport: message too large for this connection: {} > {}",
                buf.len(),
                size_of::<T>()
            );
            return Err(SendError::OsBufferFull);
        }
        let mut payload = T::init();
        payload.as_mut()[..buf.len()].copy_from_slice(buf);
        match self.tx.try_send((buf.len(), payload)) {
            Ok(()) => Ok(buf.len()),
            Err(spsc::SendError::NoSpaceLeft(_)) => {
                // Peer's inbound queue is full — publisher-visible backpressure.
                Err(SendError::OsBufferFull)
            }
        }
    }

    fn recv_one_msg(&mut self, _socket: SOCKET, buf: &mut [u8]) -> Result<usize, ReceiveError> {
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
    use crate::transport::{TransportConnection, TransportListener};
    use std::time::Duration;

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

        for _ in 0..4 {
            assert_eq!(client.send(b"msg"), Ok(3));
        }
        // 5th send: queue full — publisher-visible backpressure error.
        assert_eq!(client.send(b"msg"), Err(SendError::OsBufferFull));

        let mut buf = [0u8; 64];
        assert_eq!(server.recv(&mut buf), Ok(3));
        assert_eq!(client.send(b"msg"), Ok(3));
    }
}
