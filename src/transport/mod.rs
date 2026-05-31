use thiserror::Error;
#[cfg(target_family = "unix")]
pub mod uds;
#[cfg(target_family = "windows")]
pub mod win;
pub use win as uds;

#[cfg(target_family = "unix")]
pub type OsError = nix::Error;
#[cfg(target_family = "windows")]
pub type OsError = win::WindowsSocketError;

#[derive(Error, Debug, PartialEq)]

pub enum TransportError {
    #[error("Peer does not exist")]
    PeerDoesNotExist,
    #[error("Blocking operation is still pending")]
    WouldBlock,
    #[error("TransportError {0}")]
    Os(#[from] OsError),
}

#[derive(Error, Debug, PartialEq)]
pub enum ReceiveError {
    #[error("System Error {0}")]
    OsError(#[from] OsError),
    #[error("No Message")]
    NoMessageAvailable,
    #[error("The publisher has disconnected. We are trying to establish a new connection.")]
    DisconnectDetected,
    #[error("Waiting for a publisher. Not yet connected.")]
    NotConnectedToPublisher,
    #[error("Cannot connect to subscriber.")]
    ConnectImpossible,
    #[error("Internal buffer too small.")]
    InternalBufferToSmall,
    #[error("Your external buffer is too small (buffer_len, received_len): {0:?}")]
    ExternalBufferToSmall((usize, usize)),
}

#[derive(Error, Debug, PartialEq)]
pub enum SendError {
    #[error("Buffer of der underlying OS full")]
    OsBufferFull,
    #[error("ConnectionEnded")]
    ConnectionEnded,
    #[error("System Error {0}")]
    OsError(#[from] OsError),
    #[error("Sending zero bytes is not allowed")]
    ZeroBytes,
}

pub trait TransportListener<T> {
    fn accept(&mut self) -> Result<T, TransportError>
    where
        T: TransportConnection;
}

pub trait TransportConnection {
    /// Returns the address of this socket.
    fn local_addr(&self) -> Option<String>;
    fn send(&self, msg: &[u8]) -> Result<usize, SendError>;
    fn recv(&self, recv_buffer: &mut [u8]) -> Result<usize, ReceiveError>;
}
