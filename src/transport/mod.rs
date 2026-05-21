use thiserror::Error;
#[cfg(target_family = "unix")]
pub mod uds;

#[derive(Error, Debug, PartialEq)]
#[error("System Error {0}")]
pub struct TransportError(#[from] pub nix::Error);

#[derive(Error, Debug, PartialEq)]
pub enum ReceiveError {
    #[error("System Error {0}")]
    OsError(#[from] nix::Error),
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
    #[error("System Error {0}")]
    OsError(#[from] nix::Error),
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
