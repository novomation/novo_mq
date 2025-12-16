use thiserror::Error;
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
}

#[derive(Error, Debug, PartialEq)]
pub enum SendError {
    #[error("System Error {0}")]
    OsError(#[from] nix::Error),
    #[error("Sending zero bytes is not allowed")]
    ZeroBytes,
}

/// A `ManagedTransport` splits the transport into the managing calls and the messaging call and makes the transport shareable between threads.
pub trait ManagedTransport<M, S>
where
    M: TransportManaging + Sized,
    S: TransportMessaging + Sized,
{
    fn into_managed(self) -> (M, S);
}
pub trait TransportManaging {
    fn bind(&mut self, addr: &str) -> Result<(), TransportError>;
    fn connect(&mut self, addr: &str) -> Result<(), TransportError>;
    fn accept(&mut self) -> Result<impl TransportMessaging + 'static, TransportError>;
    // fn addr(&self) -> Option<String>;
}
pub trait TransportMessaging {
    fn addr(&self) -> Option<String>;
    fn send(&self, msg: &[u8]) -> Result<usize, SendError>;
    fn recv(&self, recv_buffer: &mut [u8]) -> Result<usize, ReceiveError>;
    // fn recv_blocking(&self, recv_buffer: &mut [u8]) -> Result<usize, ReceiveError>;

    // fn recvfrom(&self, recv_buffer: &mut [u8]) -> Result<usize, ReceiveError>;
    // fn raw_fd(&self) -> i32;
}
