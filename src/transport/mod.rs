use std::io;
use thiserror::Error;
pub mod uds;

#[derive(Error, Debug)]
pub enum TransportError {
    #[error("System Error {0}")]
    OsError(#[from] nix::Error),
    #[error("Io Error {0}")]
    IoError(#[from] io::Error),
}
impl PartialEq for TransportError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::OsError(l0), Self::OsError(r0)) => l0 == r0,
            (Self::IoError(l0), Self::IoError(r0)) => l0.kind() == r0.kind(),
            _ => false,
        }
    }
}

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
}

#[derive(Error, Debug, PartialEq)]
pub enum SendError {
    #[error("System Error {0}")]
    OsError(#[from] nix::Error),
    #[error("Zero bytes are not allowed")]
    ZeroBytes,
}

/// A `ManagedTransport` splits the transport into the managing calls and the messaging call and makes the transport shareable between threads.
pub trait ManagedTransport<M, S>
where
    M: TransportManaging + Sized,
    S: TransportMessaging + Sized,
{
    fn into_managed(self) -> (M, S);
    fn into_managed_boxed(self) -> (Box<M>, Box<S>)
    where
        Self: Sized,
    {
        let (a, b) = self.into_managed();
        (Box::new(a), Box::new(b))
    }
}
pub trait TransportManaging {
    fn bind(&mut self, addr: &str) -> Result<(), TransportError>;
    fn connect(&mut self, addr: &str) -> Result<(), TransportError>;
    fn accept(&mut self) -> Result<impl TransportMessaging + 'static, TransportError>;
    // fn get_peer_addr(&self) -> Option<String>;
    fn addr(&self) -> Option<String>;
}
pub trait TransportMessaging {
    fn addr(&self) -> Option<String>;

    fn send(&self, msg: &[u8]) -> Result<usize, SendError>;
    fn recv(&self, recv_buffer: &mut [u8]) -> Result<usize, ReceiveError>;
    fn recv_blocking(&self, recv_buffer: &mut [u8]) -> Result<usize, ReceiveError>;

    fn recvfrom(&self, recv_buffer: &mut [u8]) -> Result<usize, ReceiveError>;
    fn raw_fd(&self) -> i32;
}
