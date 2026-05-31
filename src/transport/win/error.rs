use crate::transport::win::winsock_error::WinsockError;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum WindowsSocketError {
    #[error("{0}")]
    Windows(#[from] windows::core::Error),
    #[error("{0}")]
    WinsockError(#[from] WinsockError),
}
