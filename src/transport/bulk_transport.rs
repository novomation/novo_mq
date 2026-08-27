use crate::TransportError;
use nix::sys::socket::MsgFlags;
use std::os::fd::RawFd;

/// Hook for attaching connection-specific state/behaviour (e.g. a shared-memory
/// mapping negotiated over the socket) to a plain [`UnixDatagram`].
///
/// The default `()` implementation is a no-op, so existing code that doesn't
/// need any extra per-connection data keeps working unchanged.
pub trait BulkTransport: Sized + Send {
    type Config;
    /// Called right after the listener's `accept()` succeeds, before the
    /// connection is handed back to the caller.
    fn on_accept(fd: RawFd) -> Result<Self, TransportError>;
    /// Called right after `connect()` succeeds.
    fn on_connect(fd: RawFd, config: Self::Config) -> Result<Self, TransportError>;
    /// Called to send messages
    fn send_one_msg(&mut self, fd: RawFd, buf: &[u8], flags: MsgFlags) -> nix::Result<usize>;
    /// Called to receive messages
    fn recv_one_msg(
        &mut self,
        fd: RawFd,
        buf: &mut [u8],
        flags: MsgFlags,
    ) -> Result<usize, super::ReceiveError>;
}
