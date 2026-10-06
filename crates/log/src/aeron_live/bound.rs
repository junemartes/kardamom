//! The control address the media driver binds for a dynamic MDC
//! publication whose control endpoint names port 0.

use std::ffi::CStr;
use std::net::SocketAddr;
use std::ops::ControlFlow;
use std::time::{Duration, Instant};

use super::{BIND_TIMEOUT, Pub};
use crate::error::LogError;

/// The size of the buffer that receives the address text. A counter key
/// holds at most 112 bytes, so any address and its NUL fit.
const ADDRESS_CAPACITY: usize = 128;

/// The pause between two reads of the bound address.
const POLL_PAUSE: Duration = Duration::from_millis(1);

/// One wait for the bound control address of `publication`. The driver
/// publishes the address in a local-sockaddr counter and marks the
/// counter active once the socket is bound. Until then a read finds no
/// address.
pub(super) struct BoundControl<'a> {
    publication: &'a Pub,
    uri: &'a str,
    deadline: Instant,
}

impl<'a> BoundControl<'a> {
    /// A wait that ends [`BIND_TIMEOUT`] from now.
    pub(super) fn new(publication: &'a Pub, uri: &'a str) -> Self {
        Self {
            publication,
            uri,
            deadline: Instant::now() + BIND_TIMEOUT,
        }
    }

    /// Read until the driver reports the bound control address, or until
    /// the deadline passes. This blocks the Aeron thread, so it runs only
    /// when a publication opens.
    pub(super) fn wait(&self) -> Result<SocketAddr, LogError> {
        loop {
            if let ControlFlow::Break(result) = self.step() {
                return result;
            }
        }
    }

    /// One read. `Break` carries the address, a read error, or a timeout.
    /// `Continue` means the caller reads again, after this pauses for
    /// [`POLL_PAUSE`].
    fn step(&self) -> ControlFlow<Result<SocketAddr, LogError>> {
        if let Some(done) = self.read().transpose() {
            return ControlFlow::Break(done);
        }
        if Instant::now() > self.deadline {
            return ControlFlow::Break(Err(LogError::Aeron(format!(
                "{}: the driver reported no bound control address",
                self.uri
            ))));
        }
        std::thread::sleep(POLL_PAUSE);
        ControlFlow::Continue(())
    }

    /// The bound control address, or `None` while the counter is not
    /// active.
    fn read(&self) -> Result<Option<SocketAddr>, LogError> {
        let mut text = [0u8; ADDRESS_CAPACITY];
        let iov = rusteron_client::AeronIovec::new(text.as_mut_ptr(), text.len())
            .map_err(|e| LogError::Aeron(format!("{}: address buffer: {e}", self.uri)))?;
        let found = self
            .publication
            .local_sockaddrs(&iov, 1)
            .map_err(|e| LogError::Aeron(format!("{}: local_sockaddrs: {e}", self.uri)))?;
        if found == 0 {
            return Ok(None);
        }
        let address = CStr::from_bytes_until_nul(&text)
            .ok()
            .and_then(|c| c.to_str().ok())
            .ok_or_else(|| {
                LogError::Aeron(format!(
                    "{}: the bound address is not a UTF-8 C string",
                    self.uri
                ))
            })?;
        address
            .parse()
            .map(Some)
            .map_err(|e| LogError::Aeron(format!("{}: bound address {address}: {e}", self.uri)))
    }
}
