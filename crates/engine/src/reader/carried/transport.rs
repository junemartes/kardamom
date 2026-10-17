//! [`Transport`]: one source of the canonical stream with its bytes. The
//! member feed and the serving service implement it. Both send the frames
//! of the member feed, byte for byte, so one reader reads both.

use std::time::Duration;

/// A replay request: replay from the cursor `(from_index, from_block)`.
/// Each frame of the answer that names a request carries `request_id`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Request {
    pub from_index: u64,
    pub from_block: u64,
    pub request_id: u64,
}

/// The result of one [`Transport::poll`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Poll {
    /// One whole frame of the feed layout.
    Frame(Vec<u8>),
    /// No frame came within the wait.
    Idle,
    /// The transport stopped for good: the process shuts down.
    Closed,
}

/// One source of the canonical stream with its bytes.
///
/// The source sends the frames of the member feed: kinds 1 and 2 (the
/// retained frames), and the answers 3, 4, 11 and 16 with the request id
/// as their tail. The reader drops every other kind.
///
/// The reader drives the source from one thread. It asks [`Self::connected`]
/// before it sends a request, starts each connection with a request, and
/// sends a new request with a new id when the answer does not come.
pub trait Transport: Send {
    /// Whether the source can take a request now: for the member feed, the
    /// feed subscription has an image of the member's publication.
    fn connected(&mut self) -> bool;

    /// Send `request`. The source answers the newest request that it got,
    /// and ends every older answer. A lost request is not an error: the
    /// reader sends a new one when no frame of the answer comes.
    fn request(&mut self, request: Request);

    /// The next frame, or [`Poll::Idle`] when none comes within `wait`.
    fn poll(&mut self, wait: Duration) -> Poll;

    /// Stop reading this source: discard the frames that it already sent.
    /// The reader sends a new request before it reads the source again.
    fn close(&mut self);
}

/// The source of a reader that has no second source. It never connects,
/// so the reader comes back to its first source after the silence window.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoSource;

impl Transport for NoSource {
    fn connected(&mut self) -> bool {
        false
    }

    fn request(&mut self, _request: Request) {}

    /// Wait the whole `wait`: the reader polls a source in a loop, so a
    /// source that returns at once would spin the reader thread.
    fn poll(&mut self, wait: Duration) -> Poll {
        std::thread::sleep(wait);
        Poll::Idle
    }

    fn close(&mut self) {}
}
