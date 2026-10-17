//! An in-memory [`Transport`] for tests: the test plays the source through
//! its [`MemoryRemote`].

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, unbounded};

use super::transport::{Poll, Request, Transport};

/// The reader's side of an in-memory source.
pub(crate) struct MemoryTransport {
    frames: Receiver<Vec<u8>>,
    requests: Sender<Request>,
    /// The id of the connection, 0 while disconnected. Shared with the
    /// remote side. The transport must be `Send`, so the id is atomic.
    connection: Arc<AtomicU64>,
}

/// The test's side of an in-memory source: it sends frames, reads the
/// requests, and connects or disconnects the source.
#[derive(Clone)]
pub(crate) struct MemoryRemote {
    frames: Sender<Vec<u8>>,
    requests: Receiver<Request>,
    connection: Arc<AtomicU64>,
    /// The id of the last connection.
    last: Arc<AtomicU64>,
}

impl MemoryTransport {
    /// A source and its remote side. The source starts connected.
    #[must_use]
    pub(crate) fn pair() -> (Self, MemoryRemote) {
        let (frames_tx, frames_rx) = unbounded();
        let (requests_tx, requests_rx) = unbounded();
        let connection = Arc::new(AtomicU64::new(1));
        (
            Self {
                frames: frames_rx,
                requests: requests_tx,
                connection: Arc::clone(&connection),
            },
            MemoryRemote {
                frames: frames_tx,
                requests: requests_rx,
                connection,
                last: Arc::new(AtomicU64::new(1)),
            },
        )
    }
}

impl MemoryRemote {
    /// Send one frame to the reader.
    pub(crate) fn send(&self, frame: Vec<u8>) {
        let _ = self.frames.send(frame);
    }

    /// Connect or disconnect the source. A connect after a disconnect is a
    /// new connection.
    pub(crate) fn set_connected(&self, connected: bool) {
        let now = self.connection.load(Ordering::Relaxed);
        match (connected, now) {
            (true, 0) => self.reconnect(),
            (false, _) => self.connection.store(0, Ordering::Relaxed),
            (true, _) => {}
        }
    }

    /// Replace the connection with a new one at once, with no disconnect
    /// between: a publisher that restarts within one poll.
    pub(crate) fn reconnect(&self) {
        let id = self.last.fetch_add(1, Ordering::Relaxed) + 1;
        self.connection.store(id, Ordering::Relaxed);
    }

    /// The next request of the reader, if one comes within `wait`.
    #[must_use]
    pub(crate) fn request(&self, wait: Duration) -> Option<Request> {
        self.requests.recv_timeout(wait).ok()
    }

    /// The requests that the reader sent and the test did not read yet.
    #[must_use]
    pub(crate) fn requests(&self) -> Vec<Request> {
        self.requests.try_iter().collect()
    }
}

impl Transport for MemoryTransport {
    fn connection(&mut self) -> Option<u64> {
        Some(self.connection.load(Ordering::Relaxed)).filter(|id| *id != 0)
    }

    fn request(&mut self, request: Request) {
        let _ = self.requests.send(request);
    }

    fn poll(&mut self, wait: Duration) -> Poll {
        match self.frames.recv_timeout(wait) {
            Ok(frame) => Poll::Frame(frame),
            Err(RecvTimeoutError::Timeout) => Poll::Idle,
            Err(RecvTimeoutError::Disconnected) => Poll::Closed,
        }
    }

    fn close(&mut self) {
        self.frames.try_iter().for_each(|_| ());
    }
}
