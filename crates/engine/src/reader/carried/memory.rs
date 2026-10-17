//! An in-memory [`Transport`] for tests: the test plays the source through
//! its [`MemoryRemote`].

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, unbounded};

use super::transport::{Poll, Request, Transport};

/// The reader's side of an in-memory source.
pub(crate) struct MemoryTransport {
    frames: Receiver<Vec<u8>>,
    requests: Sender<Request>,
    /// Shared with the remote side. The transport must be `Send`, so the
    /// flag is atomic.
    connected: Arc<AtomicBool>,
}

/// The test's side of an in-memory source: it sends frames, reads the
/// requests, and connects or disconnects the source.
#[derive(Clone)]
pub(crate) struct MemoryRemote {
    frames: Sender<Vec<u8>>,
    requests: Receiver<Request>,
    connected: Arc<AtomicBool>,
}

impl MemoryTransport {
    /// A source and its remote side. The source starts connected.
    #[must_use]
    pub(crate) fn pair() -> (Self, MemoryRemote) {
        let (frames_tx, frames_rx) = unbounded();
        let (requests_tx, requests_rx) = unbounded();
        let connected = Arc::new(AtomicBool::new(true));
        (
            Self {
                frames: frames_rx,
                requests: requests_tx,
                connected: Arc::clone(&connected),
            },
            MemoryRemote {
                frames: frames_tx,
                requests: requests_rx,
                connected,
            },
        )
    }
}

impl MemoryRemote {
    /// Send one frame to the reader.
    pub(crate) fn send(&self, frame: Vec<u8>) {
        let _ = self.frames.send(frame);
    }

    /// Connect or disconnect the source.
    pub(crate) fn set_connected(&self, connected: bool) {
        self.connected.store(connected, Ordering::Relaxed);
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
    fn connected(&mut self) -> bool {
        self.connected.load(Ordering::Relaxed)
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
