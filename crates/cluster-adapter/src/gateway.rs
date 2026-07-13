//! The cluster-client seam the adapters depend on.
//!
//! The real implementation (behind `kardamom-cluster-client`'s `aeron-live`
//! feature) drives an [`crate`]-level Aeron ingress publication + egress
//! subscription through the `SessionDriver`. The adapters only need two
//! capabilities — offer an app message to ingress, and receive the next app
//! message from egress — so they are written against these traits and tested
//! with the in-memory fakes below.

/// Outcome of offering an app message to cluster ingress.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OfferOutcome {
    #[default]
    Accepted,
    BackPressured,
    NotConnected,
}

/// Offer app messages to cluster ingress (used by the ref publisher).
pub trait ClusterIngress: Send {
    fn offer(&mut self, payload: &[u8]) -> OfferOutcome;
}

/// Receive app messages from cluster egress (used by the tx_ordering
/// subscription). `recv` blocks until the next payload or returns `None` when
/// the session/stream has closed.
pub trait ClusterEgress: Send {
    fn recv(&mut self) -> Option<Vec<u8>>;
}

#[cfg(any(test, feature = "testing"))]
pub mod fakes {
    //! In-memory `ClusterIngress`/`ClusterEgress` fakes.

    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use super::{ClusterEgress, ClusterIngress, OfferOutcome};

    /// In-memory ingress sink. Records every accepted payload; a togglable flag
    /// simulates back-pressure / disconnection.
    #[derive(Clone, Default)]
    pub struct FakeIngress {
        pub accepted: Arc<Mutex<Vec<Vec<u8>>>>,
        pub outcome: Arc<Mutex<OfferOutcome>>,
    }

    impl FakeIngress {
        pub fn new() -> Self {
            Self {
                accepted: Arc::new(Mutex::new(Vec::new())),
                outcome: Arc::new(Mutex::new(OfferOutcome::Accepted)),
            }
        }
        pub fn set_outcome(&self, o: OfferOutcome) {
            *self.outcome.lock().unwrap() = o;
        }
        pub fn accepted(&self) -> Vec<Vec<u8>> {
            self.accepted.lock().unwrap().clone()
        }
    }

    impl ClusterIngress for FakeIngress {
        fn offer(&mut self, payload: &[u8]) -> OfferOutcome {
            let o = *self.outcome.lock().unwrap();
            if o == OfferOutcome::Accepted {
                self.accepted.lock().unwrap().push(payload.to_vec());
            }
            o
        }
    }

    /// In-memory egress source backed by a queue. `recv` pops the next payload;
    /// returns `None` once the queue is empty and the source is marked closed.
    #[derive(Clone, Default)]
    pub struct FakeEgress {
        queue: Arc<Mutex<VecDeque<Vec<u8>>>>,
        closed: Arc<Mutex<bool>>,
    }

    impl FakeEgress {
        pub fn new() -> Self {
            Self {
                queue: Arc::new(Mutex::new(VecDeque::new())),
                closed: Arc::new(Mutex::new(false)),
            }
        }
        pub fn push(&self, payload: Vec<u8>) {
            self.queue.lock().unwrap().push_back(payload);
        }
        /// Mark the stream closed; once the queue drains, `recv` returns `None`.
        pub fn close(&self) {
            *self.closed.lock().unwrap() = true;
        }
    }

    impl ClusterEgress for FakeEgress {
        fn recv(&mut self) -> Option<Vec<u8>> {
            loop {
                if let Some(v) = self.queue.lock().unwrap().pop_front() {
                    return Some(v);
                }
                if *self.closed.lock().unwrap() {
                    return None;
                }
                // Deterministic tests always close before draining, so this is
                // only reached in a misuse; yield to avoid a hot spin.
                std::thread::yield_now();
            }
        }
    }
}
