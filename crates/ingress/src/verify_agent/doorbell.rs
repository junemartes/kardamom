//! The wake of a parked agent thread, after the Agrona handshake.
//!
//! The agent sets `parked`, then reads its input once more, and parks only
//! when the input is still empty. A producer pushes its item, then reads
//! `parked`, and unparks the agent only when the flag is set. A fence on
//! each side orders the flag and the input (the Dekker pattern): either the
//! agent sees the item before it parks, or the producer sees the flag and
//! unparks it. So no push between the last poll and the park is lost, and
//! a busy agent costs its producers no system call.
//!
//! The park also has a time cap. The cap is not part of the proof: it
//! bounds the wait of the agent for its own timed work.
//!
//! With the crate feature `loom`, the atomics and the park are those of
//! `loom`, and `tests/doorbell_loom.rs` checks the handshake in each
//! interleaving of a model.

use std::sync::OnceLock;
use std::time::Duration;

#[cfg(feature = "loom")]
use loom::sync::atomic::{AtomicBool, Ordering, fence};
#[cfg(feature = "loom")]
use loom::thread::{self, Thread};
#[cfg(not(feature = "loom"))]
use std::sync::atomic::{AtomicBool, Ordering, fence};
#[cfg(not(feature = "loom"))]
use std::thread::{self, Thread};

/// The doorbell of one agent thread. See the module text.
#[derive(Debug, Default)]
pub struct Doorbell {
    parked: AtomicBool,
    /// The agent thread. The agent sets it as its first action, before it
    /// can park. A producer reads it only after it read `parked` as set,
    /// and that read sees the set through the same atomic.
    agent: OnceLock<Thread>,
}

impl Doorbell {
    /// A doorbell with no agent yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Name the calling thread as the agent. The agent calls it once, as
    /// its first action.
    pub fn attach_current(&self) {
        // A second attach keeps the first thread: one doorbell has one agent.
        let _ = self.agent.set(thread::current());
    }

    /// The producer side: call it after each push of an item.
    pub fn ring(&self) {
        fence(Ordering::SeqCst);
        if !self.parked.load(Ordering::SeqCst) {
            return;
        }
        if let Some(agent) = self.agent.get() {
            agent.unpark();
        }
    }

    /// The agent side: park until a producer rings, or for at most `cap`.
    /// `has_work` reads the input of the agent. It runs after the flag is
    /// set, so it sees each item that a producer pushed before it read the
    /// flag.
    pub fn park(&self, cap: Duration, has_work: impl Fn() -> bool) {
        self.parked.store(true, Ordering::SeqCst);
        fence(Ordering::SeqCst);
        if !has_work() {
            park_for(cap);
        }
        self.parked.store(false, Ordering::SeqCst);
    }
}

/// Park the current thread for at most `cap`. A `loom` model has no time,
/// so there the park waits for an unpark: a lost wake is then a deadlock
/// that the model reports.
#[cfg(not(feature = "loom"))]
fn park_for(cap: Duration) {
    thread::park_timeout(cap);
}

#[cfg(feature = "loom")]
fn park_for(_cap: Duration) {
    thread::park();
}
