//! Cooperative shutdown signal for the sequencer loop.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Cooperative shutdown signal shared with the loop driver. Cloneable so the
/// signal handler thread can keep one copy and the loop thread another.
#[derive(Clone)]
pub struct Shutdown {
    flag: Arc<AtomicBool>,
}

impl Shutdown {
    pub fn from_atomic(flag: Arc<AtomicBool>) -> Self {
        Self { flag }
    }
    pub fn new() -> Self {
        Self {
            flag: Arc::new(AtomicBool::new(false)),
        }
    }
    pub fn signal(&self) {
        self.flag.store(true, Ordering::Release);
    }
    pub fn is_signaled(&self) -> bool {
        self.flag.load(Ordering::Acquire)
    }
    pub fn atomic(&self) -> Arc<AtomicBool> {
        self.flag.clone()
    }
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::new()
    }
}
