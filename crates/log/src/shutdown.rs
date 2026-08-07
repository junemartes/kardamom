//! Tiny shutdown gate: an atomic flag whose waiters park on a Condvar
//! instead of sleep-polling (docs/agents/push-model-spec.md, Push-1a).
//!
//! Replaces the `Arc<AtomicBool>` + 500 ms park loops in the ingress and
//! da-watcher recorder threads: `is_set()` serves the polled `should_stop`
//! closures that remain (recorder startup), `wait()` parks the
//! hold-the-archive-session threads until `signal()`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex};

#[derive(Default)]
pub struct Gate {
    flag: AtomicBool,
    lock: Mutex<()>,
    cv: Condvar,
}

impl Gate {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the flag and wake every `wait`er. Idempotent.
    pub fn signal(&self) {
        self.flag.store(true, Ordering::SeqCst);
        // Taking the lock orders the store before any waiter's re-check, so
        // a `wait`er can never park after missing the flag.
        let _guard = self.lock.lock().expect("gate lock poisoned");
        self.cv.notify_all();
    }

    pub fn is_set(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }

    /// Park until `signal`. Spurious wakeups re-check the flag.
    pub fn wait(&self) {
        let mut guard = self.lock.lock().expect("gate lock poisoned");
        while !self.flag.load(Ordering::SeqCst) {
            guard = self.cv.wait(guard).expect("gate wait poisoned");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    #[test]
    fn wait_returns_promptly_after_signal() {
        let gate = Arc::new(Gate::new());
        let g2 = gate.clone();
        let waiter = std::thread::spawn(move || {
            let start = Instant::now();
            g2.wait();
            start.elapsed()
        });
        std::thread::sleep(Duration::from_millis(50));
        gate.signal();
        let waited = waiter.join().expect("waiter panicked");
        assert!(waited >= Duration::from_millis(45));
        assert!(
            waited < Duration::from_millis(500),
            "waiter overslept: {waited:?}"
        );
        assert!(gate.is_set());
    }

    #[test]
    fn signal_before_wait_never_parks() {
        let gate = Gate::new();
        gate.signal();
        let start = Instant::now();
        gate.wait();
        assert!(start.elapsed() < Duration::from_millis(50));
    }
}
