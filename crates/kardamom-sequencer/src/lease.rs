//! Lease orchestration: bridges [`crate::standby::HotStandbyTailer`] and
//! [`crate::primary::PrimarySequencer`] across a takeover. The deterministic
//! lowest-host-id-among-caught-up-recorders implementation lives in
//! `kardamom_leases::Lease`; this module wraps it with the per-role shutdown
//! signals + takeover-arm flag needed by the sequencer process.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::error::SequencerError;
use crate::primary::Shutdown;

/// Lease primitive abstracted for testability. `kardamom_leases::Lease`
/// implements its own state machine (driven by `observe_fsync` +
/// `observe_quorum`); we wrap it via [`KardamomLeaseHandle`] below.
pub trait LeaseHandle: Send {
    /// True if this process currently holds the slice lease.
    fn is_held(&self) -> bool;
    /// Attempt to acquire the lease. Returns true on success. For the
    /// deterministic v0 lease this is a no-op observer — the lease
    /// "acquires" itself the moment the host becomes the lowest-id
    /// caught-up recorder.
    fn try_acquire(&mut self) -> bool;
    /// Send a heartbeat. For the deterministic v0 lease there's no explicit
    /// heartbeat (freshness is implicit in the most recent fsync watermark
    /// observation); this returns `Ok(())` for the wrapper.
    fn renew(&mut self) -> Result<(), SequencerError>;
    /// The lease TTL — the renewer ticks at `ttl / 3`.
    fn ttl(&self) -> Duration;
}

/// Thin wrapper over `kardamom_leases::Lease` adapting it to
/// [`LeaseHandle`]. Callers feed `observe_fsync` / `observe_quorum` from the
/// supervisor's watermark stream; this wrapper only exposes the read surface
/// the orchestrator uses.
pub struct KardamomLeaseHandle {
    pub lease: kardamom_leases::Lease,
    pub ttl: Duration,
}

impl LeaseHandle for KardamomLeaseHandle {
    fn is_held(&self) -> bool {
        self.lease.held_by_us()
    }
    fn try_acquire(&mut self) -> bool {
        self.lease.held_by_us()
    }
    fn renew(&mut self) -> Result<(), SequencerError> {
        Ok(())
    }
    fn ttl(&self) -> Duration {
        self.ttl
    }
}

/// Per-process orchestrator shared between the renewer thread and the
/// primary/standby runners.
pub struct LeaseOrchestrator {
    pub shutdown_standby: Shutdown,
    pub shutdown_primary: Shutdown,
    pub takeover_armed: Arc<AtomicBool>,
}

impl LeaseOrchestrator {
    pub fn new() -> Self {
        Self {
            shutdown_standby: Shutdown::new(),
            shutdown_primary: Shutdown::new(),
            takeover_armed: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Renew loop. Caller runs this on a dedicated thread. Behavior:
    ///   * Just acquired: signal standby shutdown, arm takeover.
    ///   * Just lost: signal primary shutdown, arm takeover.
    ///   * Held steady: renew.
    ///   * Not held: poll.
    pub fn run_renew_loop<L: LeaseHandle>(
        &self,
        lease: &mut L,
        shutdown: Shutdown,
    ) -> Result<(), SequencerError> {
        let tick = lease.ttl() / 3;
        let mut last_held = lease.is_held();
        loop {
            if shutdown.is_signaled() {
                return Ok(());
            }
            std::thread::sleep(tick);
            let held = lease.is_held();
            if held && !last_held {
                self.shutdown_standby.signal();
                self.takeover_armed.store(true, Ordering::Release);
            } else if !held && last_held {
                self.shutdown_primary.signal();
                self.takeover_armed.store(true, Ordering::Release);
            } else if held && let Err(e) = lease.renew() {
                tracing::error!("lease renew failed: {e}");
                return Err(e);
            }
            last_held = held;
        }
    }
}

impl Default for LeaseOrchestrator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(any(test, feature = "testing"))]
pub mod fakes {
    use std::sync::{Arc, Mutex};

    use super::*;

    /// Lease whose `held` flag can be flipped externally. The internal state
    /// lives in an `Arc<Mutex<_>>` so test threads can mutate it.
    #[derive(Clone)]
    pub struct SharedManualLease {
        pub held: Arc<Mutex<bool>>,
        pub renew_errors: Arc<Mutex<bool>>,
        pub ttl: Duration,
    }

    impl SharedManualLease {
        pub fn new(initially_held: bool) -> Self {
            Self {
                held: Arc::new(Mutex::new(initially_held)),
                renew_errors: Arc::new(Mutex::new(false)),
                ttl: Duration::from_millis(30),
            }
        }
        pub fn drop_lease(&self) {
            *self.held.lock().unwrap() = false;
        }
        pub fn restore_lease(&self) {
            *self.held.lock().unwrap() = true;
        }
    }

    impl LeaseHandle for SharedManualLease {
        fn is_held(&self) -> bool {
            *self.held.lock().unwrap()
        }
        fn try_acquire(&mut self) -> bool {
            *self.held.lock().unwrap() = true;
            true
        }
        fn renew(&mut self) -> Result<(), SequencerError> {
            if *self.renew_errors.lock().unwrap() {
                Err(SequencerError::LeaseLost)
            } else {
                Ok(())
            }
        }
        fn ttl(&self) -> Duration {
            self.ttl
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fakes::*;
    use super::*;

    #[test]
    fn orchestrator_exits_clean_on_shutdown() {
        let orc = LeaseOrchestrator::new();
        let stop = Shutdown::new();
        stop.signal();
        let mut lease = SharedManualLease::new(true);
        let r = orc.run_renew_loop(&mut lease, stop);
        assert!(r.is_ok(), "{r:?}");
    }

    #[test]
    fn losing_lease_arms_takeover_and_signals_primary() {
        let orc = LeaseOrchestrator::new();
        let stop = Shutdown::new();
        let stop_clone = stop.clone();
        let lease = SharedManualLease::new(true);
        let lease_for_thread = lease.clone();

        let primary = orc.shutdown_primary.clone();
        let takeover = orc.takeover_armed.clone();

        let bg = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(25));
            lease_for_thread.drop_lease();
            std::thread::sleep(Duration::from_millis(50));
            stop_clone.signal();
        });
        let mut lease_mut = lease.clone();
        let _ = orc.run_renew_loop(&mut lease_mut, stop);
        bg.join().unwrap();
        assert!(
            primary.is_signaled(),
            "primary shutdown must be signalled after the lease is lost"
        );
        assert!(
            takeover.load(Ordering::Acquire),
            "takeover_armed must be set when the lease state changes"
        );
    }

    #[test]
    fn acquiring_lease_arms_takeover_and_signals_standby() {
        let orc = LeaseOrchestrator::new();
        let stop = Shutdown::new();
        let stop_clone = stop.clone();
        let lease = SharedManualLease::new(false);
        let lease_for_thread = lease.clone();

        let standby = orc.shutdown_standby.clone();
        let takeover = orc.takeover_armed.clone();

        let bg = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(25));
            lease_for_thread.restore_lease();
            std::thread::sleep(Duration::from_millis(50));
            stop_clone.signal();
        });
        let mut lease_mut = lease.clone();
        let _ = orc.run_renew_loop(&mut lease_mut, stop);
        bg.join().unwrap();
        assert!(
            standby.is_signaled(),
            "standby shutdown must be signalled after we acquire the lease"
        );
        assert!(
            takeover.load(Ordering::Acquire),
            "takeover_armed must be set"
        );
    }
}
