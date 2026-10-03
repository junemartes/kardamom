//! The sequencer's pause gate. While the process lifecycle is paused (the
//! sealer emits no boundary, or an operator paused the replica), the
//! publish loop offers nothing and reads no `tx_data`: an offer would
//! only back-pressure, and the envelopes stay in the stream and its
//! archive until the pause ends.
//!
//! The loop turns millions of times a second, so the gate caches the flag
//! and reads the watch only when its version changed: one atomic load
//! per turn.

use kardamom_obs::lifecycle::Slots;
use tokio::sync::watch;

/// The cached pause flag over a lifecycle's watch.
#[derive(Debug, Default)]
pub(crate) struct PauseGate {
    /// `None` when the binary wired no lifecycle (tests, dev runs): the
    /// gate never closes.
    rx: Option<watch::Receiver<Slots>>,
    paused: bool,
}

impl PauseGate {
    /// A gate over `rx`.
    pub(crate) fn new(rx: watch::Receiver<Slots>) -> Self {
        let paused = rx.borrow().pause.is_some();
        Self {
            rx: Some(rx),
            paused,
        }
    }

    /// Whether the loop must offer nothing this turn.
    pub(crate) fn paused(&mut self) -> bool {
        if let Some(rx) = self
            .rx
            .as_mut()
            .filter(|rx| rx.has_changed().unwrap_or(false))
        {
            self.paused = rx.borrow_and_update().pause.is_some();
        }
        self.paused
    }
}

#[cfg(test)]
mod tests {
    use kardamom_obs::halt::{HaltCause, HaltRef};
    use kardamom_obs::lifecycle::Lifecycle;

    use super::PauseGate;

    #[test]
    fn the_gate_follows_the_lifecycle_pause() {
        let life = Lifecycle::new(Some("sequencer"));
        let mut gate = PauseGate::new(life.subscribe());
        assert!(!gate.paused());
        life.follow(Some(HaltRef::sealer(HaltCause::SealerNoQuorum)));
        assert!(gate.paused());
        assert!(gate.paused(), "the cached flag holds without a change");
        life.follow(None);
        assert!(!gate.paused());
        assert!(!PauseGate::default().paused(), "no lifecycle never pauses");
    }
}
