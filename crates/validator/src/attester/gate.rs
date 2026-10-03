//! The attester's pause. The attester posts output roots to L1, and a
//! posted root is what a withdrawal proves against. While any live
//! validator stands halted on a divergence, the chain's state is in
//! doubt, so the attester posts nothing: it pauses on that validator's
//! halt as its root, keeps its leaves and roots pending, and posts again
//! when the root clears.
//!
//! The attester has a lifecycle of its own (`service = "attester"`), so
//! the chain status shows it paused while the validator process beside
//! it keeps verifying.

use std::sync::Arc;

use kardamom_obs::events::BoardView;
use kardamom_obs::halt::{HaltCause, HaltRef};
use kardamom_obs::lifecycle::{Lifecycle, Slots};
use tokio::sync::watch;

/// The attester's lifecycle, and a receiver of it for the posting loop.
#[derive(Clone)]
pub struct AttesterGate {
    life: Arc<Lifecycle>,
    rx: watch::Receiver<Slots>,
}

impl AttesterGate {
    /// The attester's service name on the `events` stream.
    pub const SERVICE: &'static str = "attester";
    /// The validator's service name on the `events` stream.
    pub const VALIDATOR: &'static str = "validator";

    /// A gate over a new attester lifecycle, open.
    #[must_use]
    pub fn new() -> Self {
        let life = Arc::new(Lifecycle::new(Some(Self::SERVICE)));
        let rx = life.subscribe();
        Self { life, rx }
    }

    /// The attester's lifecycle, for its beacon.
    #[must_use]
    pub fn lifecycle(&self) -> &Arc<Lifecycle> {
        &self.life
    }

    /// Whether the attester must post nothing now.
    #[must_use]
    pub fn paused(&self) -> bool {
        self.rx.borrow().pause.is_some()
    }

    /// Resolve on the next change of the pause. The lifecycle lives as
    /// long as `self`, so the wait never ends on a closed channel.
    pub async fn changed(&mut self) {
        let _ = self.rx.changed().await;
    }

    /// Pause on `root` while it stands, resume when it goes.
    pub fn follow(&self, root: Option<HaltRef>) {
        self.life.follow(root);
    }

    /// The root the attester pauses on: this validator halted on a
    /// divergence (`own`, named `own_instance`), or any live validator on
    /// the board halted on one. `None` while no divergence stands.
    #[must_use]
    pub fn root(own: &Slots, own_instance: &str, board: &BoardView) -> Option<HaltRef> {
        own.halt
            .as_ref()
            .filter(|halt| halt.cause == HaltCause::ValidatorDivergence)
            .map(|halt| HaltRef {
                service: Self::VALIDATOR.to_string(),
                instance: own_instance.to_string(),
                cause: halt.cause,
            })
            .or_else(|| board.halted_on(Self::VALIDATOR, HaltCause::ValidatorDivergence))
    }
}

impl Default for AttesterGate {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use kardamom_obs::events::Board;
    use kardamom_obs::halt::{Halt, ServiceEvent, ServiceState};
    use tokio::time::Instant;

    use super::*;

    fn board(state: ServiceState) -> BoardView {
        let now = Instant::now();
        let mut board = Board::default();
        board.observe(
            ServiceEvent {
                service: AttesterGate::VALIDATOR.into(),
                instance: "v2".into(),
                seq: 1,
                state,
            },
            now,
        );
        board.view(now)
    }

    #[test]
    fn a_divergence_of_another_validator_pauses_the_attester() {
        let gate = AttesterGate::new();
        let diverged = board(ServiceState::Halted(Halt::new(
            HaltCause::ValidatorDivergence,
            "state root mismatch at block 9",
        )));
        let root = AttesterGate::root(&Slots::default(), "v1", &diverged).unwrap();
        assert_eq!(root.instance, "v2");
        gate.follow(Some(root));
        assert!(gate.paused());
        assert!(matches!(gate.lifecycle().state(), ServiceState::Paused(_)));

        let healed = board(ServiceState::Resumed);
        gate.follow(AttesterGate::root(&Slots::default(), "v1", &healed));
        assert!(!gate.paused());
    }

    #[test]
    fn its_own_validator_diverged_pauses_the_attester() {
        let own = Slots {
            halt: Some(Halt::new(HaltCause::ValidatorDivergence, "receipt mismatch")),
            pause: None,
        };
        let root = AttesterGate::root(&own, "v1", &BoardView::default()).unwrap();
        assert_eq!(
            (root.service.as_str(), root.instance.as_str()),
            ("validator", "v1")
        );
    }

    #[test]
    fn another_halt_of_a_validator_does_not_pause_the_attester() {
        let own = Slots {
            halt: Some(Halt::new(HaltCause::L1Unreachable, "down")),
            pause: None,
        };
        let other = board(ServiceState::Halted(Halt::new(
            HaltCause::ReplayUnavailable,
            "below the floor",
        )));
        assert_eq!(AttesterGate::root(&own, "v1", &other), None);
    }
}
