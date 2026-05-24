//! Hot-standby tailer.
//!
//! Subscribes to channel B and replays each in-slice tx's nonce into a local
//! [`PartitionState`]. Block-boundary markers are decoded and skipped. On
//! lease takeover, [`HotStandbyTailer::into_state`] hands the populated state
//! to a brand-new [`crate::primary::PrimarySequencer`] (via
//! `PrimarySequencer::with_state`) so the promoted primary begins publishing
//! with the in-lockstep next-nonce map.

use std::time::Duration;

use alloy_primitives::Address;

use crate::config::SequencerConfig;
use crate::error::SequencerError;
use crate::inbound::{BMessage, BReplaySource};
use crate::partition::partition_for;
use crate::primary::Shutdown;
use crate::state::PartitionState;

pub struct HotStandbyTailer {
    cfg: SequencerConfig,
    state: PartitionState<()>,
}

impl HotStandbyTailer {
    pub fn new(cfg: SequencerConfig) -> Self {
        cfg.validate().expect("validated config");
        let cap = cfg.max_pending_per_sender;
        Self {
            cfg,
            state: PartitionState::new(cap),
        }
    }

    pub fn config(&self) -> &SequencerConfig {
        &self.cfg
    }

    pub fn next_nonce(&self, sender: Address) -> u64 {
        self.state.next_nonce(sender)
    }

    /// Process one B message. Returns `true` if a message was consumed.
    pub fn run_once<S: BReplaySource>(&mut self, b: &mut S) -> Result<bool, SequencerError> {
        let Some(msg) = b.poll()? else {
            return Ok(false);
        };
        match msg {
            BMessage::Tx { sender, nonce } => {
                let part = partition_for(sender, self.cfg.partition_count);
                if part == self.cfg.partition_index {
                    self.state.replay(sender, nonce);
                }
            }
            BMessage::BlockBoundary => {
                // Sealer marker; ignore for nonce purposes.
            }
        }
        Ok(true)
    }

    /// Pin to core (if configured) and loop until shutdown.
    pub fn run<S: BReplaySource>(
        &mut self,
        b: &mut S,
        shutdown: Shutdown,
    ) -> Result<(), SequencerError> {
        if let Some(core) = self.cfg.core_id {
            let id = core_affinity::CoreId { id: core };
            if !core_affinity::set_for_current(id) {
                tracing::warn!(core, "failed to pin standby thread to core");
            }
        }
        loop {
            if shutdown.is_signaled() {
                return Ok(());
            }
            if !self.run_once(b)? {
                std::thread::sleep(Duration::from_micros(50));
            }
        }
    }

    /// Consume the tailer and hand its populated state to a new primary
    /// (via [`crate::primary::PrimarySequencer::with_state`]).
    pub fn into_state(self) -> PartitionState<()> {
        self.state
    }
}
