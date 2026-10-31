use std::collections::HashMap;

use crossbeam_channel::Receiver;
use kardamom_cache::AppliedRollbacks;

use super::{
    DEADLINE_CHANNEL_BOUND, EnterReason, FloorEvent, ResyncChannel, ResyncConfig,
    ResyncConfigError, ResyncController, SealerAnswer, SealerRefusal, SharedWatermark,
};
use crate::config::SequencerConfig;

enum StartupRecovery {
    ReceiptFloors,
    Admission,
}

impl StartupRecovery {
    fn start(self, controller: &mut ResyncController) {
        match self {
            Self::ReceiptFloors => controller.enter(EnterReason::Startup),
            Self::Admission => crate::metrics::record_resync_mode(controller.partition, false),
        }
    }
}

impl ResyncController {
    /// # Errors
    ///
    /// Returns [`ResyncConfigError`] if `cfg`'s watermark-jump threshold
    /// cannot be computed (see [`ResyncConfig::enter_threshold`]).
    pub fn new(
        cfg: ResyncConfig,
        partition: u32,
        floor_rx: Receiver<FloorEvent>,
        answer_rx: Receiver<SealerAnswer>,
        deadline_rx: Receiver<SealerRefusal>,
        watermark: SharedWatermark,
    ) -> Result<Self, ResyncConfigError> {
        Self::with_recovery(
            cfg,
            partition,
            floor_rx,
            answer_rx,
            deadline_rx,
            watermark,
            StartupRecovery::ReceiptFloors,
        )
    }

    fn with_recovery(
        cfg: ResyncConfig,
        partition: u32,
        floor_rx: Receiver<FloorEvent>,
        answer_rx: Receiver<SealerAnswer>,
        deadline_rx: Receiver<SealerRefusal>,
        watermark: SharedWatermark,
        recovery: StartupRecovery,
    ) -> Result<Self, ResyncConfigError> {
        let enter_threshold = cfg.enter_threshold()?;
        let mut c = Self {
            cfg,
            enter_threshold,
            partition,
            active: false,
            floors: HashMap::new(),
            generation: 0,
            rollbacks: AppliedRollbacks::default(),
            floor_rx,
            floor_rx_dead: false,
            answer_rx,
            answer_rx_dead: false,
            deadline_rx,
            deadline_rx_dead: false,
            watermark,
            last_watermark: 0,
            watermark_seen: false,
            stall_since: None,
            calm_since: None,
        };
        recovery.start(&mut c);
        Ok(c)
    }
}

impl ResyncChannel {
    /// Open the recovery channels for the configured sequencer loop.
    ///
    /// # Errors
    ///
    /// Returns an error when the resync bounds cannot be represented.
    pub fn for_config(cfg: &SequencerConfig) -> Result<Self, ResyncConfigError> {
        let recovery = if cfg.admission.enabled {
            StartupRecovery::Admission
        } else {
            StartupRecovery::ReceiptFloors
        };
        Self::open_with(cfg.resync.clone(), cfg.partition_index, recovery)
    }

    /// Build the controller, plus the sender halves of the floor-update
    /// channel (handed to the receipts thread) and the rewind channel
    /// (handed to the egress-watermark thread, alongside the
    /// shared watermark).
    ///
    /// Both channels are bounded to `cfg.dedup_capacity`: the cluster's
    /// first-seen dedup horizon, the same resync window the rest of this
    /// module protects. A backlog past that many entries already means
    /// the publish loop has stalled longer than the window this
    /// mechanism covers. The binary's egress feed drops on overflow
    /// instead of growing without bound.
    ///
    /// Each channel is `crossbeam_channel::bounded`, which allocates
    /// every slot up front rather than growing on demand: at the default
    /// `dedup_capacity` (2^17), each channel's backing ring buffer is
    /// about 6 MB, so this allocates about 12 MB total.
    ///
    /// # Errors
    ///
    /// Returns [`ResyncConfigError`] if `cfg`'s watermark-jump threshold
    /// cannot be computed (see [`ResyncConfig::enter_threshold`]), or if
    /// `cfg.dedup_capacity` does not fit in a `usize` on this target
    /// (only possible on a 32-bit target with a `dedup_capacity` above
    /// `u32::MAX`).
    pub fn open(cfg: ResyncConfig, partition: u32) -> Result<Self, ResyncConfigError> {
        Self::open_with(cfg, partition, StartupRecovery::ReceiptFloors)
    }

    fn open_with(
        cfg: ResyncConfig,
        partition: u32,
        recovery: StartupRecovery,
    ) -> Result<Self, ResyncConfigError> {
        let cap = usize::try_from(cfg.dedup_capacity.get()).map_err(|_| {
            ResyncConfigError::CapacityNotUsize {
                dedup_capacity: cfg.dedup_capacity.get(),
            }
        })?;
        let (floor_tx, floor_rx) = crossbeam_channel::bounded(cap);
        let (answer_tx, answer_rx) = crossbeam_channel::bounded(cap);
        // A past-deadline reject is rare next to a contiguity reject: it
        // needs a replica to stall past the whole horizon. A small bound
        // is enough, and it keeps the up-front allocation where it was.
        let (deadline_tx, deadline_rx) = crossbeam_channel::bounded(DEADLINE_CHANNEL_BOUND);
        let watermark = SharedWatermark::new();
        let controller = ResyncController::with_recovery(
            cfg,
            partition,
            floor_rx,
            answer_rx,
            deadline_rx,
            watermark.clone(),
            recovery,
        )?;
        Ok(Self {
            controller,
            floor_tx,
            answer_tx,
            deadline_tx,
            watermark,
        })
    }
}

#[cfg(test)]
mod tests;
