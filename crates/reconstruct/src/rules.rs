//! The decision rules of the sealer at a rebuilt head: the version switch
//! that a sealer seed carries.
//!
//! The sealer keeps the switch between decision versions 3, 4 and 5, and
//! the carry window flag, as replicated state (`VersionSwitch.java`). A
//! sealer cluster that starts from a seed must start with the switch of
//! the chain, or it decides the next entries with other rules. The switch
//! follows features 3, 4 and 5: the features of the genesis, then each
//! upgrade transaction on L1. [`SealerRules`] holds the rules of
//! `VersionSwitch.java`, and [`SealerRules::at_head`] walks the rebuilt
//! blocks through them.
//!
//! The walk takes the feature activation record of an epoch where a
//! sequencer sends it: in front of the epoch, while the block before it
//! is open. The sealer takes the record under the version of that open
//! block. A boundary closes that block before or after the record: a timer
//! tick before it, or the epoch after it. When the order changes the rules,
//! the walk cannot know the rules of the sealer, and it refuses the head.

use kardamom_engine::ReplayBlock;
use kardamom_types::upgrades::{
    FEATURE_CARRY_WINDOW, FEATURE_SEALER_V4, FEATURE_SEALER_V5, FeatureSchedule, GenesisFeatures,
    SEALER_FEATURES,
};

use crate::ReconstructError;

/// The decision version with no feature on.
const OFF_VERSION: u8 = 3;
/// The decision version of feature 3.
const ON_VERSION: u8 = 4;
/// The decision version of features 3 and 4.
const CARRY_VERSION: u8 = 5;
/// The most activations of one feature that the switch holds: the visible
/// one and two that wait.
const MAX_HELD: usize = 3;
/// The time in ms of an activation at genesis: at or below the time of
/// every block.
const GENESIS_MS: u64 = 1;
/// The activation of a feature at genesis: L1 origin 0, time 1 ms.
const GENESIS_HELD: Held = Held {
    origin: 0,
    time_ms: GENESIS_MS,
};

/// One held activation: its L1 origin and its time in ms.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Held {
    origin: u64,
    time_ms: u64,
}

/// The held activations of one feature, oldest first, each above the one
/// before: the visible one and the ones whose epoch is not sealed yet.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Schedule(Vec<Held>);

impl Schedule {
    /// The origin of the newest held activation, or 0 when none.
    fn newest(&self) -> u64 {
        self.0.last().map_or(0, |held| held.origin)
    }

    /// How many held activations, from the oldest, have an epoch at or
    /// below `l1_origin`.
    fn visible_at(&self, l1_origin: u64) -> usize {
        self.0
            .iter()
            .take_while(|held| held.origin <= l1_origin)
            .count()
    }

    /// Whether the feature is on in the block after the boundary `at`: the
    /// newest visible activation has a time that is not 0 and is at or
    /// below the time of the next block. With no visible activation, the
    /// feature stays as `current`.
    fn on_at(&self, at: &Boundary, sealed_origin: u64, current: bool) -> bool {
        self.newest_visible(at, sealed_origin)
            .map_or(current, |time| time != 0 && time <= at.l2_timestamp)
    }

    /// The resolved time of the newest visible activation at the boundary
    /// `at`. A time of 0 of one that becomes visible there is the time of
    /// the sealed block.
    fn newest_visible(&self, at: &Boundary, sealed_origin: u64) -> Option<u64> {
        let visible = self.visible_at(at.l1_origin);
        self.0[..visible].last().map(|held| {
            let becomes_visible = held.origin > sealed_origin;
            if becomes_visible && held.time_ms == 0 {
                at.sealed_ms
            } else {
                held.time_ms
            }
        })
    }

    /// Resolve the time of the newest visible activation at the boundary
    /// `at`, and drop the older ones.
    fn settle(&mut self, at: &Boundary, sealed_origin: u64) {
        let visible = self.visible_at(at.l1_origin);
        if let Some(time_ms) = self.newest_visible(at, sealed_origin) {
            self.0[visible - 1].time_ms = time_ms;
            self.0.drain(..visible - 1);
        }
    }

    /// The section `[held:u8]`, then `[origin:u64][time_ms:u64]` for each
    /// held activation, oldest first, big-endian.
    fn encode(&self) -> impl Iterator<Item = u8> + '_ {
        let held = u8::try_from(self.0.len()).expect("a schedule holds at most three activations");
        std::iter::once(held).chain(self.0.iter().flat_map(|held| {
            held.origin
                .to_be_bytes()
                .into_iter()
                .chain(held.time_ms.to_be_bytes())
        }))
    }
}

/// What a feature activation record did, as `VersionSwitch.Outcome`. The
/// walk follows a member with its retained frames on the heap, so it never
/// gives `UNSUPPORTED`: a member with the retained file drops the carry
/// window of a seed at its start.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// The record changed nothing: another feature, a copy, or a later epoch.
    Ignored,
    /// The switch holds the schedule.
    Stored,
    /// The switch holds the schedule, but the first block of its epoch is sealed.
    StoredLate,
    /// The epoch of the record is below the epoch of the state.
    Missed,
    /// The switch holds the most activations of the feature.
    Full,
    /// Feature 4 while version 3 runs, or feature 5 below version 5.
    Early,
}

/// A feature activation record: the upgrade transaction of the epoch at
/// `l1_origin` schedules `feature_id` at `time_ms`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Activation {
    pub(crate) l1_origin: u64,
    pub(crate) feature_id: u64,
    pub(crate) time_ms: u64,
}

/// A block boundary: the L1 origin of the sealed block, the time that the
/// sealed block executed with, and the time of the next block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Boundary {
    pub(crate) l1_origin: u64,
    pub(crate) sealed_ms: u64,
    pub(crate) l2_timestamp: u64,
}

/// The version switch of the sealer: the version of the open block, the
/// carry window flag, and the schedules of features 3, 4 and 5.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SealerRules {
    version: u8,
    carry_window: bool,
    /// The L1 origin of the last block that a boundary sealed.
    sealed_origin: u64,
    v4: Schedule,
    carry: Schedule,
    window: Schedule,
}

impl SealerRules {
    /// The switch of a chain at genesis with `features`: one held
    /// activation of each feature at L1 origin 0 and time 1 ms, and the
    /// version and the flag of a boundary.
    #[must_use]
    pub fn genesis(features: GenesisFeatures) -> Self {
        let count = features.ids().len();
        let [v4, carry, window] =
            [0, 1, 2].map(|k| Schedule((k < count).then_some(GENESIS_HELD).into_iter().collect()));
        let mut rules = Self {
            version: OFF_VERSION,
            carry_window: false,
            sealed_origin: 0,
            v4,
            carry,
            window,
        };
        rules.at_boundary(&Boundary {
            l1_origin: 0,
            sealed_ms: 0,
            l2_timestamp: GENESIS_MS,
        });
        rules
    }

    /// The rules of the sealer after the boundary of the last of `blocks`:
    /// the genesis of `features`, then each activation of features 3, 4
    /// and 5 that the epochs of the blocks carry.
    ///
    /// # Errors
    ///
    /// Returns an error when the blocks do not run from block 1 with no
    /// gap, or when the rules depend on the order of a boundary and an
    /// activation record.
    pub fn at_head(
        features: GenesisFeatures,
        blocks: &[ReplayBlock],
    ) -> Result<Self, ReconstructError> {
        blocks
            .iter()
            .try_fold(RulesWalk::new(features), RulesWalk::step)
            .map(RulesWalk::finish)
    }

    /// The version of the open block.
    #[must_use]
    pub const fn version(&self) -> u8 {
        self.version
    }

    /// The carry window flag of the open block.
    #[must_use]
    pub const fn carry_window(&self) -> bool {
        self.carry_window
    }

    /// Whether the switch holds an activation: only then does a seed carry
    /// the rules.
    #[must_use]
    pub fn holds_activation(&self) -> bool {
        [&self.v4, &self.carry, &self.window]
            .iter()
            .any(|schedule| !schedule.0.is_empty())
    }

    /// The schedule of `feature_id`, or `None` for a feature that the
    /// switch does not follow.
    fn schedule_mut(&mut self, feature_id: u64) -> Option<&mut Schedule> {
        match feature_id {
            FEATURE_SEALER_V4 => Some(&mut self.v4),
            FEATURE_SEALER_V5 => Some(&mut self.carry),
            FEATURE_CARRY_WINDOW => Some(&mut self.window),
            _ => None,
        }
    }

    /// Take a feature activation record while the state is at L1 origin
    /// `state_origin` and the open block executes with `open_block_ms`. The
    /// record only stores the schedule.
    pub(crate) fn on_activation(
        &mut self,
        activation: Activation,
        state_origin: u64,
        open_block_ms: u64,
    ) -> Outcome {
        let version = self.version;
        let sealed_origin = self.sealed_origin;
        let feature = activation.feature_id;
        let origin = activation.l1_origin;
        let Some(schedule) = self.schedule_mut(feature) else {
            return Outcome::Ignored;
        };
        if origin <= schedule.newest() {
            return Outcome::Ignored;
        }
        if state_origin != 0 && origin < state_origin {
            return Outcome::Missed;
        }
        if state_origin != 0
            && origin != state_origin
            && Some(origin) != state_origin.checked_add(1)
        {
            return Outcome::Ignored;
        }
        if feature == FEATURE_SEALER_V5 && version < ON_VERSION
            || feature == FEATURE_CARRY_WINDOW && version != CARRY_VERSION
        {
            return Outcome::Early;
        }
        if schedule.0.len() == MAX_HELD {
            return Outcome::Full;
        }
        let late = origin <= sealed_origin;
        let time_ms = if late && activation.time_ms == 0 {
            open_block_ms
        } else {
            activation.time_ms
        };
        schedule.0.push(Held { origin, time_ms });
        if late {
            Outcome::StoredLate
        } else {
            Outcome::Stored
        }
    }

    /// Set the version and the carry window flag of the next block at the
    /// boundary `at`. The newest visible activation of each feature
    /// decides, and the older ones go. Version 5 needs version 4.
    pub(crate) fn at_boundary(&mut self, at: &Boundary) {
        let origin = self.sealed_origin;
        let v4 = self.v4.on_at(at, origin, self.version >= ON_VERSION);
        let carry = self.carry.on_at(at, origin, self.version == CARRY_VERSION);
        self.version = match (v4, carry) {
            (false, _) => OFF_VERSION,
            (true, false) => ON_VERSION,
            (true, true) => CARRY_VERSION,
        };
        self.carry_window = self.window.on_at(at, origin, self.carry_window);
        for schedule in [&mut self.v4, &mut self.carry, &mut self.window] {
            schedule.settle(at, origin);
        }
        self.sealed_origin = at.l1_origin;
    }

    /// The rules section of a seed, big-endian, in the layout of the
    /// sealer snapshot: `[version:u8][sealed_origin:u64]` and the schedule
    /// of feature 3, the schedule of feature 4, then
    /// `[carry_window:u8]` and the schedule of feature 5. A schedule is
    /// `[held:u8]`, then `[origin:u64][time_ms:u64]` for each held
    /// activation, oldest first.
    pub(crate) fn encode(&self) -> impl Iterator<Item = u8> + '_ {
        std::iter::once(self.version)
            .chain(self.sealed_origin.to_be_bytes())
            .chain(self.v4.encode())
            .chain(self.carry.encode())
            .chain(std::iter::once(u8::from(self.carry_window)))
            .chain(self.window.encode())
    }

    /// These rules after they take each of `records`, in order, while the
    /// open block executes with `open_block_ms`.
    fn taken(mut self, records: &[(Activation, u64)], open_block_ms: u64) -> Self {
        for &(activation, state_origin) in records {
            self.on_activation(activation, state_origin, open_block_ms);
        }
        self
    }
}

/// The walk of [`SealerRules::at_head`] over the rebuilt blocks.
struct RulesWalk {
    rules: SealerRules,
    /// The boundary of the last block seen, which the walk applies once it
    /// knows the records in front of the next block. `None` before the
    /// first block.
    pending: Option<PendingBoundary>,
    /// The L1 origin of the last block seen.
    origin: u64,
    /// The time of the last block seen: the time that the next block
    /// executes with. 0 at genesis.
    last_ms: u64,
    /// The canonical end of the last block seen, when its payload carries it.
    last_end: Option<u64>,
    /// The number of the block the walk takes next.
    next_block: u64,
}

/// The boundary of a block, and whether that block held a record. Only a
/// block that holds a record can close at the boundary that an epoch
/// forces.
#[derive(Clone, Copy)]
struct PendingBoundary {
    at: Boundary,
    held_records: bool,
}

impl RulesWalk {
    fn new(features: GenesisFeatures) -> Self {
        Self {
            rules: SealerRules::genesis(features),
            pending: None,
            origin: 0,
            last_ms: 0,
            last_end: Some(0),
            next_block: 1,
        }
    }

    /// Take `block`: the activation records in front of its epochs, then
    /// its boundary, which the next step applies.
    fn step(mut self, block: &ReplayBlock) -> Result<Self, ReconstructError> {
        if block.block_number != self.next_block {
            return Err(ReconstructError(format!(
                "the sealer rules need every block from block 1: block {} comes where block {} belongs",
                block.block_number, self.next_block
            )));
        }
        let records = self.records(block);
        self.rules = match self.pending {
            None => self.rules.taken(&records, self.last_ms),
            Some(pending) => Self::in_order(self.rules, pending, &records, block.block_number)?,
        };
        let origin = block.canonical_end.map_or(self.origin, |end| end.l1_origin);
        let end = block.canonical_end.map(|end| end.end_tx_idx);
        self.pending = Some(PendingBoundary {
            at: Boundary {
                l1_origin: origin,
                sealed_ms: self.last_ms,
                l2_timestamp: block.l2_timestamp,
            },
            held_records: end.zip(self.last_end).is_none_or(|(end, last)| end > last),
        });
        self.origin = origin;
        self.last_ms = block.l2_timestamp;
        self.last_end = end;
        self.next_block = block.block_number.checked_add(1).ok_or_else(|| {
            ReconstructError(format!("block {} has no next block", block.block_number))
        })?;
        Ok(self)
    }

    /// The activation records in front of the epochs of `block`, each with
    /// the L1 origin of the state when it comes: the origin before its epoch.
    fn records(&self, block: &ReplayBlock) -> Vec<(Activation, u64)> {
        block
            .l1_epochs
            .iter()
            .scan(self.origin, |state_origin, epoch| {
                let before = std::mem::replace(state_origin, epoch.l1_number);
                Some(SEALER_FEATURES.iter().filter_map(move |&feature_id| {
                    FeatureSchedule::last_in(epoch, feature_id).map(|schedule| {
                        let activation = Activation {
                            l1_origin: epoch.l1_number,
                            feature_id,
                            time_ms: schedule.activation_timestamp,
                        };
                        (activation, before)
                    })
                }))
            })
            .flatten()
            .collect()
    }

    /// The rules after `pending` and `records` in the order of the sealer.
    /// A timer tick can close the block of `pending` before the records,
    /// and the epoch after them closes it when it holds a record. Both
    /// orders must give the same rules.
    fn in_order(
        rules: SealerRules,
        pending: PendingBoundary,
        records: &[(Activation, u64)],
        block: u64,
    ) -> Result<SealerRules, ReconstructError> {
        let forced = pending.held_records.then(|| {
            let mut first = rules.clone().taken(records, pending.at.sealed_ms);
            first.at_boundary(&pending.at);
            first
        });
        let mut ticked = rules;
        ticked.at_boundary(&pending.at);
        let ticked = ticked.taken(records, pending.at.l2_timestamp);
        match forced {
            Some(forced) if forced != ticked => Err(ReconstructError(format!(
                "block {block}: the sealer rules depend on whether its activation records came before \
                 or after the boundary of the block before it; the seed cannot know them"
            ))),
            _ => Ok(ticked),
        }
    }

    /// The rules with the boundary of the last block applied.
    fn finish(self) -> SealerRules {
        let mut rules = self.rules;
        if let Some(pending) = self.pending {
            rules.at_boundary(&pending.at);
        }
        rules
    }
}

#[cfg(test)]
#[path = "rules_tests.rs"]
mod tests;
