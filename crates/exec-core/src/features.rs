//! Feature flags, and the block-close protocol actions they gate.
//!
//! A feature flag lives in the `KardamomChainState` predeploy
//! ([`kardamom_types::upgrades::CHAIN_STATE`]), as
//! `featureId => activation timestamp (ms)`. Only a system deposit, derived
//! from an L1 upgrade transaction, writes it.
//!
//! ## When a flag switches
//!
//! A flag changes only at a block boundary. The flags of block N come from
//! two inputs:
//!
//! - the activation words in the state after block N-1, and
//! - the time that block N executes with: the header time of block N-1, or
//!   0 for the first block after genesis.
//!
//! A feature is active when its word is not 0 and is at most that time.
//! [`BlockFeatures::read`] reads every flag once, before the first record of
//! block N, into the block environment ([`crate::block_env::ExecEnv`]).
//! Every transaction, every deposit and the block-close actions of block N
//! use this one set. A `setFeature` deposit in block N changes the flags
//! from block N+1 on. `setFeature(id, 0)` stores the time that its block
//! executes with, so the feature is active from the next block.
//!
//! Every function here is pure. Given the same state and the same header
//! times, every role and every block driver computes the same flags and
//! the same actions. This makes an upgrade activate at the same block on
//! every node, instead of whenever each operator restarts a binary.
//!
//! ## Why this lives in `exec-core`
//!
//! This workspace has more than one block driver: the live engine's exec
//! thread, the Block-STM strategy, the validator's parallel path, the
//! offline replay driver, and the stateless/zk guest shape. A flag read or
//! a block-close action implemented in only some of them causes a
//! consensus divergence. [`BlockFeatures::read`] and [`BlockClose::apply`]
//! are the single implementation for all drivers.

use alloy_primitives::{B256, U256};
use kardamom_types::StateDatabase;
use kardamom_types::upgrades::{CHAIN_STATE, FEATURE_SEALER_V3};

use crate::delta::{ParentState, PendingDelta};
use crate::error::ExecutorError;

/// Health check. This is the first feature, and it exercises the upgrade
/// path itself. It must match `KardamomChainState.FEATURE_HEALTH_CHECK`.
///
/// While active, every block records a [health beacon](HEALTH_BEACON_SLOT).
pub const FEATURE_HEALTH_CHECK: u64 = 1;

/// Storage slot of `KardamomChainState.healthBeacon`. Declaration order puts
/// the `_activation` mapping at slot 0 and this field at slot 1. The
/// contract's own test pins this against the compiled storage layout.
pub const HEALTH_BEACON_SLOT: B256 = B256::new([
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
]);

pub use kardamom_types::upgrades::activation_slot;

/// The feature flags of one block, frozen before its first record. See the
/// module doc for the rule.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BlockFeatures {
    /// [`FEATURE_HEALTH_CHECK`]: the block records a health beacon.
    pub(crate) health_check: bool,
    /// [`FEATURE_SEALER_V3`]: a skip at the chain nonce of its sender uses
    /// its nonce.
    pub(crate) sealer_v3: bool,
}

impl BlockFeatures {
    /// The flags of a block with no active feature.
    pub const NONE: Self = Self {
        health_check: false,
        sealer_v3: false,
    };

    /// Read the flags of a block from the state before it: `snapshot` under
    /// the unsettled `parent` layer, which together hold the state after
    /// the previous block. `exec_ts_ms` is the time that the block executes
    /// with (the header time of the previous block).
    ///
    /// # Errors
    ///
    /// Returns `Err` when a state read fails.
    pub fn read<S: StateDatabase>(
        snapshot: &S,
        parent: Option<&PendingDelta>,
        exec_ts_ms: u64,
    ) -> Result<Self, ExecutorError> {
        let none = PendingDelta::new();
        let state = ParentState::new(&none, parent, snapshot);
        // Activation is inclusive (`<=`), as in `KardamomChainState.isActive`.
        // A word of 0 means never scheduled.
        let active = |feature_id: u64| -> Result<bool, ExecutorError> {
            let word = state.storage(CHAIN_STATE, activation_slot(feature_id))?;
            Ok(!word.is_zero() && word <= U256::from(exec_ts_ms))
        };
        Ok(Self {
            health_check: active(FEATURE_HEALTH_CHECK)?,
            sealer_v3: active(FEATURE_SEALER_V3)?,
        })
    }

    /// `true` when [`FEATURE_SEALER_V3`] is on in the block.
    #[must_use]
    pub fn sealer_v3(self) -> bool {
        self.sealer_v3
    }
}

/// A health beacon: a heartbeat counter, the block it was last recorded
/// at, and that block's header timestamp. Packed into one storage word
/// as `count | block << 64 | timestamp << 128`, mirroring
/// `KardamomChainState.health()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Beacon {
    pub count: u64,
    pub block_number: u64,
    pub timestamp_ms: u64,
}

impl Beacon {
    /// Pack into one word. Each field is already a native `u64`, so it
    /// fits its 64-bit lane exactly and cannot bleed into a neighbor:
    /// the type is what bounds them, not a runtime saturate or wrap.
    /// `64 + 128 < 256`, so all three lanes fit the `U256` word with
    /// room to spare.
    #[must_use]
    pub fn pack(self) -> U256 {
        U256::from(self.count)
            | (U256::from(self.block_number) << 64)
            | (U256::from(self.timestamp_ms) << 128)
    }

    /// Inverse of [`Beacon::pack`].
    #[must_use]
    pub fn unpack(word: U256) -> Self {
        let mask = U256::from(u64::MAX);
        let field = |shift: usize| -> u64 { ((word >> shift) & mask).to::<u64>() };
        Self {
            count: field(0),
            block_number: field(64),
            timestamp_ms: field(128),
        }
    }
}

/// What the block-close pass did, for logging and metrics. This is empty
/// when no feature is active, which is the common case.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BlockCloseOutcome {
    /// `Some(beat)` if the health beacon was recorded for this block.
    pub health_beat: Option<u64>,
}

/// The block-close pass of one block: its number, its own header time, and
/// the flags that it froze before its first record.
#[derive(Debug, Clone, Copy)]
pub struct BlockClose {
    pub block_number: u64,
    /// The header time of the block itself. The beacon records it. It
    /// decides no flag: the flags of the block are `features`.
    pub header_ts_ms: u64,
    pub features: BlockFeatures,
}

impl BlockClose {
    /// Apply every block-close protocol action into `delta`.
    ///
    /// Call this at block close, after the block's records are folded into
    /// `delta` and before the delta goes to the writer. The actions read the
    /// block's own writes, and their writes go into the same delta.
    ///
    /// `parent` and `snapshot` supply the state layers this function cannot
    /// see on its own: the caller's unsettled-parent layer, then its state
    /// snapshot. Together with `delta`, [`ParentState`] gives the same
    /// `delta -> parent -> snapshot` read order the EVM uses. Reading the
    /// snapshot alone would be wrong for two reasons: it lags by up to K
    /// unsettled blocks, and it cannot hold the current block's own writes.
    ///
    /// Writes go into `delta` only, not into the block's EIP-7928 BAL. The
    /// BAL attributes accesses to transaction indices, and the validator
    /// verifies claims per tx-index range. A block-close action belongs to
    /// no transaction, so attributing it to one would misdescribe the block
    /// and cause a mismatch with a validator that recomputes claims from
    /// transactions. The write is still cross-checked between roles,
    /// because the validator compares the whole `BlockDelta`.
    ///
    /// # Errors
    ///
    /// Returns `Err` when a `ParentState` read fails.
    pub fn apply<S: StateDatabase>(
        self,
        delta: &mut PendingDelta,
        parent: Option<&PendingDelta>,
        snapshot: &S,
    ) -> Result<BlockCloseOutcome, ExecutorError> {
        if !self.features.health_check {
            return Ok(BlockCloseOutcome::default());
        }
        let read = ParentState::new(delta, parent, snapshot);
        let prior = Beacon::unpack(read.storage(CHAIN_STATE, HEALTH_BEACON_SLOT)?);
        let beat = prior.count.saturating_add(1);
        delta.storage.insert(
            (CHAIN_STATE, HEALTH_BEACON_SLOT),
            Beacon {
                count: beat,
                block_number: self.block_number,
                timestamp_ms: self.header_ts_ms,
            }
            .pack(),
        );
        Ok(BlockCloseOutcome {
            health_beat: Some(beat),
        })
    }
}

#[cfg(test)]
#[path = "features_tests.rs"]
mod tests;
