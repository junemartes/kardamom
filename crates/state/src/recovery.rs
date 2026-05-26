//! Cold-start recovery (§5).
//!
//! On startup the writer:
//! 1. opens (or creates) the env (`StateEnvBuilder::open`),
//! 2. reads the meta cursors via [`read_recovery_point`],
//! 3. opens an initial snapshot,
//! 4. emits a [`RecoveryPoint`] that tells the executor where to resume
//!    reading B from.
//!
//! Recovery itself is read-only — no replay logic lives in this crate; the
//! executor consumes B from `recovery_point.last_fsynced_b_position` and
//! re-derives any blocks the writer never got to commit.

use types::BPosition;

use crate::env::StateEnv;
use crate::error::StateError;
use crate::meta::{
    DurableCursors, KEY_LAST_COMMITTED_BLOCK, KEY_LAST_COMMITTED_END_TX_POSITION,
    KEY_LAST_FSYNCED_B_POSITION, SCHEMA_VERSION, decode_b_position, decode_u64,
};
use crate::schema::TABLE_META;

/// Cursors read out of the `meta` table at startup. The writer uses this to
/// hand the executor a resume point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryPoint {
    pub last_committed_block: u64,
    pub last_committed_end_tx_position: BPosition,
    pub last_fsynced_b_position: BPosition,
}

impl RecoveryPoint {
    /// Genesis point — used when no prior data is on disk.
    pub fn genesis() -> Self {
        Self {
            last_committed_block: 0,
            last_committed_end_tx_position: BPosition::ZERO,
            last_fsynced_b_position: BPosition::ZERO,
        }
    }
}

pub fn read_recovery_point(env: &StateEnv) -> Result<RecoveryPoint, StateError> {
    let txn = env.raw().begin_ro_sync()?;
    let meta = txn.open_db(Some(TABLE_META))?;

    let last_committed_block = match txn.get::<Vec<u8>>(meta.dbi(), KEY_LAST_COMMITTED_BLOCK)? {
        Some(b) => decode_u64(&b)?,
        None => 0,
    };
    let last_committed_end_tx_position =
        match txn.get::<Vec<u8>>(meta.dbi(), KEY_LAST_COMMITTED_END_TX_POSITION)? {
            Some(b) => decode_b_position(&b)?,
            None => BPosition::ZERO,
        };
    let last_fsynced_b_position =
        match txn.get::<Vec<u8>>(meta.dbi(), KEY_LAST_FSYNCED_B_POSITION)? {
            Some(b) => decode_b_position(&b)?,
            None => BPosition::ZERO,
        };

    Ok(RecoveryPoint {
        last_committed_block,
        last_committed_end_tx_position,
        last_fsynced_b_position,
    })
}

impl From<RecoveryPoint> for DurableCursors {
    fn from(p: RecoveryPoint) -> Self {
        DurableCursors {
            last_committed_block: p.last_committed_block,
            last_committed_end_tx_position: p.last_committed_end_tx_position,
            last_fsynced_b_position: p.last_fsynced_b_position,
            schema_version: SCHEMA_VERSION,
        }
    }
}
