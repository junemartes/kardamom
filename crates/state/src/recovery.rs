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

use kardamom_types::BPosition;

use crate::env::StateEnv;
use crate::error::StateError;
use crate::meta::{
    KEY_LAST_COMMITTED_BLOCK, KEY_LAST_COMMITTED_END_TX_POSITION, KEY_LAST_FSYNCED_B_POSITION,
    decode_b_position, decode_u64,
};
use crate::schema::{TABLE_HEADERS, TABLE_META, decode_header_value, encode_block_key};

/// Cursors read out of the `meta` table at startup. The writer uses this to
/// hand the executor a resume point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryPoint {
    pub last_committed_block: u64,
    pub last_committed_end_tx_position: BPosition,
    pub last_fsynced_b_position: BPosition,
    /// The committed block's boundary `l2_timestamp` (from its `headers` row).
    /// The exec thread seeds its block-timestamp state from this on a
    /// resume-from-cursor: block N+1's txs execute with boundary N's timestamp,
    /// and a resumed replica no longer sees boundary N — deriving it from
    /// anything else would diverge from the replicas that never restarted.
    /// 0 when nothing is committed yet (fresh DB / genesis-only).
    pub last_committed_l2_timestamp: u64,
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
    // The committed block's header row is written in the same txn as the meta
    // cursors, so present-cursor/absent-header means a corrupt env — surface
    // it rather than defaulting (a wrong timestamp silently diverges state).
    let last_committed_l2_timestamp = if last_committed_block > 0 {
        let headers = txn.open_db(Some(TABLE_HEADERS))?;
        match txn.get::<Vec<u8>>(headers.dbi(), &encode_block_key(last_committed_block))? {
            Some(b) => decode_header_value(&b)?.l2_timestamp,
            None => {
                return Err(StateError::Recovery(format!(
                    "meta cursor says block {last_committed_block} committed but its headers row is missing"
                )));
            }
        }
    } else {
        0
    };

    Ok(RecoveryPoint {
        last_committed_block,
        last_committed_end_tx_position,
        last_fsynced_b_position,
        last_committed_l2_timestamp,
    })
}
