//! Per-tx revm execution. Adapted from `crates/node/src/executor.rs::execute`.
//!
//! Differences from the node executor:
//! - Reads come from a snapshot-backed `StateDatabase` via a `DatabaseRef`
//!   adapter, layered through revm's `CacheDB` so writes from earlier txs in
//!   the same block are observed.
//! - Writes are captured into a per-tx `WriteSet` (built from revm's
//!   `EvmState` output) and merged into the running `PendingDelta` by the
//!   actor.
//!
//!this module does **not** compute `tx_hash`. It copies
//! `tx_hash` (and `sender`) directly from the inbound `kardamom_types::
//! TxEnvelope`, which the proxy (S1) populated at the system boundary.
//!
//! Module layout (pure split of the former single-file `executor.rs`):
//! - `db`: snapshot-backed `DatabaseRef` adapters + the shared
//!   view-composition primitive (`seed_cache_layer`).
//! - `tx_env`: envelope decoding and `TxEnv` derivation.
//! - `scope`: [`ExecScope`] (per-block EVM + cache) and the per-call
//!   [`execute_tx`] wrapper, including the deterministic invalid-skip path.
//! - `deposit`: OP-aligned deposit execution.
//! - `write_set`: `WriteSet` extraction from revm state and BAL recording.

mod db;
mod deposit;
mod scope;
mod tx_env;
mod write_set;

pub use db::{SnapshotDb, SnapshotRef, StateRefError};
pub use deposit::execute_deposit_tx;
pub use scope::{ExecScope, TouchSet, execute_tx, invalid_skip};
pub use tx_env::{decode_alloy_envelope, tx_env_from_alloy};
pub use write_set::{record_writeset_into_bal, wire_log, write_set_from_evm_state};

#[cfg(test)]
pub(crate) mod test_support {
    use kardamom_types::{BPosition, BlockBoundaryStart};

    pub(crate) fn boundary(block_number: u64) -> BlockBoundaryStart {
        BlockBoundaryStart {
            block_number,
            end_tx_idx: BPosition {
                term_id: 0,
                term_offset: 0,
            },
            l2_timestamp: 0,
            l1_origin: 0,
        }
    }

    pub(crate) fn pos(off: i32) -> BPosition {
        BPosition {
            term_id: 0,
            term_offset: off,
        }
    }
}
