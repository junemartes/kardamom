//! Per-tx revm execution. Adapted from `crates/node/src/executor.rs::execute`.
//!
//! Differences from the node executor:
//!
//! - Reads come from a snapshot-backed `StateDatabase`, through a
//!   `DatabaseRef` adapter, layered through revm's `CacheDB`. This lets
//!   writes from earlier txs in the same block be observed.
//! - Writes are captured into a per-tx `WriteSet` (built from revm's
//!   `EvmState` output), and merged into the running `PendingDelta` by the
//!   actor.
//!
//! This module does not compute `tx_hash`. It copies `tx_hash` (and
//! `sender`) directly from the inbound `kardamom_types::TxEnvelope`, which
//! the proxy populated at the system boundary.
//!
//! Module layout:
//!
//! - `db`: snapshot-backed `DatabaseRef` adapters, and the shared
//!   view-composition primitive (`seed_cache_layer`).
//! - `tx_env`: envelope decoding and `TxEnv` derivation.
//! - `scope`: [`Executor`] (per-block EVM and cache), and the
//!   [`Executor::execute_once`] one-shot wrapper.
//! - `scope_derived`: the deposit and cross-chain execution paths on
//!   [`Executor`].
//! - `derived`: the receipt shape both derived-transaction kinds (and
//!   both entry-point pairs) share.
//! - `skip`: the deterministic skip path (reason classification and the
//!   skip receipt).
//! - `deposit`: OP-aligned deposit execution.
//! - `xchain`: cross-chain message delivery execution.
//! - `write_set`: `WriteSet` extraction from revm state, and BAL recording.

mod db;
mod deposit;
mod derived;
mod scope;
mod scope_derived;
mod skip;
mod tx_env;
mod write_set;
mod xchain;

pub use db::{SnapshotDb, SnapshotRef, StateRefError};
pub use deposit::execute_deposit_tx;
pub use scope::{Executor, TouchSet};
pub use skip::skip_reason_of_tx;
pub use tx_env::DecodedTx;
pub use xchain::{XChainDelivery, execute_xchain_tx};

#[cfg(test)]
pub(crate) mod test_support {
    use kardamom_types::{BPosition, BlockBoundaryStart, Receipt, TxEnvelope};

    use crate::delta::{PendingDelta, WriteSet};
    use crate::exec_types::{TxIndex, TxSlot};
    use crate::state::MockStateDatabase;

    use super::scope::Executor;

    /// Build a [`TxSlot`] from plain integers, for tests. `tx_idx` and
    /// `tx_position` are given separately, not derived from one counter:
    /// several call sites use a non-trivial `BPosition` (a later tx's
    /// byte offset in the `tx_data` stream) that does not equal its
    /// `TxIndex`.
    pub(crate) fn slot(
        tx_idx: u64,
        tx_position: u64,
        tx_index_in_block: u64,
        cumulative_gas_used_before: u64,
    ) -> TxSlot {
        TxSlot {
            tx_idx: TxIndex(tx_idx),
            tx_position: BPosition::from_index(tx_position),
            tx_index_in_block,
            cumulative_gas_used_before,
        }
    }

    /// One `Executor::execute_once` call, with the fixed arguments scope
    /// tests spread over up to twelve lines collapsed into one call.
    pub(crate) fn run_once(
        snap: &MockStateDatabase,
        delta: &PendingDelta,
        env: crate::block_env::ExecEnv,
        slot: TxSlot,
        tx: &TxEnvelope,
        bal: Option<(&mut revm::state::bal::Bal, u64)>,
        label: &str,
    ) -> (Receipt, WriteSet) {
        Executor::execute_once(snap, None, delta, env, slot, tx, bal)
            .unwrap_or_else(|e| panic!("{label}: {e:?}"))
    }

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
}
