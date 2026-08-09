//! Offline integrity checks over a state DB, and a deep table-level
//! comparison between two of them.
//!
//! Built for the chain-semantics e2e suite
//! (`docs/agents/chain-semantics-e2e-suite-spec.md`, S6/S9): after a run,
//! [`sweep`] proves a single DB is internally coherent (every row decodes,
//! the header chain is dense, the receipt index is bijective, the persisted
//! trie root reproduces from the trie tables), and [`deep_compare`] proves an
//! executor DB and a validator DB hold byte-identical chain state. Both are
//! read-only in effect; they open RW transactions only because the trie
//! rebuild oracle requires one, and never write.
//!
//! Also exposed operationally via the `kardamom-statecheck` binary.
//!
//! Layout: the per-table sweep checks live in [`checks`], the cross-DB diff
//! in `compare`; this module owns [`IntegrityReport`] and the [`sweep`]
//! orchestrator.

mod checks;
mod compare;

#[cfg(test)]
mod tests;

pub use compare::deep_compare;

use alloy_primitives::B256;

use crate::env::StateEnv;
use crate::error::StateError;
use crate::schema::TABLE_META;

/// Outcome of a [`sweep`]. `problems` empty ⇔ the DB passed every check.
///
/// Note on `state_root`: `seed_genesis` stores one on EVERY database, so a
/// plain-writer (executor) DB carries the genesis root frozen at block 0
/// while its chain advances — only the trie-aware (validator) writer keeps
/// it live. The rebuild check below therefore validates the genesis-era trie
/// on executor DBs and the current trie on validator DBs; both are internally
/// consistent, which is what a sweep can assert about one database alone.
#[derive(Debug, Default)]
pub struct IntegrityReport {
    pub last_committed_block: u64,
    pub headers: u64,
    pub receipts: u64,
    pub accounts: u64,
    pub storage_slots: u64,
    /// `meta[state_root]` when present (trie-aware writers only).
    pub state_root: Option<B256>,
    /// Root recomputed from the trie tables when a stored root exists.
    pub rebuilt_root: Option<B256>,
    pub problems: Vec<String>,
}

impl IntegrityReport {
    pub fn is_clean(&self) -> bool {
        self.problems.is_empty()
    }
}

/// Run the full invariant sweep over one state DB.
///
/// Delegates to the per-table checks in [`checks`], in a fixed order (meta,
/// headers, receipts + index, accounts, storage, trie). The order is part of
/// the contract: the report's counts are asserted by tests and consumed by
/// downstream tooling.
pub fn sweep(env: &StateEnv) -> Result<IntegrityReport, StateError> {
    let txn = env.raw().begin_rw_sync()?;
    let mut r = IntegrityReport::default();

    let meta = txn.open_db(Some(TABLE_META))?;

    let meta_end_tx = checks::check_meta(&txn, meta, &mut r)?;
    checks::check_headers(&txn, meta_end_tx, &mut r)?;
    checks::check_receipts_index(&txn, meta_end_tx, &mut r)?;
    checks::check_accounts(&txn, &mut r)?;
    checks::check_storage(&txn, &mut r)?;
    checks::check_trie(&txn, meta, &mut r)?;

    Ok(r)
}
