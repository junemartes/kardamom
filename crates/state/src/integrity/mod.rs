//! Offline integrity checks over a state DB, and a deep table-level
//! comparison between two of them.
//!
//! This module supports the chain-semantics end-to-end test suite. See
//! docs/agents/chain-semantics-e2e-suite-spec.md.
//! After a test run, [`sweep`] proves that a single DB is internally
//! coherent: every row decodes, the header chain is dense, the receipt
//! index is bijective, and the persisted trie root reproduces from the
//! trie tables. [`deep_compare`] proves that an executor DB and a
//! validator DB hold byte-identical chain state.
//!
//! Both checks are read-only in effect. They open read-write transactions
//! only because the trie rebuild oracle needs one, and they never write.
//!
//! The `kardamom-statecheck` binary also exposes these checks for
//! operational use.
//!
//! Layout: the per-table sweep checks live in [`checks`], and the
//! cross-DB diff lives in `compare`. This module owns [`IntegrityReport`]
//! and the [`sweep`] orchestrator.

mod checks;
mod compare;

#[cfg(test)]
mod tests;

pub use compare::deep_compare;

use alloy_primitives::B256;

use crate::env::StateEnv;
use crate::error::StateError;
use crate::schema::TABLE_META;

/// The outcome of a [`sweep`]. `problems` is empty if and only if the DB
/// passed every check.
///
/// Note on `state_root`: `seed_genesis` stores one on every database. So a
/// plain-writer (executor) DB carries the genesis root, frozen at block 0,
/// while its chain advances. Only the trie-aware (validator) writer keeps
/// this root live. So the rebuild check below validates the genesis-era
/// trie on executor DBs, and the current trie on validator DBs. Both
/// cases are internally consistent, which is all a sweep can assert
/// about one database alone.
#[derive(Debug, Default)]
pub struct IntegrityReport {
    pub last_committed_block: u64,
    pub headers: u64,
    pub receipts: u64,
    pub accounts: u64,
    pub storage_slots: u64,
    /// `meta[state_root]`, when present. Only trie-aware writers set this.
    pub state_root: Option<B256>,
    /// The root recomputed from the trie tables, when a stored root exists.
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
/// This delegates to the per-table checks in [`checks`], in a fixed
/// order: meta, headers, receipts and index, accounts, storage, trie.
/// The order is part of the contract. Tests check the report's counts,
/// and downstream tooling consumes them.
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
