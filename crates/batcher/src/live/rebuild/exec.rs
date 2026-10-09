//! The executor archives as an [`EnvelopeSource`].
//!
//! A block's references name the canonical index and the hash of each
//! transaction. The executor archives hold the executor stream, whose
//! records carry the canonical index, so the rebuild reads them by index:
//!
//! 1. Replay from the `exec_locator` of each block, when the query
//!    endpoint gave one.
//! 2. For the lowest index still missing, ask each executor in turn where
//!    its archive holds it (`kardamom_getExecLocator`), and replay the
//!    first archive that answers `located`. One replay delivers a run of
//!    records, so the next indices often need no ask.
//! 3. Stop at an index that no executor archive delivers. The close of
//!    the block names that transaction.
//!
//! A record counts only when its `tx_ref` carries the canonical hash and
//! its bytes hash to it. A copy that fails stays out, so the copy of
//! another executor can still fill the index.

use std::collections::{BTreeMap, HashMap};

use alloy_primitives::{B256, keccak256};
use anyhow::Result;
use kardamom_engine::reader::{ArchiveLocator, ExecArchive, LocatorAnswer};
use kardamom_types::{ExecTxRecord, TxEnvelope};
use tracing::{info, warn};

use super::EnvelopeSource;
use crate::live::refs_store::BlockRefs;

/// The executor archives of a rebuild.
pub(crate) struct ExecArchiveEnvelopes<X> {
    archive: X,
}

/// The wanted records of a rebuild, and the envelopes found so far.
struct ExecFetch {
    /// The canonical hash of each wanted index.
    wanted: BTreeMap<u64, B256>,
    found: HashMap<u64, TxEnvelope>,
}

impl ExecFetch {
    fn new(blocks: &[BlockRefs]) -> Self {
        Self {
            wanted: blocks
                .iter()
                .flat_map(|b| &b.refs)
                .map(|r| (r.tx_idx, r.tx_hash))
                .collect(),
            found: HashMap::new(),
        }
    }

    /// Keep the envelope of `record` when its index is wanted and not
    /// found yet, and the record passes the check against the canonical
    /// hash.
    fn keep(&mut self, record: ExecTxRecord) {
        let passes = self.wanted.get(&record.index).is_some_and(|hash| {
            record.tx_ref.tx_hash == *hash && keccak256(&record.envelope.raw_tx) == *hash
        });
        if passes {
            self.found.entry(record.index).or_insert(record.envelope);
        }
    }

    /// The lowest wanted index not found yet, with its canonical hash.
    fn lowest_missing(&self) -> Option<(u64, B256)> {
        self.wanted
            .iter()
            .find(|(index, _)| !self.found.contains_key(index))
            .map(|(index, hash)| (*index, *hash))
    }
}

impl<X: ExecArchive> ExecArchiveEnvelopes<X> {
    pub(crate) fn new(archive: X) -> Self {
        Self { archive }
    }

    /// Ask each executor in turn where its archive holds `index`, and
    /// replay until one archive delivers it. `false` when none does.
    fn ask_all(&mut self, fetch: &mut ExecFetch, index: u64, tx_hash: B256) -> bool {
        let executors = self.archive.executors();
        (0..executors).any(|executor| self.ask(fetch, executor, index, tx_hash))
    }

    /// Ask one executor, and replay its archive on a `located` answer.
    /// `true` when the replay delivered `index`.
    fn ask(&mut self, fetch: &mut ExecFetch, executor: usize, index: u64, tx_hash: B256) -> bool {
        match self.archive.locate(executor, index, tx_hash) {
            Ok(LocatorAnswer::Located(at)) => {
                self.replay(fetch, &at);
                fetch.found.contains_key(&index)
            }
            Ok(other) => {
                info!(
                    executor,
                    index,
                    answer = other.label(),
                    "rebuild: executor holds no locator for the index"
                );
                false
            }
            Err(e) => {
                warn!(executor, index, error = %e, "rebuild: locator query failed");
                false
            }
        }
    }

    /// One replay of the archive that `at` names, unless every wanted
    /// record is found. A failed replay is no answer: the next executor
    /// can still deliver the record.
    fn replay(&mut self, fetch: &mut ExecFetch, at: &ArchiveLocator) {
        if fetch.lowest_missing().is_none() {
            return;
        }
        let before = fetch.found.len();
        match self.archive.replay(at, |record| fetch.keep(record)) {
            Ok(delivered) => info!(
                archive_id = %at.archive_id,
                session_id = at.session_id,
                position = at.position,
                delivered,
                found_before = before,
                found = fetch.found.len(),
                wanted = fetch.wanted.len(),
                "rebuild: executor archive replay"
            ),
            Err(e) => warn!(
                archive_id = %at.archive_id,
                session_id = at.session_id,
                error = %e,
                "rebuild: the replay of an executor archive failed"
            ),
        }
    }
}

impl<X: ExecArchive> EnvelopeSource for ExecArchiveEnvelopes<X> {
    fn fetch(&mut self, blocks: &[BlockRefs]) -> Result<HashMap<u64, TxEnvelope>> {
        let mut fetch = ExecFetch::new(blocks);
        info!(
            blocks = blocks.len(),
            wanted = fetch.wanted.len(),
            executors = self.archive.executors(),
            "rebuild: reading the executor archives"
        );
        let mut hints: Vec<&ArchiveLocator> = blocks
            .iter()
            .filter_map(|b| b.exec_locator.as_ref())
            .collect();
        hints.dedup();
        for at in hints {
            self.replay(&mut fetch, at);
        }
        while let Some((index, tx_hash)) = fetch.lowest_missing() {
            if !self.ask_all(&mut fetch, index, tx_hash) {
                break;
            }
        }
        Ok(fetch.found)
    }
}
