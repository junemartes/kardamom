//! The rebuild of a block range from its references: the bytes of every
//! transaction come from the `tx_data` archives, through the refetch the
//! engine uses for a join miss, and the blocks close as the live feed
//! closes them, so they pack to the bytes the live path posts.

use std::collections::{BTreeSet, HashMap};

use alloy_primitives::keccak256;
use anyhow::{Context, Result, bail};
use kardamom_engine::reader::{JoinRecovery, JoinRecoveryFactory};
use kardamom_types::{BPosition, TxEnvelope};
use tracing::info;

use super::refs_store::{BlockRefs, BlockTxRef};
use crate::batch::{ClosedBlock, RecordedTx};

/// Where a transaction's bytes are: the join key of the `tx_data`
/// archives.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ArchiveLoc {
    pub(crate) shard_id: u8,
    pub(crate) session_id: i32,
    pub(crate) position: BPosition,
}

impl BlockTxRef {
    fn loc(&self) -> ArchiveLoc {
        ArchiveLoc {
            shard_id: self.shard_id,
            session_id: self.session_id,
            position: BPosition::from_index(self.position),
        }
    }
}

/// A source of transaction bytes by archive location: the durability
/// archives, or a map in a test.
pub(crate) trait EnvelopeSource {
    /// The envelopes at `wanted`, keyed by location. A location the source
    /// cannot serve is absent; the caller names it.
    ///
    /// # Errors
    ///
    /// Returns an error when the source fails before it can say which
    /// locations it holds.
    fn fetch(&mut self, wanted: &BTreeSet<ArchiveLoc>) -> Result<HashMap<ArchiveLoc, TxEnvelope>>;
}

impl EnvelopeSource for HashMap<ArchiveLoc, TxEnvelope> {
    fn fetch(&mut self, wanted: &BTreeSet<ArchiveLoc>) -> Result<HashMap<ArchiveLoc, TxEnvelope>> {
        Ok(wanted
            .iter()
            .filter_map(|loc| self.get(loc).map(|env| (*loc, env.clone())))
            .collect())
    }
}

/// The durability archives as an [`EnvelopeSource`]. One bounded replay
/// per publisher session, from the lowest wanted position, delivers the
/// recording's tail; the sink keeps the wanted locations and drops the
/// rest. A replay the drain budget cuts short leaves locations missing,
/// and the next replay starts at the lowest of them, until nothing is
/// missing or a replay delivers nothing new.
pub(crate) struct ArchiveEnvelopes {
    recovery: JoinRecovery,
}

/// One publisher session's wanted locations and what is found so far.
struct SessionFetch<'a> {
    shard_id: u8,
    session_id: i32,
    wanted: Vec<&'a ArchiveLoc>,
    found: HashMap<ArchiveLoc, TxEnvelope>,
}

impl SessionFetch<'_> {
    /// The lowest wanted location not found yet.
    fn lowest_missing(&self) -> Option<ArchiveLoc> {
        self.wanted
            .iter()
            .copied()
            .find(|loc| !self.found.contains_key(loc))
            .copied()
    }
}

impl ArchiveEnvelopes {
    pub(crate) fn new(factory: JoinRecoveryFactory) -> Self {
        Self {
            recovery: factory.build(),
        }
    }

    /// Fetch one session's wanted locations, replay by replay: the
    /// envelopes found.
    fn fetch_session(
        &mut self,
        mut fetch: SessionFetch<'_>,
    ) -> Result<HashMap<ArchiveLoc, TxEnvelope>> {
        while let Some(from) = fetch.lowest_missing() {
            self.replay_from(&mut fetch, from)?;
        }
        Ok(fetch.found)
    }

    /// One replay from `from`: the wanted locations it delivers land in
    /// `fetch.found`. A replay that delivers none of them is the end of
    /// what the archives hold for this session.
    fn replay_from(&mut self, fetch: &mut SessionFetch<'_>, from: ArchiveLoc) -> Result<()> {
        let before = fetch.found.len();
        let shard_id = fetch.shard_id;
        let session_id = fetch.session_id;
        let wanted: BTreeSet<ArchiveLoc> = fetch.wanted.iter().copied().copied().collect();
        let found = &mut fetch.found;
        let delivered = self
            .recovery
            .recover_tx_data(shard_id, session_id, from.position, |loc, env| {
                let at = ArchiveLoc {
                    shard_id,
                    session_id: loc.session_id,
                    position: loc.position,
                };
                if wanted.contains(&at) {
                    found.insert(at, env);
                }
            })
            .with_context(|| {
                format!("refetch tx_data shard {shard_id} session {session_id} from {from:?}")
            })?;
        info!(
            shard_id,
            session_id,
            from = ?from.position,
            delivered,
            found = fetch.found.len(),
            wanted = fetch.wanted.len(),
            "rebuild: archive replay"
        );
        if fetch.found.len() == before {
            bail!(
                "the tx_data archives of shard {shard_id} session {session_id} do not hold the \
                 transaction at {:?}",
                from.position
            );
        }
        Ok(())
    }
}

impl EnvelopeSource for ArchiveEnvelopes {
    fn fetch(&mut self, wanted: &BTreeSet<ArchiveLoc>) -> Result<HashMap<ArchiveLoc, TxEnvelope>> {
        // Group by publisher session, in location order, so each replay
        // starts at the lowest wanted position of its recording.
        let mut sessions: Vec<SessionFetch<'_>> = Vec::new();
        for loc in wanted {
            Self::group(&mut sessions, loc);
        }
        let mut all = HashMap::new();
        for fetch in sessions {
            all.extend(self.fetch_session(fetch)?);
        }
        Ok(all)
    }
}

impl ArchiveEnvelopes {
    /// Add `loc` to its session's group, or open the group. The locations
    /// arrive in order, so a group is the last one or a new one.
    fn group<'a>(sessions: &mut Vec<SessionFetch<'a>>, loc: &'a ArchiveLoc) {
        match sessions.last_mut() {
            Some(last) if last.shard_id == loc.shard_id && last.session_id == loc.session_id => {
                last.wanted.push(loc);
            }
            _ => sessions.push(SessionFetch {
                shard_id: loc.shard_id,
                session_id: loc.session_id,
                wanted: vec![loc],
                found: HashMap::new(),
            }),
        }
    }
}

/// Builds closed blocks from their references: a trait so the live
/// service rebuilds from the archives and a test from a map, through one
/// recovery turn.
pub(crate) trait Rebuilder {
    /// The closed blocks of `blocks`, in order, with every transaction's
    /// bytes checked against its hash.
    ///
    /// # Errors
    ///
    /// Returns an error when a transaction's bytes are not served, or do
    /// not hash to the reference.
    fn rebuild(self, blocks: Vec<BlockRefs>) -> Result<Vec<ClosedBlock>>;
}

/// The live rebuilder: the archives, reached through the join-miss
/// refetch the reader stack is configured with.
pub(crate) struct ArchiveRebuilder {
    pub(crate) factory: JoinRecoveryFactory,
}

impl Rebuilder for ArchiveRebuilder {
    fn rebuild(self, blocks: Vec<BlockRefs>) -> Result<Vec<ClosedBlock>> {
        rebuild(&blocks, &mut ArchiveEnvelopes::new(self.factory))
    }
}

/// Close every block of `blocks` from the envelopes `source` serves for
/// its references. Each envelope's hash is checked against the reference
/// and recomputed from its bytes: the archive is trusted the way the live
/// stream is, after the same check the engine does.
///
/// # Errors
///
/// Returns an error when the source does not serve a reference, or when
/// an envelope does not hash to its reference.
pub(crate) fn rebuild<E: EnvelopeSource>(
    blocks: &[BlockRefs],
    source: &mut E,
) -> Result<Vec<ClosedBlock>> {
    let wanted: BTreeSet<ArchiveLoc> = blocks
        .iter()
        .flat_map(|b| b.refs.iter().map(BlockTxRef::loc))
        .collect();
    let found = source.fetch(&wanted)?;
    let closed = blocks
        .iter()
        .map(|block| close(block, &found))
        .collect::<Result<Vec<_>>>()?;
    info!(
        blocks = closed.len(),
        transactions = wanted.len(),
        "rebuild: blocks closed from the archives"
    );
    Ok(closed)
}

/// Close one block: its boundary from the references, its transactions
/// from the envelopes. A block rebuilt this way carries no remote-epoch
/// record; the query endpoint refuses a block that had one.
fn close(block: &BlockRefs, found: &HashMap<ArchiveLoc, TxEnvelope>) -> Result<ClosedBlock> {
    let txs = block
        .refs
        .iter()
        .map(|r| recorded(r, found, block.block_number))
        .collect::<Result<Vec<_>>>()?;
    Ok(ClosedBlock {
        block_number: block.block_number,
        l2_timestamp: block.l2_timestamp,
        end_tx_idx: BPosition::from_index(block.end_tx_idx),
        l1_origin: block.l1_origin,
        remote_epochs: Vec::new(),
        txs,
    })
}

/// The recorded transaction of one reference: the envelope at its
/// location, whose hash and bytes must agree with the reference.
fn recorded(
    r: &BlockTxRef,
    found: &HashMap<ArchiveLoc, TxEnvelope>,
    block_number: u64,
) -> Result<RecordedTx> {
    let envelope = found.get(&r.loc()).with_context(|| {
        format!(
            "the archives do not serve transaction {} of block {block_number} (shard {} \
             session {} position {})",
            r.tx_hash, r.shard_id, r.session_id, r.position
        )
    })?;
    let digest = keccak256(&envelope.raw_tx);
    if envelope.tx_hash != r.tx_hash || digest != r.tx_hash {
        bail!(
            "transaction {} of block {block_number}: the archive served an envelope with hash \
             {} whose bytes hash to {digest}",
            r.tx_hash,
            envelope.tx_hash
        );
    }
    Ok(RecordedTx {
        position: BPosition::from_index(r.tx_idx),
        envelope: envelope.clone(),
    })
}

/// A `tx_data` location as the refetch sink reports it, for the tests of
/// this module's callers.
#[cfg(test)]
pub(crate) fn loc_of(shard_id: u8, loc: kardamom_types::TxDataLoc) -> ArchiveLoc {
    ArchiveLoc {
        shard_id,
        session_id: loc.session_id,
        position: loc.position,
    }
}

#[cfg(test)]
#[path = "rebuild_tests.rs"]
pub(crate) mod tests;
