//! The rebuild from references, on a map of envelopes and on fake
//! executor archives: the property that matters, that a rebuilt range
//! packs to the bytes the live path packs, and the refusals, an envelope
//! not served and an envelope that does not hash to its reference.

use alloy_primitives::{Address, B256, keccak256};
use bytes::Bytes;
use kardamom_engine::reader::{ArchiveLocator, ExecArchive, ExecFetchError, LocatorAnswer};
use kardamom_types::{ExecTxRecord, TxRef};

use super::*;
use crate::batcher::{BatcherConfig, pack_blocks};

/// A signed-looking envelope: the hash is the keccak of the bytes, as the
/// proxy computes it.
pub(crate) fn envelope(seed: u8) -> TxEnvelope {
    let raw_tx = Bytes::from(vec![seed; 16]);
    TxEnvelope {
        correlation_id: u64::from(seed),
        tx_hash: keccak256(&raw_tx),
        raw_tx,
        sender: Address::repeat_byte(seed),
        max_inclusion_block: 99,
    }
}

/// A live closed block as the feed loop builds it from the sealer, with
/// `txs` transactions at the end of its range.
pub(crate) fn live_block(number: u64, end: u64, txs: u8) -> ClosedBlock {
    ClosedBlock {
        block_number: number,
        l2_timestamp: 1_700_000_000 + number,
        end_tx_idx: BPosition::from_index(end),
        l1_origin: 40 + number,
        remote_epochs: Vec::new(),
        txs: (0..txs)
            .map(|i| RecordedTx {
                position: BPosition::from_index(end - u64::from(txs) + u64::from(i)),
                envelope: envelope(u8::try_from((end * 8 + u64::from(i)) % 251).unwrap()),
            })
            .collect(),
    }
}

/// The references the query endpoint serves for `block`, with each
/// transaction on shard `tx_idx % 2`, session 7, at archive position
/// `1000 + tx_idx`.
pub(crate) fn refs_of(block: &ClosedBlock) -> BlockRefs {
    BlockRefs {
        block_number: block.block_number,
        end_tx_idx: block.end_tx_idx.as_index(),
        l1_origin: block.l1_origin,
        l2_timestamp: block.l2_timestamp,
        refs: block
            .txs
            .iter()
            .map(|t| BlockTxRef {
                tx_hash: t.envelope.tx_hash,
                tx_idx: t.position.as_index(),
                shard_id: u8::try_from(t.position.as_index() % 2).unwrap(),
                session_id: 7,
                position: 1000 + t.position.as_index(),
            })
            .collect(),
        exec_locator: None,
    }
}

/// The archive's content for `blocks`: every envelope at its canonical
/// index.
pub(crate) fn archive_of(blocks: &[ClosedBlock]) -> HashMap<u64, TxEnvelope> {
    blocks
        .iter()
        .flat_map(|b| &b.txs)
        .map(|t| (t.position.as_index(), t.envelope.clone()))
        .collect()
}

/// The executor stream records of `blocks`, in canonical order.
fn records_of(blocks: &[ClosedBlock]) -> Vec<ExecTxRecord> {
    blocks
        .iter()
        .flat_map(|b| refs_of(b).refs.into_iter().zip(b.txs.iter().cloned()))
        .map(|(r, t)| ExecTxRecord {
            index: r.tx_idx,
            tx_ref: TxRef::new(
                r.tx_hash,
                r.shard_id,
                BPosition::from_index(r.position),
                r.session_id,
            ),
            envelope: t.envelope,
        })
        .collect()
}

/// The locator of the archive `archive_id`.
fn locator(archive_id: &str) -> ArchiveLocator {
    ArchiveLocator {
        archive_id: archive_id.to_owned(),
        session_id: 3,
        position: 0,
    }
}

/// Executor archives with no network behind them: the locator answer of
/// each executor, and the records each archive replays from any locator.
#[derive(Default)]
struct FakeExecArchives {
    answers: Vec<LocatorAnswer>,
    archives: HashMap<String, Vec<ExecTxRecord>>,
}

impl FakeExecArchives {
    /// Add an executor whose archive `archive_id` holds `records`, and
    /// which answers `located` for every index.
    fn located(mut self, archive_id: &str, records: Vec<ExecTxRecord>) -> Self {
        self.answers
            .push(LocatorAnswer::Located(locator(archive_id)));
        self.archives.insert(archive_id.to_owned(), records);
        self
    }

    /// Add an executor that answers `answer` for every index.
    fn answering(mut self, answer: LocatorAnswer) -> Self {
        self.answers.push(answer);
        self
    }

    /// Add the archive `archive_id` with `records`, which no executor
    /// names in an answer.
    fn holding(mut self, archive_id: &str, records: Vec<ExecTxRecord>) -> Self {
        self.archives.insert(archive_id.to_owned(), records);
        self
    }
}

impl ExecArchive for FakeExecArchives {
    fn executors(&self) -> usize {
        self.answers.len()
    }

    fn locate(
        &mut self,
        executor: usize,
        _index: u64,
        _tx_hash: B256,
    ) -> Result<LocatorAnswer, ExecFetchError> {
        Ok(self.answers[executor].clone())
    }

    fn replay(
        &mut self,
        at: &ArchiveLocator,
        sink: impl FnMut(ExecTxRecord),
    ) -> Result<u64, ExecFetchError> {
        let records = self
            .archives
            .get(&at.archive_id)
            .cloned()
            .unwrap_or_default();
        let count = u64::try_from(records.len()).unwrap();
        records.into_iter().for_each(sink);
        Ok(count)
    }
}

/// The blocks of the exec-archive tests: two blocks with transactions
/// around an empty one.
fn live_range() -> Vec<ClosedBlock> {
    vec![
        live_block(11, 12, 2),
        live_block(12, 12, 0),
        live_block(13, 15, 3),
    ]
}

/// Rebuild `live` from `archives`.
fn rebuild_exec(live: &[ClosedBlock], archives: FakeExecArchives) -> Result<Vec<ClosedBlock>> {
    let refs = live.iter().map(refs_of).collect::<Vec<_>>();
    rebuild(&refs, &mut ExecArchiveEnvelopes::new(archives))
}

/// The rebuilt range packs to the same payload and commitment as the
/// live blocks: the archive's bytes are the posted bytes.
#[test]
fn a_rebuilt_range_packs_to_the_live_bytes() {
    let live = vec![
        live_block(11, 12, 2),
        live_block(12, 12, 0),
        live_block(13, 15, 3),
    ];
    let mut archive = archive_of(&live);
    let rebuilt = rebuild(&live.iter().map(refs_of).collect::<Vec<_>>(), &mut archive).unwrap();
    assert_eq!(rebuilt, live);
    let cfg = BatcherConfig {
        compress: false,
        ..BatcherConfig::default()
    };
    let from_live = pack_blocks(&cfg, &live).unwrap();
    let from_rebuilt = pack_blocks(&cfg, &rebuilt).unwrap();
    assert_eq!(from_rebuilt.payload, from_live.payload);
    assert_eq!(
        from_rebuilt.records_commitment,
        from_live.records_commitment
    );
}

/// An envelope the archives do not serve names its transaction and
/// block.
#[test]
fn an_envelope_not_served_is_named() {
    let live = vec![live_block(11, 12, 2)];
    let mut archive = archive_of(&live);
    let lost = live[0].txs[1].envelope.tx_hash;
    archive.retain(|_, env| env.tx_hash != lost);
    let err = rebuild(&live.iter().map(refs_of).collect::<Vec<_>>(), &mut archive).unwrap_err();
    let text = format!("{err:#}");
    assert!(
        text.contains(&format!("{lost}")) && text.contains("block 11"),
        "{text}"
    );
}

/// An envelope whose bytes do not hash to the reference is refused: the
/// archive is checked the way the live stream is.
#[test]
fn an_envelope_that_does_not_hash_to_its_reference_is_refused() {
    let live = vec![live_block(11, 12, 2)];
    let mut archive = archive_of(&live);
    let forged = live[0].txs[0].envelope.tx_hash;
    archive
        .values_mut()
        .filter(|env| env.tx_hash == forged)
        .for_each(|env| env.raw_tx = Bytes::from_static(b"not those bytes"));
    let err = rebuild(&live.iter().map(refs_of).collect::<Vec<_>>(), &mut archive).unwrap_err();
    assert!(
        format!("{err:#}").contains("whose bytes hash to"),
        "{err:#}"
    );

    let mut archive = archive_of(&live);
    archive
        .values_mut()
        .filter(|env| env.tx_hash == forged)
        .for_each(|env| env.tx_hash = B256::repeat_byte(0xEE));
    let err = rebuild(&live.iter().map(refs_of).collect::<Vec<_>>(), &mut archive).unwrap_err();
    assert!(format!("{err:#}").contains("with hash"), "{err:#}");
}

/// The range rebuilt from the executor archives is the live range, and it
/// packs to the same payload and commitment as the range rebuilt from the
/// `tx_data` archives: the two sources serve the same bytes.
#[test]
fn a_range_rebuilt_from_the_executor_archives_packs_to_the_live_bytes() {
    let live = live_range();
    let archives = FakeExecArchives::default()
        .answering(LocatorAnswer::NotReached)
        .located("executor-1", records_of(&live));
    let from_exec = rebuild_exec(&live, archives).unwrap();
    assert_eq!(from_exec, live);
    let from_tx_data = rebuild(
        &live.iter().map(refs_of).collect::<Vec<_>>(),
        &mut archive_of(&live),
    )
    .unwrap();
    let cfg = BatcherConfig::default();
    let packed_exec = pack_blocks(&cfg, &from_exec).unwrap();
    let packed_tx_data = pack_blocks(&cfg, &from_tx_data).unwrap();
    assert_eq!(packed_exec.payload, packed_tx_data.payload);
    assert_eq!(
        packed_exec.records_commitment,
        packed_tx_data.records_commitment
    );
}

/// A record that no executor archive holds names its transaction, its
/// canonical index and its block.
#[test]
fn a_block_with_a_record_no_executor_archive_holds_names_it() {
    let live = live_range();
    let lost = &live[2].txs[1];
    let records = records_of(&live)
        .into_iter()
        .filter(|r| r.index != lost.position.as_index())
        .collect();
    let archives = FakeExecArchives::default()
        .located("executor-0", records)
        .answering(LocatorAnswer::NotHeld)
        .answering(LocatorAnswer::Lost);
    let err = rebuild_exec(&live, archives).unwrap_err();
    let text = format!("{err:#}");
    assert!(
        text.contains(&format!("{}", lost.envelope.tx_hash))
            && text.contains(&format!("canonical index {}", lost.position.as_index()))
            && text.contains("block 13"),
        "{text}"
    );
}

/// A copy whose bytes do not hash to the canonical hash stays out, and
/// the copy of the next executor fills the index.
#[test]
fn a_copy_that_fails_the_check_is_filled_from_the_next_executor() {
    let live = live_range();
    let forged = live[0].txs[1].position.as_index();
    let mut bad = records_of(&live);
    bad.iter_mut()
        .filter(|r| r.index == forged)
        .for_each(|r| r.envelope.raw_tx = Bytes::from_static(b"not those bytes"));
    let archives = FakeExecArchives::default()
        .located("executor-0", bad)
        .located("executor-1", records_of(&live));
    assert_eq!(rebuild_exec(&live, archives).unwrap(), live);
}

/// The `exec_locator` of a block's references replays its archive with no
/// locator query: the executors here answer none.
#[test]
fn the_locator_of_the_block_refs_needs_no_query() {
    let live = live_range();
    let mut refs = live.iter().map(refs_of).collect::<Vec<_>>();
    for b in &mut refs {
        b.exec_locator = Some(locator("executor-2"));
    }
    let archives = FakeExecArchives::default()
        .answering(LocatorAnswer::NotReached)
        .holding("executor-2", records_of(&live));
    let rebuilt = rebuild(&refs, &mut ExecArchiveEnvelopes::new(archives)).unwrap();
    assert_eq!(rebuilt, live);
}
