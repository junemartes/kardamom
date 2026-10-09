//! The live feed packs the same bytes from either transaction source: the
//! `tx_ordering` reader of the batcher runs once over the `tx_data` lanes
//! and once over the executor stream, on one canonical order, and the
//! closed blocks pack to the same payload and commitment.

use std::collections::VecDeque;
use std::time::Duration;

use kardamom_engine::ExecutorError;
use kardamom_engine::reader::{
    ExecRecordSubscription, ExecStreamSource, LiveExecArchiveSeed, TxDataSource, TxDataSubscription,
};
use kardamom_types::{
    BPosition, BlockBoundaryStart, ExecTxRecord, TxDataLoc, TxEnvelope, TxOrderingMessage, TxRef,
};

use super::*;
use crate::batch::{BatchAccumulator, ClosedBlock};
use crate::batcher::{BatcherConfig, pack_blocks};
use crate::live::rebuild::tests::envelope;

/// A canonical order that closes when empty.
struct Order(VecDeque<(BPosition, TxOrderingMessage)>);

impl TxOrderingSubscription for Order {
    fn next(&mut self) -> Result<(BPosition, TxOrderingMessage), ExecutorError> {
        self.0.pop_front().ok_or(ExecutorError::TxOrderingClosed)
    }
}

/// One `tx_data` lane that closes when empty.
struct Lane {
    id: u8,
    queue: VecDeque<(TxDataLoc, TxEnvelope)>,
}

impl TxDataSubscription for Lane {
    fn sequencer_id(&self) -> u8 {
        self.id
    }

    fn next(&mut self) -> Result<(TxDataLoc, TxEnvelope), ExecutorError> {
        self.queue.pop_front().ok_or(ExecutorError::TxDataClosed {
            sequencer_id: self.id,
        })
    }
}

/// The live executor stream, closed when empty.
struct Stream(VecDeque<ExecTxRecord>);

impl ExecRecordSubscription for Stream {
    fn next(&mut self) -> Option<ExecTxRecord> {
        self.0.pop_front()
    }
}

/// One transaction of the test chain: its canonical index, its
/// reference, and its envelope. Lane `index % 2`, session 7, at lane
/// position `500 + index`.
struct Tx {
    index: u64,
    reference: TxRef,
    envelope: TxEnvelope,
}

impl Tx {
    fn at(index: u64) -> Self {
        let envelope = envelope(u8::try_from(index + 1).unwrap());
        let shard = u8::try_from(index % 2).unwrap();
        let reference = TxRef::new(
            envelope.tx_hash,
            shard,
            BPosition::from_index(500 + index),
            7,
        );
        Self {
            index,
            reference,
            envelope,
        }
    }

    fn record(&self) -> ExecTxRecord {
        ExecTxRecord {
            index: self.index,
            tx_ref: self.reference,
            envelope: self.envelope.clone(),
        }
    }
}

/// The test chain: block 1 with two transactions, block 2 empty, block 3
/// with three. Every message takes the next canonical index.
struct Chain {
    order: Vec<(BPosition, TxOrderingMessage)>,
    txs: Vec<Tx>,
}

impl Chain {
    fn new() -> Self {
        let mut chain = Self {
            order: Vec::new(),
            txs: Vec::new(),
        };
        for (block, txs) in [(1, 2), (2, 0), (3, 3)] {
            chain.block(block, txs);
        }
        chain
    }

    fn next_position(&self) -> BPosition {
        BPosition::from_index(u64::try_from(self.order.len()).unwrap())
    }

    /// Append `txs` transactions and the boundary of `block`.
    fn block(&mut self, block: u64, txs: u64) {
        (0..txs).for_each(|_| self.tx());
        let at = self.next_position();
        let boundary = BlockBoundaryStart {
            block_number: block,
            end_tx_idx: at,
            l2_timestamp: 1_700_000_000 + block,
            l1_origin: 40 + block,
        };
        self.order
            .push((at, TxOrderingMessage::BoundaryStart(boundary)));
    }

    fn tx(&mut self) {
        let at = self.next_position();
        let tx = Tx::at(at.as_index());
        self.order
            .push((at, TxOrderingMessage::TxRef(tx.reference)));
        self.txs.push(tx);
    }

    fn order(&self) -> Order {
        Order(self.order.iter().cloned().collect())
    }

    /// The `tx_data` source: two lanes that hold every envelope.
    fn tx_data(&self) -> TxDataSource<Lane> {
        let lanes = (0..2)
            .map(|id| Lane {
                id,
                queue: self
                    .txs
                    .iter()
                    .filter(|tx| tx.reference.shard_id == id)
                    .map(|tx| {
                        let loc = TxDataLoc::new(7, tx.reference.tx_data_position);
                        (loc, tx.envelope.clone())
                    })
                    .collect(),
            })
            .collect();
        TxDataSource::new(lanes, None)
    }

    /// The executor stream source: three executors publish each record,
    /// and no archive backs a miss.
    fn exec_stream(&self) -> ExecStreamSource<Stream, Option<LiveExecArchiveSeed>> {
        let records = self
            .txs
            .iter()
            .flat_map(|tx| [tx.record(), tx.record(), tx.record()])
            .collect();
        ExecStreamSource::new(Stream(records), None)
    }
}

/// Run the reader of the batcher over `source`, and close the blocks as
/// the feed loop closes them.
fn closed_blocks<S: TxSource>(chain: &Chain, source: S) -> Vec<ClosedBlock> {
    let run = FeedReader {
        order: chain.order(),
        source,
        cfg: ReaderConfig {
            join_timeout: Duration::from_secs(10),
            ..ReaderConfig::default()
        },
    }
    .spawn();
    let FeedReaderRun {
        join_handles,
        ordering_handle,
        mut feed_rx,
    } = run;
    let mut acc = BatchAccumulator::new();
    let blocks = std::iter::from_fn(|| feed_rx.blocking_recv())
        .filter_map(|msg| match msg {
            ReaderToExec::Tx {
                envelope, position, ..
            } => {
                acc.observe_tx(envelope, position);
                None
            }
            ReaderToExec::Boundary(b) => Some(acc.observe_boundary(&b)),
            _ => None,
        })
        .collect();
    ordering_handle
        .join()
        .expect("no panic")
        .expect("a clean reader");
    for h in join_handles {
        // A lane that closes ends its reader with `TxDataClosed`.
        h.join().expect("no panic").ok();
    }
    blocks
}

/// The packed group of both sources is the same, byte for byte: the
/// executor stream changes where the bytes come from, not what the
/// batcher posts.
#[test]
fn both_sources_pack_a_group_to_the_same_bytes() {
    let chain = Chain::new();
    let from_tx_data = closed_blocks(&chain, chain.tx_data());
    let from_exec_stream = closed_blocks(&chain, chain.exec_stream());
    assert_eq!(from_tx_data.len(), 3);
    assert_eq!(
        from_tx_data.iter().map(|b| b.txs.len()).collect::<Vec<_>>(),
        [2, 0, 3]
    );
    assert_eq!(from_exec_stream, from_tx_data);
    let cfg = BatcherConfig::default();
    let packed_tx_data = pack_blocks(&cfg, &from_tx_data).unwrap();
    let packed_exec_stream = pack_blocks(&cfg, &from_exec_stream).unwrap();
    assert_eq!(packed_exec_stream.payload, packed_tx_data.payload);
    assert_eq!(
        packed_exec_stream.records_commitment,
        packed_tx_data.records_commitment
    );
}

/// A reader that stops because every executor archive holds a record that
/// fails the check ends the stack in the operator-cleared halt; a refused
/// replay and any other failure keep their ends.
#[test]
fn a_mismatch_on_every_archive_is_the_operator_cleared_halt() {
    let feed_err = anyhow::anyhow!("tx_ordering reader channel closed");
    let mismatch = ExecutorError::ExecRecordMismatch {
        index: 9,
        tx_hash: alloy_primitives::B256::repeat_byte(7),
        executors: 3,
    };
    let ReaderEnd::Halted(halt) = ReaderEnd::of_reader(mismatch, &feed_err) else {
        panic!("a mismatch is a halt");
    };
    assert_eq!(halt.cause, HaltCause::ExecRecordMismatch);
    assert_eq!(halt.clears, kardamom_obs::halt::Clears::Operator);
    assert!(halt.detail.contains("index=9"), "{}", halt.detail);
    assert!(matches!(
        ReaderEnd::of_reader(ExecutorError::TxOrderingClosed, &feed_err),
        ReaderEnd::Failed(_)
    ));
}
