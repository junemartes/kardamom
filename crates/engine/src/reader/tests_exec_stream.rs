//! Tests for the executor-stream sink of the `tx_ordering` reader.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use alloy_primitives::{Address, B256};
use alloy_signer_local::PrivateKeySigner;
use kardamom_types::{
    BPosition, BlockBoundaryStart, Deposit, EpochRecord, TxEnvelope, TxOrderingMessage, TxRef,
};

use super::join::TxDataKey;
use super::tests::pos;
use super::*;
use crate::error::ExecutorError;

/// What one of the two sinks saw, in the order the reader sent it.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Seen {
    StreamRecord(u64),
    StreamPassed(u64),
    ExecTx(u64),
    ExecOther,
}

/// One log that both sinks append to, so a test sees the order across
/// the two sinks.
#[derive(Clone, Default)]
struct SharedLog(Arc<Mutex<Vec<Seen>>>);

impl SharedLog {
    fn push(&self, seen: Seen) {
        self.0.lock().expect("log lock").push(seen);
    }

    fn take(&self) -> Vec<Seen> {
        std::mem::take(&mut *self.0.lock().expect("log lock"))
    }
}

impl ExecSink for SharedLog {
    fn send(&self, msg: ReaderToExec) -> Result<(), SinkClosed> {
        let seen = match msg {
            ReaderToExec::Tx { position, .. } => Seen::ExecTx(position.as_index()),
            _ => Seen::ExecOther,
        };
        self.push(seen);
        Ok(())
    }
}

/// The stream side of the shared log. With `open = false` it reports a
/// gone publisher.
#[derive(Clone)]
struct StreamLog {
    log: SharedLog,
    open: bool,
}

impl ExecStreamSink for StreamLog {
    fn record(&self, index: u64, tx_ref: &TxRef, envelope: &TxEnvelope) -> Result<(), SinkClosed> {
        assert_eq!(
            tx_ref.tx_hash, envelope.tx_hash,
            "the record pairs ref and envelope"
        );
        self.open
            .then(|| self.log.push(Seen::StreamRecord(index)))
            .ok_or(SinkClosed)
    }

    fn passed(&self, through: u64) -> Result<(), SinkClosed> {
        self.open
            .then(|| self.log.push(Seen::StreamPassed(through)))
            .ok_or(SinkClosed)
    }
}

struct QueueSub(VecDeque<(BPosition, TxOrderingMessage)>);

impl TxOrderingSubscription for QueueSub {
    fn next(&mut self) -> Result<(BPosition, TxOrderingMessage), ExecutorError> {
        self.0.pop_front().ok_or(ExecutorError::TxOrderingClosed)
    }
}

/// A joinable `TxRef` on lane 0 at `off`, with its envelope in `buffer`.
fn joinable(buffer: &JoinBuffer, signer: &PrivateKeySigner, off: i32) -> TxOrderingMessage {
    let nonce = u64::try_from(off).expect("a small offset");
    let env = crate::actor::test_support::legacy(signer, Address::from([0x22u8; 20]), nonce, 1);
    let tx_ref = TxRef::new(env.tx_hash, 0, pos(off), 0);
    buffer.insert(TxDataKey::new(0, 0, pos(off)), env);
    TxOrderingMessage::TxRef(tx_ref)
}

/// Run the reader over `queue` with both sinks on one log.
fn run(
    queue: Vec<(BPosition, TxOrderingMessage)>,
    buffer: JoinBuffer,
    open: bool,
) -> (Result<(), ExecutorError>, Vec<Seen>) {
    let log = SharedLog::default();
    let reader = TxOrderingReader::new(TxOrderingInputs {
        sub: QueueSub(queue.into()),
        buffer,
        cfg: ReaderConfig::default(),
        exec_out: log.clone(),
        exec_stream: StreamLog {
            log: log.clone(),
            open,
        },
        recovery_factory: None,
    });
    let outcome = reader.run();
    (outcome, log.take())
}

#[test]
fn a_joined_record_reaches_the_stream_before_the_exec_thread() {
    let signer = PrivateKeySigner::random();
    let buffer = JoinBuffer::new();
    let queue = vec![
        (pos(0), joinable(&buffer, &signer, 0)),
        (pos(1), joinable(&buffer, &signer, 1)),
    ];
    let (outcome, seen) = run(queue, buffer, true);
    outcome.expect("the reader ends on the clean close");
    assert_eq!(
        seen,
        [
            Seen::StreamRecord(0),
            Seen::ExecTx(0),
            Seen::StreamPassed(0),
            Seen::StreamRecord(1),
            Seen::ExecTx(1),
            Seen::StreamPassed(1),
        ]
    );
}

#[test]
fn an_epoch_marks_its_last_slot_and_a_boundary_marks_nothing() {
    let deposits: Vec<Deposit> = (0..2)
        .map(|i| Deposit {
            source_hash: B256::repeat_byte(0xD0 + i),
            ..Default::default()
        })
        .collect();
    let epoch = EpochRecord {
        l1_number: 7,
        l1_hash: B256::repeat_byte(0xE1),
        deposits,
    };
    let boundary = BlockBoundaryStart {
        block_number: 1,
        end_tx_idx: pos(8),
        l2_timestamp: 1_700_000_000,
        l1_origin: 7,
    };
    let queue = vec![
        (pos(5), TxOrderingMessage::Epoch(epoch)),
        (pos(8), TxOrderingMessage::BoundaryStart(boundary)),
    ];
    let (outcome, seen) = run(queue, JoinBuffer::new(), true);
    outcome.expect("the reader ends on the clean close");
    let marks: Vec<Seen> = seen
        .into_iter()
        .filter(|s| matches!(s, Seen::StreamPassed(_)))
        .collect();
    assert_eq!(marks, [Seen::StreamPassed(7)]);
}

#[test]
fn a_gone_stream_publisher_stops_the_reader_before_execution() {
    let signer = PrivateKeySigner::random();
    let buffer = JoinBuffer::new();
    let queue = vec![(pos(0), joinable(&buffer, &signer, 0))];
    let (outcome, seen) = run(queue, buffer, false);
    let err = outcome.expect_err("a gone publisher is an error");
    assert!(err.to_string().contains("executor stream"), "got {err}");
    assert_eq!(seen, []);
}
