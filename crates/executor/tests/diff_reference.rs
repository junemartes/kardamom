//! Differential test: the actor's receipt for each tx must match a
//! naive single-threaded `revm` loop's receipt for the same tx.
//!
//! The corpus covers transfers, a contract `SSTORE`, and a revert.
//!
//! The topology is M=1 `tx_data`, plus one `tx_ordering`. The demux
//! shape does not affect determinism.
//!
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    reason = "indices here are bounded by the small fixed test corpus, never near a truncation or wrap boundary"
)]

use std::thread;
use std::time::Duration;

use alloy_consensus::{SignableTransaction, TxLegacy};
use alloy_eips::eip2718::Decodable2718;
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{
    Address, Bytes as AlloyBytes, TxKind as APTxKind, U256, address, keccak256,
};
use alloy_signer_local::PrivateKeySigner;
use bytes::Bytes;
use crossbeam_channel::{Receiver, Sender, bounded};
use revm::context::result::ExecutionResult;
use revm::context::{BlockEnv, CfgEnv, TxEnv};
use revm::database::CacheDB;
use revm::primitives::{KECCAK_EMPTY, TxKind};
use revm::state::Bytecode;
use revm::{Context, ExecuteCommitEvm, MainBuilder, MainContext};

use kardamom_engine::executor::SnapshotRef;
use kardamom_engine::{
    BPosition, BlockBoundaryStart, CMessage, EngineWiring, Executor, ExecutorConfig, ExecutorError,
    Inbound, MockStateDatabase, MutatingSnapshotSource, NoEpochCheck, Outbound, ResumePoint,
    RoleHooks, StateWriterSignal, TxDataSubscription, TxEnvelope as KtTxEnvelope,
    TxOrderingMessage, TxOrderingSubscription, TxReceiptsPublication, TxRef, WriterApplyingQueue,
};

// Minimal: PUSH1 0x42; PUSH1 0x00; SSTORE; STOP
const SSTORE_42_AT_0: [u8; 6] = [0x60, 0x42, 0x60, 0x00, 0x55, 0x00];
// PUSH1 0x00; PUSH1 0x00; REVERT
const REVERT_CODE: [u8; 5] = [0x60, 0x00, 0x60, 0x00, 0xfd];

struct ChanTxDataSub {
    sequencer_id: u8,
    rx: Receiver<(BPosition, KtTxEnvelope)>,
}
impl TxDataSubscription for ChanTxDataSub {
    fn sequencer_id(&self) -> u8 {
        self.sequencer_id
    }
    fn next(&mut self) -> Result<(kardamom_types::TxDataLoc, KtTxEnvelope), ExecutorError> {
        self.rx
            .recv()
            .map(|(pos, env)| (kardamom_types::TxDataLoc::new(0, pos), env))
            .map_err(|_| ExecutorError::TxDataClosed {
                sequencer_id: self.sequencer_id,
            })
    }
}
struct ChanTxOrderingSub(Receiver<(BPosition, TxOrderingMessage)>);
impl TxOrderingSubscription for ChanTxOrderingSub {
    fn next(&mut self) -> Result<(BPosition, TxOrderingMessage), ExecutorError> {
        self.0.recv().map_err(|_| ExecutorError::TxOrderingClosed)
    }
}
struct ChanReceiptsPub(Sender<CMessage>);
impl TxReceiptsPublication for ChanReceiptsPub {
    fn publish(&mut self, m: CMessage) -> Result<(), ExecutorError> {
        self.0.send(m).map_err(|_| ExecutorError::TxReceiptsClosed)
    }
}
struct Imm;
impl StateWriterSignal for Imm {
    fn committed(&mut self) -> Result<u64, ExecutorError> {
        Ok(u64::MAX)
    }
    fn wait_committed(&mut self, b: u64) -> Result<u64, ExecutorError> {
        Ok(b)
    }
}

/// Port types for this test's channel-backed fakes.
struct TestWiring;
impl EngineWiring for TestWiring {
    type TxData = ChanTxDataSub;
    type TxOrdering = ChanTxOrderingSub;
    type TxReceipts = ChanReceiptsPub;
    type Snapshots = MutatingSnapshotSource;
    type WriterSignal = Imm;
    type WriterQueue = WriterApplyingQueue;
    type Epoch = NoEpochCheck;
}

fn bpos(off: i32) -> BPosition {
    BPosition {
        term_id: 0,
        term_offset: off,
    }
}

/// Build a proxy-style `kardamom_types::TxEnvelope` (with `raw_tx`,
/// sender, and `tx_hash` filled in). The naive reference decodes it back
/// to alloy for revm.
fn legacy(
    signer: &PrivateKeySigner,
    to: APTxKind,
    nonce: u64,
    value: u64,
    data: AlloyBytes,
    gas: u64,
) -> KtTxEnvelope {
    let mut tx = TxLegacy {
        chain_id: Some(1),
        nonce,
        gas_price: 0,
        gas_limit: gas,
        to,
        value: U256::from(value),
        input: data,
    };
    let sig = signer.sign_transaction_sync(&mut tx).unwrap();
    let alloy_env: alloy_consensus::TxEnvelope = tx.into_signed(sig).into();
    let raw_tx = Bytes::from(alloy_env.encoded_2718());
    let tx_hash = keccak256(&raw_tx);
    KtTxEnvelope {
        correlation_id: 0,
        raw_tx,
        sender: signer.address(),
        tx_hash,
    }
}

/// One transaction and the sender address it commits under, for
/// [`naive_reference`] and the actor comparison in
/// `actor_receipts_match_naive_reference`.
#[derive(Clone)]
struct TxWithSender {
    env: KtTxEnvelope,
    sender: Address,
}

/// One transaction's outcome: whether it succeeded, and its gas used.
/// [`naive_reference`] and the actor path both produce a `Vec` of
/// these, compared entry by entry.
#[derive(Debug, PartialEq, Eq)]
struct TxResult {
    status: bool,
    gas_used: u64,
}

/// Run one transaction from `entry` against `cache`, and return its
/// outcome.
fn run_one_naive(
    cache: &mut CacheDB<SnapshotRef<'_, MockStateDatabase>>,
    entry: &TxWithSender,
) -> TxResult {
    use alloy_consensus::Transaction;
    let mut slice: &[u8] = entry.env.raw_tx.as_ref();
    let env = alloy_consensus::TxEnvelope::decode_2718(&mut slice).expect("decode raw_tx");
    let tx_env = TxEnv {
        caller: entry.sender,
        chain_id: env.chain_id(),
        nonce: env.nonce(),
        gas_limit: env.gas_limit(),
        value: env.value(),
        data: env.input().clone(),
        kind: match env.to() {
            Some(a) => TxKind::Call(a),
            None => TxKind::Create,
        },
        gas_price: env.gas_price().unwrap_or_else(|| env.max_fee_per_gas()),
        ..Default::default()
    };
    #[allow(
        clippy::field_reassign_with_default,
        reason = "CfgEnv has many fields; building the default then setting chain_id is clearer than a full literal"
    )]
    let cfg: CfgEnv = {
        let mut c = CfgEnv::default();
        c.chain_id = 1;
        c
    };
    let blk = BlockEnv {
        number: U256::from(1u64),
        timestamp: U256::from(1_700_000_000u64),
        gas_limit: 30_000_000,
        basefee: 0,
        prevrandao: Some(alloy_primitives::B256::default()),
        ..Default::default()
    };
    let mut evm = Context::mainnet()
        .with_db(cache)
        .with_block(blk)
        .with_cfg(cfg)
        .build_mainnet();
    let r = evm.transact_commit(tx_env).expect("commit");
    TxResult {
        status: matches!(r, ExecutionResult::Success { .. }),
        gas_used: r.gas().tx_gas_used(),
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "each call constructs a fresh snapshot and has no further use for it"
)]
fn naive_reference(snap: MockStateDatabase, txs: &[TxWithSender]) -> Vec<TxResult> {
    let snap_ref = SnapshotRef { inner: &snap };
    let mut cache: CacheDB<SnapshotRef<'_, MockStateDatabase>> = CacheDB::new(snap_ref);
    txs.iter()
        .map(|entry| run_one_naive(&mut cache, entry))
        .collect()
}

/// Two snapshots of the same fixture accounts (a funded sender, an
/// `SSTORE` contract, and a reverting contract), plus the
/// three-transaction corpus every diff-reference test runs. The
/// reference path mutates its `CacheDB` in place; a second snapshot
/// keeps that out of the actor's reads.
struct DiffFixture {
    snap_ref: MockStateDatabase,
    snap_actor: MockStateDatabase,
    pairs: Vec<TxWithSender>,
}

fn build_diff_fixture() -> DiffFixture {
    let signer = PrivateKeySigner::random();
    let from = signer.address();
    let to = address!("00000000000000000000000000000000000ABCDE");
    let sstore_addr = address!("00000000000000000000000000000000000ABC55");
    let revert_addr = address!("00000000000000000000000000000000000ABCFD");

    let sstore_code = AlloyBytes::from_static(&SSTORE_42_AT_0);
    let revert_code = AlloyBytes::from_static(&REVERT_CODE);
    let sstore_hash = Bytecode::new_raw(sstore_code.clone()).hash_slow();
    let revert_hash = Bytecode::new_raw(revert_code.clone()).hash_slow();

    let build_snap = || {
        MockStateDatabase::builder()
            .account(from, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
            .account(sstore_addr, U256::ZERO, 1, sstore_hash)
            .account(revert_addr, U256::ZERO, 1, revert_hash)
            .code(sstore_hash, Bytes::copy_from_slice(sstore_code.as_ref()))
            .code(revert_hash, Bytes::copy_from_slice(revert_code.as_ref()))
            .build()
    };

    let txs: [KtTxEnvelope; 3] = [
        legacy(
            &signer,
            APTxKind::Call(to),
            0,
            10,
            AlloyBytes::new(),
            21_000,
        ),
        legacy(
            &signer,
            APTxKind::Call(sstore_addr),
            1,
            0,
            AlloyBytes::new(),
            100_000,
        ),
        legacy(
            &signer,
            APTxKind::Call(revert_addr),
            2,
            0,
            AlloyBytes::new(),
            100_000,
        ),
    ];
    let pairs: Vec<TxWithSender> = txs
        .iter()
        .cloned()
        .map(|env| TxWithSender { env, sender: from })
        .collect();

    DiffFixture {
        snap_ref: build_snap(),
        snap_actor: build_snap(),
        pairs,
    }
}

/// The receiving ends [`publish_diff_corpus`] hands to
/// [`spawn_actor_run`].
struct DiffChannels {
    a_rx: Receiver<(BPosition, KtTxEnvelope)>,
    b_rx: Receiver<(BPosition, TxOrderingMessage)>,
}

/// Publish `pairs` onto fresh `tx_data`/`tx_ordering` channels, then
/// the closing boundary. Returns the receiving ends, ready for
/// [`spawn_actor_run`].
fn publish_diff_corpus(pairs: &[TxWithSender]) -> DiffChannels {
    let (a_tx, a_rx) = bounded::<(BPosition, KtTxEnvelope)>(8);
    let (b_tx, b_rx) = bounded::<(BPosition, TxOrderingMessage)>(8);
    for (i, entry) in pairs.iter().enumerate() {
        let tx_data_position = bpos((i as i32) * 200);
        let tx_hash = entry.env.tx_hash;
        a_tx.send((tx_data_position, entry.env.clone())).unwrap();
        b_tx.send((
            bpos(i as i32),
            TxOrderingMessage::TxRef(TxRef::new(tx_hash, 0, tx_data_position, 0)),
        ))
        .unwrap();
    }
    b_tx.send((
        bpos(pairs.len() as i32),
        TxOrderingMessage::BoundaryStart(BlockBoundaryStart {
            block_number: 1,
            // end_tx_idx equals the cumulative count of canonical records
            // (the number of txs applied), encoded with bpos (the same as
            // BPosition::from_index here).
            end_tx_idx: bpos(pairs.len() as i32),
            l2_timestamp: 1_700_000_000,
            l1_origin: 0,
        }),
    ))
    .unwrap();
    DiffChannels { a_rx, b_rx }
}

/// Run the actor on its own thread, reading `a_rx`/`b_rx` and writing
/// into `snap_actor`. Returns its join handle and receipt channel.
struct ActorRun {
    join: thread::JoinHandle<Result<(), ExecutorError>>,
    c_rx: Receiver<CMessage>,
}

fn spawn_actor_run(snap_actor: MockStateDatabase, chans: DiffChannels) -> ActorRun {
    let (c_tx, c_rx) = bounded::<CMessage>(8);
    let writer_q = WriterApplyingQueue::new(snap_actor.clone());
    let snapshots = MutatingSnapshotSource(snap_actor);
    let tx_data_subs = vec![ChanTxDataSub {
        sequencer_id: 0,
        rx: chans.a_rx,
    }];
    let join = thread::spawn(move || {
        Executor::run::<TestWiring>(
            ExecutorConfig {
                chain_id: 1,
                receipt_queue_depth: 8,
                ..Default::default()
            },
            Inbound {
                tx_data: tx_data_subs,
                tx_ordering: ChanTxOrderingSub(chans.b_rx),
                join_recovery: None,
            },
            Outbound {
                tx_receipts: ChanReceiptsPub(c_tx),
                snapshots,
                writer_signal: Imm,
                writer_queue: writer_q,
            },
            ResumePoint::GENESIS,
            RoleHooks::none(),
        )
    });
    ActorRun { join, c_rx }
}

/// Collect every receipt `c_rx` delivers before it closes or times out.
fn collect_actor_receipts(c_rx: &Receiver<CMessage>) -> Vec<TxResult> {
    let mut actor = Vec::new();
    while let Ok(m) = c_rx.recv_timeout(Duration::from_secs(5)) {
        push_if_receipt(&mut actor, m);
    }
    actor
}

/// Append `m`'s receipt to `actor`, if `m` is one.
fn push_if_receipt(actor: &mut Vec<TxResult>, m: CMessage) {
    if let CMessage::Receipt(r) = m {
        actor.push(TxResult {
            status: r.status,
            gas_used: r.gas_used,
        });
    }
}

#[test]
fn actor_receipts_match_naive_reference() {
    let fixture = build_diff_fixture();
    let reference = naive_reference(fixture.snap_ref, &fixture.pairs);

    let chans = publish_diff_corpus(&fixture.pairs);
    let run = spawn_actor_run(fixture.snap_actor, chans);
    let actor = collect_actor_receipts(&run.c_rx);
    run.join.join().expect("no panic").expect("ok");

    assert_eq!(actor.len(), reference.len());
    for (i, (a, r)) in actor.iter().zip(reference.iter()).enumerate() {
        assert_eq!(a, r, "diff at idx {i}: actor={a:?} reference={r:?}");
    }
}
