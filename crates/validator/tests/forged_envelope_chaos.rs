//! Forged-envelope chaos test (spec: no-std-exec-core, phase 3a.1).
//!
//! The S0 pipeline trusts `TxEnvelope.sender` / `tx_hash` from the proxy. A
//! compromised proxy or sequencer can therefore attribute an attacker-signed
//! tx to a victim — the theft shape: `envelope.sender = victim`, signature by
//! the attacker, value flowing to the attacker's sink. 3a.1 closes this in
//! the LIVE validator: `ExecutorConfig::verify_record_identity` re-derives
//! every tx record's identity at arrival (the same
//! `exec_core::stateless::verify_record_identity` the zk guest runs) and
//! halts the pipeline with `ExecutorError::RecordIdentity`, which the
//! validator binary classifies as an INTEGRITY failure (divergence latch →
//! exit 2), not an availability restart.
//!
//! All three cases drive the REAL `Executor::run` pipeline — the same
//! reader-join → exec → commit threads production runs, over channel-backed
//! subscriptions (the determinism suite's harness shape):
//!
//! - flag ON + honest traffic → executes and commits normally (the check
//!   must not false-positive on well-formed envelopes);
//! - flag ON + forged sender → `RecordIdentity` halt before the first EVM
//!   step, integrity latch set, victim untouched;
//! - flag OFF + the same forgery → the theft COMMITS. This is the
//!   documented pre-3a.1 blind spot, pinned as a test so the executor-side
//!   decision (defense-in-depth vs latency) is made against a red/green
//!   fact, not a claim.

use std::thread;
use std::time::Duration;

use alloy_consensus::{SignableTransaction, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{
    Address, Bytes as AlloyBytes, TxKind as APTxKind, U256, address, keccak256,
};
use alloy_signer_local::PrivateKeySigner;
use bytes::Bytes;
use crossbeam_channel::{Receiver, Sender, bounded};
use kardamom_engine::{
    BPosition, BlockBoundaryStart, CMessage, Executor, ExecutorConfig, ExecutorError,
    MockStateDatabase, MutatingSnapshotSource, StateDatabase, StateWriterSignal,
    TxDataSubscription, TxEnvelope as KtTxEnvelope, TxOrderingMessage, TxOrderingSubscription,
    TxReceiptsPublication, TxRef, WriterApplyingQueue,
};
use kardamom_validator::{Divergence, latch_integrity_failure};
use revm::primitives::KECCAK_EMPTY;

const CHAIN_ID: u64 = 1;
const SINK: Address = address!("00000000000000000000000000000000DEAD0666");
const LOOT: u64 = 250_000;

struct ChanASub {
    rx: Receiver<(BPosition, KtTxEnvelope)>,
}
impl TxDataSubscription for ChanASub {
    fn sequencer_id(&self) -> u8 {
        0
    }
    fn next(&mut self) -> Result<(kardamom_types::TxDataLoc, KtTxEnvelope), ExecutorError> {
        self.rx
            .recv()
            .map(|(pos, env)| (kardamom_types::TxDataLoc::new(0, pos), env))
            .map_err(|_| ExecutorError::TxDataClosed { sequencer_id: 0 })
    }
}
struct ChanBSub(Receiver<(BPosition, TxOrderingMessage)>);
impl TxOrderingSubscription for ChanBSub {
    fn next(&mut self) -> Result<(BPosition, TxOrderingMessage), ExecutorError> {
        self.0.recv().map_err(|_| ExecutorError::TxOrderingClosed)
    }
}
struct ChanCPub(Sender<CMessage>);
impl TxReceiptsPublication for ChanCPub {
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

fn bpos(off: i32) -> BPosition {
    BPosition {
        term_id: 0,
        term_offset: off,
    }
}

/// A value transfer to [`SINK`], signed by `signer` but CLAIMING `sender` as
/// its origin. With `sender == signer.address()` this is an honest envelope;
/// with a different `sender` it is the theft shape (the `tx_hash` stays
/// honest — a forged hash would already fail the reader's reference join,
/// and hash forgery is covered by the exec-core unit tests).
fn envelope_claiming(signer: &PrivateKeySigner, sender: Address) -> KtTxEnvelope {
    let mut tx = TxLegacy {
        chain_id: Some(CHAIN_ID),
        nonce: 0,
        gas_price: 0,
        gas_limit: 21_000,
        to: APTxKind::Call(SINK),
        value: U256::from(LOOT),
        input: AlloyBytes::new(),
    };
    let sig = signer.sign_transaction_sync(&mut tx).unwrap();
    let alloy_env: alloy_consensus::TxEnvelope = tx.into_signed(sig).into();
    let raw_tx = Bytes::from(alloy_env.encoded_2718());
    let tx_hash = keccak256(&raw_tx);
    KtTxEnvelope {
        correlation_id: 0,
        raw_tx,
        sender,
        tx_hash,
    }
}

/// Drive one single-tx block through the full pipeline. Returns the engine
/// result, the C-stream output, and the (shared) post-run state DB.
fn run_pipeline(
    envelope: KtTxEnvelope,
    victim: Address,
    verify_record_identity: bool,
) -> (Result<(), ExecutorError>, Vec<CMessage>, MockStateDatabase) {
    let snap = MockStateDatabase::builder()
        .account(victim, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
        .build();
    let writer_q = WriterApplyingQueue::new(snap.clone());
    let snapshots = MutatingSnapshotSource(snap.clone());

    let (a_tx, a_rx) = bounded::<(BPosition, KtTxEnvelope)>(8);
    let (b_tx, b_rx) = bounded::<(BPosition, TxOrderingMessage)>(8);
    let (c_tx, c_rx) = bounded::<CMessage>(8);

    let tx_hash = envelope.tx_hash;
    a_tx.send((bpos(0), envelope)).unwrap();
    b_tx.send((
        bpos(0),
        TxOrderingMessage::TxRef(TxRef::new(tx_hash, 0, bpos(0), 0)),
    ))
    .unwrap();
    b_tx.send((
        bpos(1),
        TxOrderingMessage::BoundaryStart(BlockBoundaryStart {
            block_number: 1,
            end_tx_idx: bpos(1),
            l2_timestamp: 1_700_000_000,
            l1_origin: 0,
        }),
    ))
    .unwrap();
    drop(a_tx);
    drop(b_tx);

    let cfg = ExecutorConfig {
        chain_id: CHAIN_ID,
        verify_record_identity,
        ..Default::default()
    };
    let a_subs: Vec<Box<dyn TxDataSubscription>> = vec![Box::new(ChanASub { rx: a_rx })];
    let b_sub: Box<dyn TxOrderingSubscription> = Box::new(ChanBSub(b_rx));
    let h = thread::spawn(move || {
        Executor::run(
            cfg,
            a_subs,
            b_sub,
            ChanCPub(c_tx),
            snapshots,
            Imm,
            writer_q,
            0,
            None,
            None,
            None,
            None,
            None,
        )
    });

    let mut out = Vec::new();
    while let Ok(m) = c_rx.recv_timeout(Duration::from_secs(5)) {
        out.push(m);
    }
    let res = h.join().expect("no panic");
    (res, out, snap)
}

#[test]
fn honest_traffic_executes_with_identity_verification_on() {
    let signer = PrivateKeySigner::from_bytes(&alloy_primitives::B256::repeat_byte(0x11)).unwrap();
    let sender = signer.address();

    let (res, out, snap) = run_pipeline(envelope_claiming(&signer, sender), sender, true);

    res.expect("honest envelope must pass the identity check");
    assert!(
        out.iter()
            .any(|m| matches!(m, CMessage::BlockBoundary(b) if b.block_number == 1)),
        "block 1 must close normally"
    );
    let (_, sink_balance, _) = snap.basic(SINK).unwrap().expect("sink credited");
    assert_eq!(sink_balance, U256::from(LOOT));
}

#[test]
fn forged_sender_halts_and_latches_with_verification_on() {
    let attacker =
        PrivateKeySigner::from_bytes(&alloy_primitives::B256::repeat_byte(0x22)).unwrap();
    let victim = address!("00000000000000000000000000000000000F1C71");
    assert_ne!(attacker.address(), victim);

    let (res, out, snap) = run_pipeline(envelope_claiming(&attacker, victim), victim, true);

    // The pipeline halts with the identity error before the first EVM step:
    // nothing reaches the C stream, nothing reaches the writer.
    let err = res.expect_err("forged sender must halt the pipeline");
    assert!(
        matches!(err, ExecutorError::RecordIdentity(_)),
        "expected RecordIdentity, got: {err:?}"
    );
    assert!(out.is_empty(), "no receipt/boundary may be published");
    let (_, victim_balance, _) = snap.basic(victim).unwrap().expect("victim account");
    assert_eq!(
        victim_balance,
        U256::from(10u128.pow(18)),
        "victim must be untouched"
    );
    assert!(snap.basic(SINK).unwrap().is_none(), "no loot may land");

    // The validator binary's exit classification: RecordIdentity is an
    // INTEGRITY failure — it must latch (exit 2, page the humans), not
    // restart as an availability blip.
    let divergence = Divergence::new();
    assert!(latch_integrity_failure(&divergence, &err));
    assert!(divergence.is_halted());
    let reason = divergence.reason().expect("latched reason");
    assert!(
        reason.contains("sender mismatch"),
        "reason must carry the proof: {reason}"
    );

    // Availability errors must NOT impersonate integrity.
    let availability = Divergence::new();
    assert!(!latch_integrity_failure(
        &availability,
        &ExecutorError::TxOrderingClosed
    ));
    assert!(!availability.is_halted());
}

#[test]
fn forged_sender_commits_theft_with_verification_off() {
    let attacker =
        PrivateKeySigner::from_bytes(&alloy_primitives::B256::repeat_byte(0x33)).unwrap();
    let victim = address!("00000000000000000000000000000000000F1C72");

    let (res, out, snap) = run_pipeline(envelope_claiming(&attacker, victim), victim, false);

    // The documented pre-3a.1 blind spot: with the check off, the proxy's
    // claimed sender is trusted and the attacker-signed tx spends the
    // victim's funds. If closing the executor-side gap ever flips this test,
    // that is the intended signal — delete it alongside the flag decision.
    res.expect("with verification off the forgery executes");
    assert!(
        out.iter()
            .any(|m| matches!(m, CMessage::BlockBoundary(b) if b.block_number == 1)),
        "the forged block commits"
    );
    let (_, victim_balance, _) = snap.basic(victim).unwrap().expect("victim account");
    assert_eq!(
        victim_balance,
        U256::from(10u128.pow(18)) - U256::from(LOOT),
        "the theft debits the victim"
    );
    let (_, sink_balance, _) = snap.basic(SINK).unwrap().expect("sink credited");
    assert_eq!(sink_balance, U256::from(LOOT));
}
