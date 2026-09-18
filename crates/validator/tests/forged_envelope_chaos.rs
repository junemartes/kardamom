//! Forged-envelope chaos test.
//!
//! The pipeline trusts `TxEnvelope.sender` and `tx_hash` from the
//! proxy. A compromised proxy or sequencer can therefore attribute an
//! attacker-signed tx to a victim: the theft shape is
//! `envelope.sender = victim`, signed by the attacker, with value
//! flowing to the attacker's sink. The live validator closes this:
//! `ExecutorConfig::verify_record_identity` re-derives every
//! tx record's identity at arrival, the same check
//! `exec_core::stateless::verify_record_identity` runs for the zk guest,
//! and stops the pipeline with `ExecutorError::RecordIdentity`. The
//! validator binary classifies this as an integrity failure (divergence
//! latch, exit 2), not an availability restart.
//!
//! All three cases drive the real `Executor::run` pipeline, the same
//! reader-join, exec, and commit threads production runs, over
//! channel-backed subscriptions:
//!
//! - flag on, honest traffic: executes and commits normally (the check
//!   must not false-positive on well-formed envelopes).
//! - flag on, forged sender: `RecordIdentity` stops the pipeline before
//!   the first EVM step, the integrity latch is set, and the victim is
//!   untouched.
//! - flag off, the same forgery: the theft commits. This is a documented
//!   blind spot, pinned as a test so the executor-side decision
//!   (defense-in-depth against latency) rests on a red/green fact, not a
//!   claim.

use alloy_primitives::{Address, U256, address};
use alloy_signer_local::PrivateKeySigner;
use kardamom_engine::actor::fixtures::{ChannelHarness, HarnessInput, HarnessOutcome, LegacyTx};
use kardamom_engine::{
    BPosition, BlockBoundaryStart, CMessage, ExecutorConfig, ExecutorError, MockStateDatabase,
    StateDatabase, TxEnvelope as KtTxEnvelope, TxOrderingMessage, TxRef,
};
use kardamom_validator::{Divergence, latch_integrity_failure};
use revm::primitives::KECCAK_EMPTY;

const CHAIN_ID: u64 = 1;
/// [`CHAIN_ID`], as `ExecutorConfig::chain_id` now requires.
const CHAIN_ID_NONZERO: std::num::NonZeroU64 = std::num::NonZeroU64::new(CHAIN_ID).unwrap();
const SINK: Address = address!("00000000000000000000000000000000DEAD0666");
const LOOT: u64 = 250_000;

fn bpos(off: i32) -> BPosition {
    BPosition {
        term_id: 0,
        term_offset: off,
    }
}

/// A value transfer to [`SINK`], signed by `signer` but claiming `sender`
/// as its origin. With `sender == signer.address()`, this is an honest
/// envelope. With a different `sender`, it is the theft shape. The
/// `tx_hash` stays honest: a forged hash would already fail the reader's
/// reference join, and hash forgery is covered by the exec-core unit tests.
fn envelope_claiming(signer: &PrivateKeySigner, sender: Address) -> KtTxEnvelope {
    let envelope = LegacyTx {
        chain_id: CHAIN_ID,
        to: SINK,
        nonce: 0,
        value: LOOT,
        gas_limit: 21_000,
        gas_price: 0,
        ..Default::default()
    }
    .sign(signer);
    // The theft shape: keep the honestly-signed bytes and hash, but
    // claim a different `sender` than the one that actually signed.
    KtTxEnvelope { sender, ..envelope }
}

/// Drive one single-tx block through the full pipeline. Returns the
/// engine result, the C-stream output, and the shared post-run state DB.
fn run_pipeline(
    envelope: KtTxEnvelope,
    victim: Address,
    verify_record_identity: bool,
) -> HarnessOutcome {
    let snap = MockStateDatabase::builder()
        .account(victim, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
        .build();

    let tx_hash = envelope.tx_hash;
    let tx_data = vec![(bpos(0), envelope)];
    let tx_ordering = vec![
        (
            bpos(0),
            TxOrderingMessage::TxRef(TxRef::new(tx_hash, 0, bpos(0), 0)),
        ),
        (
            bpos(1),
            TxOrderingMessage::BoundaryStart(BlockBoundaryStart {
                block_number: 1,
                end_tx_idx: bpos(1),
                l2_timestamp: 1_700_000_000,
                l1_origin: 0,
            }),
        ),
    ];

    let cfg = ExecutorConfig {
        chain_id: CHAIN_ID_NONZERO,
        verify_record_identity,
        ..Default::default()
    };
    ChannelHarness::run(HarnessInput {
        cfg,
        tx_data,
        tx_ordering,
        snap,
    })
}

#[test]
fn honest_traffic_executes_with_identity_verification_on() {
    let signer = PrivateKeySigner::from_bytes(&alloy_primitives::B256::repeat_byte(0x11)).unwrap();
    let sender = signer.address();

    let HarnessOutcome {
        result: res,
        receipts: out,
        state: snap,
    } = run_pipeline(envelope_claiming(&signer, sender), sender, true);

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

    let HarnessOutcome {
        result: res,
        receipts: out,
        state: snap,
    } = run_pipeline(envelope_claiming(&attacker, victim), victim, true);

    // The pipeline halts with the identity error before the first EVM
    // step: nothing reaches the C stream, and nothing reaches the writer.
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
    // integrity failure. It must latch (exit 2, page the humans), not
    // restart as an availability blip.
    let divergence = Divergence::new();
    assert!(latch_integrity_failure(&divergence, &err));
    assert!(divergence.is_halted());
    let reason = divergence.reason().expect("latched reason");
    assert!(
        reason.contains("sender mismatch"),
        "reason must carry the proof: {reason}"
    );

    // Availability errors must not look like integrity failures.
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

    let HarnessOutcome {
        result: res,
        receipts: out,
        state: snap,
    } = run_pipeline(envelope_claiming(&attacker, victim), victim, false);

    // This is the documented blind spot: with the check off, the proxy's
    // claimed sender is trusted, and the attacker-signed tx spends the
    // victim's funds. If closing the executor-side gap ever flips this
    // test, that is the intended signal: delete it alongside the flag
    // decision.
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
