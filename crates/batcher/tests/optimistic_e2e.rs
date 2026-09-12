//! The optimistic path end to end, on anvil.
//!
//! Scenario A (the equilibrium): a real batch is posted, claimed from the
//! spool's attestations, watched (honest), and finalized once the window
//! elapses. The root advances with zero proofs generated.
//!
//! Scenario B (the defense): a lying claim (correct digests, so the fold
//! check passes, but a wrong root at offset 1) is detected by the watcher
//! against the spool. It is challenged with the single-block proof files
//! at the first divergent offset, slashed, and rewound. Then it is
//! honestly re-claimed and finalized.

use std::num::NonZeroU64;
use std::path::Path;

use alloy_primitives::{Address, B256, U256};
use alloy_provider::Provider;
use kardamom_batcher::BatchAccumulator;
use kardamom_batcher::batcher::pack_blocks;
use kardamom_batcher::optimistic::{BatchClaimer, BatchWatcher, ClaimOutcome, WatchOutcome};
use kardamom_batcher::prover_submit::IKardamomProofOracle;
use kardamom_batcher::settlement::IKardamomL2Settlement;
use kardamom_batcher::testkit::{
    AcceptingVerifier, BATCHER, DEV_OWNER, L2_CHAIN_ID, advance_past_window, env_tx,
};
use kardamom_deployer::Deployer;
use kardamom_deployer::testkit::{AnvilRig, Funding, OracleInitArgs};
use kardamom_types::{BPosition, BlockBoundaryStart, BlockRecordsDigest, PublicOutputs};

const VKEY: B256 = B256::repeat_byte(0x5E);
const GENESIS_ROOT: B256 = B256::repeat_byte(0x99);
const WINDOW: NonZeroU64 = NonZeroU64::new(3600).unwrap();

/// The honest per-block roots the "validator" computed.
fn honest_root(block: u64) -> B256 {
    B256::repeat_byte(0xA0u8.wrapping_add(u8::try_from(block).unwrap()))
}

/// Write a spool entry the way the validator's spool would. The 160-byte
/// expected-outputs layout feeds both the claim poster and the watcher.
fn write_spool_block(spool: &Path, block: u64, pre: B256, digest: B256) {
    let out = PublicOutputs {
        pre_state_root: pre,
        post_state_root: honest_root(block),
        block_number: block,
        records_digest: digest,
        bal_commitment: B256::repeat_byte(0xBA),
    };
    let dir = spool.join(format!("block-{block}"));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("expected-outputs.bin"), out.encode()).unwrap();
}

/// Anvil, a funded owner/batcher pair, and a deployed settlement plus a
/// proof oracle (accepting verifier, real challenge window, real bond).
/// Everything the two scenarios below need.
struct Scenario<P: Provider + Clone> {
    _anvil: alloy_node_bindings::AnvilInstance,
    provider: P,
    oracle: IKardamomProofOracle::IKardamomProofOracleInstance<P>,
    oracle_addr: Address,
    settlement: IKardamomL2Settlement::IKardamomL2SettlementInstance<P>,
    spool_dir: tempfile::TempDir,
}

impl<P: Provider + Clone> Scenario<P> {
    fn spool(&self) -> &Path {
        self.spool_dir.path()
    }
}

async fn setup() -> Option<Scenario<impl Provider + Clone>> {
    let rig = AnvilRig::spawn(
        alloy_node_bindings::Anvil::new(),
        &[
            (DEV_OWNER, Funding::FundAndImpersonate),
            (BATCHER, Funding::FundAndImpersonate),
        ],
    )
    .await?;
    let deployer = Deployer::new(rig.provider.clone(), DEV_OWNER);
    let verifier = AcceptingVerifier::deploy(rig.provider.clone())
        .await
        .unwrap();
    let deployment = deployer
        .deploy_settlement_and_oracle(
            DEV_OWNER,
            L2_CHAIN_ID,
            BATCHER,
            OracleInitArgs {
                verifier: *verifier.address(),
                batch_vkey: VKEY,
                block_vkey: VKEY,
                genesis_root: GENESIS_ROOT,
                challenge_window_secs: WINDOW,
                min_bond_wei: U256::from(10u128.pow(18)), // 1 ETH bond
            },
        )
        .await;
    let oracle = IKardamomProofOracle::new(deployment.oracle, rig.provider.clone());
    let settlement = IKardamomL2Settlement::new(deployment.settlement, rig.provider.clone());
    let spool_dir = tempfile::tempdir().unwrap();
    Some(Scenario {
        _anvil: rig.anvil,
        provider: rig.provider,
        oracle,
        oracle_addr: deployment.oracle,
        settlement,
        spool_dir,
    })
}

/// A real batch (blocks 7..8) plus the digests the honest spool needs.
struct RealBatch {
    records_commitment: B256,
    d7: B256,
    d8: B256,
}

/// Pack blocks 7..8 through the accumulator and write the honest spool
/// entries the validator would have produced for them, chained from
/// `block7_pre` (the root the honest chain currently starts from).
fn build_real_batch_and_spool(spool: &Path, block7_pre: B256) -> RealBatch {
    let mut acc = BatchAccumulator::new();
    acc.observe_tx(env_tx(0), BPosition::from_index(0));
    acc.observe_tx(env_tx(1), BPosition::from_index(1));
    let b1 = acc.observe_boundary(&BlockBoundaryStart {
        block_number: 7,
        end_tx_idx: BPosition::from_index(2),
        l2_timestamp: 1_700_000_007,
        l1_origin: 0,
    });
    acc.observe_tx(env_tx(2), BPosition::from_index(2));
    let b2 = acc.observe_boundary(&BlockBoundaryStart {
        block_number: 8,
        end_tx_idx: BPosition::from_index(3),
        l2_timestamp: 1_700_000_008,
        l1_origin: 0,
    });
    let batch = pack_blocks(
        &kardamom_batcher::batcher::BatcherConfig::default(),
        &[b1, b2],
    )
    .unwrap();

    let d7 = {
        let mut d = BlockRecordsDigest::new(7);
        d.add_tx(&env_tx(0).raw_tx);
        d.add_tx(&env_tx(1).raw_tx);
        d.finish()
    };
    let d8 = {
        let mut d = BlockRecordsDigest::new(8);
        d.add_tx(&env_tx(2).raw_tx);
        d.finish()
    };
    write_spool_block(spool, 7, block7_pre, d7);
    write_spool_block(spool, 8, honest_root(7), d8);
    RealBatch {
        records_commitment: batch.records_commitment,
        d7,
        d8,
    }
}

/// Write the single-block proof-and-public-values files a prover would
/// produce for block 8, honest throughout.
fn write_single_block_proof_files(spool: &Path, d8: B256) {
    let dir = spool.join("block-8");
    let honest_pv = PublicOutputs {
        pre_state_root: honest_root(7),
        post_state_root: honest_root(8),
        block_number: 8,
        records_digest: d8,
        bal_commitment: B256::repeat_byte(0xBA),
    };
    std::fs::write(dir.join("public-values.bin"), honest_pv.encode()).unwrap();
    std::fs::write(dir.join("proof.bin"), b"mock-proof").unwrap();
}

/// The equilibrium: no batch, then a real batch posted, honestly claimed
/// from the spool, watched clean, and finalized once the window elapses.
/// Zero proofs generated.
async fn scenario_a_honest_claim_and_finalize<P: Provider + Clone>(
    s: &Scenario<P>,
    batch: &RealBatch,
) {
    let out = BatchClaimer::new(s.oracle_addr, s.spool())
        .claim_next(s.provider.clone())
        .await
        .unwrap();
    assert_eq!(out, ClaimOutcome::NoBatchPosted { batch_index: 1 });

    s.settlement
        .postBatch(
            0,
            vec![B256::repeat_byte(0xA1)],
            7,
            8,
            batch.records_commitment,
        )
        .from(BATCHER)
        .send()
        .await
        .unwrap()
        .get_receipt()
        .await
        .unwrap();

    let out = BatchClaimer::new(s.oracle_addr, s.spool())
        .claim_next(s.provider.clone())
        .await
        .unwrap();
    assert_eq!(out, ClaimOutcome::Claimed { batch_index: 1 });

    let out = BatchWatcher::new(s.oracle_addr, s.spool())
        .watch_and_challenge(s.provider.clone())
        .await
        .unwrap();
    assert_eq!(out, WatchOutcome::ClaimHonest { batch_index: 1 });

    advance_past_window(&s.provider, WINDOW).await;
    s.oracle
        .finalizeBatch(1)
        .send()
        .await
        .unwrap()
        .get_receipt()
        .await
        .unwrap();

    assert_eq!(s.oracle.stateRoot().call().await.unwrap(), honest_root(8));
    assert_eq!(s.oracle.lastFinalizedBatch().call().await.unwrap(), 1);
}

/// The lie: a claim with correct digests (the fold check passes) but a
/// wrong root at offset 1. The watcher detects the divergence once the
/// single-block proof lands, and challenges it. The batch rewinds without
/// moving the root.
async fn lying_claim_is_challenged_and_rewound<P: Provider + Clone>(
    s: &Scenario<P>,
    batch: &RealBatch,
) {
    // ----- act: post the batch, then the liar claims it -----
    s.settlement
        .postBatch(
            1,
            vec![B256::repeat_byte(0xA2)],
            7,
            8,
            batch.records_commitment,
        )
        .from(BATCHER)
        .send()
        .await
        .unwrap()
        .get_receipt()
        .await
        .unwrap();

    // The liar claims directly. The digests are correct (the fold passes),
    // but the root is wrong at offset 1.
    let lie = B256::repeat_byte(0x66);
    s.oracle
        .claimBatch(2, vec![honest_root(7), lie], vec![batch.d7, batch.d8])
        .value(U256::from(10u128.pow(18)))
        .from(DEV_OWNER)
        .send()
        .await
        .unwrap()
        .get_receipt()
        .await
        .unwrap();

    // ----- act + assert: the watcher sees the divergence, but the proof
    // is not ready yet -----
    let out = BatchWatcher::new(s.oracle_addr, s.spool())
        .watch_and_challenge(s.provider.clone())
        .await
        .unwrap();
    assert_eq!(
        out,
        WatchOutcome::ProofNotReady {
            batch_index: 2,
            divergent_block: 8
        }
    );

    // ----- act: the prover produces the single-block files (the
    // zk-host --prove shape); the watcher challenges -----
    write_single_block_proof_files(s.spool(), batch.d8);

    let out = BatchWatcher::new(s.oracle_addr, s.spool())
        .watch_and_challenge(s.provider.clone())
        .await
        .unwrap();

    // ----- assert: challenged, and the rewind leaves the root unchanged
    // (the slash is credited to the challenger, the provider's default
    // account) -----
    assert_eq!(
        out,
        WatchOutcome::Challenged {
            batch_index: 2,
            block_offset: 1
        }
    );
    assert_eq!(s.oracle.stateRoot().call().await.unwrap(), honest_root(8));
    assert_eq!(s.oracle.highestClaimedBatch().call().await.unwrap(), 1);
}

/// The re-claim: an honest claim on the reopened batch, then finalize once
/// the window elapses.
async fn honest_reclaim_finalizes<P: Provider + Clone>(s: &Scenario<P>) {
    // ----- act -----
    let out = BatchClaimer::new(s.oracle_addr, s.spool())
        .claim_next(s.provider.clone())
        .await
        .unwrap();
    assert_eq!(out, ClaimOutcome::Claimed { batch_index: 2 });
    advance_past_window(&s.provider, WINDOW).await;
    s.oracle
        .finalizeBatch(2)
        .send()
        .await
        .unwrap()
        .get_receipt()
        .await
        .unwrap();

    // ----- assert -----
    assert_eq!(s.oracle.lastFinalizedBatch().call().await.unwrap(), 2);
    assert_eq!(s.oracle.stateRoot().call().await.unwrap(), honest_root(8));
}

#[tokio::test]
async fn optimistic_claim_finalize_and_challenge_paths() {
    // ----- setup -----
    let Some(s) = setup().await else {
        eprintln!("SKIP: anvil unavailable");
        return;
    };

    // ----- act + assert: scenario A, then scenario B on the same chain -----
    let batch_a = build_real_batch_and_spool(s.spool(), GENESIS_ROOT);
    scenario_a_honest_claim_and_finalize(&s, &batch_a).await;

    // The honest chain now starts at honest_root(8), block 1's finalized
    // root.
    let batch_b = build_real_batch_and_spool(s.spool(), honest_root(8));
    lying_claim_is_challenged_and_rewound(&s, &batch_b).await;
    honest_reclaim_finalizes(&s).await;
}
