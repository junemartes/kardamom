//! An end-to-end anvil test of the closing contract: a real batch
//! (accumulator, records commitment, `postBatch`) posted to the
//! settlement, a batch proof's output files in the zk-host layout, and
//! `submit_next_proof` advancing the `KardamomProofOracle`'s root chain.
//! The batcher, settlement, prover queue, and oracle all align on the
//! L1-as-truth cursor.
//!
//! The verifier here is the accepting mock, deployed from the forge test
//! artifact. The forge suite covers contract-level proof rejection. The
//! zk-host batch round trip covers guest-side public-values authenticity.
//! This test owns the cursor-alignment plumbing between them.

use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

use alloy_primitives::{Address, B256, U256};
use alloy_provider::Provider;
use kardamom_batcher::BatchAccumulator;
use kardamom_batcher::batcher::pack_blocks;
use kardamom_batcher::prover_submit::{IKardamomProofOracle, SubmitOutcome, submit_next_proof};
use kardamom_batcher::settlement::IKardamomL2Settlement;
use kardamom_batcher::testkit::{AcceptingVerifier, BATCHER, DEV_OWNER, L2_CHAIN_ID, env_tx};
use kardamom_deployer::Deployer;
use kardamom_deployer::testkit::{AnvilRig, Funding, OracleInitArgs};
use kardamom_types::{BPosition, BatchPublicOutputs, BlockBoundaryStart, batch_records_commitment};

const VKEY: B256 = B256::repeat_byte(0x5E);
const GENESIS_ROOT: B256 = B256::repeat_byte(0x99);
const POST_ROOT: B256 = B256::repeat_byte(0xAB);

/// A window this test's minimum-legal proof oracle deploys with. The
/// validity path (`submitBatchProof`) never reads `challengeWindow` — see
/// `KardamomProofOracle.sol` — so this test's timing is unaffected by the
/// value. Nonzero at the type level because a zero window would finalize
/// an optimistic claim with no dispute period; this validity-mode test
/// never waits on it.
const MINIMAL_WINDOW: NonZeroU64 = NonZeroU64::new(1).unwrap();

/// Anvil, a funded owner/batcher pair, and a deployed settlement plus a
/// proof oracle (accepting verifier, no bond, minimal window) —
/// validity-mode finalizes purely by proof, so there is no dispute period
/// to wait out.
struct Scenario<P: Provider + Clone> {
    _anvil: alloy_node_bindings::AnvilInstance,
    provider: P,
    oracle: IKardamomProofOracle::IKardamomProofOracleInstance<P>,
    oracle_addr: Address,
    settlement_addr: Address,
    proofs_dir: tempfile::TempDir,
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
        .expect("deploy accepting verifier");
    let deployment = deployer
        .deploy_settlement_and_oracle(
            DEV_OWNER,
            L2_CHAIN_ID,
            BATCHER,
            OracleInitArgs {
                verifier: *verifier.address(),
                batch_vkey: VKEY,
                block_vkey: VKEY, // mock verifier ignores both
                genesis_root: GENESIS_ROOT,
                challenge_window_secs: MINIMAL_WINDOW,
                min_bond_wei: U256::ZERO,
            },
        )
        .await;
    let oracle = IKardamomProofOracle::new(deployment.oracle, rig.provider.clone());
    Some(Scenario {
        _anvil: rig.anvil,
        provider: rig.provider,
        oracle,
        oracle_addr: deployment.oracle,
        settlement_addr: deployment.settlement,
        proofs_dir: tempfile::tempdir().unwrap(),
    })
}

/// A real batch (blocks 7..8) plus the commitment the guest side would
/// independently compute for it, cross-checked against the batcher's.
struct RealBatch {
    l2_block_start: u64,
    l2_block_end: u64,
    records_commitment: B256,
}

/// Pack blocks 7..8 through the accumulator, the same shape
/// `optimistic_e2e.rs` uses, and check the batcher's records commitment
/// agrees with an independent guest-side computation.
fn build_real_batch() -> RealBatch {
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
    .expect("pack batch");
    assert_eq!((batch.l2_block_start, batch.l2_block_end), (7, 8));

    let expected_commitment = batch_records_commitment([7u64, 8].map(|n| {
        let mut d = kardamom_types::BlockRecordsDigest::new(n);
        match n {
            7 => {
                d.add_tx(&env_tx(0).raw_tx);
                d.add_tx(&env_tx(1).raw_tx);
            }
            _ => d.add_tx(&env_tx(2).raw_tx),
        }
        d.finish()
    }));
    assert_eq!(
        batch.records_commitment, expected_commitment,
        "batcher and guest-side commitment must agree"
    );
    RealBatch {
        l2_block_start: batch.l2_block_start,
        l2_block_end: batch.l2_block_end,
        records_commitment: expected_commitment,
    }
}

/// Write the prover's batch output files (zk-host batch layout) claiming
/// `POST_ROOT` for `batch`.
fn write_batch_proof_files(proofs_dir: &Path, batch: &RealBatch) {
    let pv = BatchPublicOutputs {
        pre_state_root: GENESIS_ROOT,
        post_state_root: POST_ROOT,
        first_block: batch.l2_block_start,
        last_block: batch.l2_block_end,
        records_commitment: batch.records_commitment,
    };
    let dir: PathBuf = proofs_dir.join("batch-7-8");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("public-values.bin"), pv.encode()).unwrap();
    std::fs::write(dir.join("proof.bin"), b"mock-proof").unwrap();
}

#[tokio::test]
async fn posted_batch_proof_advances_the_oracle_root_chain() {
    // ----- setup -----
    let Some(s) = setup().await else {
        eprintln!("SKIP: anvil unavailable");
        return;
    };
    let settlement = IKardamomL2Settlement::new(s.settlement_addr, s.provider.clone());
    let batch = build_real_batch();

    // ----- act: nothing posted yet -----
    let out = submit_next_proof(s.provider.clone(), s.oracle_addr, s.proofs_dir.path())
        .await
        .unwrap();
    assert_eq!(out, SubmitOutcome::NoBatchPosted { batch_index: 1 });

    // ----- act: the batcher posts the batch -----
    let receipt = settlement
        .postBatch(
            0,
            vec![B256::repeat_byte(0xA1)],
            batch.l2_block_start,
            batch.l2_block_end,
            batch.records_commitment,
        )
        .from(BATCHER)
        .send()
        .await
        .expect("postBatch")
        .get_receipt()
        .await
        .expect("postBatch receipt");
    assert!(receipt.status());

    // ----- act + assert: batch posted, but the prover has not produced
    // files yet -----
    let out = submit_next_proof(s.provider.clone(), s.oracle_addr, s.proofs_dir.path())
        .await
        .unwrap();
    assert_eq!(out, SubmitOutcome::ProofNotReady { batch_index: 1 });

    // ----- act: the prover's output files land -----
    write_batch_proof_files(s.proofs_dir.path(), &batch);

    // ----- act + assert: submit advances the oracle's root chain -----
    let out = submit_next_proof(s.provider.clone(), s.oracle_addr, s.proofs_dir.path())
        .await
        .unwrap();
    assert_eq!(out, SubmitOutcome::Submitted { batch_index: 1 });
    assert_eq!(s.oracle.stateRoot().call().await.unwrap(), POST_ROOT);
    assert_eq!(s.oracle.lastFinalizedBatch().call().await.unwrap(), 1);

    // ----- act + assert: idempotence at the cursor — batch 2 is not
    // posted, so this returns NoBatchPosted -----
    let out = submit_next_proof(s.provider.clone(), s.oracle_addr, s.proofs_dir.path())
        .await
        .unwrap();
    assert_eq!(out, SubmitOutcome::NoBatchPosted { batch_index: 2 });
}
