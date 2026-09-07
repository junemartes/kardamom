//! The full optimistic path with a real proof and the real SP1 verifier.
//! See the no-std-exec-core spec. Unlike `optimistic_e2e` (mock verifier,
//! flow plumbing), this verifies a genuine SP1 Groth16 proof of a real
//! kardamom block against the vendored SP1 verifier (circuit v6.1.0) on
//! anvil. This is the core case: generate a proof, submit it on chain, and
//! check the contract accepts it.
//!
//! Two cases share one real proof, because proving is expensive; the
//! fixture is committed (see fixtures/README.md):
//!
//!   A. A false block claim (wrong root) is challenged with the real proof.
//!      The real verifier accepts it, the bond is slashed, and the chain
//!      rewinds.
//!   B. The opposite: an honest claim (the true root) cannot be griefed.
//!      The same real proof reproduces the claimed root, so
//!      `challengeBlock` reverts with `ProofAgreesWithClaim` before the
//!      verifier is even reached.
//!
//! Skips cleanly if the fixtures are absent (not yet generated) or anvil is
//! unavailable.

use std::num::NonZeroU64;
use std::path::PathBuf;

use alloy_primitives::{B256, Bytes, Keccak256, U256};
use alloy_provider::Provider;
use alloy_sol_types::sol;
use kardamom_batcher::prover_submit::IKardamomProofOracle;
use kardamom_batcher::settlement::IKardamomL2Settlement;
use kardamom_batcher::testkit::{BATCHER, DEV_OWNER, L2_CHAIN_ID};
use kardamom_deployer::Deployer;
use kardamom_deployer::testkit::{AnvilRig, OracleInitArgs};

sol!(
    #[sol(rpc)]
    SP1Verifier,
    concat!(
        env!("CARGO_WORKSPACE_DIR"),
        "/contracts/out/SP1VerifierGroth16.sol/SP1Verifier.json"
    )
);

const WINDOW: NonZeroU64 = NonZeroU64::new(3600).unwrap();
const BOND: u128 = 1_000_000_000_000_000_000;

/// The single-block public outputs the guest committed. This is a 160-byte
/// layout, sharing fields with `BatchPublicOutputs`: pre, post,
/// blockNumber, the `records_digest` slot as `records_commitment`, and the
/// bal slot dropped here.
struct BlockPv {
    pre: B256,
    post: B256,
    block_number: u64,
    records_digest: B256,
    raw: Vec<u8>,
}

fn load_block_pv(bytes: Vec<u8>) -> BlockPv {
    // The single-block PublicOutputs layout is pre, post,
    // block_number (u256), records_digest, bal_commitment, in that order.
    // Decode it with BatchPublicOutputs' matching prefix. The first and
    // last u256 fields differ in meaning, but the byte slots this needs
    // (pre, post, word at 64, word at 96) line up.
    let pre = B256::from_slice(&bytes[0..32]);
    let post = B256::from_slice(&bytes[32..64]);
    let block_number = U256::from_be_slice(&bytes[64..96]).to::<u64>();
    let records_digest = B256::from_slice(&bytes[96..128]);
    BlockPv {
        pre,
        post,
        block_number,
        records_digest,
        raw: bytes,
    }
}

fn fold_one(digest: B256) -> B256 {
    let mut h = Keccak256::new();
    h.update(b"KBAT");
    h.update(digest.as_slice());
    h.finalize()
}

/// The committed real-proof fixture: the guest's public values, the
/// Groth16 proof bytes, and the circuit vkey.
struct Fixtures {
    pv: BlockPv,
    proof: Bytes,
    vkey: B256,
}

/// Load the committed fixture, `None` if it has not been generated yet
/// (the convention this test uses to skip cleanly, rather than fail).
fn load_fixtures() -> Option<Fixtures> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let pv_bytes = std::fs::read(dir.join("public-values.bin")).ok()?;
    let proof_bytes = std::fs::read(dir.join("proof.bin")).ok()?;
    let vkey_hex = std::fs::read_to_string(dir.join("vkey.hex")).ok()?;
    let pv = load_block_pv(pv_bytes);
    assert_eq!(
        pv.raw.len(),
        160,
        "single-block public values are 160 bytes"
    );
    Some(Fixtures {
        pv,
        proof: Bytes::from(proof_bytes),
        vkey: vkey_hex.trim().parse().expect("vkey.hex is a bytes32"),
    })
}

/// Anvil, the real vendored SP1 verifier, and a settlement plus proof
/// oracle deployed for `fx`'s proven block, with its proven pre-root as
/// genesis. The batch covering exactly that block is already posted.
struct Scenario<P: Provider + Clone> {
    _anvil: alloy_node_bindings::AnvilInstance,
    oracle: IKardamomProofOracle::IKardamomProofOracleInstance<P>,
}

async fn setup(fx: &Fixtures) -> Option<Scenario<impl Provider + Clone>> {
    let rig = AnvilRig::spawn(&[DEV_OWNER, BATCHER]).await?;

    // The real SP1 verifier, settlement, and oracle v2, wired to both. The
    // batch and block vkey are the fixture's guest vkey. Genesis is the
    // block's proven pre-root, so the offset-0 pre-root check lines up.
    let verifier = SP1Verifier::deploy(rig.provider.clone()).await.unwrap();
    assert_eq!(
        verifier.VERSION().call().await.unwrap(),
        "v6.1.0",
        "vendored verifier is the circuit version the SDK proves against"
    );
    let deployer = Deployer::new(rig.provider.clone(), DEV_OWNER);
    let deployment = deployer
        .deploy_settlement_and_oracle(
            DEV_OWNER,
            L2_CHAIN_ID,
            BATCHER,
            OracleInitArgs {
                verifier: *verifier.address(),
                batch_vkey: fx.vkey,
                block_vkey: fx.vkey,
                genesis_root: fx.pv.pre,
                challenge_window_secs: WINDOW,
                min_bond_wei: U256::from(BOND),
            },
        )
        .await;
    let oracle = IKardamomProofOracle::new(deployment.oracle, rig.provider.clone());
    let settlement = IKardamomL2Settlement::new(deployment.settlement, rig.provider.clone());

    // The batch covers exactly the proven block. Its records commitment is
    // the fold of the block's digest, from the proof's public values.
    let commitment = fold_one(fx.pv.records_digest);
    settlement
        .postBatch(
            0,
            vec![B256::repeat_byte(0xA1)],
            fx.pv.block_number,
            fx.pv.block_number,
            commitment,
        )
        .from(BATCHER)
        .send()
        .await
        .unwrap()
        .get_receipt()
        .await
        .unwrap();

    Some(Scenario {
        _anvil: rig.anvil,
        oracle,
    })
}

/// Case A: a false claim (wrong root) is challenged with the real proof.
/// The real verifier accepts it, the bond is slashed, and the chain
/// rewinds without moving the root.
async fn false_claim_is_challenged_with_real_proof<P: Provider + Clone>(
    s: &Scenario<P>,
    fx: &Fixtures,
) {
    // ----- act: the false claim -----
    let wrong_root = B256::repeat_byte(0x66);
    assert_ne!(wrong_root, fx.pv.post, "the lie must differ from the truth");
    s.oracle
        .claimBatch(1, vec![wrong_root], vec![fx.pv.records_digest])
        .value(U256::from(BOND))
        .from(DEV_OWNER)
        .send()
        .await
        .unwrap()
        .get_receipt()
        .await
        .unwrap();

    // ----- act: the challenger submits the real proof at offset 0; the
    // contract checks preconditions, then calls the real SP1 verifier -----
    let receipt = s
        .oracle
        .challengeBlock(
            1,
            0,
            vec![wrong_root],
            vec![fx.pv.records_digest],
            Bytes::from(fx.pv.raw.clone()),
            fx.proof.clone(),
        )
        .send()
        .await
        .expect("challengeBlock tx send")
        .get_receipt()
        .await
        .expect("challengeBlock receipt");

    // ----- assert: the verifier accepted; slashed and rewound (the batch
    // reopened, the root untouched at genesis) -----
    assert!(
        receipt.status(),
        "the real SP1 verifier must ACCEPT the real groth16 proof on chain"
    );
    assert_eq!(s.oracle.highestClaimedBatch().call().await.unwrap(), 0);
    assert_eq!(s.oracle.stateRoot().call().await.unwrap(), fx.pv.pre);
}

/// Case B: an honest claim (the true root) cannot be griefed. The same
/// real proof reproduces the claimed root, so `challengeBlock` reverts
/// with `ProofAgreesWithClaim` before the verifier is even reached.
async fn honest_claim_cannot_be_griefed<P: Provider + Clone>(s: &Scenario<P>, fx: &Fixtures) {
    // ----- act: the truthful claim (the block's real post root) -----
    s.oracle
        .claimBatch(1, vec![fx.pv.post], vec![fx.pv.records_digest])
        .value(U256::from(BOND))
        .from(DEV_OWNER)
        .send()
        .await
        .unwrap()
        .get_receipt()
        .await
        .unwrap();

    // ----- act: a griefer submits the same real proof. `.send()` returns
    // `Ok` for a tx that reverts on chain (the revert shows up in the
    // receipt), so check the receipt reverted. An `Err`, from the node
    // simulating and rejecting at submit, is equally acceptable. -----
    let grief = s
        .oracle
        .challengeBlock(
            1,
            0,
            vec![fx.pv.post],
            vec![fx.pv.records_digest],
            Bytes::from(fx.pv.raw.clone()),
            fx.proof.clone(),
        )
        .send()
        .await;
    let grief_reverted = match grief {
        Err(_) => true,
        Ok(pending) => match pending.get_receipt().await {
            Ok(receipt) => !receipt.status(),
            Err(_) => true,
        },
    };

    // ----- assert: the grief was rejected, and the honest claim survives
    // untouched -----
    assert!(
        grief_reverted,
        "challenging an honest claim must revert (ProofAgreesWithClaim)"
    );
    let claimer = s.oracle.claims(1).call().await.unwrap().claimer;
    assert_eq!(
        claimer, DEV_OWNER,
        "honest claim intact after the grief attempt"
    );
}

#[tokio::test]
async fn real_groth16_proof_accepted_on_chain_challenge_and_grief_rejected() {
    // ----- setup -----
    let Some(fx) = load_fixtures() else {
        eprintln!(
            "SKIP: real-proof fixtures absent — generate on GPU/capable hardware (fixtures/README.md); the CPU groth16 wrap is a ~1h job"
        );
        return;
    };
    let Some(s) = setup(&fx).await else {
        eprintln!("SKIP: anvil unavailable");
        return;
    };

    // ----- act + assert: both cases share the one posted batch -----
    false_claim_is_challenged_with_real_proof(&s, &fx).await;
    honest_claim_cannot_be_griefed(&s, &fx).await;
}
