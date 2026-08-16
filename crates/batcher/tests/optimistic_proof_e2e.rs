//! The full optimistic path with a REAL proof and the REAL SP1 verifier
//! (spec: no-std-exec-core, PR 5). Unlike `optimistic_e2e` (mock verifier,
//! flow plumbing), this verifies a genuine SP1 Groth16 proof of a real
//! kardamom block against the vendored SP1 verifier (circuit v6.1.0) on
//! anvil — the crux the user asked for: "generate a proof, submit it on
//! chain, and ensure the contract accepts it."
//!
//! Two cases, one shared real proof (proving is expensive; the fixture is
//! committed — see fixtures/README.md):
//!
//!   A. A FALSE block claim (wrong root) is challenged with the real proof;
//!      the real verifier accepts, the bond is slashed, the chain rewinds.
//!   B. The OPPOSITE — an HONEST claim (the true root) cannot be griefed:
//!      the same real proof reproduces the claimed root, so `challengeBlock`
//!      reverts `ProofAgreesWithClaim` before the verifier is even reached.
//!
//! Skips cleanly if the fixtures are absent (unregenerated) or anvil is
//! unavailable.

use alloy_node_bindings::Anvil;
use alloy_primitives::{Address, B256, Bytes, Keccak256, U256, address};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_sol_types::sol;
use kardamom_batcher::prover_submit::IKardamomProofOracle;
use kardamom_batcher::settlement::IKardamomL2Settlement;
use kardamom_deployer::addresses::{ERC7955_FACTORY, ERC7955_RUNTIME_HEX};
use kardamom_deployer::{
    ContractId, Deployer, Op, encode_address_arg, encode_proof_oracle_init_args,
};
use std::path::PathBuf;

sol!(
    #[sol(rpc)]
    SP1Verifier,
    concat!(
        env!("CARGO_WORKSPACE_DIR"),
        "/contracts/out/SP1VerifierGroth16.sol/SP1Verifier.json"
    )
);

const DEV_OWNER: Address = address!("f39Fd6e51aad88F6F4ce6aB8827279cffFb92266");
const BATCHER: Address = address!("00000000000000000000000000000000000000BA");
const L2_CHAIN_ID: u64 = 412346;
const WINDOW: u64 = 3600;
const BOND: u128 = 1_000_000_000_000_000_000;

fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// The single-block public outputs the guest committed (160-byte layout,
/// shared with BatchPublicOutputs field-for-field: pre, post, blockNumber,
/// records_digest slot as `records_commitment`, bal slot dropped here).
struct BlockPv {
    pre: B256,
    post: B256,
    block_number: u64,
    records_digest: B256,
    raw: Vec<u8>,
}

fn load_block_pv(bytes: Vec<u8>) -> BlockPv {
    // The single-block PublicOutputs layout: pre || post || block_number(u256)
    // || records_digest || bal_commitment. Decode via BatchPublicOutputs'
    // matching prefix (first/last u256 fields differ semantically but the
    // byte slots we need — pre, post, word@64, word@96 — line up).
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

#[tokio::test]
async fn real_groth16_proof_accepted_on_chain_challenge_and_grief_rejected() {
    let fx = fixtures();
    let (Ok(pv_bytes), Ok(proof_bytes), Ok(vkey_hex)) = (
        std::fs::read(fx.join("public-values.bin")),
        std::fs::read(fx.join("proof.bin")),
        std::fs::read_to_string(fx.join("vkey.hex")),
    ) else {
        eprintln!("SKIP: real-proof fixtures absent (see fixtures/README.md to regenerate)");
        return;
    };
    let Some(anvil) = Anvil::new().try_spawn().ok() else {
        eprintln!("SKIP: anvil unavailable");
        return;
    };
    let pv = load_block_pv(pv_bytes);
    let proof = Bytes::from(proof_bytes);
    let vkey: B256 = vkey_hex.trim().parse().expect("vkey.hex is a bytes32");
    assert_eq!(pv.raw.len(), 160, "single-block public values are 160 bytes");

    let provider = ProviderBuilder::new()
        .disable_recommended_fillers()
        .connect_http(anvil.endpoint_url());
    let bytes_hex = format!("0x{ERC7955_RUNTIME_HEX}");
    for req in [
        ("anvil_setCode", serde_json::json!([ERC7955_FACTORY, bytes_hex])),
        (
            "anvil_setBalance",
            serde_json::json!([DEV_OWNER, U256::from(10u128.pow(21))]),
        ),
        (
            "anvil_setBalance",
            serde_json::json!([BATCHER, U256::from(10u128.pow(21))]),
        ),
        ("anvil_impersonateAccount", serde_json::json!([DEV_OWNER])),
        ("anvil_impersonateAccount", serde_json::json!([BATCHER])),
    ] {
        let _: serde_json::Value = provider.raw_request(req.0.into(), req.1).await.unwrap();
    }

    // --- Deploy the REAL SP1 verifier, settlement, and oracle v2 wired to
    // BOTH (batch + block vkey = the fixture's guest vkey; genesis = the
    // block's proven PRE root, so the offset-0 pre-root check lines up).
    let verifier = SP1Verifier::deploy(provider.clone()).await.unwrap();
    assert_eq!(
        verifier.VERSION().call().await.unwrap(),
        "v6.1.0",
        "vendored verifier is the circuit version the SDK proves against"
    );

    let deployer = Deployer::new(provider.clone(), DEV_OWNER);
    deployer.ensure_factory(DEV_OWNER).await.unwrap();
    deployer
        .apply(
            &[Op::Deploy {
                l2_chain_id: L2_CHAIN_ID,
                id: ContractId::KardamomL2Settlement,
                init_args: encode_address_arg(BATCHER),
            }],
            DEV_OWNER,
        )
        .await
        .unwrap();
    let settlement_addr = deployer.addresses(Some(L2_CHAIN_ID)).await.unwrap()[0].proxy;
    deployer
        .apply(
            &[Op::Deploy {
                l2_chain_id: L2_CHAIN_ID,
                id: ContractId::KardamomProofOracle,
                init_args: encode_proof_oracle_init_args(
                    settlement_addr,
                    *verifier.address(),
                    vkey,
                    vkey,
                    pv.pre, // genesis root == the block's pre-state root
                    WINDOW,
                    U256::from(BOND),
                ),
            }],
            DEV_OWNER,
        )
        .await
        .unwrap();
    let oracle_addr = deployer
        .addresses(Some(L2_CHAIN_ID))
        .await
        .unwrap()
        .into_iter()
        .find(|e| e.id == ContractId::KardamomProofOracle.id())
        .unwrap()
        .proxy;
    let oracle = IKardamomProofOracle::new(oracle_addr, provider.clone());
    let settlement = IKardamomL2Settlement::new(settlement_addr, provider.clone());

    // The batch covers exactly the proven block; its records commitment is
    // the fold of the block's digest (from the proof's public values).
    let commitment = fold_one(pv.records_digest);
    settlement
        .postBatch(
            0,
            vec![B256::repeat_byte(0xA1)],
            pv.block_number,
            pv.block_number,
            commitment,
        )
        .from(BATCHER)
        .send()
        .await
        .unwrap()
        .get_receipt()
        .await
        .unwrap();

    // ===================== Case A: false claim, real proof accepted ======
    let wrong_root = B256::repeat_byte(0x66);
    assert_ne!(wrong_root, pv.post, "the lie must differ from the truth");
    oracle
        .claimBatch(1, vec![wrong_root], vec![pv.records_digest])
        .value(U256::from(BOND))
        .from(DEV_OWNER)
        .send()
        .await
        .unwrap()
        .get_receipt()
        .await
        .unwrap();

    // The challenger submits the REAL proof at offset 0. The contract checks
    // preconditions, then calls the REAL SP1 verifier — which must accept.
    let receipt = oracle
        .challengeBlock(
            1,
            0,
            vec![wrong_root],
            vec![pv.records_digest],
            Bytes::from(pv.raw.clone()),
            proof.clone(),
        )
        .send()
        .await
        .expect("challengeBlock tx send")
        .get_receipt()
        .await
        .expect("challengeBlock receipt");
    assert!(
        receipt.status(),
        "the real SP1 verifier must ACCEPT the real groth16 proof on chain"
    );
    // Slashed + rewound: the batch reopened, the root is untouched (genesis).
    assert_eq!(oracle.highestClaimedBatch().call().await.unwrap(), 0);
    assert_eq!(oracle.stateRoot().call().await.unwrap(), pv.pre);

    // ===================== Case B: honest claim can't be griefed =========
    // The truthful claim: the block's real post root.
    oracle
        .claimBatch(1, vec![pv.post], vec![pv.records_digest])
        .value(U256::from(BOND))
        .from(DEV_OWNER)
        .send()
        .await
        .unwrap()
        .get_receipt()
        .await
        .unwrap();
    // A griefer submits the SAME real proof. Its proven post == the claimed
    // root, so the contract rejects on `ProofAgreesWithClaim` BEFORE the
    // verifier — an honest proposer cannot be challenged even with a valid
    // proof.
    let grief = oracle
        .challengeBlock(
            1,
            0,
            vec![pv.post],
            vec![pv.records_digest],
            Bytes::from(pv.raw.clone()),
            proof.clone(),
        )
        .send()
        .await;
    assert!(
        grief.is_err(),
        "challenging an honest claim must revert (ProofAgreesWithClaim)"
    );
    // The honest claim survives untouched.
    let (claimer, _, _, _, _, _) = {
        let c = oracle.claims(1).call().await.unwrap();
        (c.claimer, c.bond, c.claimedAt, c.preRoot, c.finalRoot, c.seqHash)
    };
    assert_eq!(claimer, DEV_OWNER, "honest claim intact after the grief attempt");
}
