//! PR 4's closing contract, end to end on anvil: a REAL batch (accumulator
//! → records commitment → `postBatch`) posted to the settlement, a batch
//! proof's output files in the zk-host layout, and `submit_next_proof`
//! advancing the `KardamomProofOracle`'s root chain — batcher, settlement,
//! prover queue, and oracle aligned on the L1-as-truth cursor.
//!
//! The verifier here is the ACCEPTING mock (deployed from the forge test
//! artifact): contract-level proof rejection is covered by the forge suite,
//! and guest-side public-values authenticity by the zk-host batch round
//! trip — this test owns the CURSOR-ALIGNMENT plumbing between them.

use std::path::PathBuf;

use alloy_node_bindings::Anvil;
use alloy_primitives::{Address, B256, U256, address};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_sol_types::sol;
use kardamom_batcher::BatchAccumulator;
use kardamom_batcher::batcher::pack_blocks;
use kardamom_batcher::prover_submit::{IKardamomProofOracle, SubmitOutcome, submit_next_proof};
use kardamom_batcher::settlement::IKardamomL2Settlement;
use kardamom_deployer::addresses::{ERC7955_FACTORY, ERC7955_RUNTIME_HEX};
use kardamom_deployer::{
    ContractId, Deployer, Op, encode_address_arg, encode_proof_oracle_init_args,
};
use kardamom_types::{
    BPosition, BatchPublicOutputs, BlockBoundaryStart, TxEnvelope, batch_records_commitment,
};

sol!(
    #[sol(rpc)]
    AcceptingVerifier,
    concat!(
        env!("CARGO_WORKSPACE_DIR"),
        "/contracts/out/KardamomProofOracle.t.sol/AcceptingVerifier.json"
    )
);

const DEV_OWNER: Address = address!("f39Fd6e51aad88F6F4ce6aB8827279cffFb92266");
const BATCHER: Address = address!("00000000000000000000000000000000000000BA");
const L2_CHAIN_ID: u64 = 412346;
const VKEY: B256 = B256::repeat_byte(0x5E);
const GENESIS_ROOT: B256 = B256::repeat_byte(0x99);
const POST_ROOT: B256 = B256::repeat_byte(0xAB);

fn env_tx(i: u64) -> TxEnvelope {
    TxEnvelope {
        correlation_id: i,
        raw_tx: vec![0xF0u8, i as u8, 0xBA, 0x12].into(),
        sender: Address::repeat_byte(0x11),
        tx_hash: B256::repeat_byte(i as u8 + 1),
    }
}

#[tokio::test]
async fn posted_batch_proof_advances_the_oracle_root_chain() {
    let Some(anvil) = Anvil::new().try_spawn().ok() else {
        eprintln!("SKIP: anvil unavailable");
        return;
    };
    let provider = ProviderBuilder::new()
        .disable_recommended_fillers()
        .connect_http(anvil.endpoint_url());
    let bytes_hex = format!("0x{ERC7955_RUNTIME_HEX}");
    for req in [
        (
            "anvil_setCode",
            serde_json::json!([ERC7955_FACTORY, bytes_hex]),
        ),
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
        let _: serde_json::Value = provider
            .raw_request(req.0.into(), req.1)
            .await
            .expect("anvil setup");
    }

    // --- Deploy: settlement (factory), accepting verifier (plain), oracle
    // (factory, wired to both).
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
        .expect("deploy settlement");
    let settlement_addr = deployer.addresses(Some(L2_CHAIN_ID)).await.unwrap()[0].proxy;

    let verifier = AcceptingVerifier::deploy(provider.clone())
        .await
        .expect("deploy accepting verifier");
    deployer
        .apply(
            &[Op::Deploy {
                l2_chain_id: L2_CHAIN_ID,
                id: ContractId::KardamomProofOracle,
                init_args: encode_proof_oracle_init_args(
                    settlement_addr,
                    *verifier.address(),
                    VKEY,
                    VKEY, // block vkey (mock verifier ignores both)
                    GENESIS_ROOT,
                    0, // window 0: validity-mode e2e finalizes by proof
                    U256::ZERO,
                ),
            }],
            DEV_OWNER,
        )
        .await
        .expect("deploy proof oracle");
    let oracle_addr = deployer
        .addresses(Some(L2_CHAIN_ID))
        .await
        .unwrap()
        .into_iter()
        .find(|e| e.id == ContractId::KardamomProofOracle.id())
        .expect("oracle entry")
        .proxy;

    let proofs_dir = tempfile::tempdir().unwrap();

    // Prover and batcher both lagging: nothing posted, nothing to do.
    let out = submit_next_proof(provider.clone(), oracle_addr, proofs_dir.path())
        .await
        .unwrap();
    assert_eq!(out, SubmitOutcome::NoBatchPosted { batch_index: 1 });

    // --- A REAL batch through the accumulator: two blocks, three txs; the
    // records commitment comes out of the production close path.
    let mut acc = BatchAccumulator::new();
    acc.observe_tx(env_tx(0), BPosition::from_index(0));
    acc.observe_tx(env_tx(1), BPosition::from_index(1));
    let b1 = acc.observe_boundary(BlockBoundaryStart {
        block_number: 7,
        end_tx_idx: BPosition::from_index(2),
        l2_timestamp: 1_700_000_007,
        l1_origin: 0,
    });
    acc.observe_tx(env_tx(2), BPosition::from_index(2));
    let b2 = acc.observe_boundary(BlockBoundaryStart {
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

    let settlement = IKardamomL2Settlement::new(settlement_addr, provider.clone());
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

    // Batch posted but the prover hasn't produced files yet.
    let out = submit_next_proof(provider.clone(), oracle_addr, proofs_dir.path())
        .await
        .unwrap();
    assert_eq!(out, SubmitOutcome::ProofNotReady { batch_index: 1 });

    // --- The prover's output files (zk-host batch layout). Public values
    // must carry the SAME commitment the batcher posted — cross-computed
    // here via the shared primitives, exactly as the batch guest commits.
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
    let pv = BatchPublicOutputs {
        pre_state_root: GENESIS_ROOT,
        post_state_root: POST_ROOT,
        first_block: 7,
        last_block: 8,
        records_commitment: expected_commitment,
    };
    let dir: PathBuf = proofs_dir.path().join("batch-7-8");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("public-values.bin"), pv.encode()).unwrap();
    std::fs::write(dir.join("proof.bin"), b"mock-proof").unwrap();

    // --- Submit: the oracle's root chain advances.
    let out = submit_next_proof(provider.clone(), oracle_addr, proofs_dir.path())
        .await
        .unwrap();
    assert_eq!(out, SubmitOutcome::Submitted { batch_index: 1 });
    let oracle = IKardamomProofOracle::new(oracle_addr, provider.clone());
    assert_eq!(oracle.stateRoot().call().await.unwrap(), POST_ROOT);
    assert_eq!(oracle.lastFinalizedBatch().call().await.unwrap(), 1);

    // Idempotence at the cursor: batch 2 not posted → NoBatchPosted.
    let out = submit_next_proof(provider.clone(), oracle_addr, proofs_dir.path())
        .await
        .unwrap();
    assert_eq!(out, SubmitOutcome::NoBatchPosted { batch_index: 2 });
}
