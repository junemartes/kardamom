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

use alloy_node_bindings::Anvil;
use alloy_primitives::{Address, B256, U256, address};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_sol_types::sol;
use kardamom_batcher::BatchAccumulator;
use kardamom_batcher::batcher::pack_blocks;
use kardamom_batcher::optimistic::{
    ClaimOutcome, WatchOutcome, claim_next_batch, watch_and_challenge,
};
use kardamom_batcher::prover_submit::IKardamomProofOracle;
use kardamom_batcher::settlement::IKardamomL2Settlement;
use kardamom_deployer::addresses::{ERC7955_FACTORY, ERC7955_RUNTIME_HEX};
use kardamom_deployer::{
    ContractId, Deployer, Op, encode_address_arg, encode_proof_oracle_init_args,
};
use kardamom_types::{
    BPosition, BlockBoundaryStart, BlockRecordsDigest, PublicOutputs, TxEnvelope,
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
const WINDOW: u64 = 3600;

fn env_tx(i: u64) -> TxEnvelope {
    TxEnvelope {
        correlation_id: i,
        raw_tx: vec![0xF0u8, i as u8, 0xBA, 0x12].into(),
        sender: Address::repeat_byte(0x11),
        tx_hash: B256::repeat_byte(i as u8 + 1),
    }
}

/// The honest per-block roots the "validator" computed.
fn honest_root(block: u64) -> B256 {
    B256::repeat_byte(0xA0u8.wrapping_add(block as u8))
}

/// Write a spool entry the way the validator's spool would. The 160-byte
/// expected-outputs layout feeds both the claim poster and the watcher.
fn write_spool_block(spool: &std::path::Path, block: u64, pre: B256, digest: B256) {
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

#[tokio::test]
async fn optimistic_claim_finalize_and_challenge_paths() {
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

    // --- Deploy settlement + oracle v2 (accepting verifier, real window,
    // real bond).
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
    let verifier = AcceptingVerifier::deploy(provider.clone()).await.unwrap();
    deployer
        .apply(
            &[Op::Deploy {
                l2_chain_id: L2_CHAIN_ID,
                id: ContractId::KardamomProofOracle,
                init_args: encode_proof_oracle_init_args(
                    settlement_addr,
                    *verifier.address(),
                    VKEY,
                    VKEY,
                    GENESIS_ROOT,
                    WINDOW,
                    U256::from(10u128.pow(18)), // 1 ETH bond
                ),
            }],
            DEV_OWNER,
        )
        .await
        .expect("deploy oracle");
    let oracle_addr = deployer
        .addresses(Some(L2_CHAIN_ID))
        .await
        .unwrap()
        .into_iter()
        .find(|e| e.id == ContractId::KardamomProofOracle.id())
        .unwrap()
        .proxy;
    let oracle = IKardamomProofOracle::new(oracle_addr, provider.clone());

    // --- A real batch: blocks 7..8 through the accumulator.
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
    .unwrap();

    // The spool the "validator" produced. The digests must be the
    // batcher's per-block digests (shared primitives). The roots are the
    // honest chain.
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
    let spool_dir = tempfile::tempdir().unwrap();
    let spool = spool_dir.path().to_path_buf();
    write_spool_block(&spool, 7, GENESIS_ROOT, d7);
    write_spool_block(&spool, 8, honest_root(7), d8);

    let settlement = IKardamomL2Settlement::new(settlement_addr, provider.clone());

    // ================= Scenario A: honest claim, zero proofs =============
    let out = claim_next_batch(provider.clone(), oracle_addr, &spool)
        .await
        .unwrap();
    assert_eq!(out, ClaimOutcome::NoBatchPosted { batch_index: 1 });

    settlement
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

    let out = claim_next_batch(provider.clone(), oracle_addr, &spool)
        .await
        .unwrap();
    assert_eq!(out, ClaimOutcome::Claimed { batch_index: 1 });

    let out = watch_and_challenge(provider.clone(), oracle_addr, &spool)
        .await
        .unwrap();
    assert_eq!(out, WatchOutcome::ClaimHonest { batch_index: 1 });

    let _: serde_json::Value = provider
        .raw_request("evm_increaseTime".into(), serde_json::json!([WINDOW + 1]))
        .await
        .unwrap();
    let _: serde_json::Value = provider
        .raw_request("evm_mine".into(), serde_json::json!([]))
        .await
        .unwrap();
    oracle
        .finalizeBatch(1)
        .send()
        .await
        .unwrap()
        .get_receipt()
        .await
        .unwrap();
    assert_eq!(oracle.stateRoot().call().await.unwrap(), honest_root(8));
    assert_eq!(oracle.lastFinalizedBatch().call().await.unwrap(), 1);

    // ================= Scenario B: lying claim, one proof ================
    settlement
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
    // Batch 2 reuses the 7..8 range shape. The spool's honest roots chain
    // from the current state root, so rewrite the spool for the new
    // pre-root context (the honest chain now starts at honest_root(8)).
    // For simplicity, the lie keeps offset 0 honest and lies at offset 1.
    write_spool_block(&spool, 7, honest_root(8), d7);
    write_spool_block(&spool, 8, honest_root(7), d8);

    // The liar claims directly. The digests are correct (the fold passes),
    // but the root is wrong at offset 1.
    let lie = B256::repeat_byte(0x66);
    oracle
        .claimBatch(2, vec![honest_root(7), lie], vec![d7, d8])
        .value(U256::from(10u128.pow(18)))
        .from(DEV_OWNER)
        .send()
        .await
        .unwrap()
        .get_receipt()
        .await
        .unwrap();

    // The watcher detects the divergence but the proof is not ready.
    let out = watch_and_challenge(provider.clone(), oracle_addr, &spool)
        .await
        .unwrap();
    assert_eq!(
        out,
        WatchOutcome::ProofNotReady {
            batch_index: 2,
            divergent_block: 8
        }
    );

    // The prover produces the single-block files (the zk-host --prove
    // shape). The public values are the honest block 8; the proof is a
    // mock (accepting verifier).
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

    let out = watch_and_challenge(provider.clone(), oracle_addr, &spool)
        .await
        .unwrap();
    assert_eq!(
        out,
        WatchOutcome::Challenged {
            batch_index: 2,
            block_offset: 1
        }
    );

    // Rewind: the root is unchanged, the batch reopens, and the slash is
    // credited to the challenger (the provider's default account submitted
    // the challenge).
    assert_eq!(oracle.stateRoot().call().await.unwrap(), honest_root(8));
    assert_eq!(oracle.highestClaimedBatch().call().await.unwrap(), 1);

    // Honest re-claim from the spool, then finalize.
    let out = claim_next_batch(provider.clone(), oracle_addr, &spool)
        .await
        .unwrap();
    assert_eq!(out, ClaimOutcome::Claimed { batch_index: 2 });
    let _: serde_json::Value = provider
        .raw_request("evm_increaseTime".into(), serde_json::json!([WINDOW + 1]))
        .await
        .unwrap();
    let _: serde_json::Value = provider
        .raw_request("evm_mine".into(), serde_json::json!([]))
        .await
        .unwrap();
    oracle
        .finalizeBatch(2)
        .send()
        .await
        .unwrap()
        .get_receipt()
        .await
        .unwrap();
    assert_eq!(oracle.lastFinalizedBatch().call().await.unwrap(), 2);
    assert_eq!(oracle.stateRoot().call().await.unwrap(), honest_root(8));
}
