//! The prover-spool contract: a block
//! captured, anchored, and spooled against a real writer-committed
//! pre-state snapshot must produce a frame that re-verifies one-shot in
//! the guest shape, and that names the exact post root the live
//! trie-aware writer then commits for the same block.
//!
//! This drives `spool_block`, the per-block body of the async spool
//! task, against a production `StateWriter` (TrieMode::Incremental) and
//! the MVCC `StateSnapshot` pin: the live wiring, minus the tokio loop.

use std::time::{Duration, Instant};

use alloy_consensus::{SignableTransaction, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, B256, TxKind, U256, address, keccak256};
use alloy_signer_local::PrivateKeySigner;
use kardamom_engine::actor::BufferedRecord;
use kardamom_engine::exec_types::TxIndex;
use kardamom_engine::{ExecEnv, error::ExecutorError};
use kardamom_state::writer::{StateWriter, WriteBatch};
use kardamom_state::{Durability, StateEnvBuilder, TrieMode};
use kardamom_types::{
    AccountChange, BPosition, BlockBoundary, BlockBoundaryStart, BlockDelta, CodeEntry,
    ProverInput, StorageChange, TxEnvelope,
};
use kardamom_validator::prover::spool_block;

const CHAIN_ID: u64 = 412346;
const RECIPIENT: Address = address!("000000000000000000000000000000000000dEaD");
const ZEROER: Address = address!("00000000000000000000000000000000000000Aa");
const ZEROER_CODE: [u8; 6] = [0x60, 0x00, 0x60, 0x00, 0x55, 0x00];
const S0: B256 = B256::with_last_byte(0);
const S1: B256 = B256::with_last_byte(1);

fn tx(signer: &PrivateKeySigner, to: Address, nonce: u64, value: u64, i: u64) -> BufferedRecord {
    let mut inner = TxLegacy {
        chain_id: Some(CHAIN_ID),
        nonce,
        gas_price: 0,
        gas_limit: 300_000,
        to: TxKind::Call(to),
        value: U256::from(value),
        input: Default::default(),
    };
    let sig = signer.sign_transaction_sync(&mut inner).unwrap();
    let env: alloy_consensus::TxEnvelope = inner.into_signed(sig).into();
    let mut raw = Vec::new();
    env.encode_2718(&mut raw);
    let tx_hash = keccak256(&raw);
    BufferedRecord::Tx {
        tx_idx: TxIndex(i),
        position: BPosition {
            term_id: 0,
            term_offset: (i * 64) as i32,
        },
        envelope: TxEnvelope {
            correlation_id: i,
            raw_tx: raw.into(),
            sender: signer.address(),
            tx_hash,
        },
    }
}

fn boundary(block_number: u64, ts: u64) -> BlockBoundary {
    BlockBoundary {
        block_number,
        end_tx_idx: BPosition::from_index(0),
        l2_timestamp: ts,
        l1_origin: 0,
    }
}

#[test]
fn spooled_frame_reverifies_and_matches_the_live_writer_root() {
    let signer = PrivateKeySigner::from_bytes(&B256::repeat_byte(0x77)).unwrap();
    let sender = signer.address();
    let zeroer_hash = keccak256(ZEROER_CODE);

    // When KARDAMOM_EMIT_BATCH_SPOOL=dir is set, the spool lands there
    // (blocks 2 and 3, a real contiguous batch) for the zk-host batch
    // round trip.
    let export = std::env::var("KARDAMOM_EMIT_BATCH_SPOOL").ok();
    let dir = tempfile::tempdir().unwrap();
    let env = StateEnvBuilder::new(dir.path().join("state"))
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap();
    let writer = StateWriter::spawn_with_trie(env, TrieMode::Incremental).unwrap();

    // --- Block 1: the seed, through the production writer (accounts,
    // code, storage, trie, and meta all land as live commits do).
    let seed = BlockDelta {
        block_number: 1,
        accounts: vec![
            AccountChange {
                address: sender,
                nonce: 0,
                balance: U256::from(10u128.pow(18)),
                code_hash: B256::ZERO,
            },
            AccountChange {
                address: ZEROER,
                nonce: 1,
                balance: U256::ZERO,
                code_hash: zeroer_hash,
            },
        ],
        storage: vec![
            StorageChange {
                address: ZEROER,
                key: S0,
                value: U256::from(5),
            },
            StorageChange {
                address: ZEROER,
                key: S1,
                value: U256::from(7),
            },
        ],
        code: vec![CodeEntry {
            code_hash: zeroer_hash,
            code: ZEROER_CODE.as_slice().into(),
        }],
        receipts: Vec::new(),
    };
    writer
        .delta_tx
        .send(WriteBatch::new(boundary(1, 1_700_000_000), seed))
        .unwrap();

    // Wait for the committed snapshot at block 1: the spool's pre-state pin.
    let deadline = Instant::now() + Duration::from_secs(10);
    let snap = loop {
        if let Some(s) = writer.snapshot_rx.current()
            && s.block_number() == 1
        {
            break s;
        }
        assert!(Instant::now() < deadline, "writer never committed block 1");
        std::thread::sleep(Duration::from_millis(20));
    };

    // --- Block 2: transfer to a fresh account and zero a slot (a
    // storage deletion collapse, the anchoring shape that needs the
    // fixed point).
    let records = vec![
        tx(&signer, RECIPIENT, 0, 250_000, 0),
        tx(&signer, ZEROER, 1, 0, 1),
    ];
    let env2 = ExecEnv::new(
        CHAIN_ID,
        &BlockBoundaryStart {
            block_number: 2,
            end_tx_idx: BPosition::from_index(0),
            l2_timestamp: 1_700_000_002,
            l1_origin: 0,
        },
    );
    let spool = export
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| dir.path().join("spool"));
    let outputs = spool_block(&spool, CHAIN_ID, &snap, 2, env2, &records).expect("spool block 2");

    // (a) The spooled frame re-verifies one-shot in the guest shape.
    let bytes = std::fs::read(spool.join("block-2/prover-input.rkyv")).unwrap();
    let expected = std::fs::read(spool.join("block-2/expected-outputs.bin")).unwrap();
    assert_eq!(expected, outputs.encode());
    let input: ProverInput =
        rkyv::from_bytes::<ProverInput, rkyv::rancor::Error>(&bytes).expect("frame decodes");
    let guest_records: Vec<BufferedRecord> = input
        .records
        .iter()
        .map(|r| match r {
            kardamom_types::ProverRecord::Tx {
                tx_idx,
                envelope,
                position,
            } => BufferedRecord::Tx {
                tx_idx: TxIndex(*tx_idx),
                envelope: envelope.clone(),
                position: *position,
            },
            kardamom_types::ProverRecord::Deposit {
                tx_idx,
                deposit,
                position,
            } => BufferedRecord::Deposit {
                tx_idx: TxIndex(*tx_idx),
                deposit: deposit.clone(),
                position: *position,
            },
        })
        .collect();
    let genv = ExecEnv::new(input.chain_id, &input.boundary);
    let mut bal_slice: &[u8] = &input.bal_rlp;
    let expected_bal =
        <alloy_eip7928::BlockAccessList as alloy_rlp::Decodable>::decode(&mut bal_slice).unwrap();
    let anchored = kardamom_engine::stateless::execute_block_anchored(
        &input.witness,
        &input.proofs,
        None,
        &guest_records,
        genv,
        &expected_bal,
        input.granularity,
    )
    .expect("guest-shape re-verification");
    assert_eq!(anchored.pre_state_root, outputs.pre_state_root);
    assert_eq!(anchored.post_state_root, outputs.post_state_root);
    assert_eq!(anchored.bal_commitment, outputs.bal_commitment);

    // (b) The live writer commits block 2 and lands on the spooled post
    // root: the proof queue and the chain agree before any proving happens.
    let mut delta2 = BlockDelta {
        block_number: 2,
        accounts: anchored
            .out
            .delta
            .accounts
            .iter()
            .map(|(address, (nonce, balance, code_hash))| AccountChange {
                address: *address,
                nonce: *nonce,
                balance: *balance,
                code_hash: *code_hash,
            })
            .collect(),
        storage: anchored
            .out
            .delta
            .storage
            .iter()
            .map(|((address, key), value)| StorageChange {
                address: *address,
                key: *key,
                value: *value,
            })
            .collect(),
        code: Vec::new(),
        receipts: Vec::new(),
    };
    delta2.accounts.sort_by_key(|a| a.address);
    delta2.storage.sort_by_key(|s| (s.address, s.key));
    writer
        .delta_tx
        .send(WriteBatch::new(boundary(2, 1_700_000_002), delta2))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let live_root = loop {
        if let Some(s) = writer.snapshot_rx.current()
            && s.block_number() == 2
        {
            break s.state_root().unwrap().expect("trie root at block 2");
        }
        assert!(Instant::now() < deadline, "writer never committed block 2");
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(
        live_root, outputs.post_state_root,
        "spooled post root must equal the live writer's root"
    );

    // --- Block 3: one more transfer, spooled against the pinned
    // snapshot at block 2, the second half of a real contiguous batch.
    // The batch guest requires the root chain to link 2 to 3.
    let snap2 = {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(s) = writer.snapshot_rx.current()
                && s.block_number() == 2
            {
                break s;
            }
            assert!(Instant::now() < deadline, "no snapshot at block 2");
            std::thread::sleep(Duration::from_millis(20));
        }
    };
    let records3 = vec![tx(&signer, RECIPIENT, 2, 111, 0)];
    let env3 = ExecEnv::new(
        CHAIN_ID,
        &BlockBoundaryStart {
            block_number: 3,
            end_tx_idx: BPosition::from_index(0),
            l2_timestamp: 1_700_000_003,
            l1_origin: 0,
        },
    );
    let outputs3 =
        spool_block(&spool, CHAIN_ID, &snap2, 3, env3, &records3).expect("spool block 3");
    assert_eq!(
        outputs3.pre_state_root, outputs.post_state_root,
        "the spooled chain must link 2 -> 3"
    );

    // A spool against the wrong pre-state snapshot must be rejected.
    let stale = spool_block(
        &spool, CHAIN_ID, &snap, // Still pinned at block 1.
        3,     // But claims to be block 3 (pre-state 2).
        env2, &records,
    );
    assert!(
        matches!(
            stale,
            Err(ExecutorError::WitnessUnanchored(_)) | Err(ExecutorError::State(_))
        ),
        "wrong-window spool must fail closed: {stale:?}"
    );

    writer.shutdown().unwrap();
}
