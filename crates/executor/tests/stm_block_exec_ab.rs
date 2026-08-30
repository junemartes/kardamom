//! Merge gate for `--parallel-execution` (scheduler unification B2). The
//! Block-STM block-at-a-time strategy must be byte-identical to the
//! sequential capture driver: receipts, delta, and the published BAL's
//! RLP, on blocks with real fees, deposits interleaved between tx runs,
//! and an invalid skip. The validator's live cross-check fail-stops on
//! any drift. This test is the offline form of that gate.

use alloy_consensus::{SignableTransaction, TxLegacy};
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, B256, Bytes as AlloyBytes, TxKind, U256, address};
use alloy_rlp::Encodable;
use alloy_signer_local::PrivateKeySigner;
use bytes::Bytes;
use revm::primitives::KECCAK_EMPTY;

use kardamom_engine::{
    BPosition, MockStateDatabase, TxEnvelope as KtTxEnvelope, TxIndex, actor::BufferedRecord,
    block_env::ExecEnv,
};
use kardamom_executor::parallel::{StmExecConfig, stm_block_exec};

fn tx_record(
    signer: &PrivateKeySigner,
    to: Address,
    nonce: u64,
    value: u64,
    i: u64,
) -> BufferedRecord {
    let mut inner = TxLegacy {
        chain_id: Some(1),
        // Real fees: a zero gas price would fully hide fee-sink attribution
        // bugs (the sink is the one account STM tracks specially).
        gas_price: 1_000_000_000,
        nonce,
        gas_limit: 100_000,
        to: TxKind::Call(to),
        value: U256::from(value),
        input: AlloyBytes::new(),
    };
    let sig = signer.sign_transaction_sync(&mut inner).unwrap();
    let env: alloy_consensus::TxEnvelope = inner.into_signed(sig).into();
    let mut raw = Vec::new();
    alloy_eips::eip2718::Encodable2718::encode_2718(&env, &mut raw);
    let raw_tx = Bytes::from(raw);
    let tx_hash = alloy_primitives::keccak256(&raw_tx);
    BufferedRecord::Tx {
        tx_idx: TxIndex(i),
        envelope: KtTxEnvelope {
            correlation_id: i,
            raw_tx,
            sender: signer.address(),
            tx_hash,
        },
        position: BPosition {
            term_id: 0,
            term_offset: (i * 64) as i32,
        },
    }
}

fn deposit_record(mint: u128, to: Address, i: u64) -> BufferedRecord {
    BufferedRecord::Deposit {
        tx_idx: TxIndex(i),
        deposit: kardamom_types::Deposit {
            source_hash: B256::repeat_byte(0xD0 + i as u8),
            from: to,
            to: Some(to),
            mint,
            value: U256::ZERO,
            gas_limit: 100_000,
            is_system_transaction: false,
            input: Default::default(),
        },
        position: BPosition {
            term_id: 0,
            term_offset: (i * 64) as i32,
        },
    }
}

#[test]
fn stm_strategy_matches_sequential_capture_byte_for_byte() {
    let alice = PrivateKeySigner::random();
    let bob = PrivateKeySigner::random();
    let to = address!("00000000000000000000000000000000000ABCDE");
    let dep_to = address!("00000000000000000000000000000000000BEEF0");

    let snap = MockStateDatabase::builder()
        .account(alice.address(), U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
        .account(bob.address(), U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
        .build();

    // Records: tx-run, deposit, tx-run, deposit, tx-run, with an invalid
    // skip in the middle (a nonce gap causes NonceTooHigh). This also
    // exercises skip-hole parity.
    let mut records: Vec<BufferedRecord> = Vec::new();
    let mut i = 0u64;
    let mut a_nonce = 0u64;
    let mut b_nonce = 0u64;
    for _ in 0..5 {
        records.push(tx_record(&alice, to, a_nonce, 100 + i, i));
        a_nonce += 1;
        i += 1;
    }
    records.push(deposit_record(5 * 10u128.pow(17), dep_to, i));
    i += 1;
    for k in 0..7 {
        if k == 3 {
            // Nonce far ahead: deterministic invalid skip on both paths.
            records.push(tx_record(&bob, to, 999, 1, i));
        } else {
            records.push(tx_record(&bob, to, b_nonce, 200 + i, i));
            b_nonce += 1;
        }
        i += 1;
    }
    records.push(deposit_record(3 * 10u128.pow(17), dep_to, i));
    i += 1;
    for _ in 0..6 {
        records.push(tx_record(&alice, to, a_nonce, 300 + i, i));
        a_nonce += 1;
        i += 1;
    }

    let env = ExecEnv {
        chain_id: 1,
        block_number: 1,
        l2_timestamp: 1_700_000_000,
    };

    // A: the sequential capture driver, the streaming path's semantics.
    let seq = kardamom_engine::stateless::execute_block_capture(&snap, None, &records, env)
        .expect("sequential");

    for workers in [1usize, 4, 8] {
        // B: the Block-STM strategy (fresh pool per worker count).
        let strategy = stm_block_exec::<MockStateDatabase>(StmExecConfig {
            workers,
            pin_cores: Vec::new(),
            keep_hot: false,
        });
        let stm = strategy(&snap, None, &records, env, 1).expect("stm strategy");

        assert_eq!(
            stm.receipts, seq.receipts,
            "receipts diverge at w={workers}"
        );
        assert_eq!(
            stm.delta.accounts, seq.delta.accounts,
            "account writes diverge at w={workers}"
        );
        assert_eq!(
            stm.delta.storage, seq.delta.storage,
            "storage writes diverge at w={workers}"
        );

        // The published artifact: raw granularity-1 BAL RLP. Raw equality
        // implies quantized equality at every K, through the shared
        // `quantize`.
        let a = seq
            .bal
            .clone()
            .expect("sequential capture BAL")
            .into_alloy_bal();
        let b = stm.bal.clone().expect("stm strategy BAL").into_alloy_bal();
        let mut seq_rlp = Vec::new();
        a.encode(&mut seq_rlp);
        let mut stm_rlp = Vec::new();
        b.encode(&mut stm_rlp);
        assert_eq!(
            stm_rlp, seq_rlp,
            "published BAL RLP diverges at w={workers}"
        );
    }
}
