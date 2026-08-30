//! `TouchSet` capture (footprint shadow, spec block-stm-executor §P1):
//! per-tx READ attribution from `outcome.state` — accounts loaded but never
//! touched, and slots accessed without a value change. The block-scoped BAL
//! cannot see either per tx, which is why the capture lives in
//! `Executor::execute_tx` itself.

use alloy_consensus::{SignableTransaction, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, B256, TxKind, U256, address, keccak256};
use alloy_signer_local::PrivateKeySigner;
use kardamom_exec_core::block_env::ExecEnv;
use kardamom_exec_core::exec_types::TxIndex;
use kardamom_exec_core::executor::{Executor, TouchSet};
use kardamom_exec_core::state::MockStateDatabase;
use kardamom_types::{BPosition, TxEnvelope};

const CHAIN_ID: u64 = 412346;
/// PUSH1 0, SLOAD, STOP — reads slot 0, writes nothing.
const READER: Address = address!("00000000000000000000000000000000000000AA");
const READER_CODE: [u8; 4] = [0x60, 0x00, 0x54, 0x00];
/// PUSH1 1, PUSH1 0, SSTORE, STOP — writes slot 0 := 1.
const WRITER: Address = address!("00000000000000000000000000000000000000BB");
const WRITER_CODE: [u8; 6] = [0x60, 0x01, 0x60, 0x00, 0x55, 0x00];
/// PUSH20 SUBJECT, BALANCE, STOP — a pure BALANCE read of a third-party
/// account (the class the P2 Accumulator runtime guard cares about).
const BAL_READER: Address = address!("00000000000000000000000000000000000000CC");
const SUBJECT: Address = address!("00000000000000000000000000000000000000DD");
fn bal_reader_code() -> Vec<u8> {
    let mut code = vec![0x73];
    code.extend_from_slice(SUBJECT.as_slice());
    code.extend_from_slice(&[0x31, 0x00]);
    code
}

fn signer() -> PrivateKeySigner {
    // Anvil dev key #0 — public, dev only.
    "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
        .parse()
        .unwrap()
}

fn call(nonce: u64, to: Address) -> TxEnvelope {
    let s = signer();
    let mut tx = TxLegacy {
        chain_id: Some(CHAIN_ID),
        nonce,
        gas_price: 1_000_000_000,
        gas_limit: 100_000,
        to: TxKind::Call(to),
        value: U256::ZERO,
        input: Default::default(),
    };
    let sig = s.sign_transaction_sync(&mut tx).unwrap();
    let env = alloy_consensus::TxEnvelope::Legacy(tx.into_signed(sig));
    let mut raw = Vec::new();
    env.encode_2718(&mut raw);
    TxEnvelope {
        correlation_id: 0,
        raw_tx: bytes::Bytes::from(raw),
        sender: s.address(),
        tx_hash: *env.tx_hash(),
    }
}

fn db() -> MockStateDatabase {
    let reader_hash = keccak256(READER_CODE);
    let writer_hash = keccak256(WRITER_CODE);
    let bal_reader_hash = keccak256(bal_reader_code());
    MockStateDatabase::builder()
        .account(
            signer().address(),
            U256::from(1000u64) * U256::from(10u64).pow(U256::from(18)),
            0,
            B256::ZERO,
        )
        .account(READER, U256::ZERO, 1, reader_hash)
        .code(reader_hash, READER_CODE.to_vec().into())
        .storage(READER, B256::ZERO, U256::from(42u64))
        .account(WRITER, U256::ZERO, 1, writer_hash)
        .code(writer_hash, WRITER_CODE.to_vec().into())
        .storage(WRITER, B256::ZERO, U256::from(42u64))
        .account(BAL_READER, U256::ZERO, 1, bal_reader_hash)
        .code(bal_reader_hash, bal_reader_code().into())
        .account(SUBJECT, U256::from(7u64), 0, B256::ZERO)
        .build()
}

fn env() -> ExecEnv {
    ExecEnv {
        chain_id: CHAIN_ID,
        block_number: 1,
        l2_timestamp: 0,
    }
}

#[test]
fn sload_only_call_lands_in_reads() {
    let db = db();
    let mut scope = Executor::new(&db, None, env()).unwrap();
    let mut touches = TouchSet::default();
    let (receipt, _ws) = scope
        .execute_tx(
            TxIndex(0),
            BPosition::from_index(0),
            &call(0, READER),
            0,
            0,
            None,
            Some(&mut touches),
        )
        .unwrap();
    assert!(receipt.status, "setup: call must succeed");
    assert!(
        touches.slot_reads.contains(&(READER, B256::ZERO)),
        "SLOAD-only slot must be captured as a read: {touches:?}"
    );
    // EIP-161: even a zero-value CALL touches its recipient, so the CALL
    // target is NOT a pure account read (see the TouchSet doc).
    assert!(!touches.account_reads.contains(&READER));
    // The sender is written (nonce + gas), never a pure read.
    assert!(!touches.account_reads.contains(&signer().address()));
}

#[test]
fn balance_subject_lands_in_account_reads() {
    let db = db();
    let mut scope = Executor::new(&db, None, env()).unwrap();
    let mut touches = TouchSet::default();
    let (receipt, _ws) = scope
        .execute_tx(
            TxIndex(0),
            BPosition::from_index(0),
            &call(0, BAL_READER),
            0,
            0,
            None,
            Some(&mut touches),
        )
        .unwrap();
    assert!(receipt.status, "setup: call must succeed");
    assert!(
        touches.account_reads.contains(&SUBJECT),
        "BALANCE subject (loaded, never touched) must be captured: {touches:?}"
    );
}

#[test]
fn sstore_call_is_a_write_not_a_read() {
    let db = db();
    let mut scope = Executor::new(&db, None, env()).unwrap();
    let mut touches = TouchSet::default();
    let (receipt, ws) = scope
        .execute_tx(
            TxIndex(0),
            BPosition::from_index(0),
            &call(0, WRITER),
            0,
            0,
            None,
            Some(&mut touches),
        )
        .unwrap();
    assert!(receipt.status, "setup: call must succeed");
    assert!(
        !touches.slot_reads.contains(&(WRITER, B256::ZERO)),
        "written slot must not appear as a read: {touches:?}"
    );
    assert!(
        !touches.account_reads.contains(&WRITER),
        "storage-touched account must not appear as a pure read: {touches:?}"
    );
    assert!(
        ws.storage
            .iter()
            .any(|((a, k), v)| *a == WRITER && *k == B256::ZERO && *v == U256::from(1u64)),
        "the SSTORE must land in the WriteSet: {ws:?}"
    );
}

#[test]
fn touches_none_is_a_no_op() {
    // The default path (every caller except the shadow-enabled executor)
    // passes `None` and must behave identically.
    let db = db();
    let mut scope = Executor::new(&db, None, env()).unwrap();
    let (receipt, _) = scope
        .execute_tx(
            TxIndex(0),
            BPosition::from_index(0),
            &call(0, READER),
            0,
            0,
            None,
            None,
        )
        .unwrap();
    assert!(receipt.status);
}
