//! Phase-2 stateless re-execution contract (spec: no-std-exec-core).
//!
//! Replaying a block from its captured witness ALONE must reproduce the
//! recorded execution exactly: same receipts, same delta, same post-state
//! root (via the pure trie oracle). The witness must be minimal (only
//! touched keys) and fail-closed (a tampered/incomplete witness aborts the
//! replay instead of warping it).
//!
//! The block mixes the three read/write shapes: a plain transfer (account
//! reads + fresh-account creation), a contract call (code load + storage
//! read + storage write), and a deposit (mint pre-credit + proven-absent
//! recipient) — so the witness carries every table.

use alloy_consensus::{SignableTransaction, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, B256, TxKind, U256, address, keccak256};
use alloy_signer_local::PrivateKeySigner;
use kardamom_engine::actor::BufferedRecord;
use kardamom_engine::{ExecEnv, MockStateDatabase, PendingDelta};
use kardamom_state::{AccountTrieParts, state_root, storage_root};
use kardamom_types::{BPosition, Deposit, TxEnvelope};
use kardamom_validator::witness::{capture_block_witness, reexecute_stateless};

const CHAIN_ID: u64 = 412346;
const DEAD: Address = address!("000000000000000000000000000000000000dEaD");
/// Pre-seeded contract: slot0 = SLOAD(0) + 1 (a genuine snapshot storage
/// READ feeding a WRITE), then falls off the end (STOP).
const CONTRACT: Address = address!("00000000000000000000000000000000000000Cc");
const CONTRACT_CODE: [u8; 9] = [0x60, 0x00, 0x54, 0x60, 0x01, 0x01, 0x60, 0x00, 0x55];
/// Untouched genesis account — must NOT appear in the witness.
const DECOY: Address = address!("00000000000000000000000000000000000000dd");
/// Aliased L1 sender + fresh L2 recipient for the deposit.
const DEP_FROM: Address = address!("1111111111111111111111111111111111112222");
const DEP_TO: Address = address!("00000000000000000000000000000000000000eE");

fn signer() -> PrivateKeySigner {
    // Anvil dev key #0 — public, dev only.
    "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
        .parse()
        .unwrap()
}

fn signed_tx(nonce: u64, to: Address, value: u64, gas_limit: u64) -> TxEnvelope {
    let s = signer();
    let mut tx = TxLegacy {
        chain_id: Some(CHAIN_ID),
        nonce,
        gas_price: 1_000_000_000,
        gas_limit,
        to: TxKind::Call(to),
        value: U256::from(value),
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

fn eth(n: u64) -> U256 {
    U256::from(n) * U256::from(10u64).pow(U256::from(18))
}

fn genesis() -> MockStateDatabase {
    let code_hash = keccak256(CONTRACT_CODE);
    MockStateDatabase::builder()
        // Genesis convention: EOAs seeded with code_hash = ZERO.
        .account(signer().address(), eth(1000), 0, B256::ZERO)
        .account(DECOY, eth(1000), 0, B256::ZERO)
        .account(CONTRACT, U256::ZERO, 1, code_hash)
        .code(code_hash, bytes::Bytes::from_static(&CONTRACT_CODE))
        .storage(CONTRACT, B256::ZERO, U256::from(7u64))
        .build()
}

fn records() -> Vec<BufferedRecord> {
    let pos = |off: i32| BPosition {
        term_id: 0,
        term_offset: off,
    };
    vec![
        BufferedRecord::Tx {
            tx_idx: kardamom_engine::TxIndex(0),
            envelope: signed_tx(0, DEAD, 1, 21_000),
            position: pos(0),
        },
        BufferedRecord::Tx {
            tx_idx: kardamom_engine::TxIndex(1),
            envelope: signed_tx(1, CONTRACT, 0, 80_000),
            position: pos(1),
        },
        BufferedRecord::Deposit {
            tx_idx: kardamom_engine::TxIndex(2),
            deposit: Deposit {
                source_hash: B256::repeat_byte(0x5d),
                from: DEP_FROM,
                to: Some(DEP_TO),
                mint: 5,
                value: U256::from(3u64),
                gas_limit: 21_000,
                is_system_transaction: false,
                input: bytes::Bytes::new(),
            },
            position: pos(2),
        },
    ]
}

fn env() -> ExecEnv {
    ExecEnv {
        chain_id: CHAIN_ID,
        block_number: 1,
        l2_timestamp: 1_700_000_000,
    }
}

/// Post-state root from genesis + a delta, via the pure trie oracle.
fn oracle_root(delta: &PendingDelta) -> B256 {
    // Genesis account set the mock was built from.
    let code_hash = keccak256(CONTRACT_CODE);
    let mut accounts: std::collections::BTreeMap<Address, (u64, U256, B256)> = [
        (signer().address(), (0, eth(1000), B256::ZERO)),
        (DECOY, (0, eth(1000), B256::ZERO)),
        (CONTRACT, (1, U256::ZERO, code_hash)),
    ]
    .into_iter()
    .collect();
    let mut storage: std::collections::BTreeMap<(Address, B256), U256> =
        [((CONTRACT, B256::ZERO), U256::from(7u64))]
            .into_iter()
            .collect();
    for (addr, v) in &delta.accounts {
        accounts.insert(*addr, *v);
    }
    for (k, v) in &delta.storage {
        storage.insert(*k, *v);
    }
    state_root(
        accounts
            .into_iter()
            .map(|(addr, (nonce, balance, code_hash))| {
                let slots = storage
                    .iter()
                    .filter(|((a, _), _)| *a == addr)
                    .map(|((_, k), v)| (*k, *v));
                (
                    addr,
                    AccountTrieParts {
                        nonce,
                        balance,
                        code_hash,
                        storage_root: storage_root(slots),
                    },
                )
            }),
    )
}

#[test]
fn stateless_replay_reproduces_recorded_execution() {
    let recs = records();

    // Reference execution + witness capture in one pass (the recorder is a
    // transparent decorator — asserted implicitly by the replay equality).
    let snap = genesis();
    let (reference, witness) = capture_block_witness(&snap, None, &recs, env()).unwrap();
    assert!(
        reference.receipts.iter().all(|r| r.status),
        "setup: every record must execute successfully"
    );

    // Stateless replay: witness only, no state DB.
    let stateless = reexecute_stateless(&witness, None, &recs, env()).unwrap();

    assert_eq!(reference.receipts, stateless.receipts, "receipts diverged");
    assert_eq!(
        reference.delta.accounts, stateless.delta.accounts,
        "account writes diverged"
    );
    assert_eq!(
        reference.delta.storage, stateless.delta.storage,
        "storage writes diverged"
    );
    assert_eq!(
        reference.delta.code, stateless.delta.code,
        "code writes diverged"
    );

    // Post-state roots via the pure trie oracle.
    let root_ref = oracle_root(&reference.delta);
    let root_stateless = oracle_root(&stateless.delta);
    assert_eq!(root_ref, root_stateless, "post-state roots diverged");
    assert_ne!(root_ref, kardamom_state::empty_root(), "vacuous root");

    // The contract's storage write actually happened (slot0: 7 -> 8).
    assert_eq!(
        reference
            .delta
            .storage
            .get(&(CONTRACT, B256::ZERO))
            .copied(),
        Some(U256::from(8u64)),
        "contract call did not write storage"
    );

    // Witness minimality: only touched keys — never the decoy.
    assert!(
        witness.accounts.iter().all(|a| a.address != DECOY),
        "untouched genesis account leaked into the witness"
    );
    // Proven absence is recorded for the fresh recipients.
    for fresh in [DEAD, DEP_TO, DEP_FROM] {
        assert!(
            witness
                .accounts
                .iter()
                .any(|a| a.address == fresh && !a.exists),
            "expected proven-absent witness entry for {fresh}"
        );
    }
    // The contract's code and its read slot are carried.
    assert!(
        witness
            .code
            .iter()
            .any(|c| c.code_hash == keccak256(CONTRACT_CODE))
    );
    assert!(
        witness
            .storage
            .iter()
            .any(|s| s.address == CONTRACT && s.key == B256::ZERO && s.value == U256::from(7u64))
    );

    // Digest is stable across capture repetitions (canonical ordering).
    let (_, witness2) = capture_block_witness(&genesis(), None, &recs, env()).unwrap();
    assert_eq!(witness.digest(), witness2.digest());
}

#[test]
fn incomplete_witness_fails_closed() {
    let recs = records();
    let (_, witness) = capture_block_witness(&genesis(), None, &recs, env()).unwrap();

    // Drop the sender's account entry: the replay must ERROR, not execute
    // against a defaulted account.
    let mut tampered = witness.clone();
    tampered
        .accounts
        .retain(|a| a.address != signer().address());
    let err = reexecute_stateless(&tampered, None, &recs, env());
    assert!(err.is_err(), "replay over an incomplete witness must fail");

    // Drop the contract's read slot: same contract.
    let mut tampered = witness.clone();
    tampered.storage.retain(|s| s.address != CONTRACT);
    assert!(reexecute_stateless(&tampered, None, &recs, env()).is_err());

    // Drop the contract code: same contract.
    let mut tampered = witness;
    tampered.code.clear();
    assert!(reexecute_stateless(&tampered, None, &recs, env()).is_err());
}
