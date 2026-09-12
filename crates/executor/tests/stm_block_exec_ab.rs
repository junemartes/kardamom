//! Merge gate for `--parallel-execution`. The Block-STM
//! block-at-a-time strategy must be byte-identical to the
//! sequential capture driver: receipts, delta, and the published BAL's
//! RLP, on blocks with real fees, deposits interleaved between tx runs,
//! and an invalid skip. The validator's live cross-check fail-stops on
//! any drift. This test is the offline form of that gate.
//!
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    reason = "indices and gas values here are bounded by the small fixed test blocks, never near a truncation boundary"
)]

use std::num::NonZeroUsize;

use alloy_primitives::{Address, B256, U256, address};
use alloy_rlp::Encodable;
use alloy_signer_local::PrivateKeySigner;
use revm::primitives::KECCAK_EMPTY;

use kardamom_engine::actor::fixtures::LegacyTx;
use kardamom_engine::actor::{BlockExecOutput, BlockExecStrategy, BufferedRecord};
use kardamom_engine::{
    BPosition, MockStateDatabase, TxEnvelope as KtTxEnvelope, TxIndex, block_env::ExecEnv,
};
use kardamom_executor::parallel::{StmBlockExec, StmExecConfig};

/// Real fees: a zero gas price would fully hide fee-sink attribution
/// bugs (the sink is the one account STM tracks specially).
const REAL_GAS_PRICE: u128 = 1_000_000_000;

fn tx_record(
    signer: &PrivateKeySigner,
    to: Address,
    nonce: u64,
    value: u64,
    i: u64,
) -> BufferedRecord {
    let envelope = LegacyTx {
        chain_id: 1,
        to,
        nonce,
        value,
        gas_limit: 100_000,
        gas_price: REAL_GAS_PRICE,
    }
    .sign(signer);
    BufferedRecord::Tx {
        tx_idx: TxIndex(i),
        envelope: KtTxEnvelope {
            correlation_id: i,
            ..envelope
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
            input: bytes::Bytes::default(),
        },
        position: BPosition {
            term_id: 0,
            term_offset: (i * 64) as i32,
        },
    }
}

/// One sender's running nonce and global record index `i`, used to
/// build a contiguous run of transfer records.
struct SenderRun<'a> {
    signer: &'a PrivateKeySigner,
    to: Address,
    nonce: u64,
    i: u64,
}

impl<'a> SenderRun<'a> {
    fn new(signer: &'a PrivateKeySigner, to: Address, nonce: u64, i: u64) -> Self {
        Self {
            signer,
            to,
            nonce,
            i,
        }
    }

    /// One plain transfer at the current nonce and index, value
    /// `value_base + i`. Advances both `nonce` and `i`.
    fn next_record(&mut self, value_base: u64) -> BufferedRecord {
        let rec = tx_record(
            self.signer,
            self.to,
            self.nonce,
            value_base + self.i,
            self.i,
        );
        self.nonce += 1;
        self.i += 1;
        rec
    }

    /// One record of an interleaved run, at loop offset `k` (0..7). `k
    /// == 3` is a deterministic invalid skip (a nonce far ahead of
    /// `nonce`), which does not advance `nonce`; every other `k` is a
    /// normal transfer that does. `i` always advances.
    fn next_interleaved(&mut self, k: u64) -> BufferedRecord {
        let rec = if k == 3 {
            tx_record(self.signer, self.to, 999, 1, self.i)
        } else {
            let rec = tx_record(self.signer, self.to, self.nonce, 200 + self.i, self.i);
            self.nonce += 1;
            rec
        };
        self.i += 1;
        rec
    }
}

/// Spawn the Block-STM strategy at `workers`, run it over `records`,
/// and assert it is byte-identical to the sequential capture driver's
/// `seq` outcome: receipts, delta, and the published BAL RLP.
fn assert_strategy_matches(
    snap: &MockStateDatabase,
    records: &[BufferedRecord],
    env: ExecEnv,
    seq: &BlockExecOutput,
    workers: NonZeroUsize,
) {
    let strategy = StmBlockExec::<MockStateDatabase>::spawn(StmExecConfig {
        workers,
        pin_cores: Vec::new(),
        keep_hot: false,
    });
    let stm = strategy
        .execute_block(snap, None, records, env, 1)
        .expect("stm strategy");

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
    let mut alice_run = SenderRun::new(&alice, to, 0, 0);
    (0..5).for_each(|_| records.push(alice_run.next_record(100)));

    records.push(deposit_record(5 * 10u128.pow(17), dep_to, alice_run.i));
    let mut bob_run = SenderRun::new(&bob, to, 0, alice_run.i + 1);
    let base_i = bob_run.i;
    (0..7u64).for_each(|k| records.push(bob_run.next_interleaved(k)));

    records.push(deposit_record(3 * 10u128.pow(17), dep_to, base_i + 7));
    alice_run.i = base_i + 8;
    (0..6).for_each(|_| records.push(alice_run.next_record(300)));

    let env = ExecEnv {
        chain_id: 1,
        block_number: 1,
        l2_timestamp: 1_700_000_000,
    };

    // A: the sequential capture driver, the streaming path's semantics.
    let seq = kardamom_engine::stateless::execute_block_capture(&snap, None, &records, env)
        .expect("sequential");

    // B: the Block-STM strategy, at each worker count (fresh pool per
    // count), must match A byte-for-byte.
    for workers in [1usize, 4, 8].map(|n| NonZeroUsize::new(n).expect("worker counts are non-zero"))
    {
        assert_strategy_matches(&snap, &records, env, &seq, workers);
    }
}
