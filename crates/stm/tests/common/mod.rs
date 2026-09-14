#![allow(
    dead_code,
    reason = "each test binary uses a subset of the shared fixtures"
)]
#![allow(
    clippy::cast_possible_truncation,
    reason = "fixture indices (signer count, tx count) stay far below u8/u32::MAX in every test"
)]
//! Invariant #1: receipts and the delta must
//! be byte-identical to sequential execution. This means the same
//! write-set hashes (accumulator fixup included), the same cumulative
//! gas, and the same logs, regardless of schedule quality, worker count,
//! or interleaving. Prediction quality may only ever cost throughput
//! (fallback), never bytes.

use alloy_primitives::{Address, B256, Bytes as AlloyBytes, TxKind, U256, keccak256};
use alloy_signer_local::PrivateKeySigner;
use kardamom_exec_core::block_env::ExecEnv;
use kardamom_exec_core::delta::PendingDelta;
use kardamom_exec_core::exec_types::TxIndex;
use kardamom_exec_core::state::MockStateDatabase;
use kardamom_footprint::classifier::Stats;
use kardamom_footprint::{Cell, TxObs};
use kardamom_test_support::{LegacyTx, seeded_signer};
use kardamom_types::{BPosition, TxEnvelope};

pub(crate) const CHAIN_ID: u64 = 412_346;

/// A worker or shard count for a fixture. Every literal these tests use
/// is non-zero, so a bad literal panics loudly at the call site instead
/// of clamping into `1`.
pub(crate) fn nz(n: usize) -> std::num::NonZeroUsize {
    std::num::NonZeroUsize::new(n).expect("fixture worker count is never 0")
}
/// SLOAD s0, PUSH1 1, ADD, PUSH1 0, SSTORE, STOP: a read-modify-write
/// counter. The result depends on execution order, so any ordering bug
/// changes the bytes.
pub(crate) const COUNTER: Address = Address::with_last_byte(0xC0);
pub(crate) const COUNTER_CODE: [u8; 10] =
    [0x60, 0x00, 0x54, 0x60, 0x01, 0x01, 0x60, 0x00, 0x55, 0x00];
pub(crate) const COUNTER_SEL: [u8; 4] = [0xAA, 0xBB, 0xCC, 0xDD];

/// `n` deterministic dev keys, seeded 1 through `n`. Test-only.
pub(crate) fn signers(n: usize) -> Vec<PrivateKeySigner> {
    (1..=n as u64).map(seeded_signer).collect()
}

/// A signed call or contract creation on [`CHAIN_ID`]. The gas price is
/// nonzero, so every tx credits the fee sink.
pub(crate) fn tx(
    signer: &PrivateKeySigner,
    nonce: u64,
    to: TxKind,
    value: u64,
    input: &[u8],
) -> TxEnvelope {
    let legacy = LegacyTx {
        chain_id: CHAIN_ID,
        to: to.to().copied().unwrap_or(Address::ZERO),
        nonce,
        value,
        gas_limit: 500_000,
        gas_price: 1_000_000_000,
        input: AlloyBytes::copy_from_slice(input),
        ..Default::default()
    };
    match to {
        TxKind::Call(_) => legacy.sign(signer),
        TxKind::Create => legacy.sign_create(signer),
    }
}

/// Feed every record in `recs` into `sess`, panicking on the first
/// rejection: the "for record, push, unwrap" loop every pipelined test
/// in this crate repeats around its own `BlockSession`.
pub(crate) fn feed<S: kardamom_types::StateDatabase + Sync>(
    sess: &mut kardamom_stm::execute::BlockSession<'_, '_, S>,
    recs: &[(TxIndex, BPosition, TxEnvelope)],
) {
    for (t, p, e) in recs {
        sess.push_tx(*t, *p, e.clone()).unwrap();
    }
}

/// A fixed interleaved-transfer fixture: same-sender chains and cross
/// transfers among the first four of `signers`, with value flows that
/// depend on order within a chain.
pub(crate) fn transfer_block(signers: &[PrivateKeySigner]) -> Vec<TxEnvelope> {
    vec![
        tx(&signers[0], 0, TxKind::Call(signers[1].address()), 500, &[]),
        tx(&signers[1], 0, TxKind::Call(signers[2].address()), 300, &[]),
        tx(&signers[2], 0, TxKind::Call(signers[3].address()), 200, &[]),
        tx(&signers[0], 1, TxKind::Call(signers[2].address()), 100, &[]),
        tx(&signers[3], 0, TxKind::Call(signers[0].address()), 50, &[]),
        tx(&signers[1], 1, TxKind::Call(signers[3].address()), 25, &[]),
    ]
}

/// One round of COUNTER calls, one per signer in `signers`, all at
/// nonce `nonce`.
pub(crate) fn counter_block(signers: &[PrivateKeySigner], nonce: u64) -> Vec<TxEnvelope> {
    signers
        .iter()
        .map(|s| tx(s, nonce, TxKind::Call(COUNTER), 0, &COUNTER_SEL))
        .collect()
}

/// A COUNTER chain: `rounds` rounds of one increment per signer in
/// `signers`, interleaved round-major (every signer's round-`n` tx
/// before any round-`n+1` tx), so the whole chain and every signer's
/// own sub-chain are both in canonical order.
pub(crate) fn counter_chain(signers: &[PrivateKeySigner], rounds: u64) -> Vec<TxEnvelope> {
    (0..rounds)
        .flat_map(|n| signers.iter().map(move |s| (s, n)).collect::<Vec<_>>())
        .map(|(s, n)| tx(s, n, TxKind::Call(COUNTER), 0, &COUNTER_SEL))
        .collect()
}

pub(crate) fn db(signers: &[PrivateKeySigner]) -> MockStateDatabase {
    let counter_hash = keccak256(COUNTER_CODE);
    let mut b = MockStateDatabase::builder()
        .account(COUNTER, U256::ZERO, 1, counter_hash)
        .code(counter_hash, COUNTER_CODE.to_vec().into());
    for s in signers {
        b = b.account(s.address(), U256::from(10u128.pow(18)), 0, B256::ZERO);
    }
    b.build()
}

pub(crate) fn env() -> ExecEnv {
    ExecEnv {
        chain_id: CHAIN_ID,
        block_number: 1,
        l2_timestamp: 1_700_000_000,
    }
}

/// [`env`] at a chosen block number, for a multi-block test sequence.
pub(crate) fn env_at(block_number: u64) -> ExecEnv {
    ExecEnv {
        block_number,
        ..env()
    }
}

pub(crate) fn records(envs: Vec<TxEnvelope>) -> Vec<(TxIndex, BPosition, TxEnvelope)> {
    envs.into_iter()
        .enumerate()
        .map(|(i, e)| (TxIndex(i as u64), BPosition::from_index(i as u64), e))
        .collect()
}

/// Two deltas' accounts, storage, and code, field by field: one
/// `assert_eq!` failure names the exact table that diverged, instead
/// of one opaque `PendingDelta != PendingDelta` from comparing the
/// structs whole.
pub(crate) fn assert_delta_eq(expected: &PendingDelta, actual: &PendingDelta, label: &str) {
    assert_eq!(
        expected.accounts, actual.accounts,
        "{label}: delta accounts must match"
    );
    assert_eq!(
        expected.storage, actual.storage,
        "{label}: delta storage must match"
    );
    assert_eq!(expected.code, actual.code, "{label}: delta code must match");
}

/// Run one block through a 4-worker pool with the default stats and
/// scheduler: the minimal-configuration path several scheduler tests
/// share.
///
/// # Panics
/// Panics if the block fails to drain.
pub(crate) fn run_pool(
    database: &MockStateDatabase,
    recs: &[(TxIndex, BPosition, TxEnvelope)],
) -> kardamom_stm::execute::StmOutcome {
    kardamom_stm::execute::with_pool(
        kardamom_stm::execute::PoolConfig {
            workers: nz(4),
            prune_batch: nz(8),
            ..Default::default()
        },
        |pool| {
            pool.run_block(
                vec![database.clone(); 4],
                PendingDelta::new(),
                env(),
                recs,
                &Stats::default(),
            )
            .expect("block must drain")
        },
    )
}

/// Hunt for a wound: races are timing-dependent, and a fast machine
/// may win every one, so repeat `attempt` (which asserts the protocol
/// on every repetition, and returns whether that repetition wounded)
/// until a wound fires, `extra` further repetitions run past it (a
/// "shaped" run once, then a normal-path rep or two more), or `max` is
/// reached. Returns `(wounded_reps, attempts_run)`; a caller logs both,
/// but does not assert on `wounded_reps` — a fast host may legitimately
/// win every race.
pub(crate) fn hunt_wounds(
    max: usize,
    extra: usize,
    mut attempt: impl FnMut(usize) -> bool,
) -> (usize, usize) {
    let outcome = (0..max).try_fold((0usize, 0usize), |(wounded, _), rep| {
        let wounded = wounded + usize::from(attempt(rep));
        let attempts = rep + 1;
        if wounded > 0 && rep >= extra {
            std::ops::ControlFlow::Break((wounded, attempts))
        } else {
            std::ops::ControlFlow::Continue((wounded, attempts))
        }
    });
    match outcome {
        std::ops::ControlFlow::Continue(v) | std::ops::ControlFlow::Break(v) => v,
    }
}

/// Every `(a, b)` pair from two fixed lists, flattened so a caller
/// walks one `for` loop instead of nesting two. Used both for
/// `(shards, workers)` sweeps and for `(workers, rep)` sweeps.
pub(crate) fn shard_worker_pairs<const N: usize, const M: usize>(
    a: [usize; N],
    b: [usize; M],
) -> impl Iterator<Item = (usize, usize)> {
    a.into_iter()
        .flat_map(move |x| b.into_iter().map(move |y| (x, y)))
}

pub(crate) fn assert_identical(
    seq: &(Vec<kardamom_types::Receipt>, PendingDelta),
    stm_receipts: &[kardamom_types::Receipt],
    stm_delta: &PendingDelta,
    label: &str,
) {
    assert_eq!(
        seq.0, stm_receipts,
        "{label}: receipts must be byte-identical"
    );
    assert_delta_eq(&seq.1, stm_delta, label);
}

/// Stats that know the counter selector writes the fixed slot 0: the
/// trained, predicted-conflict path. Expect a chain, and no fallback.
pub(crate) fn counter_stats() -> Stats {
    let obs: Vec<TxObs> = (0..4)
        .map(|i| TxObs {
            index: i,
            block: 1,
            sender: Address::with_last_byte(i as u8 + 0x10),
            to: Some(COUNTER),
            selector: Some(COUNTER_SEL),
            args: Vec::new(),
            gas: 30_000,
            has_value: false,
            reads: vec![Cell::Slot(COUNTER, B256::ZERO)],
            writes: vec![Cell::Slot(COUNTER, B256::ZERO)],
        })
        .collect();
    Stats::learn(&obs)
}

/// Stats that claim every sender's counter call writes a
/// sender-derived slot (not the real, shared slot 0): a lie that makes
/// wounds fire across repetitions, so the wound-and-recover leg is
/// tested for real, not just in theory.
pub(crate) fn lying_stats() -> Stats {
    let obs: Vec<TxObs> = (0..4)
        .map(|i| {
            let sender = Address::with_last_byte(i as u8 + 0x10);
            let mut buf = [0u8; 64];
            buf[..32].copy_from_slice(&U256::from_be_slice(sender.as_slice()).to_be_bytes::<32>());
            buf[32..].copy_from_slice(&U256::from(3u8).to_be_bytes::<32>());
            TxObs {
                index: i,
                block: 1,
                sender,
                to: Some(COUNTER),
                selector: Some(COUNTER_SEL),
                args: Vec::new(),
                gas: 30_000,
                has_value: false,
                reads: Vec::new(),
                writes: vec![Cell::Slot(COUNTER, keccak256(buf))],
            }
        })
        .collect();
    Stats::learn(&obs)
}
