//! Shared record builders and fixtures for the parallel-engine tests:
//! transaction, deposit, and cross-chain record builders, the sequential
//! capture path, and a small test pool.
//!
//! Test records here always run from a `u64` loop index (`i`), converted
//! into position and byte fields through [`position`] and [`fixture_byte`],
//! which panic loudly instead of truncating if a future fixture ever grows
//! past what those fields hold.

use alloy_primitives::{Address, B256, U256};
use alloy_signer_local::PrivateKeySigner;
use kardamom_engine::actor::BufferedRecord;
use kardamom_engine::block_env::ExecEnv;
use kardamom_engine::delta::PendingDelta;
use kardamom_engine::exec_types::TxIndex;
use kardamom_engine::executor::{Executor, execute_deposit_tx, execute_xchain_tx};
use kardamom_exec_core::exec_types::TxSlot;
use kardamom_exec_core::executor::XChainDelivery;
use kardamom_types::{BPosition, BlockBoundaryStart, Deposit, StateDatabase, TxEnvelope};

use crate::parallel::ClaimIndex;

/// The fixture position for loop index `i`: `term_offset` scales by 64,
/// well under `i32::MAX` for every fixture's index range.
fn position(i: u64) -> BPosition {
    BPosition {
        term_id: 0,
        term_offset: i32::try_from(i * 64).expect("fixture index"),
    }
}

/// Loop index `i` as a single byte, for a fixture id or hash seed.
fn fixture_byte(i: u64) -> u8 {
    u8::try_from(i).expect("fixture index < 256")
}

pub(crate) fn tx(
    signer: &PrivateKeySigner,
    to: Address,
    nonce: u64,
    value: u64,
    i: u64,
) -> BufferedRecord {
    let envelope = kardamom_engine::actor::fixtures::LegacyTx {
        chain_id: 1,
        to,
        nonce,
        value,
        gas_limit: 100_000,
        gas_price: 1_000_000_000,
    }
    .sign(signer);
    BufferedRecord::Tx {
        tx_idx: TxIndex(i),
        position: position(i),
        envelope: TxEnvelope {
            correlation_id: i,
            tx_hash: alloy_primitives::B256::repeat_byte(fixture_byte(i) + 1),
            ..envelope
        },
    }
}

/// A zero-value call: [`tx`] with `value = 0`. Kept so call sites read
/// clearly as calls.
pub(crate) fn call_tx(
    signer: &PrivateKeySigner,
    to: Address,
    nonce: u64,
    i: u64,
) -> BufferedRecord {
    tx(signer, to, nonce, 0, i)
}

pub(crate) fn dep(
    from: Address,
    to: Option<Address>,
    mint: u128,
    input: Vec<u8>,
    i: u64,
) -> BufferedRecord {
    BufferedRecord::Deposit {
        tx_idx: TxIndex(i),
        position: position(i),
        deposit: Deposit {
            source_hash: B256::repeat_byte(0xd0u8.wrapping_add(fixture_byte(i))),
            from,
            to,
            mint,
            value: U256::ZERO,
            gas_limit: 1_000_000,
            is_system_transaction: false,
            input: input.into(),
        },
    }
}

pub(crate) fn xchain(origin: u64, seq: u64, target: Address, i: u64) -> BufferedRecord {
    use kardamom_types::xchain;
    BufferedRecord::XChain {
        tx_idx: TxIndex(i),
        origin_chain_id: origin,
        position: position(i),
        message: Box::new(xchain::XChainMessage {
            source_hash: xchain::remote_source_hash(origin, seq),
            seq,
            origin_sender: Address::repeat_byte(0xA5),
            target,
            value: 0,
            gas_limit: 100_000,
            input: bytes::Bytes::default(),
            callback: None,
        }),
    }
}

pub(crate) fn env() -> ExecEnv {
    ExecEnv::new(
        1,
        &BlockBoundaryStart {
            block_number: 1,
            end_tx_idx: BPosition::from_index(0),
            l2_timestamp: 1_700_000_000,
            l1_origin: 0,
        },
    )
}

/// One record through the free executor path, a tx or a deposit, with a
/// fresh scope per call. This is deliberately not the exec core's
/// `execute_record_in_scope`, so the parity tests compare the shared
/// production dispatch against an independently built reference, instead
/// of against itself.
pub(crate) fn exec_record<S: StateDatabase>(
    snap: &S,
    parent: Option<&PendingDelta>,
    delta: &PendingDelta,
    rec: &BufferedRecord,
    i: u64,
    cumulative: u64,
    bal: Option<&mut revm::state::bal::Bal>,
) -> (kardamom_types::Receipt, kardamom_engine::delta::WriteSet) {
    let bal = bal.map(|b| (b, i + 1));
    let slot = |tx_idx: TxIndex, position: BPosition| TxSlot {
        tx_idx,
        tx_position: position,
        tx_index_in_block: i,
        cumulative_gas_used_before: cumulative,
    };
    match rec {
        BufferedRecord::Tx {
            tx_idx,
            envelope,
            position,
        } => Executor::execute_once(
            snap,
            parent,
            delta,
            env(),
            slot(*tx_idx, *position),
            envelope,
            bal,
        )
        .expect("seq execute"),
        BufferedRecord::Deposit {
            tx_idx,
            deposit,
            position,
        } => execute_deposit_tx(
            snap,
            parent,
            delta,
            env(),
            slot(*tx_idx, *position),
            deposit,
            bal,
        )
        .expect("seq deposit"),
        BufferedRecord::XChain {
            tx_idx,
            origin_chain_id,
            message,
            position,
        } => execute_xchain_tx(
            snap,
            parent,
            delta,
            env(),
            slot(*tx_idx, *position),
            XChainDelivery {
                origin_chain_id: *origin_chain_id,
                message,
            },
            bal,
        )
        .expect("seq xchain"),
    }
}

/// The sequential-capture fixture: run `records` in order through the
/// free executor path with Bal capture, folding writes as it goes.
/// Returns the folded delta, the sequential ground truth, and the
/// captured `Bal`, the claim source. `assert_status` also requires every
/// record to execute.
pub(crate) fn seq_capture<S: StateDatabase>(
    snap: &S,
    parent: Option<&PendingDelta>,
    records: &[BufferedRecord],
    assert_status: bool,
) -> (PendingDelta, revm::state::bal::Bal) {
    let mut acc = SeqCaptureAcc {
        delta: PendingDelta::new(),
        bal: revm::state::bal::Bal::new(),
        cumulative: 0,
    };
    for (i, rec) in records.iter().enumerate() {
        acc.apply_record(snap, parent, rec, i, assert_status);
    }
    (acc.delta, acc.bal)
}

/// The running fold [`seq_capture`] threads through its records: the
/// merged delta, the captured `Bal`, and the cumulative gas so far. The
/// loop in [`seq_capture`] stays free of a branch.
struct SeqCaptureAcc {
    delta: PendingDelta,
    bal: revm::state::bal::Bal,
    cumulative: u64,
}

impl SeqCaptureAcc {
    fn apply_record<S: StateDatabase>(
        &mut self,
        snap: &S,
        parent: Option<&PendingDelta>,
        rec: &BufferedRecord,
        i: usize,
        assert_status: bool,
    ) {
        let (r, ws) = exec_record(
            snap,
            parent,
            &self.delta,
            rec,
            i as u64,
            self.cumulative,
            Some(&mut self.bal),
        );
        if assert_status {
            assert!(r.status, "record {i} must execute");
        }
        self.cumulative = r.cumulative_gas_used;
        self.delta.apply(ws);
    }
}

/// Build a claim index by executing the block sequentially through the
/// executor's real capture path (`execute_tx` and `execute_deposit_tx`,
/// through revm `Bal`), exactly as the live executor produces claims.
/// The fixture and the producer must share code, not only the same shape.
pub(crate) fn honest_claims<S: StateDatabase>(snap: &S, records: &[BufferedRecord]) -> ClaimIndex {
    let (_, bal) = seq_capture(snap, None, records, false);
    ClaimIndex::from_alloy(&bal.into_alloy_bal())
}

pub(crate) fn seq_delta<S: StateDatabase>(snap: &S, records: &[BufferedRecord]) -> PendingDelta {
    seq_capture(snap, None, records, false).0
}

/// Each test gets one small pool: the production strategy holds a
/// persistent pool, but tests build a fresh one so each case is isolated.
pub(crate) fn test_pool() -> kardamom_stm::pool::WorkerPool {
    kardamom_stm::pool::WorkerPool::new(std::num::NonZeroUsize::new(4).expect("4 != 0"), &[])
}

/// A fixture batch size. Every caller passes a literal `> 0`.
pub(crate) fn nz(n: usize) -> crate::parallel::BatchSize {
    crate::parallel::BatchSize::new(std::num::NonZeroUsize::new(n).expect("fixture batch size"))
}

/// A fixture wire granularity. Every caller passes a literal `> 0`.
pub(crate) fn nz16(n: u16) -> std::num::NonZeroU16 {
    std::num::NonZeroU16::new(n).expect("fixture granularity")
}

/// The balance every funded fixture account starts with — enough that
/// value and gas never run out mid-test.
const FUNDED_BALANCE: u128 = 10u128.pow(18);

/// Add an account funded with [`FUNDED_BALANCE`] and no code to a builder
/// chain: the step every "funded account" fixture repeats.
pub(crate) fn fund(
    builder: kardamom_exec_core::state::MockStateDatabaseBuilder,
    addr: Address,
) -> kardamom_exec_core::state::MockStateDatabaseBuilder {
    builder.account(
        addr,
        U256::from(FUNDED_BALANCE),
        0,
        alloy_primitives::KECCAK256_EMPTY,
    )
}

/// A `MockStateDatabase` seeded with just one funded signer: the shape
/// most parallel-engine tests need.
pub(crate) fn funded(signer: &PrivateKeySigner) -> kardamom_engine::state::MockStateDatabase {
    fund(
        kardamom_exec_core::state::MockStateDatabase::builder(),
        signer.address(),
    )
    .build()
}
