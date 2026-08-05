//! Differential test: actor's receipt for each tx must match a naïve
//! single-threaded `revm` loop's receipt for the same tx.
//!
//! v0 corpus: transfers, a contract `SSTORE`, a revert. Mainnet-vector
//! corpus is a v1 follow-up.
//!
//! Post-S4-arch-update wiring: M=1 tx_data + 1 tx_ordering; the demux
//! doesn't affect determinism but the public Executor::run signature
//! changed.

use std::thread;
use std::time::Duration;

use alloy_consensus::{SignableTransaction, TxLegacy};
use alloy_eips::eip2718::Decodable2718;
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{
    Address, Bytes as AlloyBytes, TxKind as APTxKind, U256, address, keccak256,
};
use alloy_signer_local::PrivateKeySigner;
use bytes::Bytes;
use crossbeam_channel::{Receiver, Sender, bounded};
use revm::context::result::ExecutionResult;
use revm::context::{BlockEnv, CfgEnv, TxEnv};
use revm::database::CacheDB;
use revm::primitives::{KECCAK_EMPTY, TxKind};
use revm::state::Bytecode;
use revm::{Context, ExecuteCommitEvm, MainBuilder, MainContext};

use kardamom_executor::executor::SnapshotRef;
use kardamom_executor::{
    BPosition, BlockBoundaryStart, CMessage, Executor, ExecutorConfig, ExecutorError,
    MockStateDatabase, MutatingSnapshotSource, StateWriterSignal, TxDataSubscription,
    TxEnvelope as KtTxEnvelope, TxOrderingMessage, TxOrderingSubscription, TxReceiptsPublication,
    TxRef, WriterApplyingQueue,
};

// Minimal: PUSH1 0x42; PUSH1 0x00; SSTORE; STOP
const SSTORE_42_AT_0: [u8; 6] = [0x60, 0x42, 0x60, 0x00, 0x55, 0x00];
// PUSH1 0x00; PUSH1 0x00; REVERT
const REVERT_CODE: [u8; 5] = [0x60, 0x00, 0x60, 0x00, 0xfd];

struct ChanASub {
    sequencer_id: u8,
    rx: Receiver<(BPosition, KtTxEnvelope)>,
}
impl TxDataSubscription for ChanASub {
    fn sequencer_id(&self) -> u8 {
        self.sequencer_id
    }
    fn next(&mut self) -> Result<(kardamom_types::TxDataLoc, KtTxEnvelope), ExecutorError> {
        self.rx
            .recv()
            .map(|(pos, env)| (kardamom_types::TxDataLoc::new(0, pos), env))
            .map_err(|_| ExecutorError::TxDataClosed {
                sequencer_id: self.sequencer_id,
            })
    }
}
struct ChanBSub(Receiver<(BPosition, TxOrderingMessage)>);
impl TxOrderingSubscription for ChanBSub {
    fn next(&mut self) -> Result<(BPosition, TxOrderingMessage), ExecutorError> {
        self.0.recv().map_err(|_| ExecutorError::TxOrderingClosed)
    }
}
struct ChanCPub(Sender<CMessage>);
impl TxReceiptsPublication for ChanCPub {
    fn publish(&mut self, m: CMessage) -> Result<(), ExecutorError> {
        self.0.send(m).map_err(|_| ExecutorError::TxReceiptsClosed)
    }
}
struct Imm;
impl StateWriterSignal for Imm {
    fn committed(&mut self) -> Result<u64, ExecutorError> {
        Ok(u64::MAX)
    }
    fn wait_committed(&mut self, b: u64) -> Result<u64, ExecutorError> {
        Ok(b)
    }
}

fn bpos(off: i32) -> BPosition {
    BPosition {
        term_id: 0,
        term_offset: off,
    }
}

/// Build a proxy-style `kardamom_types::TxEnvelope` (raw_tx, sender, tx_hash
/// populated). The naïve reference decodes back to alloy for revm.
fn legacy(
    signer: &PrivateKeySigner,
    to: APTxKind,
    nonce: u64,
    value: u64,
    data: AlloyBytes,
    gas: u64,
) -> KtTxEnvelope {
    let mut tx = TxLegacy {
        chain_id: Some(1),
        nonce,
        gas_price: 0,
        gas_limit: gas,
        to,
        value: U256::from(value),
        input: data,
    };
    let sig = signer.sign_transaction_sync(&mut tx).unwrap();
    let alloy_env: alloy_consensus::TxEnvelope = tx.into_signed(sig).into();
    let raw_tx = Bytes::from(alloy_env.encoded_2718());
    let tx_hash = keccak256(&raw_tx);
    KtTxEnvelope {
        correlation_id: 0,
        raw_tx,
        sender: signer.address(),
        tx_hash,
    }
}

fn naive_reference(snap: MockStateDatabase, txs: &[(KtTxEnvelope, Address)]) -> Vec<(bool, u64)> {
    use alloy_consensus::Transaction;
    let snap_ref = SnapshotRef { inner: &snap };
    let mut cache: CacheDB<SnapshotRef<'_, MockStateDatabase>> = CacheDB::new(snap_ref);
    let mut out = Vec::new();
    for (kt_env, signer) in txs {
        let mut slice: &[u8] = kt_env.raw_tx.as_ref();
        let env = alloy_consensus::TxEnvelope::decode_2718(&mut slice).expect("decode raw_tx");
        let tx_env = TxEnv {
            caller: *signer,
            chain_id: env.chain_id(),
            nonce: env.nonce(),
            gas_limit: env.gas_limit(),
            value: env.value(),
            data: env.input().clone(),
            kind: match env.to() {
                Some(a) => TxKind::Call(a),
                None => TxKind::Create,
            },
            gas_price: env.gas_price().unwrap_or_else(|| env.max_fee_per_gas()),
            ..Default::default()
        };
        #[allow(clippy::field_reassign_with_default)]
        let cfg: CfgEnv = {
            let mut c = CfgEnv::default();
            c.chain_id = 1;
            c
        };
        let blk = BlockEnv {
            number: U256::from(1u64),
            timestamp: U256::from(1_700_000_000u64),
            gas_limit: 30_000_000,
            basefee: 0,
            prevrandao: Some(Default::default()),
            ..Default::default()
        };
        let mut evm = Context::mainnet()
            .with_db(&mut cache)
            .with_block(blk)
            .with_cfg(cfg)
            .build_mainnet();
        let r = evm.transact_commit(tx_env).expect("commit");
        let gas_used = r.gas().tx_gas_used();
        let ok = matches!(r, ExecutionResult::Success { .. });
        out.push((ok, gas_used));
    }
    out
}

#[test]
fn actor_receipts_match_naive_reference() {
    let signer = PrivateKeySigner::random();
    let from = signer.address();
    let to = address!("00000000000000000000000000000000000ABCDE");
    let sstore_addr = address!("00000000000000000000000000000000000ABC55");
    let revert_addr = address!("00000000000000000000000000000000000ABCFD");

    let sstore_code = AlloyBytes::from_static(&SSTORE_42_AT_0);
    let revert_code = AlloyBytes::from_static(&REVERT_CODE);
    let sstore_hash = Bytecode::new_raw(sstore_code.clone()).hash_slow();
    let revert_hash = Bytecode::new_raw(revert_code.clone()).hash_slow();

    // Reference fixture: independent snapshot the naive loop uses.
    let snap_ref = MockStateDatabase::builder()
        .account(from, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
        .account(sstore_addr, U256::ZERO, 1, sstore_hash)
        .account(revert_addr, U256::ZERO, 1, revert_hash)
        .code(sstore_hash, Bytes::copy_from_slice(sstore_code.as_ref()))
        .code(revert_hash, Bytes::copy_from_slice(revert_code.as_ref()))
        .build();
    // Actor fixture: separate snapshot so the in-place CacheDB the
    // reference path mutates can't bleed into the actor's reads.
    let snap_actor = MockStateDatabase::builder()
        .account(from, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
        .account(sstore_addr, U256::ZERO, 1, sstore_hash)
        .account(revert_addr, U256::ZERO, 1, revert_hash)
        .code(sstore_hash, Bytes::copy_from_slice(sstore_code.as_ref()))
        .code(revert_hash, Bytes::copy_from_slice(revert_code.as_ref()))
        .build();

    let txs: [KtTxEnvelope; 3] = [
        legacy(
            &signer,
            APTxKind::Call(to),
            0,
            10,
            AlloyBytes::new(),
            21_000,
        ),
        legacy(
            &signer,
            APTxKind::Call(sstore_addr),
            1,
            0,
            AlloyBytes::new(),
            100_000,
        ),
        legacy(
            &signer,
            APTxKind::Call(revert_addr),
            2,
            0,
            AlloyBytes::new(),
            100_000,
        ),
    ];
    let pairs: Vec<(KtTxEnvelope, Address)> = txs.iter().cloned().map(|t| (t, from)).collect();

    let reference = naive_reference(snap_ref, &pairs);

    // Now drive the actor.
    let (a_tx, a_rx) = bounded::<(BPosition, KtTxEnvelope)>(8);
    let (b_tx, b_rx) = bounded::<(BPosition, TxOrderingMessage)>(8);
    let (c_tx, c_rx) = bounded::<CMessage>(8);
    for (i, (env, _sg)) in pairs.iter().enumerate() {
        let tx_data_position = bpos((i as i32) * 200);
        let tx_hash = env.tx_hash;
        a_tx.send((tx_data_position, env.clone())).unwrap();
        b_tx.send((
            bpos(i as i32),
            TxOrderingMessage::TxRef(TxRef::new(tx_hash, 0, tx_data_position, 0)),
        ))
        .unwrap();
    }
    b_tx.send((
        bpos(pairs.len() as i32),
        TxOrderingMessage::BoundaryStart(BlockBoundaryStart {
            block_number: 1,
            // end_tx_idx = cumulative COUNT of canonical records (= number of
            // txs applied), encoded via bpos (== BPosition::from_index here).
            end_tx_idx: bpos(pairs.len() as i32),
            l2_timestamp: 1_700_000_000,
            l1_origin: 0,
        }),
    ))
    .unwrap();
    drop(a_tx);
    drop(b_tx);

    let writer_q = WriterApplyingQueue::new(snap_actor.clone());
    let snapshots = MutatingSnapshotSource(snap_actor);
    let a_subs: Vec<Box<dyn TxDataSubscription>> = vec![Box::new(ChanASub {
        sequencer_id: 0,
        rx: a_rx,
    })];
    let b_sub: Box<dyn TxOrderingSubscription> = Box::new(ChanBSub(b_rx));
    let h = thread::spawn(move || {
        Executor::run(
            ExecutorConfig {
                chain_id: 1,
                receipt_queue_depth: 8,
                ..Default::default()
            },
            a_subs,
            b_sub,
            ChanCPub(c_tx),
            snapshots,
            Imm,
            writer_q,
            0,
            None,
            None,
            // Whole-block exec strategy (validator parallel path).
            None,
            None,
            // The executor trusts the ordered stream (phase 2 would
            // give it its own L1 dependency); only the validator verifies.
            None,
        )
    });

    let mut actor = Vec::new();
    while let Ok(m) = c_rx.recv_timeout(Duration::from_secs(5)) {
        if let CMessage::Receipt(r) = m {
            actor.push((r.status, r.gas_used));
        }
    }
    h.join().expect("no panic").expect("ok");

    assert_eq!(actor.len(), reference.len());
    for (i, (a, r)) in actor.iter().zip(reference.iter()).enumerate() {
        assert_eq!(a, r, "diff at idx {i}: actor={a:?} reference={r:?}");
    }
}

// TODO(S4 v1): import a mainnet-style tx corpus (historical Uniswap swaps,
// USDC transfers) and re-run this assertion.
