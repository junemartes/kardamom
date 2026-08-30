//! [`Executor`] — the per-block EVM + committed cache — plus the free
//! [`execute_tx`] compatibility wrapper and the deterministic invalid-skip
//! path (#92).

use alloy_consensus::Transaction;
use alloy_primitives::{Address, B256};
use kardamom_types::{BPosition, Receipt, StateDatabase, TxEnvelope};
use revm::context::result::ExecutionResult;
use revm::database::CacheDB;
use revm::{Context, ExecuteEvm, MainBuilder, MainContext};

use alloc::format;
use alloc::vec::Vec;

use crate::block_env::ExecEnv;
use crate::delta::{PendingDelta, WriteSet};
use crate::error::ExecutorError;
use crate::exec_types::{ReceiptStatus, TxIndex};

use super::db::{SnapshotDb, seed_cache_layer};
use super::tx_env::DecodedTx;

/// Per-tx READ-touch capture for the footprint shadow scheduler
/// (spec: block-stm-executor §P1). The block-level EIP-7928 BAL cannot
/// attribute reads per tx — a slot read after another tx wrote it leaves no
/// trace at all (revm keeps writer indexes only), and `storage_reads` is
/// block-scoped — so the shadow captures the read side here, at the same
/// point the BAL capture runs, from the same `outcome.state`. Writes need no
/// capture: the returned `WriteSet` already carries them exactly.
///
/// `account_reads` holds accounts revm loaded but never TOUCHED — the
/// BALANCE / EXTCODE* / STATICCALL / DELEGATECALL subject class. Note that
/// plain CALL targets do NOT appear: EIP-161 marks even a zero-value CALL's
/// recipient as touched (the state-clearing rule), which revm mirrors, so a
/// call target is only visible through its storage reads / `WriteSet` entry.
/// `slot_reads` holds every accessed slot whose value did not change, on
/// touched and untouched accounts alike.
#[derive(Debug, Default, Clone)]
pub struct TouchSet {
    pub account_reads: Vec<Address>,
    pub slot_reads: Vec<(Address, B256)>,
}

/// Per-BLOCK execution scope: ONE `CacheDB` (layered over
/// `parent ∘ snapshot`) that revm COMMITS into after each tx, and ONE EVM
/// instance whose tx-env is swapped per transaction.
///
/// This replaces the old per-tx construction, which DHAT measured at ~90%
/// of all execution-path allocation (421KB/tx): eight 32KB interpreter
/// stacks per tx (the EVM rebuilt per call) plus a rehash storm from
/// re-seeding the whole block delta into a fresh cache for every tx. The
/// per-tx `delta` seeding disappears entirely — the committed cache IS the
/// intra-block view; `PendingDelta` remains the boundary/BAL artifact,
/// maintained by the caller exactly as before.
///
/// The free [`execute_tx`] wrapper (one scope per call) keeps the old
/// signature for replay and tests; hot paths hold a scope per block
/// (executor) or per batch (validator).
pub struct Executor<S: StateDatabase> {
    evm: revm::handler::MainnetEvm<
        revm::context::Context<
            revm::context::BlockEnv,
            revm::context::TxEnv,
            revm::context::CfgEnv,
            CacheDB<SnapshotDb<S>>,
        >,
    >,
    env: ExecEnv,
}

impl<S: StateDatabase> Executor<S> {
    /// Build the block's scope: cache seeded with the PARENT layer only
    /// (fixed for the whole block), EVM constructed once.
    pub fn new(
        snapshot: S,
        parent: Option<&PendingDelta>,
        env: ExecEnv,
    ) -> Result<Self, ExecutorError> {
        let (block, cfg) = (env.block_env(), env.cfg_env());
        Self::new_with_envs(snapshot, parent, env, block, cfg)
    }

    /// Like [`Executor::new`], but with caller-supplied revm envs instead of
    /// the [`ExecEnv`]-derived production ones. This is the seam the EEST
    /// conformance runner uses to execute under a *fixture's* block env
    /// (coinbase, basefee, difficulty, blob params) — it tests the engine's
    /// revm integration, not kardamom's boundary derivation. Production
    /// paths use [`Executor::new`]; `env` here only feeds the metadata on
    /// skip receipts (block number).
    pub fn new_with_envs(
        snapshot: S,
        parent: Option<&PendingDelta>,
        env: ExecEnv,
        block: revm::context::BlockEnv,
        cfg: revm::context::CfgEnv,
    ) -> Result<Self, ExecutorError> {
        let cache: CacheDB<SnapshotDb<S>> = CacheDB::new(SnapshotDb { inner: snapshot });
        let evm = Context::mainnet()
            .with_db(cache)
            .with_block(block)
            .with_cfg(cfg)
            .build_mainnet();
        let mut scope = Self { evm, env };
        if let Some(layer) = parent {
            scope.seed_layer(layer)?;
        }
        Ok(scope)
    }

    /// Seed a delta layer into the block cache (later seeds overwrite).
    /// Used for the parent layer at construction and by the compatibility
    /// wrapper for a caller-maintained live delta.
    pub fn seed_layer(&mut self, layer: &PendingDelta) -> Result<(), ExecutorError> {
        let cache = revm::context_interface::ContextTr::db_mut(&mut *self.evm);
        seed_cache_layer(cache, layer).map_err(ExecutorError::State)
    }

    #[allow(clippy::too_many_arguments)] // the per-tx entry point's shape;
    // see the equivalent allow on `execute_once`.
    pub fn execute_tx(
        &mut self,
        tx_idx: TxIndex,
        tx_position: BPosition,
        inbound_envelope: &TxEnvelope,
        tx_index_in_block: u64,
        cumulative_gas_used_before: u64,
        // EIP-7928 capture: see the free `execute_tx`.
        bal: Option<(&mut revm::state::bal::Bal, u64)>,
        // Footprint-shadow read capture: see [`TouchSet`]. `None` everywhere
        // except the executor's streaming path with the shadow enabled.
        touches: Option<&mut TouchSet>,
    ) -> Result<(Receipt, WriteSet), ExecutorError> {
        // DERIVATION IS TOTAL (#92): a canonical record that is DETERMINISTICALLY
        // invalid — undecodable bytes, or a tx revm rejects at validation
        // (NonceTooLow duplicate past every dedup layer, NonceTooHigh from a
        // sealed gap, insufficient balance, …) — must NOT halt execution: every
        // replica, the recovery replay, and the validator all see the same input
        // and would all halt in lockstep, permanently (a poisoned log wedges
        // recovery replay on the same record forever). Instead it is SKIPPED with
        // a receipt: `status=false, gas_used=0` — unreachable by real execution,
        // since any executed tx (revert or halt included) charges at least
        // intrinsic gas — so the pair is the wire-visible skip marker
        // ([`kardamom_types::Receipt::is_invalid_skip`]). The skip is part of the
        // deterministic state transition (empty write set, no state change,
        // counters advance), identical across live / replay / validator re-exec.
        // Non-deterministic failures (Database errors) still fail-stop below.
        // A skip is LOUD: any occurrence means an upstream guard failed
        // (`kardamom_executor_invalid_tx_skipped_total` deserves an alert).
        let alloy_env = match DecodedTx::decode(&inbound_envelope.raw_tx, tx_idx) {
            Ok(env_) => env_,
            Err(_) => {
                return Ok(Self::skip_receipt(
                    tx_position,
                    inbound_envelope,
                    0,
                    None,
                    self.env.block_number,
                    tx_index_in_block,
                    cumulative_gas_used_before,
                ));
            }
        };
        self.execute_tx_decoded(
            tx_idx,
            tx_position,
            inbound_envelope,
            &alloy_env,
            tx_index_in_block,
            cumulative_gas_used_before,
            bal,
            touches,
        )
    }

    /// [`Self::execute_tx`] with the RLP already decoded.
    ///
    /// Decoding is ~180ns/tx and is naturally done by whoever reads the
    /// tx stream (the STM engine's `prepare` does exactly this, off the
    /// execution thread). Exposing the pre-decoded entry point lets the
    /// SEQUENTIAL path have the same benefit — and lets the A/B harness
    /// compare the two engines on equal footing instead of charging
    /// decode to one side only.
    #[allow(clippy::too_many_arguments)]
    pub fn execute_tx_decoded(
        &mut self,
        tx_idx: TxIndex,
        tx_position: BPosition,
        inbound_envelope: &TxEnvelope,
        alloy_env: &DecodedTx,
        tx_index_in_block: u64,
        cumulative_gas_used_before: u64,
        bal: Option<(&mut revm::state::bal::Bal, u64)>,
        touches: Option<&mut TouchSet>,
    ) -> Result<(Receipt, WriteSet), ExecutorError> {
        let signer = inbound_envelope.sender; // trusted from proxy; no recovery
        let nonce = alloy_env.nonce();
        let to = alloy_env.to();
        // Effective gas price mirrors the value `tx_env_from_alloy` feeds revm:
        // legacy/2930 `gas_price` when present, otherwise the 1559/4844
        // `max_fee_per_gas` cap. v0 has basefee = 0 so the cap is what's paid.
        let effective_gas_price = alloy_env
            .gas_price()
            .unwrap_or_else(|| alloy_env.max_fee_per_gas());

        let tx_env = alloy_env.tx_env(signer);
        let outcome = match self.evm.transact(tx_env) {
            Ok(o) => o,
            // Deterministic input-invalidity: every replica computes the same
            // rejection from the same (state, tx) — skip, never halt (#92).
            Err(revm::context::result::EVMError::Transaction(_)) => {
                return Ok(Self::skip_receipt(
                    tx_position,
                    inbound_envelope,
                    nonce,
                    to,
                    self.env.block_number,
                    tx_index_in_block,
                    cumulative_gas_used_before,
                ));
            }
            Err(revm::context::result::EVMError::Header(_)) => {
                return Ok(Self::skip_receipt(
                    tx_position,
                    inbound_envelope,
                    nonce,
                    to,
                    self.env.block_number,
                    tx_index_in_block,
                    cumulative_gas_used_before,
                ));
            }
            // Database / custom failures are LOCAL, not derivable from the input:
            // halting here is correct (crash recovery replays cleanly).
            Err(e) => {
                return Err(ExecutorError::Execution {
                    idx: tx_idx,
                    detail: format!("{e:?}"),
                });
            }
        };

        let gas_used = outcome.result.gas().tx_gas_used();
        let (status, logs) = match &outcome.result {
            ExecutionResult::Success { logs, .. } => (ReceiptStatus::Success, logs.clone()),
            ExecutionResult::Revert { .. } => (ReceiptStatus::Revert, Vec::new()),
            ExecutionResult::Halt { reason, .. } => {
                (ReceiptStatus::Halt(reason.clone()), Vec::new())
            }
        };

        // Build the write set from revm's per-tx EvmState. Only touched / changed
        // accounts and slots are emitted, which keeps the per-tx hash stable
        // across replicas (revm's iteration is over an AddressMap; we re-sort
        // into BTreeMap inside WriteSet via insert).
        let ws = WriteSet::from_evm_state(&outcome.state);
        if let Some((bal, bal_index)) = bal {
            for (addr, account) in outcome.state.iter() {
                bal.update_account(bal_index, *addr, account);
            }
        }
        if let Some(t) = touches {
            for (addr, account) in outcome.state.iter() {
                if !account.is_touched() {
                    t.account_reads.push(*addr);
                }
                for (key, slot) in account.storage.iter() {
                    if slot.original_value == slot.present_value {
                        t.slot_reads
                            .push((*addr, B256::from(key.to_be_bytes::<32>())));
                    }
                }
            }
        }
        // Fold this tx's writes into the block cache: later txs read them
        // directly — no per-tx re-seeding (this WAS 84% of all allocation).
        revm::DatabaseCommit::commit(
            revm::context_interface::ContextTr::db_mut(&mut *self.evm),
            outcome.state,
        );

        let write_set_hash = ws.hash();
        let wire_logs = logs.iter().map(kardamom_types::WireLog::from).collect();
        let cumulative_gas_used = cumulative_gas_used_before + gas_used;
        // Contract address is meaningful only for successful CREATE txs.
        // Failed CREATEs and any CALL tx have `contract_address = None`.
        let contract_address = if to.is_none() && status.is_success() {
            Some(signer.create(nonce))
        } else {
            None
        };

        // CRITICAL (S0): copy `tx_hash` straight from the inbound envelope —
        // DO NOT recompute via keccak256(raw_tx). The proxy (S1) is the canonical
        // hash producer.
        let receipt = Receipt {
            tx_idx: tx_position,
            tx_hash: inbound_envelope.tx_hash,
            // EIP-2718 type read off the raw envelope (legacy ⇒ 0x00).
            tx_type: kardamom_types::tx_type_of(&inbound_envelope.raw_tx),
            status: status.is_success(),
            gas_used,
            logs: wire_logs,
            write_set_hash,
            nonce,
            from: signer,
            to,
            contract_address,
            effective_gas_price,
            block_number: self.env.block_number,
            transaction_index: tx_index_in_block,
            cumulative_gas_used,
        };
        Ok((receipt, ws))
    }
}

/// Execute one tx against a snapshot + the current PendingDelta. Returns the
/// receipt plus a fresh per-tx WriteSet. The caller folds the WriteSet into
/// the PendingDelta before invoking the next tx so later txs see the writes.
///
/// `inbound_envelope: &TxEnvelope` is `kardamom_types::TxEnvelope`. Its
/// `sender` and `tx_hash` are trusted unconditionally — the proxy (S1)
/// populated them at the system boundary. The executor **never recomputes
/// `tx_hash`** (S0) and **never recovers a sender** (S0); it
/// copies both fields straight through into the outbound `Receipt`.
///
/// `tx_index_in_block` is the zero-based index within the in-flight block
/// (resets at every `BlockBoundaryStart`). `cumulative_gas_used_before` is the
/// running gas sum for txs already executed in the same block; the returned
/// receipt's `cumulative_gas_used` equals this plus the new tx's `gas_used`.
#[allow(clippy::too_many_arguments)] // 8 args is the natural shape of an
// "execute one tx" entry point — packaging them into a struct would shuffle
// the noise around without reducing it.
impl<S: StateDatabase> Executor<S> {
    /// One-shot convenience: build a throwaway per-call executor, seed the
    /// caller's live delta, execute one tx. For replay and tests — hot
    /// paths hold an [`Executor`] per block/batch instead; that is where
    /// the allocation win lives.
    #[allow(clippy::too_many_arguments)] // the one-tx entry point's shape.
    pub fn execute_once(
        snapshot: &S,
        parent: Option<&PendingDelta>,
        delta: &PendingDelta,
        env: ExecEnv,
        tx_idx: TxIndex,
        tx_position: BPosition,
        inbound_envelope: &TxEnvelope,
        tx_index_in_block: u64,
        cumulative_gas_used_before: u64,
        // EIP-7928 capture (spec: bal-attribution-parallel-validation):
        // when set, every account/slot this tx touched is recorded into
        // the block's Bal under `bal_index` (1-based tx position per
        // revm's convention) — writes as (index, value), read-only
        // accesses into storage_reads. revm classifies from
        // original-vs-present in `outcome.state`.
        bal: Option<(&mut revm::state::bal::Bal, u64)>,
    ) -> Result<(Receipt, WriteSet), ExecutorError> {
        let mut scope = Executor::new(snapshot, parent, env)?;
        scope.seed_layer(delta)?;
        scope.execute_tx(
            tx_idx,
            tx_position,
            inbound_envelope,
            tx_index_in_block,
            cumulative_gas_used_before,
            bal,
            None,
        )
    }
}

/// The deterministic SKIP receipt, as an associated constructor (no state
/// access): the Block-STM engine builds skip receipts inside its own
/// worker EVM and must keep ONE definition of the skip artifact.
impl<S: StateDatabase> Executor<S> {
    /// Build the deterministic SKIP receipt for a canonical record that is
    /// invalid at execution (#92): `status=false, gas_used=0` (the
    /// wire-visible marker — real execution always charges intrinsic
    /// gas), empty logs, EMPTY write set (`WriteSet::default().hash()` on
    /// both the live and re-exec sides), gas accounting unchanged. Loud
    /// by design: a skip existing at all means an upstream guard let an
    /// invalid record reach the canonical log.
    #[allow(clippy::too_many_arguments)]
    pub fn skip_receipt(
        tx_position: BPosition,
        inbound_envelope: &TxEnvelope,
        nonce: u64,
        to: Option<Address>,
        block_number: u64,
        tx_index_in_block: u64,
        cumulative_gas_used_before: u64,
    ) -> (Receipt, WriteSet) {
        let ws = WriteSet::default();
        let write_set_hash = ws.hash();
        let receipt = Receipt {
            tx_idx: tx_position,
            tx_hash: inbound_envelope.tx_hash,
            tx_type: kardamom_types::tx_type_of(&inbound_envelope.raw_tx),
            status: false,
            gas_used: 0,
            logs: Vec::new(),
            write_set_hash,
            nonce,
            from: inbound_envelope.sender,
            to,
            contract_address: None,
            effective_gas_price: 0,
            block_number,
            transaction_index: tx_index_in_block,
            cumulative_gas_used: cumulative_gas_used_before,
        };
        (receipt, ws)
    }

    /// [`Self::skip_receipt`] with this executor's block number.
    #[allow(clippy::too_many_arguments)]
    pub fn skip(
        &self,
        tx_position: BPosition,
        inbound_envelope: &TxEnvelope,
        nonce: u64,
        to: Option<Address>,
        tx_index_in_block: u64,
        cumulative_gas_used_before: u64,
    ) -> (Receipt, WriteSet) {
        Self::skip_receipt(
            tx_position,
            inbound_envelope,
            nonce,
            to,
            self.env.block_number,
            tx_index_in_block,
            cumulative_gas_used_before,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::test_support::{boundary, pos};
    use crate::state::MockStateDatabase;
    use alloy_consensus::{SignableTransaction, TxLegacy};
    use alloy_eips::eip2718::Encodable2718;
    use alloy_network::TxSignerSync;
    use alloy_primitives::Bytes as AlloyBytes;
    use alloy_primitives::{TxKind as APTxKind, U256, address, keccak256};
    use alloy_signer_local::PrivateKeySigner;
    use bytes::Bytes;
    use kardamom_types::TxEnvelope as KtTxEnvelope;
    use revm::primitives::KECCAK_EMPTY;

    fn signed_transfer(
        from: &PrivateKeySigner,
        to: Address,
        value: u64,
        nonce: u64,
    ) -> KtTxEnvelope {
        let mut tx = TxLegacy {
            chain_id: Some(1),
            nonce,
            gas_price: 0,
            gas_limit: 21_000,
            to: APTxKind::Call(to),
            value: U256::from(value),
            input: AlloyBytes::new(),
        };
        let sig = from.sign_transaction_sync(&mut tx).expect("sign");
        let alloy_env: alloy_consensus::TxEnvelope = tx.into_signed(sig).into();
        let raw_tx = Bytes::from(alloy_env.encoded_2718());
        let tx_hash = keccak256(&raw_tx);
        KtTxEnvelope {
            correlation_id: 0,
            raw_tx,
            sender: from.address(),
            tx_hash,
        }
    }

    // ── #92: deterministically-invalid canonical txs SKIP, never halt ──────

    #[test]
    fn nonce_too_low_skips_with_marker_receipt_and_chain_continues() {
        let signer = PrivateKeySigner::random();
        let from = signer.address();
        let to = address!("0000000000000000000000000000000000001234");
        // Sender's canonical nonce is 5: a nonce-3 tx (a duplicate past every
        // dedup layer) is deterministically invalid.
        let snap = MockStateDatabase::builder()
            .account(from, U256::from(10u128.pow(18)), 5, KECCAK_EMPTY)
            .build();
        let delta = PendingDelta::new();
        let env = ExecEnv::new(1, &boundary(1));

        let stale = signed_transfer(&signer, to, 1_000, 3);
        let (receipt, ws) = Executor::execute_once(
            &snap,
            None,
            &delta,
            env,
            TxIndex(0),
            pos(0),
            &stale,
            0,
            77,
            None,
        )
        .expect("invalid tx must SKIP, not error");
        assert!(receipt.is_invalid_skip(), "status=false, gas_used=0 marker");
        assert_eq!(receipt.tx_hash, stale.tx_hash);
        assert_eq!(receipt.nonce, 3);
        assert_eq!(
            receipt.cumulative_gas_used, 77,
            "gas accounting unchanged by a skip"
        );
        assert_eq!(
            receipt.write_set_hash,
            WriteSet::default().hash(),
            "a skip writes NOTHING (empty-set hash on every re-exec path)"
        );
        assert!(ws.accounts.is_empty() && ws.storage.is_empty() && ws.code.is_empty());

        // The chain continues: the sender's REAL next tx (nonce 5) executes.
        let env2 = ExecEnv::new(1, &boundary(1));
        let live = signed_transfer(&signer, to, 1_000, 5);
        let (r2, _) = Executor::execute_once(
            &snap,
            None,
            &delta,
            env2,
            TxIndex(1),
            pos(64),
            &live,
            1,
            77,
            None,
        )
        .expect("valid tx after a skip");
        assert!(r2.status);
        assert!(!r2.is_invalid_skip());
    }

    #[test]
    fn undecodable_raw_tx_skips_with_marker_receipt() {
        let signer = PrivateKeySigner::random();
        let snap = MockStateDatabase::builder().build();
        let delta = PendingDelta::new();
        let env = ExecEnv::new(1, &boundary(1));
        let garbage = KtTxEnvelope {
            correlation_id: 9,
            raw_tx: Bytes::from(vec![0xde, 0xad, 0xbe, 0xef]),
            sender: signer.address(),
            tx_hash: keccak256([0xde, 0xad, 0xbe, 0xef]),
        };
        let (receipt, ws) = Executor::execute_once(
            &snap,
            None,
            &delta,
            env,
            TxIndex(0),
            pos(0),
            &garbage,
            0,
            0,
            None,
        )
        .expect("undecodable bytes must SKIP, not error");
        assert!(receipt.is_invalid_skip());
        assert_eq!(receipt.nonce, 0, "nonce unknowable from undecodable bytes");
        assert_eq!(receipt.write_set_hash, WriteSet::default().hash());
        assert!(ws.accounts.is_empty());
    }

    #[test]
    fn simple_transfer_produces_write_set_and_success_receipt() {
        let signer = PrivateKeySigner::random();
        let from = signer.address();
        let to = address!("0000000000000000000000000000000000001234");

        let snap = MockStateDatabase::builder()
            .account(from, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
            .build();
        let delta = PendingDelta::new();
        let env = ExecEnv::new(1, &boundary(1));

        let env_tx = signed_transfer(&signer, to, 1_000, 0);
        let (receipt, ws) = Executor::execute_once(
            &snap,
            None,
            &delta,
            env,
            TxIndex(0),
            pos(0),
            &env_tx,
            0,
            0,
            None,
        )
        .expect("execute");

        //the receipt's tx_hash MUST equal the inbound envelope's
        // tx_hash byte-for-byte. No recomputation in the executor.
        assert_eq!(receipt.tx_hash, env_tx.tx_hash);
        assert!(receipt.status); // success = bool true (kardamom_types::Receipt)
        assert!(receipt.gas_used >= 21_000);
        // Both accounts touched: sender (balance + nonce) and recipient (balance).
        assert!(ws.account(&from).is_some());
        assert!(ws.account(&to).is_some());
        assert_eq!(ws.account(&to).unwrap().1, U256::from(1_000u64));
        // No storage or code writes for a plain transfer.
        assert!(ws.storage.is_empty());
        assert!(ws.code.is_empty());

        // RPC enrichment populated by execute_tx.
        assert_eq!(receipt.from, from);
        assert_eq!(receipt.to, Some(to));
        assert_eq!(receipt.contract_address, None);
        assert_eq!(receipt.nonce, 0);
        assert_eq!(receipt.effective_gas_price, 0); // tx built with gas_price = 0
        assert_eq!(receipt.block_number, 1);
        assert_eq!(receipt.transaction_index, 0);
        assert_eq!(receipt.cumulative_gas_used, receipt.gas_used);
    }

    #[test]
    fn second_tx_sees_first_tx_balance_via_delta() {
        let signer = PrivateKeySigner::random();
        let from = signer.address();
        let to = address!("00000000000000000000000000000000000ABCDE");

        let snap = MockStateDatabase::builder()
            .account(from, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
            .build();
        let env = ExecEnv::new(1, &boundary(1));

        let mut delta = PendingDelta::new();
        // First transfer.
        let tx1 = signed_transfer(&signer, to, 100, 0);
        let (r1, ws1) = Executor::execute_once(
            &snap,
            None,
            &delta,
            env,
            TxIndex(0),
            pos(0),
            &tx1,
            0,
            0,
            None,
        )
        .expect("execute 1");
        assert!(r1.status);
        assert_eq!(r1.tx_hash, tx1.tx_hash); // copied, not recomputed
        assert_eq!(r1.nonce, 0);
        assert_eq!(r1.transaction_index, 0);
        let gas_after_tx1 = r1.cumulative_gas_used;
        delta.apply(ws1);

        // Second transfer from the same sender; nonce must be 1, sender
        // balance must already be debited 100.
        let tx2 = signed_transfer(&signer, to, 50, 1);
        let (r2, ws2) = Executor::execute_once(
            &snap,
            None,
            &delta,
            env,
            TxIndex(1),
            pos(1),
            &tx2,
            1,
            gas_after_tx1,
            None,
        )
        .expect("execute 2");
        assert!(r2.status);
        assert_eq!(r2.tx_hash, tx2.tx_hash);
        assert_eq!(ws2.account(&to).unwrap().1, U256::from(150u64));
        assert_eq!(ws2.account(&from).unwrap().0, 2); // nonce
        // RPC enrichment: tx2 sees a higher nonce + transaction_index;
        // cumulative_gas_used accumulates across both txs in the block.
        assert_eq!(r2.nonce, 1);
        assert_eq!(r2.transaction_index, 1);
        assert_eq!(r2.cumulative_gas_used, gas_after_tx1 + r2.gas_used);
    }

    /// Regression: EIP-7928 capture must actually populate the block Bal
    /// when a Bal handle is supplied to `execute_tx` (spec phase 1). An
    /// empty BAL means the validator has nothing to verify or seed from.
    #[test]
    fn execute_tx_captures_into_the_block_bal() {
        let signer = PrivateKeySigner::random();
        let from = signer.address();
        let to = address!("0000000000000000000000000000000000005678");
        let snap = MockStateDatabase::builder()
            .account(from, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
            .build();
        let delta = PendingDelta::new();
        let env = ExecEnv::new(1, &boundary(1));
        let tx = signed_transfer(&signer, to, 1_000, 0);

        let mut bal = revm::state::bal::Bal::new();
        let (_receipt, ws) = Executor::execute_once(
            &snap,
            None,
            &delta,
            env,
            TxIndex(0),
            pos(0),
            &tx,
            0,
            0,
            Some((&mut bal, 1)),
        )
        .expect("execute");
        assert!(!ws.accounts.is_empty(), "the tx wrote accounts");

        let alloy = bal.into_alloy_bal();
        assert!(
            !alloy.is_empty(),
            "capture produced an EMPTY BAL for a tx that wrote {} accounts",
            ws.accounts.len()
        );
        // Sender and recipient must both appear with balance/nonce claims.
        let has_sender = alloy.iter().any(|a| {
            a.address == from && (!a.balance_changes.is_empty() || !a.nonce_changes.is_empty())
        });
        assert!(
            has_sender,
            "sender's balance/nonce change must be claimed: {alloy:?}"
        );
    }

    /// Production shape: the SECOND tx in a block executes against a
    /// non-empty `delta` (seeded into the CacheDB). Capture must still
    /// record it — the first live measurement produced empty BALs while
    /// deltas were 76KB/block, and empty-delta tests passed.
    #[test]
    fn execute_tx_captures_with_a_seeded_delta() {
        let signer = PrivateKeySigner::random();
        let from = signer.address();
        let to = address!("0000000000000000000000000000000000009999");
        let snap = MockStateDatabase::builder()
            .account(from, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
            .build();
        let mut delta = PendingDelta::new();
        let env = ExecEnv::new(1, &boundary(1));

        // tx1 populates the delta (as in a real block).
        let tx1 = signed_transfer(&signer, to, 1_000, 0);
        let mut bal = revm::state::bal::Bal::new();
        let (_r1, ws1) = Executor::execute_once(
            &snap,
            None,
            &delta,
            env,
            TxIndex(0),
            pos(0),
            &tx1,
            0,
            0,
            Some((&mut bal, 1)),
        )
        .expect("execute 1");
        delta.apply(ws1);
        let after_tx1 = bal.clone().into_alloy_bal().len();

        // tx2 runs with the seeded delta — the production path.
        let tx2 = signed_transfer(&signer, to, 500, 1);
        let (_r2, ws2) = Executor::execute_once(
            &snap,
            None,
            &delta,
            env,
            TxIndex(1),
            pos(64),
            &tx2,
            1,
            21_000,
            Some((&mut bal, 2)),
        )
        .expect("execute 2");
        assert!(!ws2.accounts.is_empty(), "tx2 wrote accounts");

        let alloy = bal.into_alloy_bal();
        assert!(after_tx1 > 0, "tx1 must be captured");
        assert!(!alloy.is_empty(), "capture must survive a seeded delta");
        // tx2's claims must be present: some account carries a bal_index 2 change.
        let has_tx2 = alloy.iter().any(|a| {
            a.balance_changes.iter().any(|c| c.block_access_index == 2)
                || a.nonce_changes.iter().any(|c| c.block_access_index == 2)
        });
        assert!(has_tx2, "tx2's claims missing from BAL: {alloy:?}");
    }
}
