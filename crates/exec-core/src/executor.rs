//! Per-tx revm execution. Adapted from `crates/node/src/executor.rs::execute`.
//!
//! Differences from the node executor:
//! - Reads come from a snapshot-backed `StateDatabase` via a `DatabaseRef`
//!   adapter, layered through revm's `CacheDB` so writes from earlier txs in
//!   the same block are observed.
//! - Writes are captured into a per-tx `WriteSet` (built from revm's
//!   `EvmState` output) and merged into the running `PendingDelta` by the
//!   actor.
//!
//!this module does **not** compute `tx_hash`. It copies
//! `tx_hash` (and `sender`) directly from the inbound `kardamom_types::
//! TxEnvelope`, which the proxy (S1) populated at the system boundary.

use alloy_consensus::Transaction;
use alloy_eips::eip2718::Decodable2718;
use alloy_primitives::Bytes as AlloyBytes;
use alloy_primitives::{Address, B256, U256};
use bytes::Bytes;
use kardamom_types::{BPosition, Deposit, Receipt, StateDatabase, TxEnvelope, WireLog};
use revm::context::TxEnv;
use revm::context::result::ExecutionResult;
use revm::database::{CacheDB, DatabaseRef};
use revm::primitives::{KECCAK_EMPTY, Log, TxKind};
use revm::state::{Account, AccountInfo, Bytecode};
use revm::{Context, DatabaseCommit, ExecuteCommitEvm, ExecuteEvm, MainBuilder, MainContext};

use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use crate::block_env::ExecEnv;
use crate::delta::{PendingDelta, WriteSet};
use crate::error::ExecutorError;
use crate::exec_types::{ReceiptStatus, TxIndex};

/// `revm::DatabaseRef` adapter for a `StateDatabase` snapshot. Reads only —
/// writes go through revm's per-tx state journal returned by `transact`.
pub struct SnapshotRef<'a, S: StateDatabase> {
    pub inner: &'a S,
}

/// Owned variant of [`SnapshotRef`]: [`ExecScope`] must be storable across
/// the actor's loop iterations, so it owns its snapshot (`S` can still be a
/// `&T` via the blanket `StateDatabase for &T` impl).
pub struct SnapshotDb<S: StateDatabase> {
    pub inner: S,
}

impl<S: StateDatabase> DatabaseRef for SnapshotDb<S> {
    type Error = StateRefError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        SnapshotRef { inner: &self.inner }.basic_ref(address)
    }
    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        SnapshotRef { inner: &self.inner }.code_by_hash_ref(code_hash)
    }
    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        SnapshotRef { inner: &self.inner }.storage_ref(address, index)
    }
    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        SnapshotRef { inner: &self.inner }.block_hash_ref(number)
    }
}

impl<S: StateDatabase> DatabaseRef for SnapshotRef<'_, S> {
    type Error = StateRefError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let a = self
            .inner
            .basic(address)
            .map_err(|e| StateRefError(e.to_string()))?;
        Ok(a.map(|(nonce, balance, code_hash)| AccountInfo {
            balance,
            nonce,
            // Genesis-seeded EOAs carry `code_hash = B256::ZERO` in the state
            // DB, and revm's CacheDB normalizes zero code hashes to
            // KECCAK_EMPTY when accounts pass through an execution scope
            // (`CacheDB::insert_contract`). Without normalizing HERE too, the
            // code_hash an account's write set carries depends on WHERE it was
            // read from — the mdbx snapshot (fresh scope ⇒ ZERO) vs the
            // intra-scope commit cache (⇒ KECCAK_EMPTY). The executor batches
            // per block and the validator per BAL chunk, so a fresh account's
            // first two txs straddling a validator batch boundary while
            // sharing an executor block produced a wsh-only receipt
            // "divergence" and a validator fail-stop (#159). Two spellings of
            // "no code" must hash identically.
            code_hash: if code_hash == B256::ZERO {
                KECCAK_EMPTY
            } else {
                code_hash
            },
            account_id: None,
            code: None,
        }))
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        if code_hash == KECCAK_EMPTY {
            return Ok(Bytecode::default());
        }
        let raw = self
            .inner
            .code_by_hash(code_hash)
            .map_err(|e| StateRefError(e.to_string()))?;
        if raw.is_empty() {
            return Ok(Bytecode::default());
        }
        Ok(Bytecode::new_raw(AlloyBytes::from(raw)))
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        // kardamom-types' StateDatabase::storage takes a B256 key; revm uses
        // U256. The two are isomorphic — U256 is a 32-byte big-endian integer
        // and B256 is a 32-byte buffer.
        let key = B256::from(index.to_be_bytes::<32>());
        self.inner
            .storage(address, key)
            .map_err(|e| StateRefError(e.to_string()))
    }

    fn block_hash_ref(&self, _number: u64) -> Result<B256, Self::Error> {
        // The shipped kardamom-types StateDatabase trait does not expose
        // block_hash. v0 callers (BLOCKHASH opcode in executed contracts)
        // observe the zero hash — historically what an executor without an
        // ancestor cache returns until S6 wires one in.
        Ok(B256::ZERO)
    }
}

/// Concrete error type for `SnapshotRef`. We collapse the `StateDatabase`
/// associated error into a string so the `revm::Database` blanket impl can
/// see a single concrete type.
#[derive(Debug, thiserror::Error)]
#[error("snapshot ref error: {0}")]
pub struct StateRefError(pub String);

impl revm::database_interface::DBErrorMarker for StateRefError {}

/// Decode an `alloy_consensus::TxEnvelope` from the `raw_tx` bytes carried
/// in a `kardamom_types::TxEnvelope`. The signature is already verified by
/// the proxy (S1, S0); we just need the typed accessors to build a
/// revm `TxEnv`.
pub fn decode_alloy_envelope(
    raw_tx: &Bytes,
    tx_idx: TxIndex,
) -> Result<alloy_consensus::TxEnvelope, ExecutorError> {
    let mut slice: &[u8] = raw_tx.as_ref();
    alloy_consensus::TxEnvelope::decode_2718(&mut slice).map_err(|e| ExecutorError::Execution {
        idx: tx_idx,
        detail: format!("decode raw_tx: {e}"),
    })
}

/// Convert a recovered tx envelope into a `TxEnv`. `signer` is the proxy-
/// populated sender — never recomputed here (S0).
pub fn tx_env_from_alloy(alloy_env: &alloy_consensus::TxEnvelope, signer: Address) -> TxEnv {
    TxEnv {
        caller: signer,
        chain_id: alloy_env.chain_id(),
        nonce: alloy_env.nonce(),
        gas_limit: alloy_env.gas_limit(),
        value: alloy_env.value(),
        data: alloy_env.input().clone(),
        kind: match alloy_env.to() {
            Some(addr) => TxKind::Call(addr),
            None => TxKind::Create,
        },
        gas_price: alloy_env
            .gas_price()
            .unwrap_or_else(|| alloy_env.max_fee_per_gas()),
        ..Default::default()
    }
}

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
pub struct ExecScope<S: StateDatabase> {
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

impl<S: StateDatabase> ExecScope<S> {
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

    /// Like [`ExecScope::new`], but with caller-supplied revm envs instead of
    /// the [`ExecEnv`]-derived production ones. This is the seam the EEST
    /// conformance runner uses to execute under a *fixture's* block env
    /// (coinbase, basefee, difficulty, blob params) — it tests the engine's
    /// revm integration, not kardamom's boundary derivation. Production
    /// paths use [`ExecScope::new`]; `env` here only feeds the metadata on
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
        for (addr, (nonce, balance, code_hash)) in &layer.accounts {
            let code = layer
                .code
                .get(code_hash)
                .cloned()
                .filter(|b| !b.is_empty())
                .map(|b| Bytecode::new_raw(AlloyBytes::from(b)));
            cache.insert_account_info(
                *addr,
                AccountInfo {
                    balance: *balance,
                    nonce: *nonce,
                    code_hash: *code_hash,
                    account_id: None,
                    code,
                },
            );
        }
        for ((addr, key), value) in &layer.storage {
            let u_key = U256::from_be_bytes::<32>(key.0);
            cache
                .insert_account_storage(*addr, u_key, *value)
                .map_err(|e| ExecutorError::State(format!("seed layer storage: {e:?}")))?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)] // matches the free execute_tx's shape;
    // see the equivalent allow there for the rationale.
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
        let alloy_env = match decode_alloy_envelope(&inbound_envelope.raw_tx, tx_idx) {
            Ok(env_) => env_,
            Err(e) => {
                return Ok(invalid_skip(
                    &format!("undecodable raw_tx: {e}"),
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
        let signer = inbound_envelope.sender; // trusted from proxy; no recovery
        let nonce = alloy_env.nonce();
        let to = alloy_env.to();
        // Effective gas price mirrors the value `tx_env_from_alloy` feeds revm:
        // legacy/2930 `gas_price` when present, otherwise the 1559/4844
        // `max_fee_per_gas` cap. v0 has basefee = 0 so the cap is what's paid.
        let effective_gas_price = alloy_env
            .gas_price()
            .unwrap_or_else(|| alloy_env.max_fee_per_gas());

        let tx_env = tx_env_from_alloy(&alloy_env, signer);
        let outcome = match self.evm.transact(tx_env) {
            Ok(o) => o,
            // Deterministic input-invalidity: every replica computes the same
            // rejection from the same (state, tx) — skip, never halt (#92).
            Err(revm::context::result::EVMError::Transaction(reason)) => {
                return Ok(invalid_skip(
                    &format!("{reason:?}"),
                    tx_position,
                    inbound_envelope,
                    nonce,
                    to,
                    self.env.block_number,
                    tx_index_in_block,
                    cumulative_gas_used_before,
                ));
            }
            Err(revm::context::result::EVMError::Header(reason)) => {
                return Ok(invalid_skip(
                    &format!("{reason:?}"),
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
        let ws = write_set_from_evm_state(&outcome.state);
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
        let wire_logs = logs.iter().map(wire_log).collect();
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
pub fn execute_tx<S: StateDatabase>(
    snapshot: &S,
    parent: Option<&PendingDelta>,
    delta: &PendingDelta,
    env: ExecEnv,
    tx_idx: TxIndex,
    tx_position: BPosition,
    inbound_envelope: &TxEnvelope,
    tx_index_in_block: u64,
    cumulative_gas_used_before: u64,
    // EIP-7928 capture (spec: bal-attribution-parallel-validation): when
    // set, every account/slot this tx touched is recorded into the block's
    // Bal under `bal_index` (1-based tx position per revm's convention) —
    // writes as (index, value), read-only accesses into storage_reads.
    // revm classifies from original-vs-present in `outcome.state`.
    bal: Option<(&mut revm::state::bal::Bal, u64)>,
) -> Result<(Receipt, WriteSet), ExecutorError> {
    // Compatibility wrapper: one throwaway scope per call (replay + tests).
    // Hot paths hold an [`ExecScope`] per block/batch instead — that is
    // where the allocation win lives.
    let mut scope = ExecScope::new(snapshot, parent, env)?;
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

/// Build the deterministic SKIP receipt for a canonical record that is
/// invalid at execution (#92): `status=false, gas_used=0` (the wire-visible
/// marker — real execution always charges intrinsic gas), empty logs, EMPTY
/// write set (`WriteSet::default().hash()` on both the live and re-exec
/// sides), gas accounting unchanged. Loud by design: log + counter — a skip
/// existing at all means an upstream guard (sequencer nonce fence, cluster
/// dedup, resync floors) let an invalid record reach the canonical log.
/// Public: the Block-STM engine (`kardamom-stm`) produces the identical
/// skip artifact on its parallel path — one definition, one wire shape.
#[allow(clippy::too_many_arguments)]
#[cfg_attr(not(feature = "std"), allow(unused_variables))]
pub fn invalid_skip(
    reason: &str,
    tx_position: BPosition,
    inbound_envelope: &TxEnvelope,
    nonce: u64,
    to: Option<Address>,
    block_number: u64,
    tx_index_in_block: u64,
    cumulative_gas_used_before: u64,
) -> (Receipt, WriteSet) {
    // Loudness is a `std`-side concern; the skip receipt itself is the
    // consensus artifact and is produced identically in guest builds.
    #[cfg(feature = "std")]
    {
        tracing::error!(
            tx_hash = ?inbound_envelope.tx_hash,
            from = ?inbound_envelope.sender,
            nonce,
            block = block_number,
            reason,
            "INVALID canonical tx SKIPPED (deterministic; upstream guard failed — investigate)"
        );
        crate::metrics::record_invalid_tx_skipped();
    }
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

/// Execute one [`kardamom_types::Deposit`] against a snapshot + the current
/// `PendingDelta`. Returns the receipt plus a fresh per-tx `WriteSet`.
///
/// Deposit semantics (OP-aligned, ported from old `crates/node/src/executor.rs`):
/// 1. Pre-credit `deposit.from` with `deposit.mint`. This commit happens
///    *before* the EVM call, so a revert inside the inner call does NOT
///    roll it back — the mint is durable.
/// 2. Run a normal EVM call `from → to` with `value`/`data`, fee-free
///    (`gas_price = 0`), with `disable_nonce_check = true` (deposits do
///    not carry a nonce).
/// 3. The receipt's `tx_hash` is the deposit's `source_hash` (deposits have
///    no 2718-canonical hash on the inbound side; `source_hash` is the
///    canonical id ingress will query by).
#[allow(clippy::too_many_arguments)] // matches execute_tx's shape; see the
// equivalent allow on execute_tx for the rationale.
pub fn execute_deposit_tx<S: StateDatabase>(
    snapshot: &S,
    parent: Option<&PendingDelta>,
    delta: &PendingDelta,
    env: ExecEnv,
    tx_idx: TxIndex,
    tx_position: BPosition,
    deposit: &Deposit,
    tx_index_in_block: u64,
    cumulative_gas_used_before: u64,
    // See `execute_tx`. Deposits capture WRITES ONLY (their WriteSet keys)
    // via constructed accounts — the commit-cache shape loses original
    // values, so read attribution is unavailable here; executor and
    // validator both build deposit claims through this same path, keeping
    // claims symmetric.
    bal: Option<(&mut revm::state::bal::Bal, u64)>,
) -> Result<(Receipt, WriteSet), ExecutorError> {
    // Layer the running delta on top of the snapshot via CacheDB so revm
    // sees writes from earlier txs in the same block. Mirrors execute_tx.
    let snap_ref = SnapshotRef { inner: snapshot };
    let mut cache: CacheDB<SnapshotRef<'_, S>> = CacheDB::new(snap_ref);
    // Seed layers in order — the PARENT (the previous block's writes while
    // its commit is still fsyncing, pipelined-commit) first, then the live
    // delta, so later inserts overwrite and the view equals
    // snapshot ∘ parent ∘ delta.
    for layer in parent.into_iter().chain(core::iter::once(delta)) {
        for (addr, (nonce, balance, code_hash)) in &layer.accounts {
            let code = layer
                .code
                .get(code_hash)
                .cloned()
                .filter(|b| !b.is_empty())
                .map(|b| Bytecode::new_raw(AlloyBytes::from(b)));
            cache.insert_account_info(
                *addr,
                AccountInfo {
                    balance: *balance,
                    nonce: *nonce,
                    code_hash: *code_hash,
                    account_id: None,
                    code,
                },
            );
        }
        for ((addr, key), value) in &layer.storage {
            let u_key = U256::from_be_bytes::<32>(key.0);
            cache
                .insert_account_storage(*addr, u_key, *value)
                .map_err(|e| ExecutorError::Execution {
                    idx: tx_idx,
                    detail: format!("seed storage: {e:?}"),
                })?;
        }
    }

    // (1) Mint pre-credit. `dep.mint` is u128; widen to U256 for balance
    // arithmetic. Commit unconditionally — the mint is durable regardless
    // of inner-call outcome.
    let mut info = revm::Database::basic(&mut cache, deposit.from)
        .map_err(|e| ExecutorError::Execution {
            idx: tx_idx,
            detail: format!("basic({:?}): {e:?}", deposit.from),
        })?
        .unwrap_or_default();
    info.balance = info
        .balance
        .checked_add(U256::from(deposit.mint))
        .ok_or_else(|| ExecutorError::Execution {
            idx: tx_idx,
            detail: format!(
                "mint overflow: account {:?} balance + mint {} would exceed U256::MAX",
                deposit.from, deposit.mint
            ),
        })?;
    let mut acct = Account::from(info);
    acct.mark_touch();
    // Single-entry commit; built via `once(…).collect()` rather than a std
    // `HashMap` — the exec core is `no_std` and must not depend on
    // `RandomState`.
    cache.commit(core::iter::once((deposit.from, acct)).collect());

    // (2) Inner EVM call. Disable the nonce check — deposits do not carry
    // a nonce.
    let mut cfg = env.cfg_env();
    cfg.disable_nonce_check = true;

    let tx_env = tx_env_from_deposit(deposit);
    let mut evm = Context::mainnet()
        .with_db(&mut cache)
        .with_block(env.block_env())
        .with_cfg(cfg)
        .build_mainnet();
    let result = evm
        .transact_commit(tx_env)
        .map_err(|e| ExecutorError::Execution {
            idx: tx_idx,
            detail: format!("{e:?}"),
        })?;

    let gas_used = result.gas().tx_gas_used();
    let (status_success, logs) = match &result {
        ExecutionResult::Success { logs, .. } => (true, logs.clone()),
        ExecutionResult::Revert { .. } => (false, Vec::<Log>::new()),
        ExecutionResult::Halt { .. } => (false, Vec::<Log>::new()),
    };

    // Build the write set from revm's final-state cache. Both the mint
    // pre-credit and any inner-call writes contribute touched accounts.
    let ws = write_set_from_cache(&cache.cache);
    if let Some((bal, bal_index)) = bal {
        record_writeset_into_bal(bal, bal_index, &ws);
    }

    let write_set_hash = ws.hash();
    let wire_logs: Vec<WireLog> = logs.iter().map(wire_log).collect();
    let cumulative_gas_used = cumulative_gas_used_before + gas_used;

    let receipt = Receipt {
        tx_idx: tx_position,
        // Deposits' canonical id is the OP source_hash, NOT a 2718 keccak.
        tx_hash: deposit.source_hash,
        // L1-originated: consumes no L2 nonce (the filler `nonce: 0` below
        // is NOT a real nonce — consumers branch on this, never on the 0).
        tx_type: kardamom_types::TX_TYPE_DEPOSIT,
        status: status_success,
        gas_used,
        logs: wire_logs,
        write_set_hash,
        nonce: 0,
        from: deposit.from,
        to: deposit.to,
        contract_address: None,
        // Deposits pay no fee.
        effective_gas_price: 0,
        block_number: env.block_number,
        transaction_index: tx_index_in_block,
        cumulative_gas_used,
    };
    Ok((receipt, ws))
}

/// Build a `TxEnv` from a deposit envelope. Deposits never deduct fees
/// (`gas_price = 0`), do not assert chain-id at the envelope layer, and
/// disable the nonce check at the caller's `cfg.disable_nonce_check = true`.
fn tx_env_from_deposit(dep: &Deposit) -> TxEnv {
    TxEnv {
        caller: dep.from,
        kind: match dep.to {
            Some(addr) => TxKind::Call(addr),
            None => TxKind::Create,
        },
        value: dep.value,
        data: AlloyBytes::copy_from_slice(dep.input.as_ref()),
        gas_limit: dep.gas_limit,
        gas_price: 0,
        nonce: 0,
        chain_id: None,
        ..Default::default()
    }
}

/// Build a `WriteSet` from CacheDB's accumulated cache. Unlike
/// [`write_set_from_evm_state`] (which iterates revm's per-tx
/// `EvmState`), this iterates `CacheDB::cache.accounts` after the deposit's
/// commit cycle is complete, so the resulting WriteSet covers BOTH the
/// mint pre-credit and any inner-call writes. Accounts in state `None`
/// (loaded but unchanged) and `NotExisting` (never observed) are skipped.
/// Record a deposit's WriteSet into the block Bal as WRITES (constructed
/// accounts — the commit-cache shape loses original values, so
/// `original_value` is fabricated to differ from present, forcing write
/// classification; only present values are ever serialized). Both executor
/// and validator build deposit claims through this same path, keeping the
/// claims symmetric. Reads are not attributed for deposits.
///
/// Classification in revm's Bal is per FIELD, each compared against the
/// fabricated original — so every field a later batch may seed from must
/// have its original forced to differ. Nonce and balance always (post-values
/// only; the mint IS a balance claim), code only when this record deployed
/// it (`ws.code` carries created-only bytecode, and the claim must include
/// the BYTES — code is a seed, see `ClaimIndex::code`). The first version
/// fabricated only the nonce, and per-field classification silently dropped
/// the balance and code claims: the deposit mint never reached the BAL.
pub fn record_writeset_into_bal(bal: &mut revm::state::bal::Bal, bal_index: u64, ws: &WriteSet) {
    use revm::state::{Account, AccountInfo, AccountStatus, EvmStorageSlot};
    let mut by_addr: BTreeMap<Address, Account> = BTreeMap::new();
    for (addr, (nonce, balance, code_hash)) in &ws.accounts {
        let code = ws
            .code
            .iter()
            .find(|(h, _)| h == code_hash)
            .map(|(_, b)| Bytecode::new_raw(AlloyBytes::from(b.clone())));
        let deployed_here = code.is_some();
        let info = AccountInfo {
            nonce: *nonce,
            balance: *balance,
            code_hash: *code_hash,
            code,
            account_id: None,
        };
        let mut original = info.clone();
        original.nonce = original.nonce.wrapping_add(1);
        original.balance = original.balance.wrapping_add(U256::ONE);
        if deployed_here {
            // `ws.code` never carries empty bytecode, so KECCAK256_EMPTY is
            // guaranteed to differ from the deployed hash.
            original.code_hash = alloy_primitives::KECCAK256_EMPTY;
            original.code = None;
        }
        by_addr.insert(
            *addr,
            Account {
                info,
                original_info: Box::new(original),
                transaction_id: 0,
                storage: Default::default(),
                status: AccountStatus::Touched,
            },
        );
    }
    for ((addr, key), value) in &ws.storage {
        let entry = by_addr.entry(*addr).or_insert_with(|| {
            let info = AccountInfo::default();
            let mut original = info.clone();
            original.nonce = original.nonce.wrapping_add(1);
            Account {
                info,
                original_info: Box::new(original),
                transaction_id: 0,
                storage: Default::default(),
                status: AccountStatus::Touched,
            }
        });
        let slot_key = U256::from_be_bytes::<32>(key.0);
        entry.storage.insert(
            slot_key,
            EvmStorageSlot {
                original_value: !*value, // != present ⇒ classified as write
                present_value: *value,
                transaction_id: 0,
                is_cold: false,
            },
        );
    }
    for (addr, account) in &by_addr {
        bal.update_account(bal_index, *addr, account);
    }
}

fn write_set_from_cache(state: &revm::database::Cache) -> WriteSet {
    let mut ws = WriteSet::default();
    for (addr, account) in state.accounts.iter() {
        match account.account_state {
            revm::database::AccountState::None | revm::database::AccountState::NotExisting => {
                continue;
            }
            revm::database::AccountState::Touched
            | revm::database::AccountState::StorageCleared => {}
        }
        let info = &account.info;
        ws.accounts
            .push((*addr, (info.nonce, info.balance, info.code_hash)));

        // CacheDB stores bytecode separately by code_hash; resolve via the
        // `contracts` map. KECCAK_EMPTY (canonical empty-code) and empty
        // bytecode are not worth shipping in the delta.
        if info.code_hash != KECCAK_EMPTY
            && let Some(code) = state.contracts.get(&info.code_hash)
            && !code.is_empty()
        {
            ws.code.push((
                info.code_hash,
                Bytes::copy_from_slice(code.original_bytes().as_ref()),
            ));
        }

        for (key, value) in &account.storage {
            let b_key = B256::from(key.to_be_bytes::<32>());
            ws.storage.push(((*addr, b_key), *value));
        }
    }
    ws.finish();
    ws
}

/// Public: the Block-STM engine (`kardamom-stm`) builds per-tx write sets
/// from its own revm outcomes with exactly these emission rules — touched
/// accounts, changed slots, created code only.
pub fn write_set_from_evm_state(state: &revm::state::EvmState) -> WriteSet {
    let mut ws = WriteSet::default();
    for (addr, account) in state.iter() {
        // Only emit accounts revm marked as touched. Untouched entries are
        // pure reads.
        if !account.is_touched() {
            continue;
        }
        let info = &account.info;
        ws.accounts
            .push((*addr, (info.nonce, info.balance, info.code_hash)));

        // Code bytes: ONLY for accounts CREATED this tx. revm also loads
        // the bytecode of every contract merely CALLED (`info.code` is
        // populated on load), and the old unconditional capture copied the
        // full runtime into the WriteSet per call — 1.6KB/tx on CLOB
        // workloads, the second-largest allocation site post-ExecScope.
        // A called contract's code is already durable (snapshot/parent);
        // only a CREATE introduces new bytes the delta must carry.
        if account.is_created()
            && let Some(code) = info.code.as_ref()
            && info.code_hash != KECCAK_EMPTY
            && !code.is_empty()
        {
            ws.code.push((
                info.code_hash,
                Bytes::copy_from_slice(code.original_bytes().as_ref()),
            ));
        }

        for (key, slot) in account.storage.iter() {
            // Only record slots whose present_value differs from the
            // original_value. revm tracks both on `EvmStorageSlot`.
            if slot.original_value != slot.present_value {
                let b_key = B256::from(key.to_be_bytes::<32>());
                ws.storage.push(((*addr, b_key), slot.present_value));
            }
        }
    }
    ws.finish();
    ws
}

/// Public: the Block-STM engine mirrors the receipt log encoding.
pub fn wire_log(log: &Log) -> WireLog {
    WireLog {
        address: log.address,
        topics: log.data.topics().to_vec(),
        data: Bytes::copy_from_slice(log.data.data.as_ref()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::MockStateDatabase;
    use alloy_consensus::{SignableTransaction, TxLegacy};
    use alloy_eips::eip2718::Encodable2718;
    use alloy_network::TxSignerSync;
    use alloy_primitives::{TxKind as APTxKind, U256, address, keccak256};
    use alloy_signer_local::PrivateKeySigner;
    use kardamom_types::{BPosition, BlockBoundaryStart, TxEnvelope as KtTxEnvelope};

    fn boundary(block_number: u64) -> BlockBoundaryStart {
        BlockBoundaryStart {
            block_number,
            end_tx_idx: BPosition {
                term_id: 0,
                term_offset: 0,
            },
            l2_timestamp: 0,
            l1_origin: 0,
        }
    }

    fn pos(off: i32) -> BPosition {
        BPosition {
            term_id: 0,
            term_offset: off,
        }
    }

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
        let (receipt, ws) = execute_tx(
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
        let (r2, _) = execute_tx(
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
        let (receipt, ws) = execute_tx(
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
        let (receipt, ws) = execute_tx(
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
        let (r1, ws1) = execute_tx(
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
        let (r2, ws2) = execute_tx(
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

    // -----------------------------------------------------------------
    // Deposit-execution tests (mirror of the old `execute_deposit`
    // scenarios from `crates/node/src/executor.rs`, ported to the new
    // snapshot+delta + kardamom_types::Deposit shape).
    // -----------------------------------------------------------------
    use kardamom_types::Deposit;

    fn dep(from: Address, to: Option<Address>, mint: u128, value: u64, data: Bytes) -> Deposit {
        Deposit {
            source_hash: B256::repeat_byte(0x42),
            from,
            to,
            mint,
            value: U256::from(value),
            gas_limit: 1_000_000,
            is_system_transaction: false,
            input: data,
        }
    }

    #[test]
    fn deposit_mints_and_forwards_value() {
        let from = Address::from([0x11u8; 20]);
        let to = Address::from([0x22u8; 20]);
        let snap = MockStateDatabase::builder().build();
        let delta = PendingDelta::new();
        let env = ExecEnv::new(1, &boundary(1));

        let d = dep(from, Some(to), 1_000, 400, Bytes::new());
        let (receipt, ws) =
            execute_deposit_tx(&snap, None, &delta, env, TxIndex(0), pos(0), &d, 0, 0, None)
                .expect("execute");

        // Mint = 1000, value forwarded = 400, so:
        // - sender ends at 1000 - 400 = 600
        // - recipient ends at 400
        // (Gas price is 0 — no fee deduction.)
        assert_eq!(ws.account(&from).unwrap().1, U256::from(600u64));
        assert_eq!(ws.account(&to).unwrap().1, U256::from(400u64));

        // Receipt fields specific to deposits.
        assert_eq!(receipt.tx_hash, d.source_hash);
        assert!(receipt.status);
        assert_eq!(receipt.nonce, 0);
        assert_eq!(receipt.from, from);
        assert_eq!(receipt.to, Some(to));
        assert_eq!(receipt.contract_address, None);
        assert_eq!(receipt.effective_gas_price, 0);
        assert_eq!(receipt.block_number, 1);
        assert_eq!(receipt.transaction_index, 0);
        assert_eq!(receipt.cumulative_gas_used, receipt.gas_used);
    }

    #[test]
    fn deposit_mint_survives_inner_revert() {
        // Inline bytecode: PUSH1 0; PUSH1 0; REVERT (60 00 60 00 fd).
        let revert_code = AlloyBytes::from(vec![0x60, 0x00, 0x60, 0x00, 0xfd]);
        let bytecode = Bytecode::new_raw(revert_code);
        let code_hash = bytecode.hash_slow();
        let revert_addr = Address::from([0x33u8; 20]);

        let from = Address::from([0x11u8; 20]);
        let snap = MockStateDatabase::builder()
            .account(revert_addr, U256::ZERO, 1, code_hash)
            .code(
                code_hash,
                Bytes::copy_from_slice(bytecode.original_bytes().as_ref()),
            )
            .build();
        let delta = PendingDelta::new();
        let env = ExecEnv::new(1, &boundary(1));

        let d = dep(from, Some(revert_addr), 1_000, 200, Bytes::new());
        let (receipt, ws) =
            execute_deposit_tx(&snap, None, &delta, env, TxIndex(0), pos(0), &d, 0, 0, None)
                .expect("execute (revert is OK at the executor layer)");

        // Mint pre-credit is OUTSIDE the EVM call: from keeps the full mint.
        assert_eq!(
            ws.account(&from).unwrap().1,
            U256::from(1_000u64),
            "from must keep full mint after inner revert"
        );
        // Inner call reverted — receipt status reflects that.
        assert!(!receipt.status, "inner-revert deposit yields status=false");
        // Recipient observed no transferred value.
        assert!(
            ws.account(&revert_addr).is_none_or(|a| a.1 == U256::ZERO),
            "revert target must not retain value"
        );
    }

    #[test]
    fn deposit_mint_overflow_returns_execution_error() {
        let from = Address::from([0x11u8; 20]);
        // Sender pre-funded with U256::MAX — any mint causes overflow.
        let snap = MockStateDatabase::builder()
            .account(from, U256::MAX, 0, KECCAK_EMPTY)
            .build();
        let delta = PendingDelta::new();
        let env = ExecEnv::new(1, &boundary(1));

        let d = dep(from, Some(Address::from([0x22u8; 20])), 1, 0, Bytes::new());
        let err = execute_deposit_tx(&snap, None, &delta, env, TxIndex(0), pos(0), &d, 0, 0, None)
            .unwrap_err();
        assert!(
            matches!(err, ExecutorError::Execution { ref detail, .. } if detail.contains("mint overflow")),
            "got {err:?}"
        );
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
        let (_receipt, ws) = execute_tx(
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
        let (_r1, ws1) = execute_tx(
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
        let (_r2, ws2) = execute_tx(
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
