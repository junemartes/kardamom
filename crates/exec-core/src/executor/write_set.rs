//! `WriteSet` extraction from revm's two state shapes (per-tx `EvmState`,
//! post-commit `CacheDB` cache) and BAL recording for deposits. The
//! `WireLog` conversion lives on the type itself (`WireLog::from`).

use alloy_primitives::Bytes as AlloyBytes;
use alloy_primitives::{Address, B256, U256};
use bytes::Bytes;
use revm::primitives::KECCAK_EMPTY;
use revm::state::Bytecode;

use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::format;

use kardamom_types::StateDatabase;

use crate::delta::{PendingDelta, WriteSet};
use crate::error::ExecutorError;
use crate::exec_types::TxIndex;

/// Record a deposit's `WriteSet` into the block BAL as writes. The
/// commit-cache shape used to build a deposit's `WriteSet` loses original
/// values, so this fabricates `original_value` to differ from
/// `present_value`, forcing write classification. Only present values are
/// ever serialized. Both the executor and the validator build deposit
/// claims through this same path, so the claims stay symmetric. Reads are
/// not attributed for deposits.
///
/// Revm's BAL classifies changes per field, each compared against the
/// fabricated original. So every field a later batch may seed from must
/// have its original forced to differ. This always applies to nonce and
/// balance (post-values only; the mint is itself a balance claim), and to
/// code only when this record deployed it (`ws.code` carries only newly
/// created bytecode, and the claim must include the bytes, since code is
/// a seed; see `ClaimIndex::code`). An earlier version fabricated only
/// the nonce, and per-field classification silently dropped the balance
/// and code claims, so the deposit mint never reached the BAL.
impl WriteSet {
    /// See the module docs above this impl for the fabrication rationale.
    pub fn record_into_bal(&self, bal: &mut revm::state::bal::Bal, bal_index: u64) {
        record_writeset_into_bal_inner(bal, bal_index, self)
    }

    /// Build a `WriteSet` from revm's per-tx `EvmState`. Only touched
    /// accounts, changed slots, and created code are emitted, which keeps
    /// the per-tx hash stable across replicas.
    pub fn from_evm_state(state: &revm::state::EvmState) -> Self {
        write_set_from_evm_state_inner(state)
    }

    /// [`Self::from_evm_state`], for a deposit or a cross-chain delivery
    /// (a 0x7D message). These are commit-cache style records: fee-free,
    /// with the nonce check off, and their artifact must stay
    /// byte-identical to the historic free-function path
    /// (`execute_deposit_tx`, `execute_xchain_tx`) — the equivalence test
    /// in `deposit.rs` is the gate.
    ///
    /// Two extra rules on top of [`Self::from_evm_state`]:
    ///
    /// - An account entry survives only when it truly changed: compare
    ///   `info` against `original_info`, revm's per-transaction pre-load
    ///   snapshot (normalized so kardamom's `B256::ZERO` empty-code
    ///   sentinel compares equal to revm's `KECCAK_EMPTY`). This drops
    ///   noise from an account merely touched into existence with
    ///   nothing in it (the fee recipient at zero reward, and similar
    ///   cases) — a value pipelined-commit timing could otherwise leak
    ///   into the artifact.
    /// - A called (not created) contract's bytecode is re-added: the
    ///   historic fresh-cache path always carried it, since a called
    ///   contract's code lands in the cache on load.
    pub fn from_evm_state_deposit(state: &revm::state::EvmState) -> Self {
        // Kardamom's empty-code sentinel is `B256::ZERO`; revm's is
        // `KECCAK_EMPTY`. Normalize so "empty" compares equal on both
        // sides — `seed_cache_layer` can put kardamom's convention into
        // the scope's cache.
        let norm = |h: B256| if h == KECCAK_EMPTY { B256::ZERO } else { h };

        let mut ws = write_set_from_evm_state_inner(state);
        ws.accounts.retain(|(addr, (nonce, balance, code_hash))| {
            let Some(account) = state.get(addr) else {
                return true;
            };
            let original = &account.original_info;
            *nonce != original.nonce
                || *balance != original.balance
                || norm(*code_hash) != norm(original.code_hash)
        });

        // Re-walk for the artifact part the per-tx filter drops: the code
        // bytes of a called (not created) contract. The historic
        // fresh-cache path carried it, since a called contract's
        // bytecode lands in `cache.contracts` on load.
        for account in state.values() {
            if !account.is_touched() {
                continue;
            }
            let info = &account.info;
            if !account.is_created()
                && info.code_hash != KECCAK_EMPTY
                && let Some(code) = info.code.as_ref()
                && !code.is_empty()
                && !ws.code.iter().any(|(h, _)| *h == info.code_hash)
            {
                ws.code.push((
                    info.code_hash,
                    Bytes::copy_from_slice(code.original_bytes().as_ref()),
                ));
            }
        }
        ws.finish();
        ws
    }
}

fn record_writeset_into_bal_inner(bal: &mut revm::state::bal::Bal, bal_index: u64, ws: &WriteSet) {
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
            // `ws.code` never carries empty bytecode, so KECCAK256_EMPTY
            // always differs from the deployed hash.
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
                original_value: !*value, // differs from present_value, so this is a write
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

/// Build a `WriteSet` from `CacheDB`'s accumulated cache. Unlike
/// [`write_set_from_evm_state`] (which iterates revm's per-tx
/// `EvmState`), this iterates `CacheDB::cache.accounts` after the
/// deposit's commit cycle completes. So the resulting `WriteSet` covers
/// both the mint pre-credit and any inner-call writes. This skips
/// accounts in state `None` (loaded but unchanged) and `NotExisting`
/// (never observed).
pub(super) fn write_set_from_cache(state: &revm::database::Cache) -> WriteSet {
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

        // CacheDB stores bytecode separately, by code_hash. Resolve it
        // through the `contracts` map. Skip KECCAK_EMPTY (the canonical
        // empty-code hash) and empty bytecode; neither is worth shipping
        // in the delta.
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

/// Filter a write set down to values that changed.
///
/// [`write_set_from_cache`] copies every touched slot in an account's
/// cache entry. Some touched slots come from layer seeding
/// (`seed_cache_layer`), not from this record. The seeded slots differ by
/// commit timing: an executor with an unsettled previous block seeds
/// (and so "captures") that block's slots, while a validator that
/// already settled the block does not. So a deposit or 0x7D claim, and
/// its `write_set_hash`, can differ between the executor and the
/// validator for a reason that is not the execution itself.
///
/// This compares each entry against the pre-execution view (`snapshot`
/// composed with `parent` and `delta`) and keeps only real changes. The
/// result does not depend on which layer held a value. It also drops
/// touched-but-unchanged noise, such as a fee recipient at zero reward,
/// on both sides for the same reason.
pub(super) fn retain_changed<S: StateDatabase>(
    ws: WriteSet,
    snapshot: &S,
    parent: Option<&PendingDelta>,
    delta: &PendingDelta,
    idx: TxIndex,
) -> Result<WriteSet, ExecutorError> {
    // Kardamom's empty-code sentinel is `B256::ZERO`; revm's is
    // `KECCAK_EMPTY`. Normalize so "empty" compares equal on both sides.
    let norm = |h: B256| if h == KECCAK_EMPTY { B256::ZERO } else { h };
    let state_err = |detail: alloc::string::String| ExecutorError::Execution { idx, detail };

    let mut out = WriteSet::default();
    for (addr, triple) in ws.accounts.iter() {
        let pre = match delta
            .accounts
            .get(addr)
            .or_else(|| parent.and_then(|p| p.accounts.get(addr)))
        {
            Some(v) => Some(*v),
            None => snapshot
                .basic(*addr)
                .map_err(|e| state_err(format!("retain basic({addr:?}): {e}")))?,
        };
        let changed = match pre {
            Some((n, b, c)) => triple.0 != n || triple.1 != b || norm(triple.2) != norm(c),
            // No prior account: keep a real creation, but drop an
            // account touched into existence with nothing in it (a
            // beneficiary at zero reward, and similar cases).
            None => !(triple.0 == 0 && triple.1 == U256::ZERO && norm(triple.2) == B256::ZERO),
        };
        if changed {
            out.accounts.push((*addr, *triple));
        }
    }
    for ((addr, key), value) in ws.storage.iter() {
        let pre = match delta
            .storage
            .get(&(*addr, *key))
            .or_else(|| parent.and_then(|p| p.storage.get(&(*addr, *key))))
        {
            Some(v) => *v,
            None => snapshot
                .storage(*addr, *key)
                .map_err(|e| state_err(format!("retain storage({addr:?}, {key:?}): {e}")))?,
        };
        if *value != pre {
            out.storage.push(((*addr, *key), *value));
        }
    }
    // Code entries carry only bytecode this record created. That is
    // always a real change, so code passes through unfiltered.
    out.code = ws.code;
    // The input is already sorted (`write_set_from_cache` calls
    // `finish()`). Filtering keeps that order, so no re-sort is needed.
    // `finish()` still re-checks the order invariant in debug builds.
    out.finish();
    Ok(out)
}

/// Public API: the Block-STM engine (`kardamom-stm`) builds per-tx write
/// sets from its own revm outcomes, using exactly these emission rules:
/// touched accounts, changed slots, and created code only.
fn write_set_from_evm_state_inner(state: &revm::state::EvmState) -> WriteSet {
    let mut ws = WriteSet::default();
    for (addr, account) in state.iter() {
        // Only emit accounts revm marked as touched. Untouched entries are
        // only reads.
        if !account.is_touched() {
            continue;
        }
        let info = &account.info;
        ws.accounts
            .push((*addr, (info.nonce, info.balance, info.code_hash)));

        // Code bytes: only for accounts created this tx. Revm also loads
        // the bytecode of every contract that is merely called (`info.code`
        // is populated on load). An earlier version copied the full
        // runtime into the `WriteSet` on every call, unconditionally,
        // costing about 1.6KB/tx on CLOB workloads, the second-largest
        // allocation site after `Executor`. A called contract's code is
        // already durable (in the snapshot or parent); only a CREATE
        // introduces new bytes that the delta must carry.
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
            // original_value. Revm tracks both on `EvmStorageSlot`.
            if slot.original_value != slot.present_value {
                let b_key = B256::from(key.to_be_bytes::<32>());
                ws.storage.push(((*addr, b_key), slot.present_value));
            }
        }
    }
    ws.finish();
    ws
}
