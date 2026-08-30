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

use crate::delta::WriteSet;

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

    /// [`Self::from_evm_state`], with deposit artifact fidelity: every
    /// accessed slot of a touched account is emitted, read-only slots
    /// included, at the present value. The historic deposit path derived
    /// its WriteSet from a fresh `CacheDB` (which caches reads), so the
    /// wire artifact — `write_set_hash`, and the BAL claims built from it
    /// — includes read slots. The on-scope deposit path must keep that
    /// artifact byte-identical; the old-vs-new equivalence test in
    /// `deposit.rs` is the gate.
    pub fn from_evm_state_deposit(state: &revm::state::EvmState) -> Self {
        let mut ws = write_set_from_evm_state_inner(state);
        // Re-walk for the artifact parts the per-tx filter drops: read-only
        // slots, and the code bytes of called contracts. The fresh cache
        // carried both: reads land in the cache, and a called contract's
        // bytecode lands in `cache.contracts`.
        for (addr, account) in state.iter() {
            if !account.is_touched() {
                continue;
            }
            for (key, slot) in account.storage.iter() {
                if slot.original_value == slot.present_value {
                    let b_key = alloy_primitives::B256::from(key.to_be_bytes::<32>());
                    ws.storage.push(((*addr, b_key), slot.present_value));
                }
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
