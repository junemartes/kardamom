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

use crate::delta::{AccountFields, PendingDelta, WriteSet};
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
/// a seed; see `ClaimIndex::code`).
impl WriteSet {
    /// See the module docs above this impl for the fabrication rationale.
    pub fn record_into_bal(&self, bal: &mut revm::state::bal::Bal, bal_index: u64) {
        let mut entries = BalEntries::default();
        for (addr, fields) in &self.accounts {
            entries.account(self, *addr, fields.nonce, fields.balance, fields.code_hash);
        }
        for ((addr, key), value) in &self.storage {
            entries.slot(*addr, *key, *value);
        }
        entries.record_into(bal, bal_index);
    }

    /// Push one account's cache-shape storage entries: the inner loop of
    /// [`write_set_from_cache`]'s per-account walk. `CacheDB` carries no
    /// original/present distinction, so every touched slot survives.
    fn push_cache_storage<'a>(
        &mut self,
        addr: Address,
        storage: impl IntoIterator<Item = (&'a U256, &'a U256)>,
    ) {
        for (key, value) in storage {
            let b_key = B256::from(key.to_be_bytes::<32>());
            self.storage.push(((addr, b_key), *value));
        }
    }

    /// Push one account's evm-state-shape storage entries: the inner
    /// loop of [`write_set_from_evm_state_inner`]'s per-account walk.
    /// Only a slot whose `present_value` differs from `original_value`
    /// is a real write; revm tracks both on `EvmStorageSlot`.
    fn push_evm_state_storage<'a>(
        &mut self,
        addr: Address,
        storage: impl IntoIterator<Item = (&'a U256, &'a revm::state::EvmStorageSlot)>,
    ) {
        self.storage.extend(
            storage
                .into_iter()
                .filter(|(_, slot)| slot.original_value != slot.present_value)
                .map(|(key, slot)| {
                    (
                        (addr, B256::from(key.to_be_bytes::<32>())),
                        slot.present_value,
                    )
                }),
        );
    }

    /// Build a `WriteSet` from revm's per-tx `EvmState`. Only touched
    /// accounts, changed slots, and created code are emitted, which keeps
    /// the per-tx hash stable across replicas.
    #[must_use]
    pub fn from_evm_state(state: &revm::state::EvmState) -> Self {
        write_set_from_evm_state_inner(state)
    }

    /// [`Self::from_evm_state`], for a deposit or a cross-chain delivery
    /// (a 0x7D message). These are commit-cache style records: fee-free,
    /// with the nonce check off. The artifact must stay byte-identical to
    /// `execute_deposit_tx` and `execute_xchain_tx`; `deposit.rs`'s
    /// `old_and_new_deposit_paths_agree` test is the gate.
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
    /// - A called (not created) contract's bytecode is re-added, since a
    ///   called contract's code lands in the cache on load.
    #[must_use]
    pub fn from_evm_state_deposit(state: &revm::state::EvmState) -> Self {
        // Kardamom's empty-code sentinel is `B256::ZERO`; revm's is
        // `KECCAK_EMPTY`. Normalize so "empty" compares equal on both
        // sides — `seed_cache_layer` can put kardamom's convention into
        // the scope's cache.
        let mut ws = write_set_from_evm_state_inner(state);
        ws.accounts.retain(|(addr, fields)| {
            let Some(account) = state.get(addr) else {
                return true;
            };
            let original = &account.original_info;
            fields.nonce != original.nonce
                || fields.balance != original.balance
                || crate::code_hash::to_wire_code_hash(fields.code_hash)
                    != crate::code_hash::to_wire_code_hash(original.code_hash)
        });

        // Re-walk for the artifact part the per-tx filter drops: the code
        // bytes of a called (not created) contract. A called contract's
        // bytecode lands in `cache.contracts` on load, so it must be
        // re-added here.
        for account in state.values() {
            ws.readd_called_contract_code(account);
        }
        ws.finish();
        ws
    }

    /// Re-add a called (not created) contract's bytecode, unless it is
    /// untouched, already present, or empty. The loop in
    /// [`Self::from_evm_state_deposit`] stays free of a branch.
    fn readd_called_contract_code(&mut self, account: &revm::state::Account) {
        if !account.is_touched() {
            return;
        }
        let info = &account.info;
        if !account.is_created()
            && info.code_hash != KECCAK_EMPTY
            && let Some(code) = info.code.as_ref()
            && !code.is_empty()
            && !self.code.iter().any(|(h, _)| *h == info.code_hash)
        {
            self.code.push((
                info.code_hash,
                Bytes::copy_from_slice(code.original_bytes().as_ref()),
            ));
        }
    }
}

/// A batch of BAL entries, built up from a `WriteSet`'s accounts and
/// storage writes with fabricated `original_info`/`original_value`
/// fields (see the module docs above [`WriteSet::record_into_bal`] for
/// why fabrication is needed at all). [`WriteSet::record_into_bal`]
/// fills one of these, then records it.
#[derive(Default)]
struct BalEntries(BTreeMap<Address, revm::state::Account>);

impl BalEntries {
    /// Add one account's BAL entry, with `original_info` fabricated to
    /// differ from `info` in every field a later batch may seed from
    /// (nonce, balance, and code when this record deployed it).
    fn account(
        &mut self,
        ws: &WriteSet,
        addr: Address,
        nonce: u64,
        balance: U256,
        code_hash: B256,
    ) {
        use revm::state::{Account, AccountInfo, AccountStatus};
        let code = ws
            .code
            .iter()
            .find(|(h, _)| *h == code_hash)
            .map(|(_, b)| Bytecode::new_raw(AlloyBytes::from(b.clone())));
        let deployed_here = code.is_some();
        let info = AccountInfo {
            nonce,
            balance,
            code_hash,
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
        self.0.insert(
            addr,
            Account {
                info,
                original_info: Box::new(original),
                transaction_id: 0,
                storage: revm::state::EvmStorage::default(),
                status: AccountStatus::Touched,
            },
        );
    }

    /// Add one storage write, fabricating an empty account entry first
    /// when `addr` was seen only through this write, with no
    /// account-field change. That fabricated `original_info` differs
    /// only in nonce, since balance and code are absent from `ws.accounts`
    /// for this address. `original_value` is fabricated to differ from
    /// `present_value`, so revm's per-field classification marks the
    /// slot a write.
    fn slot(&mut self, addr: Address, key: B256, value: U256) {
        use revm::state::{Account, AccountInfo, AccountStatus, EvmStorageSlot};
        let account = self.0.entry(addr).or_insert_with(|| {
            let info = AccountInfo::default();
            let mut original = info.clone();
            original.nonce = original.nonce.wrapping_add(1);
            Account {
                info,
                original_info: Box::new(original),
                transaction_id: 0,
                storage: revm::state::EvmStorage::default(),
                status: AccountStatus::Touched,
            }
        });
        let slot_key = U256::from_be_bytes::<32>(key.0);
        account.storage.insert(
            slot_key,
            EvmStorageSlot {
                original_value: !value, // differs from present_value, so this is a write
                present_value: value,
                transaction_id: 0,
                is_cold: false,
            },
        );
    }

    /// Record every entry into `bal` at `bal_index`.
    fn record_into(&self, bal: &mut revm::state::bal::Bal, bal_index: u64) {
        for (addr, account) in &self.0 {
            bal.update_account(bal_index, *addr, account);
        }
    }
}

/// Build a `WriteSet` from `CacheDB`'s accumulated cache. Unlike
/// [`WriteSet::from_evm_state`] (which iterates revm's per-tx
/// `EvmState`), this iterates `CacheDB::cache.accounts` after the
/// deposit's commit cycle completes. So the resulting `WriteSet` covers
/// both the mint pre-credit and any inner-call writes. This skips
/// accounts in state `None` (loaded but unchanged) and `NotExisting`
/// (never observed).
pub(super) fn write_set_from_cache(state: &revm::database::Cache) -> WriteSet {
    let mut ws = WriteSet::default();
    for (addr, account) in &state.accounts {
        ws.push_cache_account(*addr, account, state);
    }
    ws.finish();
    ws
}

impl WriteSet {
    /// Push one `CacheDB` account entry, and its code and storage, unless
    /// the cache never actually observed it. The single loop in
    /// [`write_set_from_cache`] stays at one loop level.
    fn push_cache_account(
        &mut self,
        addr: Address,
        account: &revm::database::DbAccount,
        state: &revm::database::Cache,
    ) {
        match account.account_state {
            revm::database::AccountState::None | revm::database::AccountState::NotExisting => {
                return;
            }
            revm::database::AccountState::Touched
            | revm::database::AccountState::StorageCleared => {}
        }
        let info = &account.info;
        self.accounts.push((
            addr,
            AccountFields {
                nonce: info.nonce,
                balance: info.balance,
                code_hash: info.code_hash,
            },
        ));

        // CacheDB stores bytecode separately, by code_hash. Resolve it
        // through the `contracts` map. Skip KECCAK_EMPTY (the canonical
        // empty-code hash) and empty bytecode; neither is worth shipping
        // in the delta.
        if info.code_hash != KECCAK_EMPTY
            && let Some(code) = state.contracts.get(&info.code_hash)
            && !code.is_empty()
        {
            self.code.push((
                info.code_hash,
                Bytes::copy_from_slice(code.original_bytes().as_ref()),
            ));
        }

        self.push_cache_storage(addr, &account.storage);
    }
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
    let layers = RetainLayers {
        snapshot,
        parent,
        delta,
        idx,
    };
    let mut out = WriteSet::default();
    for (addr, fields) in &ws.accounts {
        layers.push_if_account_changed(&mut out, *addr, *fields)?;
    }
    for ((addr, key), value) in &ws.storage {
        layers.push_if_slot_changed(&mut out, *addr, *key, *value)?;
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

/// The pre-execution view [`retain_changed`] compares a write set
/// against: this tx's own pending delta, the unsettled parent, then the
/// snapshot, in that order. Grouped so each per-entry check takes one
/// struct instead of four loose layers.
struct RetainLayers<'a, S> {
    snapshot: &'a S,
    parent: Option<&'a PendingDelta>,
    delta: &'a PendingDelta,
    idx: TxIndex,
}

impl<S: StateDatabase> RetainLayers<'_, S> {
    fn state_err(&self, detail: alloc::string::String) -> ExecutorError {
        ExecutorError::Execution {
            idx: self.idx,
            detail,
        }
    }

    /// Push `(addr, fields)` into `out` if it differs from the
    /// pre-execution view, or if it creates an account with real content.
    /// [`retain_changed`]'s account loop stays at one loop level.
    fn push_if_account_changed(
        &self,
        out: &mut WriteSet,
        addr: Address,
        fields: AccountFields,
    ) -> Result<(), ExecutorError> {
        let pre = match self
            .delta
            .accounts
            .get(&addr)
            .or_else(|| self.parent.and_then(|p| p.accounts.get(&addr)))
        {
            Some(v) => Some(*v),
            None => self
                .snapshot
                .basic(addr)
                .map_err(|e| self.state_err(format!("retain basic({addr:?}): {e}")))?
                .map(AccountFields::from),
        };
        let changed = match pre {
            Some(p) => {
                fields.nonce != p.nonce
                    || fields.balance != p.balance
                    || crate::code_hash::to_wire_code_hash(fields.code_hash)
                        != crate::code_hash::to_wire_code_hash(p.code_hash)
            }
            // No prior account: keep a real creation, but drop an
            // account touched into existence with nothing in it (a
            // beneficiary at zero reward, and similar cases).
            None => {
                !crate::code_hash::is_empty_account(fields.nonce, fields.balance, fields.code_hash)
            }
        };
        if changed {
            out.accounts.push((addr, fields));
        }
        Ok(())
    }

    /// Push `((addr, key), value)` into `out` if it differs from the
    /// pre-execution view. [`retain_changed`]'s storage loop stays at one
    /// loop level.
    fn push_if_slot_changed(
        &self,
        out: &mut WriteSet,
        addr: Address,
        key: B256,
        value: U256,
    ) -> Result<(), ExecutorError> {
        let pre = match self
            .delta
            .storage
            .get(&(addr, key))
            .or_else(|| self.parent.and_then(|p| p.storage.get(&(addr, key))))
        {
            Some(v) => *v,
            None => self
                .snapshot
                .storage(addr, key)
                .map_err(|e| self.state_err(format!("retain storage({addr:?}, {key:?}): {e}")))?,
        };
        if value != pre {
            out.storage.push(((addr, key), value));
        }
        Ok(())
    }
}

/// The emission rules the Block-STM engine (`kardamom-stm`) must match
/// when it builds per-tx write sets from its own revm outcomes: touched
/// accounts, changed slots, and created code only.
fn write_set_from_evm_state_inner(state: &revm::state::EvmState) -> WriteSet {
    let mut ws = WriteSet::default();
    for (addr, account) in state {
        ws.push_evm_state_account(*addr, account);
    }
    ws.finish();
    ws
}

impl WriteSet {
    /// Push one revm `EvmState` account entry, and its code and storage,
    /// unless revm never marked it touched (a read only). The single loop
    /// in [`write_set_from_evm_state_inner`] stays at one loop level.
    fn push_evm_state_account(&mut self, addr: Address, account: &revm::state::Account) {
        // Only emit accounts revm marked as touched. Untouched entries are
        // only reads.
        if !account.is_touched() {
            return;
        }
        let info = &account.info;
        self.accounts.push((
            addr,
            AccountFields {
                nonce: info.nonce,
                balance: info.balance,
                code_hash: info.code_hash,
            },
        ));

        // Code bytes: only for accounts created this tx. Revm also loads
        // the bytecode of every contract that is merely called (`info.code`
        // is populated on load), but a called contract's code is already
        // durable (in the snapshot or parent); only a CREATE introduces
        // new bytes that the delta must carry.
        if account.is_created()
            && let Some(code) = info.code.as_ref()
            && info.code_hash != KECCAK_EMPTY
            && !code.is_empty()
        {
            self.code.push((
                info.code_hash,
                Bytes::copy_from_slice(code.original_bytes().as_ref()),
            ));
        }

        self.push_evm_state_storage(addr, &account.storage);
    }
}
