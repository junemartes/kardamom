//! Attribution-granularity ladder.
//!
//! `quantize` collapses per-tx BAL indices into chunks of K txs. The last
//! value in each chunk wins. This function lives in the engine so the
//! executor (which produces quantized frames) and the validator (which
//! recomputes claims at the same granularity) share one implementation.
//! Divergence checking is a structural equality check, so both sides must
//! transform data the same way by construction, not by separate
//! maintenance.

use core::num::{NonZeroU16, NonZeroU64};

use alloc::vec::Vec;

/// Merge per-tx BAL fragments into one block BAL. Each fragment is captured
/// independently by a parallel worker, through the same
/// `Bal::update_account` call the streaming path uses.
///
/// Supply fragments in ascending canonical (bal-index) order. Per-key write
/// lists are index-ordered by construction only when the append order
/// matches the canonical order, and the dedup rule below compares against
/// the last kept write.
///
/// Account insertion order does not matter. `into_alloy_bal` sorts by
/// address, and storage keys live in a `BTreeMap`. So the merged result is
/// wire-identical to a single sequential capture.
///
/// This function lives in the engine core for the same reason `quantize`
/// does. The executor's parallel (Block-STM) capture and the sequential
/// capture must transform data the same way by construction. The
/// validator's cross-check is a structural equality check on the published
/// artifact.
#[must_use]
pub fn merge_bal_fragments(
    fragments: impl IntoIterator<Item = revm::state::bal::Bal>,
) -> revm::state::bal::Bal {
    let mut out = revm::state::bal::Bal::new();
    for (addr, acct) in fragments.into_iter().flat_map(|frag| frag.accounts) {
        merge_account(&mut out, addr, acct);
    }
    out
}

/// Append `src`'s writes into `dst`, replaying the sequential capture's
/// dedup rule (see [`push_if_changed`]).
fn append<T: Clone + PartialEq, K: PartialEq + ?Sized>(
    dst: &mut revm::state::bal::BalWrites<T>,
    src: revm::state::bal::BalWrites<T>,
    key: impl Fn(&T) -> &K,
) {
    for (idx, v) in src.writes {
        push_if_changed(dst, idx, v, &key);
    }
}

/// Record one write at `idx`, only when its value differs from the last
/// recorded one (`BalWrites::update_with_key`). A per-tx fragment cannot
/// know this on its own, since its list saw only its own tx. Without
/// this step, an unchanged-value write (for example, a deposit that
/// touches the fee sink, or a slot rewritten to its previous value)
/// would appear in the merged result but not in the sequential one.
/// `key` mirrors revm's comparison: the whole value for nonce, balance,
/// and storage, and the code hash for code.
fn push_if_changed<T: Clone + PartialEq, K: PartialEq + ?Sized>(
    dst: &mut revm::state::bal::BalWrites<T>,
    idx: revm::state::bal::BalIndex,
    v: T,
    key: &impl Fn(&T) -> &K,
) {
    match dst.writes.last() {
        Some((_, last)) if key(last) == key(&v) => {}
        _ => dst.writes.push((idx, v)),
    }
}

/// Merge one storage slot's writes into `dst`, inserting the whole
/// entry on a slot `dst` has not seen yet.
fn merge_storage_slot<K: Ord, T: Clone + PartialEq>(
    dst: &mut alloc::collections::BTreeMap<K, revm::state::bal::BalWrites<T>>,
    slot: K,
    writes: revm::state::bal::BalWrites<T>,
) {
    let Some(dw) = dst.get_mut(&slot) else {
        dst.insert(slot, writes);
        return;
    };
    append(dw, writes, |v| v);
}

/// Merge one account's fragment into `out`, inserting the whole entry
/// on an address `out` has not seen yet.
fn merge_account(
    out: &mut revm::state::bal::Bal,
    addr: alloy_primitives::Address,
    acct: revm::state::bal::AccountBal,
) {
    let Some(tgt) = out.accounts.get_mut(&addr) else {
        out.accounts.insert(addr, acct);
        return;
    };
    append(&mut tgt.account_info.nonce, acct.account_info.nonce, |v| v);
    append(
        &mut tgt.account_info.balance,
        acct.account_info.balance,
        |v| v,
    );
    append(&mut tgt.account_info.code, acct.account_info.code, |v| &v.0);
    for (slot, writes) in acct.storage.storage {
        merge_storage_slot(&mut tgt.storage.storage, slot, writes);
    }
}

/// The chunk number for a 1-based BAL index, at granularity `k`.
///
/// Public and standalone because both this function and [`quantize`]'s
/// wire signature are consensus-shared with the validator, which calls
/// `chunk_of` directly.
#[must_use]
pub fn chunk_of(index: u64, k: NonZeroU64) -> u64 {
    Granularity(k).chunk_of(index)
}

/// The claim-index space a BAL index lives in, at wire granularity `k`:
/// the index itself at `k == 1` (per-tx claims), or its chunk number at
/// `k > 1` (chunk-collapsed claims). The validator looks up seeds and
/// verifies claims in this space, so a claim built at one granularity
/// and checked at another must use this one rule.
#[must_use]
pub fn claim_index(bal_index: u64, k: NonZeroU16) -> u64 {
    if k.get() > 1 {
        chunk_of(bal_index, NonZeroU64::from(k))
    } else {
        bal_index
    }
}

/// A parsed-once BAL attribution granularity, always nonzero. `chunk_of`
/// and `dedup_changes` read it as `self` instead of taking `k` as a
/// loose parameter.
#[derive(Clone, Copy)]
struct Granularity(NonZeroU64);

impl Granularity {
    /// The chunk number for a 1-based BAL index.
    fn chunk_of(self, index: u64) -> u64 {
        if index == 0 {
            0
        } else {
            index.div_ceil(self.0.get())
        }
    }

    /// Quantize the index of each change, and keep only the last entry
    /// per chunk: the same semantics as the original per-index loop
    /// (last write in a chunk wins), computed without one. Revm emits
    /// entries in ascending index order, so after quantizing, same-chunk
    /// entries are adjacent: reverse, dedup by chunk (keeping the first
    /// of each run, which is the original last), then reverse back.
    fn dedup_changes<T>(self, changes: &mut Vec<T>, index_of: impl Fn(&mut T) -> &mut u64) {
        for c in changes.iter_mut() {
            let idx = index_of(c);
            *idx = self.chunk_of(*idx);
        }
        changes.reverse();
        changes.dedup_by(|a, b| index_of(a) == index_of(b));
        changes.reverse();
    }

    /// Quantize one account's storage, balance, nonce, and code changes
    /// in place. The single loop in [`quantize`] calls this once per
    /// account, so that function stays at one loop level.
    fn quantize_account(self, acct: &mut alloy_eip7928::AccountChanges) {
        // The later write in the same chunk wins; see `dedup_changes`.
        for slot in &mut acct.storage_changes {
            self.dedup_changes(&mut slot.changes, |c| &mut c.block_access_index);
        }
        self.dedup_changes(&mut acct.balance_changes, |c| &mut c.block_access_index);
        self.dedup_changes(&mut acct.nonce_changes, |c| &mut c.block_access_index);
        self.dedup_changes(&mut acct.code_changes, |c| &mut c.block_access_index);
    }
}

/// Quantize an EIP-7928 access list into chunks of `k` txs. `k == 0` or
/// `k == 1` returns the list unchanged (no chunking below 2 txs is
/// possible or meaningful).
#[must_use]
pub fn quantize(bal: alloy_eip7928::BlockAccessList, k: u16) -> alloy_eip7928::BlockAccessList {
    let mut out = bal;
    let granularity = match NonZeroU64::new(u64::from(k)) {
        None => return out,
        Some(k) if k.get() <= 1 => return out,
        Some(k) => Granularity(k),
    };
    for acct in &mut out {
        granularity.quantize_account(acct);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_eip7928::{AccountChanges, BalanceChange, SlotChanges, StorageChange};
    use alloy_primitives::{Address, U256};

    #[test]
    fn quantize_collapses_within_chunks_and_keeps_last() {
        let mut acct = AccountChanges::new(Address::repeat_byte(1));
        acct.storage_changes.push(SlotChanges {
            slot: U256::from(7),
            changes: vec![
                StorageChange {
                    block_access_index: 1,
                    new_value: U256::from(10),
                },
                StorageChange {
                    block_access_index: 4,
                    new_value: U256::from(40),
                },
                StorageChange {
                    block_access_index: 6,
                    new_value: U256::from(60),
                },
            ],
        });
        acct.balance_changes.push(BalanceChange {
            block_access_index: 2,
            post_balance: U256::from(2),
        });
        acct.balance_changes.push(BalanceChange {
            block_access_index: 3,
            post_balance: U256::from(3),
        });
        let out = quantize(vec![acct], 5);
        // Indices 1 and 4 go to chunk 1 (keep 40). Index 6 goes to chunk 2.
        assert_eq!(
            out[0].storage_changes[0].changes,
            vec![
                StorageChange {
                    block_access_index: 1,
                    new_value: U256::from(40)
                },
                StorageChange {
                    block_access_index: 2,
                    new_value: U256::from(60)
                },
            ]
        );
        // Balances at indices 2 and 3 go to chunk 1. Keep the last one (3).
        assert_eq!(out[0].balance_changes.len(), 1);
        assert_eq!(out[0].balance_changes[0].post_balance, U256::from(3));
        assert_eq!(out[0].balance_changes[0].block_access_index, 1);
    }
}
