//! Attribution-granularity ladder (spec: bal-attribution-parallel-validation).
//!
//! `quantize` collapses per-tx BAL indices into K-tx chunks (chunk-final
//! value wins). It lives in the ENGINE so the executor (producing quantized
//! frames) and the validator (recomputing claims for verification at the
//! same granularity) share one implementation — divergence checking is a
//! structural equality, so the two sides must transform identically by
//! construction, not by parallel maintenance.

/// Chunk ordinal for a 1-based bal index at granularity `k`.
#[must_use]
pub fn chunk_of(index: u64, k: u64) -> u64 {
    if index == 0 { 0 } else { index.div_ceil(k) }
}

/// Quantize an EIP-7928 access list to `k`-tx chunks. `k <= 1` is identity.
#[must_use]
pub fn quantize(bal: alloy_eip7928::BlockAccessList, k: u16) -> alloy_eip7928::BlockAccessList {
    if k <= 1 {
        return bal;
    }
    let k = u64::from(k);
    let mut out = bal;
    for acct in out.iter_mut() {
        for slot in acct.storage_changes.iter_mut() {
            let mut seen: std::collections::BTreeMap<u64, usize> = Default::default();
            let mut kept: Vec<alloy_eip7928::StorageChange> =
                Vec::with_capacity(slot.changes.len());
            for c in slot.changes.iter() {
                let ci = chunk_of(c.block_access_index, k);
                match seen.get(&ci) {
                    // Later write in the same chunk wins (chunk-final value).
                    Some(&pos) => {
                        kept[pos] = alloy_eip7928::StorageChange {
                            block_access_index: ci,
                            new_value: c.new_value,
                        }
                    }
                    None => {
                        seen.insert(ci, kept.len());
                        kept.push(alloy_eip7928::StorageChange {
                            block_access_index: ci,
                            new_value: c.new_value,
                        });
                    }
                }
            }
            slot.changes = kept;
        }
        dedup_changes(&mut acct.balance_changes, k, |c| &mut c.block_access_index);
        dedup_changes(&mut acct.nonce_changes, k, |c| &mut c.block_access_index);
        dedup_changes(&mut acct.code_changes, k, |c| &mut c.block_access_index);
    }
    out
}

/// Quantize the index of each change and keep only the LAST entry per chunk
/// (entries are index-ascending as revm emits them).
fn dedup_changes<T>(changes: &mut Vec<T>, k: u64, index_of: impl Fn(&mut T) -> &mut u64) {
    let mut i = 0;
    while i < changes.len() {
        let ci = {
            let idx = index_of(&mut changes[i]);
            let ci = chunk_of(*idx, k);
            *idx = ci;
            ci
        };
        // Remove a previous entry with the same chunk (this one is later).
        if i > 0 {
            let prev = *index_of(&mut changes[i - 1]);
            if prev == ci {
                changes.remove(i - 1);
                continue;
            }
        }
        i += 1;
    }
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
        // indices 1,4 -> chunk 1 (keep 40); 6 -> chunk 2.
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
        // balances 2,3 -> chunk 1, keep LAST (3).
        assert_eq!(out[0].balance_changes.len(), 1);
        assert_eq!(out[0].balance_changes[0].post_balance, U256::from(3));
        assert_eq!(out[0].balance_changes[0].block_access_index, 1);
    }
}
