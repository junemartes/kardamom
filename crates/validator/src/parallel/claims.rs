//! BAL claim indexing: the seed/comparison views of the executor's claimed
//! writes (see the module doc in `mod.rs` for the induction argument).

use std::collections::BTreeMap;

use alloy_primitives::{Address, B256, U256};

/// Latest value written strictly before `bal_index` in an ordered
/// `(bal_index, value)` claim list. This is the seed a batch starting at
/// that index must observe. `None` means no earlier claim exists, so the
/// pre-block snapshot value stands.
fn latest_before<'a, K: Ord, T>(
    map: &'a BTreeMap<K, Vec<(u64, T)>>,
    key: &K,
    before: u64,
) -> Option<&'a T> {
    map.get(key)
        .and_then(|w| w.iter().rev().find(|(i, _)| *i < before).map(|(_, v)| v))
}

/// Batch-final claim per key over the inclusive bal-index range
/// `[from, to]`, projected through `f`.
fn last_in_range<K: Copy + Ord, T, U>(
    map: &BTreeMap<K, Vec<(u64, T)>>,
    from: u64,
    to: u64,
    f: impl Fn(&T) -> U,
) -> BTreeMap<K, U> {
    map.iter()
        .filter_map(|(key, writes)| {
            writes
                .iter()
                .rev()
                .find(|(i, _)| *i >= from && *i <= to)
                .map(|(_, v)| (*key, f(v)))
        })
        .collect()
}

/// A BAL claim indexed for seeding: for each address and slot, and for
/// each account field, the ordered `(bal_index, value)` writes the
/// executor claimed.
///
/// `bal_index` follows revm's convention: 0 is pre-execution, and 1..=n
/// are txs in block order (or chunk numbers when the frame's granularity
/// K > 1).
#[derive(Debug, Default, Clone)]
pub struct ClaimIndex {
    /// Maps (address, slot) to ordered (bal_index, post-value) pairs.
    pub storage: BTreeMap<(Address, B256), Vec<(u64, U256)>>,
    /// Maps address to ordered (bal_index, post-balance) pairs.
    pub balance: BTreeMap<Address, Vec<(u64, U256)>>,
    /// Maps address to ordered (bal_index, post-nonce) pairs.
    pub nonce: BTreeMap<Address, Vec<(u64, u64)>>,
    /// Read-only slots per account. Used for attribution only, not for seeds.
    pub reads: BTreeMap<Address, Vec<B256>>,
    /// Maps address to ordered (bal_index, deployed code) pairs. Code is
    /// also a seed: a CREATE in chunk i, followed by a CALL in chunk j >
    /// i in one block, must seed chunk j with the bytecode, not only the
    /// account entry. Without this, every cross-chunk call to a
    /// same-block contract sees empty code instead of running it.
    pub code: BTreeMap<Address, Vec<(u64, bytes::Bytes)>>,
}

impl ClaimIndex {
    /// Build from the decoded EIP-7928 access list.
    pub fn from_alloy(bal: &alloy_eip7928::BlockAccessList) -> Self {
        let mut out = Self::default();
        for acct in bal.iter() {
            let addr = acct.address;
            for slot in &acct.storage_changes {
                let key = (addr, B256::from(slot.slot.to_be_bytes::<32>()));
                let entry = out.storage.entry(key).or_default();
                for c in &slot.changes {
                    entry.push((c.block_access_index, c.new_value));
                }
                entry.sort_by_key(|(i, _)| *i);
            }
            if !acct.storage_reads.is_empty() {
                out.reads.insert(
                    addr,
                    acct.storage_reads
                        .iter()
                        .map(|s| B256::from(s.to_be_bytes::<32>()))
                        .collect(),
                );
            }
            if !acct.balance_changes.is_empty() {
                let mut v: Vec<(u64, U256)> = acct
                    .balance_changes
                    .iter()
                    .map(|c| (c.block_access_index, c.post_balance))
                    .collect();
                v.sort_by_key(|(i, _)| *i);
                out.balance.insert(addr, v);
            }
            if !acct.nonce_changes.is_empty() {
                let mut v: Vec<(u64, u64)> = acct
                    .nonce_changes
                    .iter()
                    .map(|c| (c.block_access_index, c.new_nonce))
                    .collect();
                v.sort_by_key(|(i, _)| *i);
                out.nonce.insert(addr, v);
            }
            if !acct.code_changes.is_empty() {
                let mut v: Vec<(u64, bytes::Bytes)> = acct
                    .code_changes
                    .iter()
                    .map(|c| {
                        (
                            c.block_access_index,
                            bytes::Bytes::copy_from_slice(c.new_code.as_ref()),
                        )
                    })
                    .collect();
                v.sort_by_key(|(i, _)| *i);
                out.code.insert(addr, v);
            }
        }
        out
    }

    /// Latest claimed storage value written strictly before `bal_index`,
    /// the seed a batch starting at that index must observe. `None` means
    /// no earlier claim exists, so the pre-block snapshot value stands.
    pub fn storage_seed(&self, addr: Address, slot: B256, before: u64) -> Option<U256> {
        latest_before(&self.storage, &(addr, slot), before).copied()
    }

    /// Latest claimed balance strictly before `bal_index`.
    pub fn balance_seed(&self, addr: Address, before: u64) -> Option<U256> {
        latest_before(&self.balance, &addr, before).copied()
    }

    /// Latest claimed code strictly before `bal_index`.
    pub fn code_seed(&self, addr: Address, before: u64) -> Option<&bytes::Bytes> {
        latest_before(&self.code, &addr, before)
    }

    /// Latest claimed nonce strictly before `bal_index`.
    pub fn nonce_seed(&self, addr: Address, before: u64) -> Option<u64> {
        latest_before(&self.nonce, &addr, before).copied()
    }

    /// The claim set attributable to bal indices in `[from, to]`: what a
    /// batch covering those indices must have produced, as a
    /// WriteSet-shaped map for comparison against re-execution.
    pub fn claims_in_range(&self, from: u64, to: u64) -> ClaimSlice {
        ClaimSlice {
            storage: last_in_range(&self.storage, from, to, |v| *v),
            balance: last_in_range(&self.balance, from, to, |v| *v),
            nonce: last_in_range(&self.nonce, from, to, |v| *v),
            code: last_in_range(&self.code, from, to, |c| alloy_primitives::keccak256(c)),
        }
    }
}

/// The batch-final claimed values over a bal-index range.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ClaimSlice {
    pub storage: BTreeMap<(Address, B256), U256>,
    pub balance: BTreeMap<Address, U256>,
    pub nonce: BTreeMap<Address, u64>,
    /// The keccak hash of the unit-final claimed code. The bytes stay out
    /// of the comparison struct; the hash pins them.
    pub code: BTreeMap<Address, B256>,
}

/// One claimed-vs-recomputed pass of [`ClaimSlice::diff_summary`]. Finds
/// the first key whose claimed value the recomputation contradicts or lacks.
fn first_mismatch<K: Ord, V: PartialEq + std::fmt::Display>(
    field: &str,
    claimed: &BTreeMap<K, V>,
    computed: &BTreeMap<K, V>,
    describe: impl Fn(&K) -> String,
) -> Option<String> {
    for (k, v) in claimed {
        match computed.get(k) {
            Some(o) if o == v => {}
            Some(o) => {
                return Some(format!(
                    "{field} {}: claimed {v}, recomputed {o}",
                    describe(k)
                ));
            }
            None => {
                return Some(format!(
                    "{field} {}: claimed {v}, recomputed absent",
                    describe(k)
                ));
            }
        }
    }
    None
}

/// The reverse pass: find the first recomputed write the claims never mention.
fn first_unclaimed<K: Ord, V: std::fmt::Display>(
    field: &str,
    verb: &str,
    claimed: &BTreeMap<K, V>,
    computed: &BTreeMap<K, V>,
    describe: impl Fn(&K) -> String,
) -> Option<String> {
    computed
        .iter()
        .find(|(k, _)| !claimed.contains_key(k))
        .map(|(k, v)| format!("{field} {}: {verb} {v}", describe(k)))
}

impl ClaimSlice {
    /// Human-readable first difference, for the divergence reason.
    pub fn diff_summary(&self, other: &Self) -> String {
        let slot_key = |k: &(Address, B256)| format!("{:?}/{:?}", k.0, k.1);
        let addr_key = |a: &Address| format!("{a:?}");
        // Keep this pass order: it is load-bearing for message stability.
        // Check storage both ways, then balance and nonce mismatches
        // before their unclaimed passes, then code.
        first_mismatch("storage", &self.storage, &other.storage, slot_key)
            .or_else(|| {
                first_unclaimed(
                    "storage",
                    "unclaimed write",
                    &self.storage,
                    &other.storage,
                    slot_key,
                )
            })
            .or_else(|| first_mismatch("balance", &self.balance, &other.balance, addr_key))
            .or_else(|| first_mismatch("nonce", &self.nonce, &other.nonce, addr_key))
            .or_else(|| {
                first_unclaimed(
                    "balance",
                    "unclaimed write",
                    &self.balance,
                    &other.balance,
                    addr_key,
                )
            })
            .or_else(|| {
                first_unclaimed(
                    "nonce",
                    "unclaimed write",
                    &self.nonce,
                    &other.nonce,
                    addr_key,
                )
            })
            .or_else(|| first_mismatch("code", &self.code, &other.code, addr_key))
            .or_else(|| {
                first_unclaimed(
                    "code",
                    "unclaimed deploy",
                    &self.code,
                    &other.code,
                    addr_key,
                )
            })
            .unwrap_or_else(|| "sets differ".to_string())
    }
}

/// Split `n` transactions into batches of at most `batch_size`, returning
/// inclusive bal-index ranges (`1..=n`, matching revm's convention).
pub fn batch_ranges(n: usize, batch_size: usize) -> Vec<(u64, u64)> {
    let bs = batch_size.max(1);
    (0..n)
        .step_by(bs)
        .map(|start| {
            let end = (start + bs).min(n);
            ((start + 1) as u64, end as u64)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(b: u8) -> Address {
        Address::repeat_byte(b)
    }
    fn slot(b: u8) -> B256 {
        B256::repeat_byte(b)
    }

    #[test]
    fn batch_ranges_cover_every_tx_once() {
        assert_eq!(batch_ranges(0, 5), vec![]);
        assert_eq!(batch_ranges(3, 5), vec![(1, 3)]);
        assert_eq!(batch_ranges(10, 5), vec![(1, 5), (6, 10)]);
        assert_eq!(batch_ranges(12, 5), vec![(1, 5), (6, 10), (11, 12)]);
        // Ranges must be contiguous, with no gaps and no overlap.
        let r = batch_ranges(97, 10);
        assert_eq!(r.first().unwrap().0, 1);
        assert_eq!(r.last().unwrap().1, 97);
        for w in r.windows(2) {
            assert_eq!(w[0].1 + 1, w[1].0);
        }
    }

    fn index_with(writes: Vec<(Address, B256, u64, u64)>) -> ClaimIndex {
        let mut idx = ClaimIndex::default();
        for (a, s, i, v) in writes {
            idx.storage
                .entry((a, s))
                .or_default()
                .push((i, U256::from(v)));
        }
        for w in idx.storage.values_mut() {
            w.sort_by_key(|(i, _)| *i);
        }
        idx
    }

    #[test]
    fn seed_is_the_latest_claim_strictly_before_the_batch() {
        // tx1 writes 10, tx4 writes 40, tx7 writes 70.
        let idx = index_with(vec![
            (addr(1), slot(9), 1, 10),
            (addr(1), slot(9), 4, 40),
            (addr(1), slot(9), 7, 70),
        ]);
        // A batch starting at tx1 sees no earlier claim, so it uses the snapshot value.
        assert_eq!(idx.storage_seed(addr(1), slot(9), 1), None);
        // A batch starting at tx4 must see tx1's value, not tx4's own.
        assert_eq!(idx.storage_seed(addr(1), slot(9), 4), Some(U256::from(10)));
        // A batch starting at tx6 sees tx4's value.
        assert_eq!(idx.storage_seed(addr(1), slot(9), 6), Some(U256::from(40)));
        // Later than every claim: the seed is the last one.
        assert_eq!(idx.storage_seed(addr(1), slot(9), 99), Some(U256::from(70)));
        // An untouched slot has no seed.
        assert_eq!(idx.storage_seed(addr(2), slot(9), 5), None);
    }

    #[test]
    fn claims_in_range_is_the_batch_final_value() {
        let idx = index_with(vec![
            (addr(1), slot(9), 1, 10),
            (addr(1), slot(9), 4, 40),
            (addr(1), slot(9), 7, 70),
        ]);
        // A batch covering tx1..=5 must claim tx4's value, the last one in range.
        let s = idx.claims_in_range(1, 5);
        assert_eq!(s.storage.get(&(addr(1), slot(9))), Some(&U256::from(40)));
        // A batch covering tx6..=10 claims tx7's value.
        let s = idx.claims_in_range(6, 10);
        assert_eq!(s.storage.get(&(addr(1), slot(9))), Some(&U256::from(70)));
        // A range with no writes claims nothing for that slot.
        let s = idx.claims_in_range(2, 3);
        assert!(s.storage.is_empty());
    }

    #[test]
    fn diff_summary_names_the_first_mismatch() {
        let mut a = ClaimSlice::default();
        a.storage.insert((addr(1), slot(2)), U256::from(5));
        let mut b = a.clone();
        assert_eq!(a, b);
        b.storage.insert((addr(1), slot(2)), U256::from(6));
        let msg = a.diff_summary(&b);
        assert!(msg.contains("claimed 5"), "{msg}");
        assert!(msg.contains("recomputed 6"), "{msg}");
        // The check also catches an unclaimed write.
        let mut c = a.clone();
        c.storage.insert((addr(3), slot(4)), U256::from(9));
        assert!(a.diff_summary(&c).contains("unclaimed write"));
    }
}
