//! Seeded parallel batch re-execution (spec:
//! `docs/agents/bal-attribution-parallel-validation-spec.md`, v3).
//!
//! # Why this can be FULLY parallel
//!
//! The BAL carries write **values**, not just locations. So each batch of
//! txs can have its inputs SEEDED from the BAL's own claims: for every
//! account/slot the batch reads, the value is either the latest claimed
//! write by an earlier tx, or the pre-block snapshot. No batch waits for
//! another — conflicts are resolved by value-passing, not ordering.
//!
//! # Why seeding from unverified claims is sound
//!
//! Verification is an INDUCTION anchored at the snapshot. Batch 1 executes
//! against pure pre-block state (ground truth), so if its computed writes
//! equal its claimed writes, those claims are true. Batch 2's seeds are
//! then verified-true inputs, and so on: a claim is always checked at the
//! batch that PRODUCES it, so a false claim cannot be laundered by
//! downstream batches that merely consume it. EVM determinism then forces
//! every verified batch to equal what sequential execution would produce.
//!
//! Any mismatch → the caller records a divergence and fail-stops. The
//! validator's other checks (per-tx receipts, merged write-set hash) are
//! unchanged and still run.

use std::collections::BTreeMap;

use alloy_primitives::{Address, B256, U256};
use kardamom_engine::WriteSet;

/// A BAL claim indexed for seeding: per (address, slot) and per account
/// field, the ordered `(bal_index, value)` writes the executor claimed.
///
/// `bal_index` follows revm's convention: 0 = pre-execution, 1..=n = txs in
/// block order (or chunk ordinals when the frame's granularity K > 1).
#[derive(Debug, Default, Clone)]
pub struct ClaimIndex {
    /// (address, slot) → ordered (bal_index, post-value).
    pub storage: BTreeMap<(Address, B256), Vec<(u64, U256)>>,
    /// address → ordered (bal_index, post-balance).
    pub balance: BTreeMap<Address, Vec<(u64, U256)>>,
    /// address → ordered (bal_index, post-nonce).
    pub nonce: BTreeMap<Address, Vec<(u64, u64)>>,
    /// Read-only slots per account (attribution only; not seeds).
    pub reads: BTreeMap<Address, Vec<B256>>,
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
        }
        out
    }

    /// Latest claimed storage value written STRICTLY BEFORE `bal_index`,
    /// i.e. the seed a batch starting at that index must observe. `None`
    /// ⇒ no earlier claim; the pre-block snapshot value stands.
    pub fn storage_seed(&self, addr: Address, slot: B256, before: u64) -> Option<U256> {
        self.storage
            .get(&(addr, slot))
            .and_then(|w| w.iter().rev().find(|(i, _)| *i < before).map(|(_, v)| *v))
    }

    /// Latest claimed balance strictly before `bal_index`.
    pub fn balance_seed(&self, addr: Address, before: u64) -> Option<U256> {
        self.balance
            .get(&addr)
            .and_then(|w| w.iter().rev().find(|(i, _)| *i < before).map(|(_, v)| *v))
    }

    /// Latest claimed nonce strictly before `bal_index`.
    pub fn nonce_seed(&self, addr: Address, before: u64) -> Option<u64> {
        self.nonce
            .get(&addr)
            .and_then(|w| w.iter().rev().find(|(i, _)| *i < before).map(|(_, v)| *v))
    }

    /// The claim set attributable to bal indices in `[from, to]` — what a
    /// batch covering those indices must have produced, as a WriteSet-shaped
    /// map for comparison against re-execution.
    pub fn claims_in_range(&self, from: u64, to: u64) -> ClaimSlice {
        let mut storage = BTreeMap::new();
        for (key, writes) in &self.storage {
            if let Some((_, v)) = writes.iter().rev().find(|(i, _)| *i >= from && *i <= to) {
                storage.insert(*key, *v);
            }
        }
        let mut balance = BTreeMap::new();
        for (addr, writes) in &self.balance {
            if let Some((_, v)) = writes.iter().rev().find(|(i, _)| *i >= from && *i <= to) {
                balance.insert(*addr, *v);
            }
        }
        let mut nonce = BTreeMap::new();
        for (addr, writes) in &self.nonce {
            if let Some((_, v)) = writes.iter().rev().find(|(i, _)| *i >= from && *i <= to) {
                nonce.insert(*addr, *v);
            }
        }
        ClaimSlice {
            storage,
            balance,
            nonce,
        }
    }
}

/// The batch-final claimed values over a bal-index range.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ClaimSlice {
    pub storage: BTreeMap<(Address, B256), U256>,
    pub balance: BTreeMap<Address, U256>,
    pub nonce: BTreeMap<Address, u64>,
}

impl ClaimSlice {
    /// Project a re-executed batch's merged `WriteSet` into the same shape,
    /// so verification is a structural equality.
    pub fn from_write_set(ws: &WriteSet) -> Self {
        let mut balance = BTreeMap::new();
        let mut nonce = BTreeMap::new();
        for (addr, (n, bal, _code)) in &ws.accounts {
            balance.insert(*addr, *bal);
            nonce.insert(*addr, *n);
        }
        Self {
            storage: ws.storage.clone(),
            balance,
            nonce,
        }
    }

    /// Human-readable first difference, for the divergence reason.
    pub fn diff_summary(&self, other: &Self) -> String {
        for (k, v) in &self.storage {
            match other.storage.get(k) {
                Some(o) if o == v => {}
                Some(o) => {
                    return format!("storage {:?}/{:?}: claimed {v}, recomputed {o}", k.0, k.1);
                }
                None => {
                    return format!(
                        "storage {:?}/{:?}: claimed {v}, recomputed absent",
                        k.0, k.1
                    );
                }
            }
        }
        for (k, v) in &other.storage {
            if !self.storage.contains_key(k) {
                return format!("storage {:?}/{:?}: unclaimed write {v}", k.0, k.1);
            }
        }
        for (a, v) in &self.balance {
            match other.balance.get(a) {
                Some(o) if o == v => {}
                Some(o) => return format!("balance {a:?}: claimed {v}, recomputed {o}"),
                None => return format!("balance {a:?}: claimed {v}, recomputed absent"),
            }
        }
        for (a, v) in &self.nonce {
            match other.nonce.get(a) {
                Some(o) if o == v => {}
                Some(o) => return format!("nonce {a:?}: claimed {v}, recomputed {o}"),
                None => return format!("nonce {a:?}: claimed {v}, recomputed absent"),
            }
        }
        for (a, v) in &other.balance {
            if !self.balance.contains_key(a) {
                return format!("balance {a:?}: unclaimed write {v}");
            }
        }
        "sets differ".to_string()
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
        // Contiguous, no gaps, no overlap.
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
        // A batch starting at tx1 sees no earlier claim → snapshot value.
        assert_eq!(idx.storage_seed(addr(1), slot(9), 1), None);
        // A batch starting at tx4 must see tx1's value, NOT tx4's own.
        assert_eq!(idx.storage_seed(addr(1), slot(9), 4), Some(U256::from(10)));
        // A batch starting at tx6 sees tx4's.
        assert_eq!(idx.storage_seed(addr(1), slot(9), 6), Some(U256::from(40)));
        // Later than every claim → the last one.
        assert_eq!(idx.storage_seed(addr(1), slot(9), 99), Some(U256::from(70)));
        // Untouched slot → no seed.
        assert_eq!(idx.storage_seed(addr(2), slot(9), 5), None);
    }

    #[test]
    fn claims_in_range_is_the_batch_final_value() {
        let idx = index_with(vec![
            (addr(1), slot(9), 1, 10),
            (addr(1), slot(9), 4, 40),
            (addr(1), slot(9), 7, 70),
        ]);
        // Batch covering tx1..=5 must claim tx4's value (the last in range).
        let s = idx.claims_in_range(1, 5);
        assert_eq!(s.storage.get(&(addr(1), slot(9))), Some(&U256::from(40)));
        // Batch covering tx6..=10 claims tx7's.
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
        // An unclaimed write is also caught.
        let mut c = a.clone();
        c.storage.insert((addr(3), slot(4)), U256::from(9));
        assert!(a.diff_summary(&c).contains("unclaimed write"));
    }
}
