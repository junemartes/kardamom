//! Footprint classifier: per `(to, selector)`, learn which state cells a
//! call touches, from the slot addresses the BAL already reports.
//!
//! HISTORY, because the simplification is the interesting part: this began
//! by recovering mapping base-slots through keccak INVERSION — testing
//! observed slots against `keccak(pad(sender|arg) ++ pad(p))` for small
//! `p` — so that a mapping entry could be predicted for a caller never
//! seen before. That machinery was measured and REMOVED: it changed no
//! schedule on any workload (identical critical-path ratios, zero missed
//! pairs, on uniswap at 96/24/12 senders, plain transfers, and the
//! CLOB-heavy worst case), while costing 93% of all CPU before caching.
//!
//! Two txs share a derived key only when they touch the SAME mapping
//! entry, and in practice such pairs already share something the cheap
//! tiers predict — the pool's fixed reserve slots, or their own sender
//! accounts. So derived keys generated cells that never matched between
//! transactions: pure cost, no edges.
//!
//! What remains needs no hashing at all:
//! - TIER 1, from the envelope: the sender's account cell, and the
//!   recipient's when value moves.
//! - TIER 3, from the BAL: slot addresses a selector touches on most
//!   calls. A fixed slot has the SAME address every call, so the observed
//!   hash IS the key.
//!
//! The cost of being wrong is bounded and self-reporting: a footprint the
//! classifier cannot predict becomes a missed edge, which the engine
//! repairs by wounding that one tx, and which the P1 shadow counts as
//! `footprint_false_independent_total`. If a workload ever appears whose
//! contention IS a shared mapping entry with nothing else in common (many
//! txs crediting one recipient; an airdrop claim), that counter is where
//! it will show up first.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use alloy_primitives::{Address, B256};

use crate::{Cell, TxObs};

/// A SCHEDULING key: the identity of a contention domain.
///
/// Two txs conflict when they name the same key, which is all the
/// scheduler ever asks. Both variants come straight from what is already
/// known — the envelope, or a slot address the BAL reported — so building
/// one costs no hashing at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DomainKey {
    /// Account cell (balance+nonce): tier-1 sender/recipient, fee sink.
    Account(Address),
    /// A slot this selector touches on most calls, at the address the BAL
    /// reported. Fixed slots have the same address every call, so the
    /// observed hash IS the key — no formula, no inversion.
    Fixed(Address, B256),
}

/// Learned footprint of one `(to, selector)`.
#[derive(Debug, Default, Clone)]
pub struct SelectorStats {
    pub observations: u64,
    /// Slot addresses seen, with counts. A slot present on most calls is
    /// FIXED (predicted); one that appears rarely is footprint this
    /// classifier does not model — a missed edge at worst, wound-repaired.
    pub slot_seen: BTreeMap<(Address, B256), u64>,
    /// Total slot-observations (denominator for shares).
    pub slot_obs: u64,
    /// Accounts touched, with counts — the fee-sink-style hot accounts.
    pub account_seen: BTreeMap<Address, u64>,
}

/// Live-stats entry cap (spec "Stats footprint"): bounded cardinality
/// whose eviction is free BY CONSTRUCTION — a cold selector schedules as
/// `Tail` with or without stats, so entries the cap refuses lose nothing.
const MAX_SELECTORS: usize = 16_384;

/// Aggregate stats over observations. Batch (`learn`) for the offline lab,
/// incremental (`learn_obs`) for the live shadow — one code path.
#[derive(Debug, Clone, Default)]
pub struct Stats {
    pub by_selector: HashMap<(Address, [u8; 4]), SelectorStats>,
}

impl Stats {
    pub fn learn(obs: &[TxObs]) -> Self {
        let mut s = Self::default();
        for o in obs {
            s.learn_obs(o);
        }
        s
    }

    /// Fold one observation into the stats: one map operation per touched
    /// cell, no hashing. At the entry cap, unseen selectors are NOT
    /// inserted — they stay cold and schedule as `Tail`, which is exactly
    /// what eviction would cost.
    pub fn learn_obs(&mut self, o: &TxObs) {
        let (Some(to), Some(sel)) = (o.to, o.selector) else {
            return;
        };
        if self.by_selector.len() >= MAX_SELECTORS && !self.by_selector.contains_key(&(to, sel)) {
            return;
        }
        let e = self.by_selector.entry((to, sel)).or_default();
        e.observations += 1;
        for cell in o.reads.iter().chain(o.writes.iter()) {
            match cell {
                Cell::Slot(addr, slot) => {
                    e.slot_obs += 1;
                    *e.slot_seen.entry((*addr, *slot)).or_default() += 1;
                }
                Cell::Account(a) => {
                    *e.account_seen.entry(*a).or_default() += 1;
                }
            }
        }
    }

    /// Predict the cell set of a holdout observation from learned
    /// formulas + fixed slots. Returns None when the selector was never
    /// trained (cold — Tail in the scheduler).
    pub fn predict(&self, o: &TxObs) -> Option<BTreeSet<Cell>> {
        let mut cells = BTreeSet::new();
        // Tier-1 exact (always available from the envelope).
        cells.insert(Cell::Account(o.sender));
        if o.has_value
            && let Some(to) = o.to
        {
            cells.insert(Cell::Account(to));
        }
        // A selector-less tx (native transfer / create) is FULLY covered
        // by tier-1 — exact, no stats needed, never cold.
        let (Some(to), Some(sel)) = (o.to, o.selector) else {
            return Some(cells);
        };
        let e = self.by_selector.get(&(to, sel))?;
        // Fixed slots: seen in >=60% of observations.
        for ((addr, slot), n) in &e.slot_seen {
            if *n * 10 >= e.observations * 6 {
                cells.insert(Cell::Slot(*addr, *slot));
            }
        }
        // Frequently-touched accounts (fee-sink style) predict as touched.
        for (a, n) in &e.account_seen {
            if *n * 10 >= e.observations * 6 {
                cells.insert(Cell::Account(*a));
            }
        }
        Some(cells)
    }

    /// Predict the SCHEDULING domains of a tx — the hot-path predictor.
    /// Same structure as [`Stats::predict`] (same tiers, same cold
    /// semantics: `None` = untrained selector = ⊤), but it names mapping
    /// entries symbolically instead of hashing them, which is what keeps
    /// admission cheap enough to stay off the critical path.
    pub fn predict_domains(&self, o: &TxObs) -> Option<Vec<DomainKey>> {
        let mut keys: Vec<DomainKey> = Vec::with_capacity(8);
        keys.push(DomainKey::Account(o.sender));
        if o.has_value
            && let Some(to) = o.to
        {
            keys.push(DomainKey::Account(to));
        }
        let (Some(to), Some(sel)) = (o.to, o.selector) else {
            // Selector-less (native transfer / create): tier-1 is the
            // whole footprint — exact, never cold.
            keys.sort_unstable();
            keys.dedup();
            return Some(keys);
        };
        let e = self.by_selector.get(&(to, sel))?;
        for ((addr, slot), n) in &e.slot_seen {
            if *n * 10 >= e.observations * 6 {
                keys.push(DomainKey::Fixed(*addr, *slot));
            }
        }
        for (a, n) in &e.account_seen {
            if *n * 10 >= e.observations * 6 {
                keys.push(DomainKey::Account(*a));
            }
        }
        keys.sort_unstable();
        keys.dedup();
        Some(keys)
    }

    /// Slot-observation shares for the report: (predicted-as-fixed,
    /// total). The remainder is footprint this classifier does not model —
    /// a missed edge at worst, which the engine wound-repairs and the
    /// shadow counts.
    pub fn class_shares(&self) -> (u64, u64) {
        let (mut fixedish, mut total) = (0u64, 0u64);
        for e in self.by_selector.values() {
            total += e.slot_obs;
            for n in e.slot_seen.values() {
                if *n * 10 >= e.observations.max(1) * 6 {
                    fixedish += *n;
                }
            }
        }
        (fixedish, total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{U256, address};

    const POOL: Address = address!("00000000000000000000000000000000000000E0");
    const SEL: [u8; 4] = [0xa9, 0x05, 0x9c, 0xbb];
    /// A slot every call of this selector touches — a pool's reserves.
    const RESERVES: B256 = B256::with_last_byte(3);

    fn addr(i: u8) -> Address {
        let mut b = [0u8; 20];
        b[19] = i;
        Address::from(b)
    }

    /// A swap-shaped observation: the sender's account, plus the pool's
    /// fixed reserve slot that every call touches.
    fn swap_obs(index: u64, sender: Address) -> TxObs {
        TxObs {
            index,
            block: 1,
            sender,
            to: Some(POOL),
            selector: Some(SEL),
            args: vec![U256::from(1u64)],
            gas: 30_000,
            has_value: false,
            reads: vec![Cell::Slot(POOL, RESERVES)],
            writes: vec![Cell::Account(sender), Cell::Slot(POOL, RESERVES)],
        }
    }

    /// The property the whole design rests on now: a slot touched by most
    /// calls is predicted from its OBSERVED address, for a caller the
    /// classifier has never seen.
    #[test]
    fn fixed_slot_predicts_for_an_unseen_sender() {
        let mut stats = Stats::default();
        for i in 0..4 {
            stats.learn_obs(&swap_obs(i, addr(i as u8 + 1)));
        }
        let unseen = swap_obs(99, addr(200));
        let p = stats.predict(&unseen).expect("selector is trained");
        assert!(
            p.contains(&Cell::Slot(POOL, RESERVES)),
            "the pool's fixed slot must be predicted: {p:?}"
        );
        assert!(p.contains(&Cell::Account(addr(200))), "tier-1 sender");

        let d = stats.predict_domains(&unseen).expect("trained");
        assert!(d.contains(&DomainKey::Fixed(POOL, RESERVES)));
        assert!(d.contains(&DomainKey::Account(addr(200))));
    }

    /// Two calls of the same selector name the same fixed domain — which
    /// is what chains them — while their sender domains stay distinct.
    #[test]
    fn same_domain_for_the_pool_distinct_for_senders() {
        let mut stats = Stats::default();
        for i in 0..4 {
            stats.learn_obs(&swap_obs(i, addr(i as u8 + 1)));
        }
        let a = stats.predict_domains(&swap_obs(10, addr(50))).unwrap();
        let b = stats.predict_domains(&swap_obs(11, addr(51))).unwrap();
        let shared: Vec<_> = a.iter().filter(|k| b.contains(k)).collect();
        assert_eq!(
            shared,
            vec![&DomainKey::Fixed(POOL, RESERVES)],
            "the pool is the shared domain; senders are not"
        );
    }

    /// A slot seen on only a few calls is NOT predicted — the classifier
    /// does not model it, so it becomes a missed edge the engine wounds
    /// rather than a false one that serialises.
    #[test]
    fn rare_slot_is_not_predicted() {
        let mut stats = Stats::default();
        for i in 0..10 {
            let mut o = swap_obs(i, addr(i as u8 + 1));
            if i == 0 {
                // One call touches an extra slot.
                o.writes.push(Cell::Slot(POOL, B256::with_last_byte(0x77)));
            }
            stats.learn_obs(&o);
        }
        let p = stats.predict(&swap_obs(20, addr(90))).unwrap();
        assert!(p.contains(&Cell::Slot(POOL, RESERVES)));
        assert!(!p.contains(&Cell::Slot(POOL, B256::with_last_byte(0x77))));
    }

    #[test]
    fn cold_selector_predicts_none_but_plain_transfer_is_tier1() {
        let stats = Stats::default();
        let cold = swap_obs(0, addr(1));
        assert!(stats.predict(&cold).is_none(), "unseen selector is cold");
        assert!(stats.predict_domains(&cold).is_none());

        let native = TxObs {
            selector: None,
            has_value: true,
            ..swap_obs(0, addr(1))
        };
        let p = stats.predict(&native).expect("tier-1 never cold");
        assert!(p.contains(&Cell::Account(addr(1))));
    }

    #[test]
    fn incremental_learning_matches_batch() {
        let obs: Vec<TxObs> = (0..4).map(|i| swap_obs(i, addr(i as u8 + 1))).collect();
        let batch = Stats::learn(&obs);
        let mut inc = Stats::default();
        for o in &obs {
            inc.learn_obs(o);
        }
        let key = (POOL, SEL);
        let (be, ie) = (&batch.by_selector[&key], &inc.by_selector[&key]);
        assert_eq!(be.observations, ie.observations);
        assert_eq!(be.slot_seen, ie.slot_seen);
        assert_eq!(be.account_seen, ie.account_seen);
    }
}
