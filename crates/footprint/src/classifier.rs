//! Footprint classifier: for each `(to, selector)` pair, learn which state
//! cells a call touches, from the slot addresses the BAL already reports.
//!
//! What remains needs no hashing at all:
//! - Tier 1, from the envelope: the sender's account cell, and the
//!   recipient's account cell when value moves.
//! - Tier 3, from the BAL: slot addresses a selector touches on most
//!   calls. A fixed slot has the same address every call, so the observed
//!   hash is the key.
//!
//! The cost of a wrong prediction is bounded and visible: a footprint the
//! classifier cannot predict becomes a missed edge. The engine repairs this
//! by wounding that one tx, and the shadow scheduler counts it as
//! `footprint_false_independent_total`. If a workload's contention is a
//! shared mapping entry with nothing else in common (for example, many
//! txs crediting one recipient, as in an airdrop claim), this counter is
//! where it shows up first.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use alloy_primitives::{Address, B256};

use crate::{Cell, TxObs};

/// A scheduling key: the identity of a contention domain.
///
/// Two txs conflict when they name the same key, which is all the
/// scheduler ever asks. Both variants come straight from what is already
/// known — the envelope, or a slot address the BAL reported — so building
/// one needs no hashing at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DomainKey {
    /// Account cell (balance and nonce): tier-1 sender or recipient, or a
    /// fee sink.
    Account(Address),
    /// A slot this selector touches on most calls, at the address the BAL
    /// reported. Fixed slots have the same address every call, so the
    /// observed hash is the key — no formula, no inversion.
    Fixed(Address, B256),
}

/// Learned footprint of one `(to, selector)` pair.
#[derive(Debug, Default, Clone)]
pub struct SelectorStats {
    pub observations: u64,
    /// Slot addresses seen, with counts. A slot present on most calls is
    /// fixed (predicted). One that appears rarely is footprint this
    /// classifier does not model — a missed edge at worst, wound-repaired.
    pub slot_seen: BTreeMap<(Address, B256), u64>,
    /// Total slot observations (denominator for shares).
    pub slot_obs: u64,
    /// Accounts touched, with counts — the fee-sink-style hot accounts.
    pub account_seen: BTreeMap<Address, u64>,
}

/// Live-stats entry cap (spec "Stats footprint"): a bounded cardinality
/// whose eviction costs nothing by construction. A cold selector schedules
/// as `Tail` with or without stats, so entries the cap refuses lose nothing.
const MAX_SELECTORS: usize = 16_384;

/// Aggregate stats over observations. `learn` runs in batch for the offline
/// lab; `learn_obs` runs incrementally for the live shadow. Both share one
/// code path.
#[derive(Debug, Clone, Default)]
pub struct Stats {
    pub by_selector: HashMap<(Address, [u8; 4]), SelectorStats>,
}

impl Stats {
    #[must_use]
    pub fn learn(obs: &[TxObs]) -> Self {
        let mut s = Self::default();
        for o in obs {
            s.learn_obs(o);
        }
        s
    }

    /// Fold one observation into the stats: one map operation per touched
    /// cell, no hashing. At the entry cap, an unseen selector is not
    /// inserted. It stays cold and schedules as `Tail`, at the same cost
    /// eviction would have.
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

    /// True when `n` observations out of `observations` total meet the
    /// fixed-prediction bar: seen on at least 60% of calls. The one
    /// spelling of this threshold; `predict`, `predict_domains`, and
    /// `class_shares` all read it through here.
    fn is_frequent(n: u64, observations: u64) -> bool {
        n.saturating_mul(10) >= observations.saturating_mul(6)
    }

    /// Tier 2/3 predicted keys for a trained selector: fixed slots and
    /// frequently touched accounts, each named through the caller's own
    /// key type. `account`/`fixed` let [`Stats::predict`] and
    /// [`Stats::predict_domains`] share this one walk of `slot_seen`/
    /// `account_seen` while returning `Cell`s or `DomainKey`s
    /// respectively.
    ///
    /// Returns `Some(vec![])` for a tx with no selector (tier 1 is the
    /// whole footprint, exact and never cold), and `None` when the
    /// selector was never trained (cold — `Tail` in the scheduler).
    fn predicted_keys<K>(
        &self,
        o: &TxObs,
        account: impl Fn(Address) -> K,
        fixed: impl Fn(Address, B256) -> K,
    ) -> Option<Vec<K>> {
        let (Some(to), Some(sel)) = (o.to, o.selector) else {
            return Some(Vec::new());
        };
        let e = self.by_selector.get(&(to, sel))?;
        let mut keys = Vec::new();
        for ((addr, slot), n) in &e.slot_seen {
            if Self::is_frequent(*n, e.observations) {
                keys.push(fixed(*addr, *slot));
            }
        }
        for (a, n) in &e.account_seen {
            if Self::is_frequent(*n, e.observations) {
                keys.push(account(*a));
            }
        }
        Some(keys)
    }

    /// Predict the cell set of a holdout observation from learned
    /// formulas and fixed slots. Returns `None` when the selector was never
    /// trained (cold — `Tail` in the scheduler).
    #[must_use]
    pub fn predict(&self, o: &TxObs) -> Option<BTreeSet<Cell>> {
        let mut cells = BTreeSet::new();
        // Tier 1, exact: always available from the envelope.
        cells.insert(Cell::Account(o.sender));
        if o.has_value
            && let Some(to) = o.to
        {
            cells.insert(Cell::Account(to));
        }
        cells.extend(self.predicted_keys(o, Cell::Account, Cell::Slot)?);
        Some(cells)
    }

    /// Predict the scheduling domains of a tx: the hot-path predictor.
    /// Same structure as [`Stats::predict`] (same tiers, same cold
    /// semantics: `None` means an untrained selector conflicts with
    /// everything), but it names mapping entries symbolically instead of
    /// hashing them. This keeps admission cheap enough to stay off the
    /// critical path.
    #[must_use]
    pub fn predict_domains(&self, o: &TxObs) -> Option<Vec<DomainKey>> {
        let mut keys: Vec<DomainKey> = Vec::with_capacity(8);
        keys.push(DomainKey::Account(o.sender));
        if o.has_value
            && let Some(to) = o.to
        {
            keys.push(DomainKey::Account(to));
        }
        keys.extend(self.predicted_keys(o, DomainKey::Account, DomainKey::Fixed)?);
        keys.sort_unstable();
        keys.dedup();
        Some(keys)
    }

    /// Slot-observation shares for the report: (predicted-as-fixed,
    /// total). The remainder is footprint this classifier does not model —
    /// a missed edge at worst, which the engine wound-repairs and the
    /// shadow counts.
    #[must_use]
    pub fn class_shares(&self) -> (u64, u64) {
        self.by_selector
            .values()
            .fold((0u64, 0u64), |(fixedish, total), e| {
                let fixed_here: u64 = e
                    .slot_seen
                    .values()
                    .filter(|n| Self::is_frequent(**n, e.observations))
                    .sum();
                (fixedish + fixed_here, total + e.slot_obs)
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::addr;
    use alloy_primitives::address;

    const POOL: Address = address!("00000000000000000000000000000000000000E0");
    const SEL: [u8; 4] = [0xa9, 0x05, 0x9c, 0xbb];
    /// A slot every call of this selector touches: a pool's reserves.
    const RESERVES: B256 = B256::with_last_byte(3);

    /// A swap-shaped observation: the sender's account, and the pool's
    /// fixed reserve slot that every call touches.
    fn swap_obs(index: u64, sender: Address) -> TxObs {
        crate::testkit::obs(
            index,
            sender,
            POOL,
            SEL,
            vec![Cell::Account(sender), Cell::Slot(POOL, RESERVES)],
            vec![Cell::Slot(POOL, RESERVES)],
        )
    }

    /// The property the whole design rests on: a slot touched by most
    /// calls is predicted from its observed address, even for a caller the
    /// classifier has never seen.
    #[test]
    fn fixed_slot_predicts_for_an_unseen_sender() {
        let mut stats = Stats::default();
        for i in 0..4 {
            #[allow(
                clippy::cast_possible_truncation,
                reason = "loop bound is small (<=10): fits u8"
            )]
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

    /// Two calls of the same selector name the same fixed domain. This is
    /// what chains them, while their sender domains stay distinct.
    #[test]
    fn same_domain_for_the_pool_distinct_for_senders() {
        let mut stats = Stats::default();
        for i in 0..4 {
            #[allow(
                clippy::cast_possible_truncation,
                reason = "loop bound is small (<=10): fits u8"
            )]
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

    /// A slot seen on only a few calls is not predicted. The classifier
    /// does not model it, so it becomes a missed edge the engine wounds,
    /// rather than a false one that forces serial order.
    #[test]
    fn rare_slot_is_not_predicted() {
        let mut stats = Stats::default();
        for i in 0..10 {
            #[allow(
                clippy::cast_possible_truncation,
                reason = "loop bound is small (<=10): fits u8"
            )]
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
        #[allow(
            clippy::cast_possible_truncation,
            reason = "loop bound is small (<=10): fits u8"
        )]
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
