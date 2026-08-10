//! Footprint classifier: per `(to, selector)`, recover mapping base-slots
//! by keccak INVERSION (an algebraic identity, not co-occurrence
//! statistics) and classify every observed slot as fixed / derived /
//! unpredictable (spec: "The graph index", Tier 2/3).

use std::collections::{BTreeMap, BTreeSet, HashMap};

use alloy_primitives::{Address, B256, U256, keccak256};

use crate::{Cell, TxObs};

/// A derivation candidate available at scheduling time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Cand {
    Sender,
    To,
    Arg(u8),
}

/// A solved mapping formula: entries live at
/// `keccak(pad32(key(outer)) ++ inner)` where `inner` is either
/// `pad32(base)` (single mapping) or `keccak(pad32(key(inner_cand)) ++
/// pad32(base))` (one nesting level).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Formula {
    pub contract: Address,
    pub base: u8,
    pub outer: Cand,
    pub inner: Option<Cand>,
}

/// Learned footprint of one `(to, selector)`.
#[derive(Debug, Default, Clone)]
pub struct SelectorStats {
    pub observations: u64,
    /// Slots present with observation counts (for the fixed test).
    pub slot_seen: BTreeMap<(Address, B256), u64>,
    /// Solved formulas with hit counts.
    pub formulas: BTreeMap<Formula, u64>,
    /// Slots that neither repeat nor solve, with counts.
    pub unpredictable: u64,
    /// Total slot-observations (denominator for shares).
    pub slot_obs: u64,
    /// Accounts (balance/nonce writes) other than the sender, keyed for
    /// the same candidate derivations (e.g. a token transfer writes the
    /// Account cell of arg0? — no: Account cells come from native value /
    /// fee flows; tracked fixed-style).
    pub account_seen: BTreeMap<Address, u64>,
}

fn cand_word(obs: &TxObs, c: Cand) -> Option<U256> {
    match c {
        Cand::Sender => Some(U256::from_be_slice(obs.sender.as_slice())),
        Cand::To => obs.to.map(|a| U256::from_be_slice(a.as_slice())),
        Cand::Arg(i) => obs.args.get(i as usize).copied(),
    }
}

const CANDS: &[Cand] = &[
    Cand::Sender,
    Cand::To,
    Cand::Arg(0),
    Cand::Arg(1),
    Cand::Arg(2),
    Cand::Arg(3),
];
const MAX_BASE: u8 = 32;

/// Live-stats entry cap (spec "Stats footprint": bounded cardinality whose
/// eviction is free BY CONSTRUCTION — a cold selector schedules as `Tail`
/// with or without stats, so entries the cap refuses lose nothing). The P0
/// working sets are tens of entries; Zipfian traffic keeps the hot set far
/// below this.
const MAX_SELECTORS: usize = 16_384;

fn keccak_pair(key: U256, inner: B256) -> B256 {
    let mut buf = [0u8; 64];
    buf[..32].copy_from_slice(&key.to_be_bytes::<32>());
    buf[32..].copy_from_slice(inner.as_slice());
    keccak256(buf)
}

/// Instantiate a KNOWN formula with this observation's words. One or two
/// keccaks — the cheap test that must be tried before brute force.
fn instantiate(f: &Formula, obs: &TxObs) -> Option<B256> {
    let base = B256::from(U256::from(f.base).to_be_bytes::<32>());
    let inner = match f.inner {
        None => base,
        Some(ic) => keccak_pair(cand_word(obs, ic)?, base),
    };
    Some(keccak_pair(cand_word(obs, f.outer)?, inner))
}

/// Try to solve `slot` as a mapping entry for this observation's
/// candidates. Single level first, then one nesting level.
///
/// COST: this is a brute force — up to `CANDS x MAX_BASE` keccaks for the
/// single level and `CANDS^2 x MAX_BASE` for the nested one, i.e. low
/// thousands of hashes for a slot that does not solve. Profiling the live
/// training loop showed it at 93% of all CPU, so callers MUST try the
/// selector's already-solved formulas first (see `learn_obs`) and reach
/// here only for genuinely novel slots.
fn solve(obs: &TxObs, contract: Address, slot: B256) -> Option<Formula> {
    let bases: Vec<B256> = (0..MAX_BASE)
        .map(|p| B256::from(U256::from(p).to_be_bytes::<32>()))
        .collect();
    for outer in CANDS {
        let Some(key) = cand_word(obs, *outer) else {
            continue;
        };
        for (p, base) in bases.iter().enumerate() {
            if keccak_pair(key, *base) == slot {
                return Some(Formula {
                    contract,
                    base: p as u8,
                    outer: *outer,
                    inner: None,
                });
            }
        }
    }
    // One nesting level: keccak(outer_key ++ keccak(inner_key ++ base)).
    for inner in CANDS {
        let Some(ik) = cand_word(obs, *inner) else {
            continue;
        };
        for (p, base) in bases.iter().enumerate() {
            let mid = keccak_pair(ik, *base);
            for outer in CANDS {
                let Some(ok) = cand_word(obs, *outer) else {
                    continue;
                };
                if keccak_pair(ok, mid) == slot {
                    return Some(Formula {
                        contract,
                        base: p as u8,
                        outer: *outer,
                        inner: Some(*inner),
                    });
                }
            }
        }
    }
    None
}

/// A SCHEDULING key: the identity of a contention domain, named
/// symbolically instead of by its keccak-derived slot address.
///
/// The scheduler only ever asks "do these two txs touch the same cell?" —
/// and two calls touch the same mapping entry exactly when their
/// `(contract, base_slot, key words)` agree. Hashing that tuple into the
/// real slot address answers the same question at the cost of a keccak per
/// predicted cell per tx (measured: 3.0us/tx, 57% of the serial feed and
/// ~20% of a uniswap tx's whole execution time). Real slot addresses are
/// needed only by VALIDATION, which reads the actual keys revm used and
/// never consults a prediction — so the hot path can stay symbolic.
///
/// Collision behavior differs in the SAFE direction: distinct tuples never
/// alias (keccak could, astronomically rarely), while a fixed slot that
/// happens to equal some mapping entry's address is no longer recognized
/// as the same domain — a missed edge, which the wound repairs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DomainKey {
    /// Account cell (balance+nonce): tier-1 sender/recipient, fee sink.
    Account(Address),
    /// A slot whose address is known outright (tier-3 fixed slots).
    Fixed(Address, B256),
    /// A mapping entry named by its formula and this tx's key words —
    /// keccak-free.
    Derived {
        contract: Address,
        base: u8,
        outer: U256,
        inner: Option<U256>,
    },
}

/// Aggregate stats over observations. Batch (`learn`) for the offline lab,
/// incremental (`learn_obs`) for the live shadow — one code path.
#[derive(Debug, Clone)]
pub struct Stats {
    pub by_selector: HashMap<(Address, [u8; 4]), SelectorStats>,
    /// When false, predictions use ONLY hashes observed directly in BALs
    /// (tier-3 fixed slots) plus tier-1 account keys — no keccak inversion,
    /// no derived mapping entries. An experiment knob: a fixed slot's
    /// address is the same every call, so it needs no formula, and the
    /// question is what the DERIVED keys are worth. See `--no-derived`.
    pub derived_keys: bool,
}

impl Default for Stats {
    /// Derived keys ON. Deriving `Default` would have silently defaulted
    /// the flag to `false` and disabled tier-2 for every caller that
    /// builds stats with `default()` — including the engine and the live
    /// shadow.
    fn default() -> Self {
        Self::new()
    }
}

impl Stats {
    /// Full predictor: tier-1 accounts, tier-3 fixed slots, and tier-2
    /// derived mapping entries recovered by keccak inversion.
    pub fn new() -> Self {
        Self {
            by_selector: HashMap::new(),
            derived_keys: true,
        }
    }

    /// Predictions from directly-observed hashes only.
    pub fn without_derived() -> Self {
        Self {
            derived_keys: false,
            ..Default::default()
        }
    }
}

impl Stats {
    pub fn learn(obs: &[TxObs]) -> Self {
        let mut s = Self::new();
        for o in obs {
            s.learn_obs(o);
        }
        s
    }

    /// Fold one observation into the stats. Inversion is ~2k keccaks worst
    /// case per novel slot (memoization would have to key on the
    /// observation's candidate words — the cheapest correct memo is none).
    /// At the entry cap, unseen selectors are NOT inserted: they stay cold
    /// and schedule as `Tail`, which is exactly what eviction would cost.
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
                    // A selector's footprint repeats: the SAME formulas,
                    // instantiated with each tx's own words. Testing the
                    // known ones costs one or two keccaks each, where the
                    // brute force costs thousands — and after the first
                    // few observations of a selector, essentially every
                    // slot matches something already known.
                    // NEGATIVE cache first. A slot address that recurs
                    // across observations CANNOT be sender- or arg-derived
                    // — those produce a different address per tx — so a
                    // previous failure to solve it is final, and the brute
                    // force must not be paid again. Fixed slots are ~40% of
                    // observations (P0), so without this they re-brute-force
                    // forever: profiling showed the inversion at 93% of all
                    // CPU, and it is the SAME slots every block.
                    if let Some(n) = e.slot_seen.get_mut(&(*addr, *slot)) {
                        *n += 1;
                        continue;
                    }
                    // POSITIVE cache: a selector's footprint repeats with
                    // the same formulas, instantiated with each tx's own
                    // words — one or two keccaks each, against thousands
                    // for the brute force.
                    let known = e
                        .formulas
                        .keys()
                        .find(|f| f.contract == *addr && instantiate(f, o) == Some(*slot))
                        .copied();
                    match known.or_else(|| solve(o, *addr, *slot)) {
                        Some(f) => *e.formulas.entry(f).or_default() += 1,
                        None => *e.slot_seen.entry((*addr, *slot)).or_default() += 1,
                    }
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
        // Formulas: instantiate with THIS tx's words.
        for (f, n) in e.formulas.iter().filter(|_| self.derived_keys) {
            if *n == 0 {
                continue;
            }
            let base = B256::from(U256::from(f.base).to_be_bytes::<32>());
            let inner = match f.inner {
                None => base,
                Some(ic) => match cand_word(o, ic) {
                    Some(k) => keccak_pair(k, base),
                    None => continue,
                },
            };
            if let Some(k) = cand_word(o, f.outer) {
                cells.insert(Cell::Slot(f.contract, keccak_pair(k, inner)));
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
        for (f, n) in e.formulas.iter().filter(|_| self.derived_keys) {
            if *n == 0 {
                continue;
            }
            let Some(outer) = cand_word(o, f.outer) else {
                continue;
            };
            let inner = match f.inner {
                None => None,
                Some(ic) => match cand_word(o, ic) {
                    Some(k) => Some(k),
                    // The formula needs a word this tx does not carry:
                    // skip the key rather than invent one (a missed edge
                    // is wound-repairable; a wrong one is silent).
                    None => continue,
                },
            };
            keys.push(DomainKey::Derived {
                contract: f.contract,
                base: f.base,
                outer,
                inner,
            });
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

    /// Share summary across all selectors: (fixed+derived slot-obs,
    /// unpredictable slot-obs is implicit) — for the report.
    pub fn class_shares(&self) -> (u64, u64, u64) {
        let (mut solved, mut fixedish, mut total) = (0u64, 0u64, 0u64);
        for e in self.by_selector.values() {
            total += e.slot_obs;
            solved += e.formulas.values().sum::<u64>();
            // slot_seen entries repeating across most observations = fixed;
            // singletons = unpredictable.
            for n in e.slot_seen.values() {
                if *n * 10 >= e.observations.max(1) * 6 {
                    fixedish += *n;
                }
            }
        }
        (solved, fixedish, total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::address;

    const TOKEN: Address = address!("00000000000000000000000000000000000000E0");
    const SEL: [u8; 4] = [0xa9, 0x05, 0x9c, 0xbb]; // transfer(address,uint256)

    /// Solidity mapping entry for `mapping at base slot p` keyed by `key`.
    fn map_slot(key: U256, p: u8) -> B256 {
        keccak_pair(key, B256::from(U256::from(p).to_be_bytes::<32>()))
    }

    fn word(a: Address) -> U256 {
        U256::from_be_slice(a.as_slice())
    }

    /// A transfer-shaped obs: sender-derived write + arg0-derived write
    /// (the two balances of an ERC20 transfer at base slot 3).
    fn transfer_obs(index: u64, sender: Address, recipient: Address) -> TxObs {
        TxObs {
            index,
            block: 1,
            sender,
            to: Some(TOKEN),
            selector: Some(SEL),
            args: vec![word(recipient), U256::from(100u64)],
            gas: 30_000,
            has_value: false,
            reads: vec![Cell::Slot(TOKEN, map_slot(word(sender), 3))],
            writes: vec![
                Cell::Account(sender),
                Cell::Slot(TOKEN, map_slot(word(sender), 3)),
                Cell::Slot(TOKEN, map_slot(word(recipient), 3)),
            ],
        }
    }

    #[test]
    fn inversion_solves_and_predicts_for_new_words() {
        let a = address!("0000000000000000000000000000000000000A01");
        let b = address!("0000000000000000000000000000000000000A02");
        let mut stats = Stats::default();
        stats.learn_obs(&transfer_obs(0, a, b));

        // A NEVER-SEEN sender/recipient pair: the formula must instantiate
        // with the new words, not replay the trained slots.
        let c = address!("0000000000000000000000000000000000000A03");
        let d = address!("0000000000000000000000000000000000000A04");
        let holdout = transfer_obs(1, c, d);
        let predicted = stats.predict(&holdout).expect("selector is trained");
        assert!(predicted.contains(&Cell::Slot(TOKEN, map_slot(word(c), 3))));
        assert!(predicted.contains(&Cell::Slot(TOKEN, map_slot(word(d), 3))));
        assert!(predicted.contains(&Cell::Account(c)), "tier-1 sender");
        // And NOT the trained pair's slots.
        assert!(!predicted.contains(&Cell::Slot(TOKEN, map_slot(word(a), 3))));
    }

    #[test]
    fn cold_selector_predicts_none_but_plain_transfer_is_tier1() {
        let stats = Stats::default();
        let a = address!("0000000000000000000000000000000000000A01");
        let cold = transfer_obs(0, a, a);
        assert!(stats.predict(&cold).is_none(), "unseen selector is cold");

        let native = TxObs {
            selector: None,
            to: Some(a),
            has_value: true,
            ..transfer_obs(0, a, a)
        };
        let p = stats.predict(&native).expect("tier-1 never cold");
        assert!(p.contains(&Cell::Account(a)));
    }

    /// The symbolic keys must agree with the hashed ones on the ONE
    /// question the scheduler asks: do two txs share a domain? Same
    /// mapping entry ⇒ equal keys; different entry ⇒ different keys —
    /// without computing a single keccak.
    #[test]
    fn domain_keys_agree_with_hashed_cells_on_conflicts() {
        let a = address!("0000000000000000000000000000000000000A01");
        let b = address!("0000000000000000000000000000000000000A02");
        let mut stats = Stats::default();
        stats.learn_obs(&transfer_obs(0, a, b));

        let c = address!("0000000000000000000000000000000000000A03");
        // Two txs sending to the SAME recipient share the recipient's
        // balance entry; the sender entries differ.
        let x = transfer_obs(1, a, c);
        let y = transfer_obs(2, b, c);
        let (dx, dy) = (
            stats.predict_domains(&x).unwrap(),
            stats.predict_domains(&y).unwrap(),
        );
        let shared: Vec<_> = dx.iter().filter(|k| dy.contains(k)).collect();
        assert!(
            !shared.is_empty(),
            "same recipient entry must produce a shared domain key"
        );
        // And the hashed predictor agrees on the intersection being
        // non-empty for exactly this reason.
        let (cx, cy) = (stats.predict(&x).unwrap(), stats.predict(&y).unwrap());
        assert!(cx.intersection(&cy).next().is_some());
    }

    #[test]
    fn domain_keys_separate_independent_txs() {
        // Train on SEVERAL distinct senders: with one observation the
        // `account_seen` rule would generalize that single trainer's own
        // account as a hot cell for everybody (the hashed predictor does
        // the same) — real traffic never looks like that.
        let mut stats = Stats::default();
        for i in 0..5u8 {
            let mut s = [0u8; 20];
            s[19] = 0xA0 + i;
            let mut r = [0u8; 20];
            r[19] = 0xD0 + i;
            stats.learn_obs(&transfer_obs(i as u64, Address::from(s), Address::from(r)));
        }
        let (s1, r1) = (
            address!("0000000000000000000000000000000000000B01"),
            address!("0000000000000000000000000000000000000B02"),
        );
        let (s2, r2) = (
            address!("0000000000000000000000000000000000000B03"),
            address!("0000000000000000000000000000000000000B04"),
        );
        let d1 = stats.predict_domains(&transfer_obs(1, s1, r1)).unwrap();
        let d2 = stats.predict_domains(&transfer_obs(2, s2, r2)).unwrap();
        assert!(
            d1.iter().all(|k| !d2.contains(k)),
            "disjoint senders/recipients must share no domain: {d1:?} vs {d2:?}"
        );
    }

    #[test]
    fn incremental_learning_matches_batch() {
        let a = address!("0000000000000000000000000000000000000A01");
        let b = address!("0000000000000000000000000000000000000A02");
        let obs: Vec<TxObs> = (0..4)
            .map(|i| transfer_obs(i, if i % 2 == 0 { a } else { b }, b))
            .collect();
        let batch = Stats::learn(&obs);
        let mut inc = Stats::default();
        for o in &obs {
            inc.learn_obs(o);
        }
        let key = (TOKEN, SEL);
        let (be, ie) = (&batch.by_selector[&key], &inc.by_selector[&key]);
        assert_eq!(be.observations, ie.observations);
        assert_eq!(be.formulas, ie.formulas);
        assert_eq!(be.slot_seen, ie.slot_seen);
        assert_eq!(be.account_seen, ie.account_seen);
    }
}
