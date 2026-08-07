//! Footprint classifier: per `(to, selector)`, recover mapping base-slots
//! by keccak INVERSION (an algebraic identity, not co-occurrence
//! statistics) and classify every observed slot as fixed / derived /
//! unpredictable (spec: "The graph index", Tier 2/3).

use std::collections::{BTreeMap, BTreeSet, HashMap};

use alloy_primitives::{Address, B256, U256, keccak256};

use super::Cell;
use super::capture::TxObs;

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

fn keccak_pair(key: U256, inner: B256) -> B256 {
    let mut buf = [0u8; 64];
    buf[..32].copy_from_slice(&key.to_be_bytes::<32>());
    buf[32..].copy_from_slice(inner.as_slice());
    keccak256(buf)
}

/// Try to solve `slot` as a mapping entry for this observation's
/// candidates. Single level first, then one nesting level.
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

/// Aggregate stats over a training set of observations.
#[derive(Debug, Default)]
pub struct Stats {
    pub by_selector: HashMap<(Address, [u8; 4]), SelectorStats>,
}

impl Stats {
    pub fn learn(obs: &[TxObs]) -> Self {
        let mut s = Self::default();
        // Memoize solved slots (same slot re-observed solves identically
        // only when the candidate words match, so key the memo on the
        // observation-specific words too — cheapest correct memo: none.
        // Inversion is ~2k keccaks worst case per novel slot; fine offline.)
        for o in obs {
            let (Some(to), Some(sel)) = (o.to, o.selector) else {
                continue;
            };
            let e = s.by_selector.entry((to, sel)).or_default();
            e.observations += 1;
            for cell in o.reads.iter().chain(o.writes.iter()) {
                match cell {
                    Cell::Slot(addr, slot) => {
                        e.slot_obs += 1;
                        if let Some(f) = solve(o, *addr, *slot) {
                            *e.formulas.entry(f).or_default() += 1;
                        } else {
                            *e.slot_seen.entry((*addr, *slot)).or_default() += 1;
                        }
                    }
                    Cell::Account(a) => {
                        *e.account_seen.entry(*a).or_default() += 1;
                    }
                }
            }
        }
        s
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
        for (f, n) in &e.formulas {
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
