//! Pessimistic DAG construction (spec: "The strategy stack" / "The graph
//! index"): the predictor maps each tx to its predicted cells BEFORE
//! execution; per-cell last-toucher chains in canonical order become the
//! edges. Predicted overlap ⇒ ordered — no abort storms by construction.
//!
//! Strategy routing, v1:
//! - `SenderChain` falls out of tier-1 (the sender Account cell is always
//!   predicted).
//! - `Accumulator`: the fee sink is EXCLUDED from the cells (spec boundary
//!   #4 — as a key it would chain every block).
//! - `Tail` / cold: an untrained selector maps to the wildcard key ⊤,
//!   which conflicts with everything — encoded as a BARRIER at the tx's
//!   canonical position (every in-flight tx before it, everything after
//!   depends on it). Running cold txs "at the end" instead would let later
//!   predicted txs read pre-cold state and convert every true conflict
//!   into a guaranteed validation failure; the barrier keeps ⊤ semantics
//!   exact ("when unsure, serialize" — at the position the sequencer chose).
//! - Deposits/epochs never reach this scheduler (serial lane upstream).
//!
//! Predictions here use the same `Stats::predict` the P1 shadow grades, so
//! the shadow's `false_independent_total` is a live upper bound on the
//! validation failures this schedule can produce.

use std::collections::{HashMap, HashSet};

use kardamom_footprint::classifier::Stats;
use kardamom_footprint::{Cell, TxObs, decoded_view, envelope_view};
use kardamom_types::TxEnvelope;

/// A block's execution plan over local tx positions `0..n`. Every tx is in
/// the DAG; ready = indegree 0.
#[derive(Debug, Default)]
pub struct BlockSchedule {
    /// DAG edges: `children[i]` = local positions depending on i.
    pub children: Vec<Vec<u32>>,
    /// Incoming-edge count per tx.
    pub indegree: Vec<u32>,
    /// Cold (barrier) txs — diagnostics.
    pub cold: usize,
    /// Edge count — diagnostics.
    pub edges: usize,
}

/// Scheduling-time view of one tx (no ground truth — nothing has executed).
pub fn scheduling_view(local_idx: u32, envelope: &TxEnvelope) -> TxObs {
    let (to, selector, args, has_value) = envelope_view(&envelope.raw_tx);
    view_from_parts(local_idx, envelope, to, selector, args, has_value)
}

/// [`scheduling_view`] over a pre-decoded envelope (None = undecodable —
/// the tier-1-only degenerate view, same as a failed decode).
pub fn scheduling_view_decoded(
    local_idx: u32,
    envelope: &TxEnvelope,
    decoded: Option<&alloy_consensus::TxEnvelope>,
) -> TxObs {
    let (to, selector, args, has_value) = match decoded {
        Some(d) => decoded_view(d),
        None => (None, None, Vec::new(), false),
    };
    view_from_parts(local_idx, envelope, to, selector, args, has_value)
}

#[allow(clippy::too_many_arguments)]
fn view_from_parts(
    local_idx: u32,
    envelope: &TxEnvelope,
    to: Option<alloy_primitives::Address>,
    selector: Option<[u8; 4]>,
    args: Vec<alloy_primitives::U256>,
    has_value: bool,
) -> TxObs {
    TxObs {
        index: local_idx as u64,
        block: 0,
        sender: envelope.sender,
        to,
        selector,
        args,
        gas: 0,
        has_value,
        reads: Vec::new(),
        writes: Vec::new(),
    }
}

/// INCREMENTAL DAG construction — the pipeline shape: the sealer streams
/// canonical records in order, so when tx i arrives every predecessor it
/// can have is already admitted; `admit` returns i's full predecessor set
/// immediately and execution may start before i+1 exists. Pure bookkeeping,
/// no locks — the engine integrates it under its per-block graph lock.
#[derive(Debug, Default)]
pub struct DagBuilder {
    // Per-cell last toucher: chain in canonical order. Reader/writer modes
    // are not split (predictions don't either — conservative, the same
    // over-merge the P0 grading priced at ~0% on trained selectors).
    last_toucher: HashMap<Cell, u32>,
    // Wildcard bookkeeping: the last barrier, and the txs admitted since
    // (each already depends on that barrier transitively).
    last_barrier: Option<u32>,
    since_barrier: Vec<u32>,
    pub cold: usize,
    pub edges: usize,
}

impl DagBuilder {
    /// Admit tx i (must be called in canonical order) and return its
    /// DEDUPED predecessor list. `cells` = the prediction (None = cold ⇒
    /// barrier).
    pub fn admit(
        &mut self,
        i: u32,
        cells: Option<impl IntoIterator<Item = Cell>>,
        exclude: &HashSet<Cell>,
    ) -> Vec<u32> {
        let mut preds: Vec<u32> = Vec::new();
        match cells {
            Some(cells) => {
                if let Some(b) = self.last_barrier {
                    preds.push(b);
                }
                for c in cells {
                    if exclude.contains(&c) {
                        continue;
                    }
                    if let Some(&p) = self.last_toucher.get(&c)
                        && p != i
                    {
                        preds.push(p);
                    }
                    self.last_toucher.insert(c, i);
                }
                self.since_barrier.push(i);
            }
            None => {
                // ⊤: barrier. Depends on every tx since the previous
                // barrier (those cover the previous barrier transitively);
                // with none in between, on the previous barrier itself.
                self.cold += 1;
                if self.since_barrier.is_empty() {
                    if let Some(b) = self.last_barrier {
                        preds.push(b);
                    }
                } else {
                    preds.extend(self.since_barrier.iter().copied());
                }
                self.last_barrier = Some(i);
                self.since_barrier.clear();
            }
        }
        preds.sort_unstable();
        preds.dedup();
        self.edges += preds.len();
        preds
    }
}

/// Batch convenience over [`DagBuilder`] (tests, offline analysis).
pub fn build(
    stats: &Stats,
    envelopes: &[TxEnvelope],
    decoded: &[Option<alloy_consensus::TxEnvelope>],
    exclude: &HashSet<Cell>,
) -> BlockSchedule {
    let n = envelopes.len();
    let mut s = BlockSchedule {
        children: vec![Vec::new(); n],
        indegree: vec![0; n],
        ..Default::default()
    };
    let mut dag = DagBuilder::default();
    for (i, env) in envelopes.iter().enumerate() {
        let view = scheduling_view_decoded(i as u32, env, decoded[i].as_ref());
        let preds = dag.admit(i as u32, stats.predict(&view), exclude);
        for p in preds {
            s.children[p as usize].push(i as u32);
            s.indegree[i] += 1;
        }
    }
    s.cold = dag.cold;
    s.edges = dag.edges;
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, keccak256};
    use bytes::Bytes;

    fn envelope(sender: Address) -> TxEnvelope {
        // Raw bytes that fail 2718 decode ⇒ selector-less view: tier-1
        // only, never cold — exactly what a native transfer degrades to.
        let raw = Bytes::from_static(&[0xff, 0x00]);
        TxEnvelope {
            correlation_id: 0,
            tx_hash: keccak256(&raw),
            raw_tx: raw,
            sender,
        }
    }

    fn addr(i: u8) -> Address {
        Address::with_last_byte(i)
    }

    fn decode_all(envs: &[TxEnvelope]) -> Vec<Option<alloy_consensus::TxEnvelope>> {
        use alloy_eips::eip2718::Decodable2718;
        envs.iter()
            .map(|e| alloy_consensus::TxEnvelope::decode_2718(&mut &e.raw_tx[..]).ok())
            .collect()
    }

    #[test]
    fn same_sender_chains_distinct_senders_do_not() {
        let stats = Stats::default();
        let envs = vec![
            envelope(addr(1)),
            envelope(addr(2)),
            envelope(addr(1)),
            envelope(addr(3)),
        ];
        let s = build(&stats, &envs, &decode_all(&envs), &HashSet::new());
        assert_eq!(s.cold, 0, "tier-1 txs are never cold");
        assert_eq!(s.edges, 1, "only the sender chain 0->2");
        assert_eq!(s.children[0], vec![2]);
        assert_eq!(s.indegree, vec![0, 0, 1, 0]);
    }

    #[test]
    fn excluded_cell_forms_no_edges() {
        let stats = Stats::default();
        let envs = vec![envelope(addr(1)), envelope(addr(1))];
        let mut exclude = HashSet::new();
        exclude.insert(Cell::Account(addr(1)));
        let s = build(&stats, &envs, &decode_all(&envs), &exclude);
        assert_eq!(s.edges, 0, "the shared sender cell was excluded");
    }

    #[test]
    fn cold_tx_is_a_barrier_at_its_position() {
        // Cold = a CALL with a selector no stats have seen.
        let cold_env = {
            use alloy_consensus::{SignableTransaction, TxLegacy};
            use alloy_eips::eip2718::Encodable2718;
            use alloy_network::TxSignerSync;
            let s: alloy_signer_local::PrivateKeySigner =
                "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
                    .parse()
                    .unwrap();
            let mut tx = TxLegacy {
                chain_id: Some(1),
                nonce: 0,
                gas_price: 1,
                gas_limit: 100_000,
                to: alloy_primitives::TxKind::Call(addr(9)),
                value: alloy_primitives::U256::ZERO,
                input: alloy_primitives::Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef]),
            };
            let sig = s.sign_transaction_sync(&mut tx).unwrap();
            let env = alloy_consensus::TxEnvelope::Legacy(tx.into_signed(sig));
            let mut raw = Vec::new();
            env.encode_2718(&mut raw);
            TxEnvelope {
                correlation_id: 0,
                raw_tx: Bytes::from(raw),
                sender: s.address(),
                tx_hash: *env.tx_hash(),
            }
        };
        let envs = vec![
            envelope(addr(1)), // 0: warm
            envelope(addr(2)), // 1: warm
            cold_env,          // 2: barrier
            envelope(addr(3)), // 3: warm, must depend on 2
        ];
        let s = build(
            &Stats::default(),
            &envs,
            &decode_all(&envs),
            &HashSet::new(),
        );
        assert_eq!(s.cold, 1);
        // 0->2, 1->2 (barrier waits on all in-flight), 2->3 (everything
        // after waits on the barrier).
        assert_eq!(s.indegree[2], 2);
        assert_eq!(s.indegree[3], 1);
        assert!(s.children[2].contains(&3));
    }
}
