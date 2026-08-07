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
    /// address → ordered (bal_index, deployed code). CODE IS A SEED: a
    /// CREATE in chunk i followed by a CALL in chunk j > i (one block)
    /// must seed chunk j with the BYTECODE, not just the account entry —
    /// the original spec excluded code from attribution reasoning from
    /// the wave-DAG model (ordering), which the seeded model does not
    /// have. Without this every cross-chunk call to a same-block contract
    /// no-ops against empty code (the burst-block divergence).
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

    /// Latest claimed CODE strictly before `bal_index`.
    pub fn code_seed(&self, addr: Address, before: u64) -> Option<&bytes::Bytes> {
        self.code
            .get(&addr)
            .and_then(|w| w.iter().rev().find(|(i, _)| *i < before).map(|(_, c)| c))
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
        let mut code = BTreeMap::new();
        for (addr, writes) in &self.code {
            if let Some((_, c)) = writes.iter().rev().find(|(i, _)| *i >= from && *i <= to) {
                code.insert(*addr, alloy_primitives::keccak256(c));
            }
        }
        ClaimSlice {
            storage,
            balance,
            nonce,
            code,
        }
    }
}

/// The batch-final claimed values over a bal-index range.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ClaimSlice {
    pub storage: BTreeMap<(Address, B256), U256>,
    pub balance: BTreeMap<Address, U256>,
    pub nonce: BTreeMap<Address, u64>,
    /// keccak of the unit-final claimed code (bytes stay out of the
    /// comparison struct; the hash pins them).
    pub code: BTreeMap<Address, B256>,
}

impl ClaimSlice {
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
        for (a, v) in &other.nonce {
            if !self.nonce.contains_key(a) {
                return format!("nonce {a:?}: unclaimed write {v}");
            }
        }
        for (a, v) in &self.code {
            match other.code.get(a) {
                Some(o) if o == v => {}
                Some(o) => return format!("code {a:?}: claimed {v}, recomputed {o}"),
                None => return format!("code {a:?}: claimed {v}, recomputed absent"),
            }
        }
        for (a, v) in &other.code {
            if !self.code.contains_key(a) {
                return format!("code {a:?}: unclaimed deploy {v}");
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

// ---------------------------------------------------------------------------
// Seeded parallel execution engine
// ---------------------------------------------------------------------------

use kardamom_engine::actor::BufferedRecord;
use kardamom_engine::block_env::ExecEnv;
use kardamom_engine::delta::PendingDelta;
use kardamom_engine::error::ExecutorError;
use kardamom_types::{Receipt, StateDatabase};

/// A batch's result: its receipts (with LOCAL cumulative gas — the caller
/// fixes up block-cumulative in order) and its merged writes.
pub struct BatchOutcome {
    pub first_index: u64,
    pub receipts: Vec<Receipt>,
    pub delta: PendingDelta,
}

/// Build the input layer a batch starting at `before` must observe:
/// snapshot state overlaid with the latest claim STRICTLY BEFORE the batch
/// (i.e. the previous batch's end state). Account fields are claimed
/// independently in EIP-7928, so each triple is assembled from whichever
/// components have earlier claims, falling back to the snapshot.
pub fn build_seed<S: StateDatabase>(
    snapshot: &S,
    parent: Option<&PendingDelta>,
    claims: &ClaimIndex,
    before: u64,
) -> Result<PendingDelta, ExecutorError> {
    // Base = the parent layer (merged not-yet-durable writes of earlier
    // blocks): the snapshot alone can be K blocks stale under the depth-K
    // commit pipeline. Claim seeds overlay ON TOP — intra-block claims are
    // newer than any parent state.
    let mut seed = parent.cloned().unwrap_or_default();

    let mut addrs: Vec<Address> = claims.balance.keys().copied().collect();
    addrs.extend(claims.nonce.keys().copied());
    addrs.extend(claims.code.keys().copied());
    addrs.sort_unstable();
    addrs.dedup();
    for addr in addrs {
        let claimed_bal = claims.balance_seed(addr, before);
        let claimed_nonce = claims.nonce_seed(addr, before);
        let claimed_code = claims.code_seed(addr, before);
        if claimed_bal.is_none() && claimed_nonce.is_none() && claimed_code.is_none() {
            continue; // nothing claimed before this batch — snapshot stands
        }
        let base = match seed.accounts.get(&addr) {
            Some(v) => *v, // parent layer already has the freshest base
            None => snapshot
                .basic(addr)
                .map_err(|e| ExecutorError::State(format!("seed basic({addr:?}): {e}")))?
                .unwrap_or((0, U256::ZERO, alloy_primitives::KECCAK256_EMPTY)),
        };
        let code_hash = match claimed_code {
            Some(code) => {
                let h = alloy_primitives::keccak256(code);
                seed.code.insert(h, code.clone());
                h
            }
            None => base.2,
        };
        seed.accounts.insert(
            addr,
            (
                claimed_nonce.unwrap_or(base.0),
                claimed_bal.unwrap_or(base.1),
                code_hash,
            ),
        );
    }

    for (addr, slot) in claims.storage.keys() {
        if let Some(v) = claims.storage_seed(*addr, *slot, before) {
            seed.storage.insert((*addr, *slot), v);
        }
    }
    Ok(seed)
}

/// Execute one batch sequentially over `snapshot ∘ seed`. `first_index` is
/// the batch's first bal index (1-based); receipts carry LOCAL cumulative
/// gas.
pub fn execute_batch<S: StateDatabase>(
    snapshot: &S,
    seed: &PendingDelta,
    records: &[BufferedRecord],
    claims: &ClaimIndex,
    env: ExecEnv,
    first_index: u64,
    granularity: u16,
) -> Result<BatchOutcome, ExecutorError> {
    let mut delta = PendingDelta::new();
    let mut receipts = Vec::with_capacity(records.len());
    let mut cumulative = 0u64;
    // At granularity K > 1 the wire claims are chunk-collapsed, so per-tx
    // comparison is impossible: verification coarsens to the CHUNK — the
    // batch is chunk-ALIGNED (batch_size == K, enforced by the caller),
    // its captured Bal is quantized through the SAME shared code the
    // executor used, and compared once at the end.
    let mut batch_bal = revm::state::bal::Bal::new();
    // ONE execution scope per batch (EVM + commit-into cache reused across
    // the batch's txs — the per-tx construction was ~90% of execution-path
    // allocation). The seed layer plays the parent role.
    let mut scope = kardamom_engine::executor::ExecScope::new(snapshot, Some(seed), env)?;
    for (i, rec) in records.iter().enumerate() {
        let bal_index = first_index + i as u64;
        let global_index_in_block = bal_index - 1;
        // Recompute each record's claims through the executor's EXACT
        // capture path (revm's Bal records per-FIELD changes for txs; the
        // synthetic WriteSet path for deposits). Comparing a WriteSet
        // projection instead diverged on every live transfer — symmetric
        // construction is the only drift-proof comparison.
        let (receipt, ws) = match rec {
            BufferedRecord::Tx {
                tx_idx,
                envelope,
                position,
            } => scope.execute_tx(
                *tx_idx,
                *position,
                envelope,
                global_index_in_block,
                cumulative,
                Some((&mut batch_bal, bal_index)),
            )?,
            BufferedRecord::Deposit {
                tx_idx,
                deposit,
                position,
            } => {
                // Deposits run outside the scope (own commit semantics —
                // the mint is durable even when the inner call reverts).
                // The seed plays the parent role; `delta` carries this
                // batch's earlier writes.
                let out = kardamom_engine::executor::execute_deposit_tx(
                    snapshot,
                    Some(seed),
                    &delta,
                    env,
                    *tx_idx,
                    *position,
                    deposit,
                    global_index_in_block,
                    cumulative,
                    Some((&mut batch_bal, bal_index)),
                )?;
                // Fold the deposit's writes into the scope cache so later
                // records in this batch observe them (mirrors the actor's
                // streaming path and the sequential fallback).
                let mut layer = PendingDelta::new();
                layer.apply(out.1.clone());
                scope.seed_layer(&layer)?;
                out
            }
        };
        cumulative = receipt.cumulative_gas_used;
        delta.apply(ws);
        receipts.push(receipt);
    }
    // Verify claims WHERE THEY ARE PRODUCED. At granularity 1 that is per
    // tx (batch-final comparison alone would leave intra-batch claims
    // unchecked — neither seeds nor outputs — so a wrong intermediate
    // attribution would ship while the final state matched); at K > 1 the
    // finest producible unit IS the chunk, and the aligned batch is one
    // chunk. Both sides of the comparison pass through the shared
    // capture/quantize path, so shape drift is impossible by construction.
    let computed_alloy =
        kardamom_engine::bal_ladder::quantize(batch_bal.into_alloy_bal(), granularity);
    let computed_idx = ClaimIndex::from_alloy(&computed_alloy);
    let k = u64::from(granularity.max(1));
    let last_index = first_index + records.len() as u64 - 1;
    if granularity <= 1 {
        for unit in first_index..=last_index {
            let claimed = claims.claims_in_range(unit, unit);
            let computed = computed_idx.claims_in_range(unit, unit);
            if claimed != computed {
                return Err(ExecutorError::Divergence(format!(
                    "tx {unit}: {}",
                    claimed.diff_summary(&computed)
                )));
            }
        }
    } else {
        let chunk = kardamom_engine::bal_ladder::chunk_of(first_index, k);
        let claimed = claims.claims_in_range(chunk, chunk);
        let computed = computed_idx.claims_in_range(chunk, chunk);
        if claimed != computed {
            return Err(ExecutorError::Divergence(format!(
                "chunk {chunk} (txs {first_index}..={last_index}): {}",
                claimed.diff_summary(&computed)
            )));
        }
    }
    Ok(BatchOutcome {
        first_index,
        receipts,
        delta,
    })
}

/// Verified result of a whole block.
#[derive(Debug)]
pub struct BlockOutcome {
    /// Receipts in block order, with block-cumulative gas fixed up.
    pub receipts: Vec<Receipt>,
    /// The block's merged writes (fold of every batch, in block order).
    pub delta: PendingDelta,
    /// Batches executed (for telemetry).
    pub batches: usize,
}

/// Re-execute a block's records (transactions AND deposits) as FULLY
/// PARALLEL batches, each seeded from the BAL's claims, verifying every
/// batch's claims where they are produced. Deposits occupy bal indices in
/// the same space as txs (the executor's streaming capture passes
/// `tx_index_in_block + 1` for both), so their claims seed later batches
/// exactly like tx claims — the mint is a balance claim, a CREATE
/// deposit's bytecode a code claim.
///
/// Returns `Err(ExecutorError::Divergence)` on the first batch whose
/// recomputed writes differ from what the executor claimed — the claim was
/// checked at its producing batch, so a false claim cannot be laundered by
/// later batches that merely consume it.
pub fn execute_block_parallel<S: StateDatabase + Sync>(
    snapshot: &S,
    parent: Option<&PendingDelta>,
    txs: &[BufferedRecord],
    claims: &ClaimIndex,
    env: ExecEnv,
    batch_size: usize,
    granularity: u16,
) -> Result<BlockOutcome, ExecutorError> {
    if txs.is_empty() {
        return Ok(BlockOutcome {
            receipts: Vec::new(),
            delta: PendingDelta::new(),
            batches: 0,
        });
    }
    // SAME-VIEW INVARIANT: the attribution granularity comes from the FRAME
    // (what the executor actually produced), never from local config. At
    // K > 1, execution batches must be chunk-ALIGNED — batch size == K and
    // ranges tile from index 1 — so the chunk a batch verifies is exactly
    // the chunk the executor collapsed. Claims (and therefore seeds) are
    // chunk-indexed at K > 1.
    let k = u64::from(granularity.max(1));
    let effective_batch = if granularity > 1 {
        granularity as usize
    } else {
        batch_size
    };
    let ranges = batch_ranges(txs.len(), effective_batch);

    // Every batch runs concurrently: its inputs come from the claims, so no
    // batch waits on another.
    let results: Vec<Result<BatchOutcome, ExecutorError>> = std::thread::scope(|scope| {
        let handles: Vec<_> = ranges
            .iter()
            .map(|(from, to)| {
                let slice = &txs[(*from as usize - 1)..(*to as usize)];
                let from = *from;
                scope.spawn(move || {
                    // Seeds look up "latest claim strictly before this
                    // batch" in the CLAIM index space: tx indices at K = 1,
                    // chunk ordinals at K > 1.
                    let before = if k > 1 {
                        kardamom_engine::bal_ladder::chunk_of(from, k)
                    } else {
                        from
                    };
                    let seed = build_seed(snapshot, parent, claims, before)?;
                    execute_batch(snapshot, &seed, slice, claims, env, from, granularity)
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| {
                h.join()
                    .unwrap_or_else(|_| Err(ExecutorError::State("batch worker panicked".into())))
            })
            .collect()
    });

    // Verify each batch's claims, then fold in block order.
    let mut outcomes = Vec::with_capacity(results.len());
    for r in results {
        outcomes.push(r?);
    }
    outcomes.sort_by_key(|o| o.first_index);

    let mut delta = PendingDelta::new();
    let mut receipts = Vec::with_capacity(txs.len());
    let mut cumulative = 0u64;
    for o in outcomes.iter() {
        // Claims were verified per tx inside each batch (strictly stronger
        // than a batch-final comparison, which cannot see intra-batch
        // attribution).
        // Fold: later batches overwrite earlier ones (block order).
        delta.merge_from(&o.delta);
        // Block-cumulative gas: batches computed locally from 0.
        for r in &o.receipts {
            let mut r = r.clone();
            cumulative += r.gas_used;
            r.cumulative_gas_used = cumulative;
            receipts.push(r);
        }
    }

    Ok(BlockOutcome {
        receipts,
        delta,
        batches: ranges.len(),
    })
}

#[cfg(test)]
mod engine_tests {
    use super::*;
    use alloy_consensus::{SignableTransaction, TxLegacy};
    use alloy_network::TxSignerSync;
    use alloy_primitives::{B256, TxKind, address};
    use alloy_signer_local::PrivateKeySigner;
    use kardamom_engine::exec_types::TxIndex;
    use kardamom_engine::executor::execute_tx;
    use kardamom_engine::state::MockStateDatabase;
    use kardamom_types::{BPosition, BlockBoundaryStart, Deposit, TxEnvelope};

    fn tx(
        signer: &PrivateKeySigner,
        to: Address,
        nonce: u64,
        value: u64,
        i: u64,
    ) -> BufferedRecord {
        let inner = TxLegacy {
            chain_id: Some(1),
            nonce,
            gas_price: 1_000_000_000,
            gas_limit: 100_000,
            to: TxKind::Call(to),
            value: U256::from(value),
            input: Default::default(),
        };
        let mut m = inner;
        let sig = signer.sign_transaction_sync(&mut m).unwrap();
        let env: alloy_consensus::TxEnvelope = m.into_signed(sig).into();
        let mut raw = Vec::new();
        alloy_eips::eip2718::Encodable2718::encode_2718(&env, &mut raw);
        BufferedRecord::Tx {
            tx_idx: TxIndex(i),
            position: BPosition {
                term_id: 0,
                term_offset: (i * 64) as i32,
            },
            envelope: TxEnvelope {
                correlation_id: i,
                raw_tx: raw.into(),
                sender: signer.address(),
                tx_hash: alloy_primitives::B256::repeat_byte(i as u8 + 1),
            },
        }
    }

    fn call_tx(signer: &PrivateKeySigner, to: Address, nonce: u64, i: u64) -> BufferedRecord {
        let inner = TxLegacy {
            chain_id: Some(1),
            nonce,
            gas_price: 1_000_000_000,
            gas_limit: 100_000,
            to: TxKind::Call(to),
            value: U256::ZERO,
            input: Default::default(),
        };
        let mut m = inner;
        let sig = signer.sign_transaction_sync(&mut m).unwrap();
        let env: alloy_consensus::TxEnvelope = m.into_signed(sig).into();
        let mut raw = Vec::new();
        alloy_eips::eip2718::Encodable2718::encode_2718(&env, &mut raw);
        BufferedRecord::Tx {
            tx_idx: TxIndex(i),
            position: BPosition {
                term_id: 0,
                term_offset: (i * 64) as i32,
            },
            envelope: TxEnvelope {
                correlation_id: i,
                raw_tx: raw.into(),
                sender: signer.address(),
                tx_hash: alloy_primitives::B256::repeat_byte(i as u8 + 1),
            },
        }
    }

    fn dep(
        from: Address,
        to: Option<Address>,
        mint: u128,
        input: Vec<u8>,
        i: u64,
    ) -> BufferedRecord {
        BufferedRecord::Deposit {
            tx_idx: TxIndex(i),
            position: BPosition {
                term_id: 0,
                term_offset: (i * 64) as i32,
            },
            deposit: Deposit {
                source_hash: B256::repeat_byte(0xd0u8.wrapping_add(i as u8)),
                from,
                to,
                mint,
                value: U256::ZERO,
                gas_limit: 1_000_000,
                is_system_transaction: false,
                input: input.into(),
            },
        }
    }

    fn env() -> ExecEnv {
        ExecEnv::new(
            1,
            &BlockBoundaryStart {
                block_number: 1,
                end_tx_idx: BPosition::from_index(0),
                l2_timestamp: 1_700_000_000,
                l1_origin: 0,
            },
        )
    }

    /// Build a claim index by SEQUENTIALLY executing the block through the
    /// executor's REAL capture path (`execute_tx` / `execute_deposit_tx` →
    /// revm `Bal`), exactly as the live executor produces claims. The first
    /// version of this fixture hand-rolled claims from WriteSets — symmetric
    /// with a verification bug (per-field vs whole-triple attribution), so
    /// both passed while live traffic diverged on every transfer. The
    /// fixture and the producer must share code, not shape.
    fn honest_claims<S: StateDatabase>(snap: &S, records: &[BufferedRecord]) -> ClaimIndex {
        let mut bal = revm::state::bal::Bal::new();
        let mut delta = PendingDelta::new();
        let mut cumulative = 0u64;
        for (i, rec) in records.iter().enumerate() {
            let (r, ws) = exec_record(
                snap,
                None,
                &delta,
                rec,
                i as u64,
                cumulative,
                Some(&mut bal),
            );
            cumulative = r.cumulative_gas_used;
            delta.apply(ws);
        }
        ClaimIndex::from_alloy(&bal.into_alloy_bal())
    }

    fn seq_delta<S: StateDatabase>(snap: &S, records: &[BufferedRecord]) -> PendingDelta {
        let mut delta = PendingDelta::new();
        let mut cumulative = 0u64;
        for (i, rec) in records.iter().enumerate() {
            let (r, ws) = exec_record(snap, None, &delta, rec, i as u64, cumulative, None);
            cumulative = r.cumulative_gas_used;
            delta.apply(ws);
        }
        delta
    }

    /// One record through the free executor path (tx or deposit), the same
    /// producer code the live executor runs.
    fn exec_record<S: StateDatabase>(
        snap: &S,
        parent: Option<&PendingDelta>,
        delta: &PendingDelta,
        rec: &BufferedRecord,
        i: u64,
        cumulative: u64,
        bal: Option<&mut revm::state::bal::Bal>,
    ) -> (kardamom_types::Receipt, kardamom_engine::delta::WriteSet) {
        let bal = bal.map(|b| (b, i + 1));
        match rec {
            BufferedRecord::Tx {
                tx_idx,
                envelope,
                position,
            } => execute_tx(
                snap,
                parent,
                delta,
                env(),
                *tx_idx,
                *position,
                envelope,
                i,
                cumulative,
                bal,
            )
            .expect("seq execute"),
            BufferedRecord::Deposit {
                tx_idx,
                deposit,
                position,
            } => execute_deposit_tx(
                snap,
                parent,
                delta,
                env(),
                *tx_idx,
                *position,
                deposit,
                i,
                cumulative,
                bal,
            )
            .expect("seq deposit"),
        }
    }

    /// THE parity property: parallel seeded batches must produce byte-identical
    /// state to sequential execution — including for a CONFLICTING workload
    /// (one sender, dependent nonces, shared recipient) where every tx depends
    /// on its predecessor. Seeding, not ordering, is what makes that safe.
    #[test]
    fn parallel_batches_equal_sequential_on_a_fully_dependent_chain() {
        let signer = PrivateKeySigner::random();
        let to = address!("00000000000000000000000000000000000000AA");
        let snap = MockStateDatabase::builder()
            .account(
                signer.address(),
                U256::from(10u128.pow(18)),
                0,
                alloy_primitives::KECCAK256_EMPTY,
            )
            .build();
        // 12 txs from ONE sender: maximal conflict (each reads the balance and
        // nonce the previous tx wrote).
        let txs: Vec<BufferedRecord> = (0..12).map(|i| tx(&signer, to, i, 1_000 + i, i)).collect();

        let claims = honest_claims(&snap, &txs);
        let expected = seq_delta(&snap, &txs);

        for batch_size in [1usize, 5, 10] {
            let out = execute_block_parallel(&snap, None, &txs, &claims, env(), batch_size, 1)
                .unwrap_or_else(|e| panic!("batch_size {batch_size}: {e:?}"));
            assert_eq!(
                out.delta.accounts, expected.accounts,
                "batch_size {batch_size}: account state must equal sequential"
            );
            assert_eq!(out.delta.storage, expected.storage);
            assert_eq!(out.receipts.len(), txs.len());
            // Block-cumulative gas must be monotonic and match the total.
            let total: u64 = out.receipts.iter().map(|r| r.gas_used).sum();
            assert_eq!(out.receipts.last().unwrap().cumulative_gas_used, total);
            for w in out.receipts.windows(2) {
                assert!(w[0].cumulative_gas_used < w[1].cumulative_gas_used);
            }
        }
    }

    /// A FORGED claim must fail-stop at the batch that produces it — this is
    /// what makes seeding from unverified claims sound.
    #[test]
    fn a_forged_claim_fails_stop_at_its_producing_batch() {
        let signer = PrivateKeySigner::random();
        let to = address!("00000000000000000000000000000000000000BB");
        let snap = MockStateDatabase::builder()
            .account(
                signer.address(),
                U256::from(10u128.pow(18)),
                0,
                alloy_primitives::KECCAK256_EMPTY,
            )
            .build();
        let txs: Vec<BufferedRecord> = (0..8).map(|i| tx(&signer, to, i, 500, i)).collect();

        let mut claims = honest_claims(&snap, &txs);
        // Tamper: inflate the recipient's claimed balance at tx 6 (batch 2 of
        // 5-tx batches) — the executor claiming a state it did not compute.
        let bogus = claims.balance.get_mut(&to).expect("recipient claims");
        if let Some(entry) = bogus.iter_mut().find(|(i, _)| *i == 6) {
            entry.1 += U256::from(1_000_000u64);
        }

        let err = execute_block_parallel(&snap, None, &txs, &claims, env(), 5, 1)
            .expect_err("a forged claim must be caught");
        match err {
            ExecutorError::Divergence(msg) => {
                assert!(msg.contains("tx 6"), "must name the producing tx: {msg}");
                assert!(
                    msg.contains("balance"),
                    "must name the mismatching item: {msg}"
                );
            }
            other => panic!("expected Divergence, got {other:?}"),
        }
    }

    /// K = 20 end-to-end: quantized wire claims + chunk-aligned batches
    /// must be parity-identical to sequential, and a forged CHUNK claim
    /// must fail-stop naming the chunk. Exercises the same-view invariant:
    /// both sides pass through the shared quantize().
    #[test]
    fn quantized_claims_verify_with_aligned_batches() {
        let signer = PrivateKeySigner::random();
        let to = address!("00000000000000000000000000000000000000CC");
        let snap = MockStateDatabase::builder()
            .account(
                signer.address(),
                U256::from(10u128.pow(18)),
                0,
                alloy_primitives::KECCAK256_EMPTY,
            )
            .build();
        let txs: Vec<BufferedRecord> = (0..47).map(|i| tx(&signer, to, i, 100 + i, i)).collect();

        // The executor's view: per-tx capture, then the SHARED quantize.
        let mut bal = revm::state::bal::Bal::new();
        let mut delta = PendingDelta::new();
        let mut cumulative = 0u64;
        for (i, t) in txs.iter().enumerate() {
            let (r, ws) = exec_record(&snap, None, &delta, t, i as u64, cumulative, Some(&mut bal));
            cumulative = r.cumulative_gas_used;
            delta.apply(ws);
        }
        let expected = delta;
        let quantized = kardamom_engine::bal_ladder::quantize(bal.into_alloy_bal(), 20);
        let claims = ClaimIndex::from_alloy(&quantized);

        let out = execute_block_parallel(&snap, None, &txs, &claims, env(), 8, 20)
            .expect("quantized parity");
        assert_eq!(out.delta.accounts, expected.accounts);
        assert_eq!(out.batches, 3, "47 txs at K=20 -> 3 aligned chunks");

        // Forge a chunk-2 claim: must fail-stop naming the chunk.
        let mut forged = claims.clone();
        if let Some(w) = forged.balance.get_mut(&to)
            && let Some(e) = w.iter_mut().find(|(i, _)| *i == 2)
        {
            e.1 += U256::from(999u64);
        }
        let err = execute_block_parallel(&snap, None, &txs, &forged, env(), 8, 20)
            .expect_err("forged chunk claim must be caught");
        match err {
            ExecutorError::Divergence(msg) => {
                assert!(msg.contains("chunk 2"), "must name the chunk: {msg}")
            }
            other => panic!("expected Divergence, got {other:?}"),
        }
    }

    /// THE depth-K regression: under the pipelined commit the snapshot can
    /// be K blocks stale — block 2's txs must observe block 1's writes via
    /// the PARENT layer. The first DeFi gate diverged exactly here: the
    /// hook dropped the parent, the validator saw stale nonces, and skipped
    /// txs the executor had executed.
    #[test]
    fn parent_layer_bridges_the_uncommitted_gap() {
        let signer = PrivateKeySigner::random();
        let to = address!("00000000000000000000000000000000000000DD");
        let snap = MockStateDatabase::builder()
            .account(
                signer.address(),
                U256::from(10u128.pow(18)),
                0,
                alloy_primitives::KECCAK256_EMPTY,
            )
            .build();

        // Block 1: nonces 0..3, executed and folded into a parent layer —
        // but NEVER committed to the snapshot (StaticSnapshotSource
        // semantics: the mock snapshot still says nonce 0).
        let b1: Vec<BufferedRecord> = (0..4).map(|i| tx(&signer, to, i, 100, i)).collect();
        let claims1 = honest_claims(&snap, &b1);
        let out1 =
            execute_block_parallel(&snap, None, &b1, &claims1, env(), 2, 1).expect("block 1");
        let parent = out1.delta.clone();

        // Block 2: nonces 4..7. Against the bare snapshot every tx is a
        // nonce-mismatch skip; with the parent layer they execute.
        let b2: Vec<BufferedRecord> = (4..8).map(|i| tx(&signer, to, i, 100, i)).collect();
        // Build block-2 claims through the same capture path, WITH parent.
        let mut bal = revm::state::bal::Bal::new();
        let mut delta = PendingDelta::new();
        let mut cumulative = 0u64;
        for (i, t) in b2.iter().enumerate() {
            let (r, ws) = exec_record(
                &snap,
                Some(&parent),
                &delta,
                t,
                i as u64,
                cumulative,
                Some(&mut bal),
            );
            assert!(r.status, "block-2 txs must execute given the parent");
            cumulative = r.cumulative_gas_used;
            delta.apply(ws);
        }
        let claims2 = ClaimIndex::from_alloy(&bal.into_alloy_bal());

        // WITHOUT parent: the stale-state bug — every tx skips.
        let stale = execute_block_parallel(&snap, None, &b2, &claims2, env(), 2, 1);
        assert!(
            stale.is_err(),
            "without the parent layer the block must diverge (skips vs claims)"
        );

        // WITH parent: byte-identical to the sequential-with-parent run.
        let out2 = execute_block_parallel(&snap, Some(&parent), &b2, &claims2, env(), 2, 1)
            .expect("block 2 with parent");
        assert_eq!(out2.delta.accounts, delta.accounts);
        assert!(out2.receipts.iter().all(|r| r.status));
    }

    #[test]
    fn empty_block_is_a_no_op() {
        let snap = MockStateDatabase::builder().build();
        let out =
            execute_block_parallel(&snap, None, &[], &ClaimIndex::default(), env(), 5, 1).unwrap();
        assert!(out.receipts.is_empty() && out.batches == 0);
    }

    /// THE deposit-emission regression: a deposit's mint MUST be claimed in
    /// the BAL as a balance write, because later batches seed the recipient's
    /// balance from it. Before the fix, `record_writeset_into_bal` fabricated
    /// only a differing nonce and revm's per-FIELD classification silently
    /// dropped the balance claim — batch 2 seeded the pre-mint balance and
    /// every spend of minted funds diverged.
    #[test]
    fn deposit_mint_seeds_later_batches() {
        let signer = PrivateKeySigner::random();
        let d = signer.address();
        let to = address!("00000000000000000000000000000000000000EE");
        // D starts at ZERO balance: only the mint can fund its txs.
        let snap = MockStateDatabase::builder()
            .account(d, U256::ZERO, 0, alloy_primitives::KECCAK256_EMPTY)
            .build();
        // The CALL-type deposit bumps d's nonce to 1 (revm bumps the caller
        // nonce for `is_call` deposits too), so the spends start at nonce 1.
        let records: Vec<BufferedRecord> = vec![
            dep(d, Some(to), 10u128.pow(18), Vec::new(), 0),
            tx(&signer, to, 1, 1_000, 1),
            tx(&signer, to, 2, 1_000, 2),
        ];

        let claims = honest_claims(&snap, &records);
        // The mint must be visible as a balance claim at the deposit's index.
        assert!(
            claims.balance_seed(d, 2).is_some(),
            "deposit mint must be claimed in the BAL (balance change at index 1)"
        );
        let expected = seq_delta(&snap, &records);

        // batch_size 1: the spends run in batches seeded ONLY from claims.
        let out = execute_block_parallel(&snap, None, &records, &claims, env(), 1, 1)
            .expect("deposit-seeded parallel block");
        assert_eq!(out.delta.accounts, expected.accounts);
        assert_eq!(out.delta.storage, expected.storage);
        assert_eq!(out.batches, 3);
        // The deposit's receipt survives the parallel path intact.
        assert_eq!(out.receipts[0].tx_type, kardamom_types::TX_TYPE_DEPOSIT);
        assert!(out.receipts.iter().all(|r| r.status));
    }

    /// A CREATE deposit's bytecode is a CODE claim: a later batch calling
    /// the deposited contract must seed the BYTES, or the call no-ops
    /// against empty code (the cross-chunk CREATE-then-CALL class, deposit
    /// edition).
    #[test]
    fn create_deposit_code_seeds_later_calls() {
        let signer = PrivateKeySigner::random();
        let l1_sender = address!("00000000000000000000000000000000000000F1");
        let snap = MockStateDatabase::builder()
            .account(
                signer.address(),
                U256::from(10u128.pow(18)),
                0,
                alloy_primitives::KECCAK256_EMPTY,
            )
            .build();
        // Initcode returning runtime `PUSH1 1 PUSH1 0 SSTORE STOP`: every
        // call writes slot 0 = 1.
        let initcode = alloy_primitives::hex::decode("656001600055006000526006601af3").unwrap();
        // CREATE address: keccak(rlp(from, nonce=0)).
        let contract = l1_sender.create(0);
        let records: Vec<BufferedRecord> = vec![
            dep(l1_sender, None, 0, initcode, 0),
            call_tx(&signer, contract, 0, 1),
        ];

        let claims = honest_claims(&snap, &records);
        assert!(
            claims.code_seed(contract, 2).is_some(),
            "CREATE deposit bytecode must be claimed in the BAL"
        );
        let expected = seq_delta(&snap, &records);
        // The call's SSTORE proves it executed real code.
        assert_eq!(
            expected.storage.get(&(contract, B256::ZERO)),
            Some(&U256::from(1u64)),
            "sequential call must hit the deployed contract"
        );

        let out = execute_block_parallel(&snap, None, &records, &claims, env(), 1, 1)
            .expect("CREATE-deposit-seeded parallel block");
        assert_eq!(out.delta.accounts, expected.accounts);
        assert_eq!(out.delta.storage, expected.storage);
        assert_eq!(out.batches, 2);
    }

    /// Deposits inside a K > 1 chunk: chunk-aligned batches with a deposit
    /// mid-chunk verify and match sequential (the deposit's claims are
    /// quantized through the same shared ladder as tx claims).
    #[test]
    fn quantized_chunk_containing_a_deposit_verifies() {
        let signer = PrivateKeySigner::random();
        let d = signer.address();
        let filler = PrivateKeySigner::random();
        let to = address!("00000000000000000000000000000000000000F2");
        let snap = MockStateDatabase::builder()
            .account(d, U256::ZERO, 0, alloy_primitives::KECCAK256_EMPTY)
            .account(
                filler.address(),
                U256::from(10u128.pow(18)),
                0,
                alloy_primitives::KECCAK256_EMPTY,
            )
            .build();
        // Chunk 1 (K=4): 3 filler txs + the deposit at bal index 4, so the
        // deposit is CHUNK-FINAL for d's balance — chunk 2's spends seed the
        // mint from the deposit's claim alone, and no same-chunk tx re-claim
        // can mask a missing deposit emission. (The CALL deposit bumps d's
        // nonce to 1; spends run nonces 1..4.)
        let mut records: Vec<BufferedRecord> =
            (0..3u64).map(|i| tx(&filler, to, i, 100, i)).collect();
        records.push(dep(d, Some(to), 10u128.pow(18), Vec::new(), 3));
        for i in 4..8u64 {
            records.push(tx(&signer, to, i - 3, 500, i));
        }

        // Executor view: per-record capture, then the SHARED quantize at K=4.
        let mut bal = revm::state::bal::Bal::new();
        let mut delta = PendingDelta::new();
        let mut cumulative = 0u64;
        for (i, r) in records.iter().enumerate() {
            let (rc, ws) =
                exec_record(&snap, None, &delta, r, i as u64, cumulative, Some(&mut bal));
            assert!(rc.status);
            cumulative = rc.cumulative_gas_used;
            delta.apply(ws);
        }
        let expected = delta;
        let quantized = kardamom_engine::bal_ladder::quantize(bal.into_alloy_bal(), 4);
        let claims = ClaimIndex::from_alloy(&quantized);

        let out = execute_block_parallel(&snap, None, &records, &claims, env(), 3, 4)
            .expect("deposit-in-chunk quantized parity");
        assert_eq!(out.delta.accounts, expected.accounts);
        assert_eq!(out.delta.storage, expected.storage);
        assert_eq!(out.batches, 2, "8 records at K=4 -> 2 aligned chunks");
    }

    /// A forged claim about a DEPOSIT fails-stop like any other — deposits
    /// get no special trust.
    #[test]
    fn a_forged_deposit_claim_fails_stop() {
        let signer = PrivateKeySigner::random();
        let d = signer.address();
        let to = address!("00000000000000000000000000000000000000F3");
        let snap = MockStateDatabase::builder()
            .account(d, U256::ZERO, 0, alloy_primitives::KECCAK256_EMPTY)
            .build();
        let records: Vec<BufferedRecord> = vec![
            dep(d, Some(to), 10u128.pow(18), Vec::new(), 0),
            tx(&signer, to, 1, 1_000, 1),
        ];
        let mut claims = honest_claims(&snap, &records);
        // Tamper the deposit's claimed mint (bal index 1).
        let w = claims.balance.get_mut(&d).expect("mint claim");
        let entry = w.iter_mut().find(|(i, _)| *i == 1).expect("index 1");
        entry.1 += U256::from(7u64);

        let err = execute_block_parallel(&snap, None, &records, &claims, env(), 1, 1)
            .expect_err("forged deposit claim must be caught");
        match err {
            ExecutorError::Divergence(msg) => {
                assert!(msg.contains("tx 1"), "must name the producing unit: {msg}");
                assert!(msg.contains("balance"), "must name the field: {msg}");
            }
            other => panic!("expected Divergence, got {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Engine strategy: what the validator hands to the exec loop
// ---------------------------------------------------------------------------

use kardamom_engine::actor::{BlockExec, BlockExecOutput};
use kardamom_engine::executor::execute_deposit_tx;
use std::sync::Arc;
use std::time::Duration;

/// How long a block waits for its BAL claims before falling back to
/// sequential re-execution. Short: liveness never depends on the BAL.
const CLAIM_WAIT: Duration = Duration::from_millis(250);

/// Sequential re-execution of a whole block — the always-available fallback
/// when the block's BAL claims don't arrive in time. Identical semantics to
/// the engine's streaming path.
pub fn execute_block_sequential<S: StateDatabase>(
    snapshot: &S,
    parent: Option<&PendingDelta>,
    records: &[BufferedRecord],
    env: ExecEnv,
) -> Result<BlockExecOutput, ExecutorError> {
    let mut delta = PendingDelta::new();
    let mut receipts = Vec::with_capacity(records.len());
    let mut cumulative = 0u64;
    let mut scope = kardamom_engine::executor::ExecScope::new(snapshot, parent, env)?;
    for (i, rec) in records.iter().enumerate() {
        let idx_in_block = i as u64;
        let (receipt, ws) = match rec {
            BufferedRecord::Tx {
                tx_idx,
                envelope,
                position,
            } => scope.execute_tx(*tx_idx, *position, envelope, idx_in_block, cumulative, None)?,
            BufferedRecord::Deposit {
                tx_idx,
                deposit,
                position,
            } => {
                let out = execute_deposit_tx(
                    snapshot,
                    parent,
                    &delta,
                    env,
                    *tx_idx,
                    *position,
                    deposit,
                    idx_in_block,
                    cumulative,
                    None,
                )?;
                // Fold deposit writes into the scope cache (mirrors the
                // actor's streaming path) so later txs observe them.
                let mut layer = PendingDelta::new();
                layer.apply(out.1.clone());
                scope.seed_layer(&layer)?;
                out
            }
        };
        cumulative = receipt.cumulative_gas_used;
        delta.apply(ws);
        receipts.push(receipt);
    }
    Ok(BlockExecOutput { receipts, delta })
}

/// Serialize a diverging block's inputs for offline replay. Best-effort:
/// failures only log. Format: JSON envelope with hex payloads — small
/// (one block), self-contained, versioned by field presence.
/// Serializable projection of a block's canonical records — shared by the
/// claim-path dump below and the receipt-path flight ring
/// (`crate::flight`): both dumps must stay replayable by the same offline
/// tooling.
pub(crate) fn records_json(records: &[BufferedRecord]) -> Vec<serde_json::Value> {
    records
        .iter()
        .map(|r| match r {
            BufferedRecord::Tx {
                tx_idx,
                envelope,
                position,
            } => serde_json::json!({
                "kind": "tx",
                "raw": alloy_primitives::hex::encode(&envelope.raw_tx),
                "sender": format!("{:?}", envelope.sender),
                "hash": format!("{:?}", envelope.tx_hash),
                "correlation_id": envelope.correlation_id,
                "idx": tx_idx.0,
                "pos": position.as_index(),
            }),
            BufferedRecord::Deposit {
                tx_idx,
                deposit,
                position,
            } => serde_json::json!({
                "kind": "deposit",
                "source_hash": format!("{:?}", deposit.source_hash),
                "from": format!("{:?}", deposit.from),
                "to": deposit.to.map(|a| format!("{a:?}")),
                "mint": deposit.mint.to_string(),
                "value": deposit.value.to_string(),
                "gas_limit": deposit.gas_limit,
                "input": alloy_primitives::hex::encode(&deposit.input),
                "idx": tx_idx.0,
                "pos": position.as_index(),
            }),
        })
        .collect()
}

/// Serializable projection of a claim index (same sharing rationale as
/// [`records_json`]).
pub(crate) fn claims_json(claims: &ClaimIndex) -> serde_json::Value {
    serde_json::json!({
        "claims_storage": claims.storage.iter().map(|((a, k), w)| serde_json::json!({
            "addr": format!("{a:?}"), "slot": format!("{k:?}"),
            "writes": w.iter().map(|(i, v)| (i, v.to_string())).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
        "claims_balance": claims.balance.iter().map(|(a, w)| serde_json::json!({
            "addr": format!("{a:?}"),
            "writes": w.iter().map(|(i, v)| (i, v.to_string())).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
        "claims_nonce": claims.nonce.iter().map(|(a, w)| serde_json::json!({
            "addr": format!("{a:?}"),
            "writes": w.clone(),
        })).collect::<Vec<_>>(),
        "claims_code": claims.code.iter().map(|(a, w)| serde_json::json!({
            "addr": format!("{a:?}"),
            "writes": w.iter().map(|(i, c)| (i, alloy_primitives::hex::encode(c))).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
    })
}

fn dump_divergence_inputs(
    block: u64,
    records: &[BufferedRecord],
    claims: &ClaimIndex,
    parent: Option<&PendingDelta>,
    granularity: u16,
    err: &ExecutorError,
) {
    // Same dir as the flight recorder's receipt-divergence dumper;
    // overridable so tests (and dev hosts) can assert the artifact without
    // /opt existing.
    let dir =
        std::env::var("KARDAMOM_FLIGHT_DIR").unwrap_or_else(|_| "/opt/kardamom/state".into());
    let path = std::path::Path::new(&dir).join(format!("divergence-{block}.json"));
    let mut payload = serde_json::json!({
        "block": block,
        "granularity": granularity,
        "error": format!("{err:?}"),
        "records": records_json(records),
        "parent_accounts": parent.map(|p| p.accounts.iter().map(|(a, (n, b, c))| serde_json::json!({
            "addr": format!("{a:?}"), "nonce": n, "balance": b.to_string(), "code_hash": format!("{c:?}"),
        })).collect::<Vec<_>>()),
        "parent_storage": parent.map(|p| p.storage.iter().map(|((a, k), v)| serde_json::json!({
            "addr": format!("{a:?}"), "slot": format!("{k:?}"), "value": v.to_string(),
        })).collect::<Vec<_>>()),
    });
    if let (serde_json::Value::Object(p), serde_json::Value::Object(c)) =
        (&mut payload, claims_json(claims))
    {
        p.extend(c);
    }
    match std::fs::write(
        &path,
        serde_json::to_vec_pretty(&payload).unwrap_or_default(),
    ) {
        Ok(()) => {
            tracing::error!(block, path = %path.display(), "divergence inputs dumped for offline replay")
        }
        Err(e) => tracing::warn!(block, error = %e, "divergence dump failed"),
    }
}

/// Build the validator's whole-block execution strategy: seeded parallel
/// batches when this block's BAL claims arrive in time; sequential
/// otherwise. Deposits participate like transactions — the executor
/// captures their writes into the BAL at their block index (mint as a
/// balance claim, CREATE bytecode as a code claim), so deposit-containing
/// blocks validate in parallel too.
pub fn parallel_block_exec<D: StateDatabase + Sync + 'static>(
    claims: Arc<crate::ClaimBuffer>,
    batch_size: usize,
    flight: Option<Arc<crate::flight::FlightRing>>,
) -> BlockExec<D> {
    Box::new(
        move |snapshot: &D,
              parent: Option<&PendingDelta>,
              records: &[BufferedRecord],
              env: ExecEnv,
              block: u64| {
            if records.is_empty() {
                return execute_block_sequential(snapshot, parent, records, env);
            }
            let Some((granularity, idx)) = claims.take(block, CLAIM_WAIT) else {
                crate::metrics::counter_parallel_fallback();
                tracing::debug!(block, "no BAL claims in time; sequential re-execution");
                if let Some(f) = flight.as_ref() {
                    f.push(block, 1, records, None);
                }
                return execute_block_sequential(snapshot, parent, records, env);
            };
            if let Some(f) = flight.as_ref() {
                f.push(block, granularity, records, Some(Arc::clone(&idx)));
            }
            let out = match execute_block_parallel(
                snapshot,
                parent,
                records,
                &idx,
                env,
                batch_size,
                granularity,
            ) {
                Ok(out) => out,
                Err(e) => {
                    // FLIGHT RECORDER: the live K=20 DeFi divergence is
                    // deterministic but has resisted offline modelling
                    // (the composition sweep passes). Dump the exact
                    // inputs so the failing block replays as a unit test.
                    dump_divergence_inputs(block, records, &idx, parent, granularity, &e);
                    return Err(e);
                }
            };
            crate::metrics::counter_parallel_block(out.batches);
            Ok(BlockExecOutput {
                receipts: out.receipts,
                delta: out.delta,
            })
        },
    )
}
