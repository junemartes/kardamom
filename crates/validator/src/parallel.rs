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
        for (a, v) in &other.nonce {
            if !self.nonce.contains_key(a) {
                return format!("nonce {a:?}: unclaimed write {v}");
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

use kardamom_engine::block_env::ExecEnv;
use kardamom_engine::delta::PendingDelta;
use kardamom_engine::error::ExecutorError;
use kardamom_engine::exec_types::TxIndex;
use kardamom_engine::executor::execute_tx;
use kardamom_types::{BPosition, Receipt, StateDatabase, TxEnvelope};

impl ClaimSlice {
    /// Project a batch's merged [`PendingDelta`] into claim shape.
    pub fn from_pending(delta: &PendingDelta) -> Self {
        let mut balance = BTreeMap::new();
        let mut nonce = BTreeMap::new();
        for (addr, (n, bal, _code)) in &delta.accounts {
            balance.insert(*addr, *bal);
            nonce.insert(*addr, *n);
        }
        Self {
            storage: delta.storage.clone(),
            balance,
            nonce,
        }
    }
}

/// One transaction as the validator receives it from the canonical stream.
pub struct BlockTx {
    pub tx_idx: TxIndex,
    pub position: BPosition,
    pub envelope: TxEnvelope,
}

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
    addrs.sort_unstable();
    addrs.dedup();
    for addr in addrs {
        let claimed_bal = claims.balance_seed(addr, before);
        let claimed_nonce = claims.nonce_seed(addr, before);
        if claimed_bal.is_none() && claimed_nonce.is_none() {
            continue; // nothing claimed before this batch — snapshot stands
        }
        let base = match seed.accounts.get(&addr) {
            Some(v) => *v, // parent layer already has the freshest base
            None => snapshot
                .basic(addr)
                .map_err(|e| ExecutorError::State(format!("seed basic({addr:?}): {e}")))?
                .unwrap_or((0, U256::ZERO, alloy_primitives::KECCAK256_EMPTY)),
        };
        seed.accounts.insert(
            addr,
            (
                claimed_nonce.unwrap_or(base.0),
                claimed_bal.unwrap_or(base.1),
                base.2,
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
    txs: &[BlockTx],
    claims: &ClaimIndex,
    env: ExecEnv,
    first_index: u64,
    granularity: u16,
) -> Result<BatchOutcome, ExecutorError> {
    let mut delta = PendingDelta::new();
    let mut receipts = Vec::with_capacity(txs.len());
    let mut cumulative = 0u64;
    // At granularity K > 1 the wire claims are chunk-collapsed, so per-tx
    // comparison is impossible: verification coarsens to the CHUNK — the
    // batch is chunk-ALIGNED (batch_size == K, enforced by the caller),
    // its captured Bal is quantized through the SAME shared code the
    // executor used, and compared once at the end.
    let mut batch_bal = revm::state::bal::Bal::new();
    for (i, tx) in txs.iter().enumerate() {
        let bal_index = first_index + i as u64;
        let global_index_in_block = bal_index - 1;
        // Recompute this tx's claims through the executor's EXACT capture
        // path (execute_tx feeds revm's Bal, which records per-FIELD
        // changes: a transfer's recipient claims a balance change but NO
        // nonce change). Comparing a WriteSet projection instead diverged
        // on every live transfer — the WriteSet carries the full
        // (nonce, balance) triple for every touched account, so the
        // recipient's UNCHANGED nonce showed up computed-but-not-claimed.
        // Symmetric construction is the only drift-proof comparison.
        let (receipt, ws) = execute_tx(
            snapshot,
            Some(seed),
            &delta,
            env,
            tx.tx_idx,
            tx.position,
            &tx.envelope,
            global_index_in_block,
            cumulative,
            Some((&mut batch_bal, bal_index)),
        )?;
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
    let last_index = first_index + txs.len() as u64 - 1;
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

/// Re-execute a block's transactions as FULLY PARALLEL batches, each seeded
/// from the BAL's claims, verifying every batch's claims where they are
/// produced.
///
/// Returns `Err(ExecutorError::Divergence)` on the first batch whose
/// recomputed writes differ from what the executor claimed — the claim was
/// checked at its producing batch, so a false claim cannot be laundered by
/// later batches that merely consume it.
pub fn execute_block_parallel<S: StateDatabase + Sync>(
    snapshot: &S,
    parent: Option<&PendingDelta>,
    txs: &[BlockTx],
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
    use alloy_primitives::{TxKind, address};
    use alloy_signer_local::PrivateKeySigner;
    use kardamom_engine::state::MockStateDatabase;
    use kardamom_types::BlockBoundaryStart;

    fn tx(signer: &PrivateKeySigner, to: Address, nonce: u64, value: u64, i: u64) -> BlockTx {
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
        BlockTx {
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

    fn env() -> ExecEnv {
        ExecEnv::new(
            1,
            &BlockBoundaryStart {
                block_number: 1,
                end_tx_idx: BPosition::from_index(0),
                l2_timestamp: 1_700_000_000,
            },
        )
    }

    /// Build a claim index by SEQUENTIALLY executing the block through the
    /// executor's REAL capture path (`execute_tx` → revm `Bal`), exactly as
    /// the live executor produces claims. The first version of this fixture
    /// hand-rolled claims from WriteSets — symmetric with a verification bug
    /// (per-field vs whole-triple attribution), so both passed while live
    /// traffic diverged on every transfer. The fixture and the producer must
    /// share code, not shape.
    fn honest_claims<S: StateDatabase>(snap: &S, txs: &[BlockTx]) -> ClaimIndex {
        let mut bal = revm::state::bal::Bal::new();
        let mut delta = PendingDelta::new();
        let mut cumulative = 0u64;
        for (i, t) in txs.iter().enumerate() {
            let (r, ws) = execute_tx(
                snap,
                None,
                &delta,
                env(),
                t.tx_idx,
                t.position,
                &t.envelope,
                i as u64,
                cumulative,
                Some((&mut bal, (i + 1) as u64)),
            )
            .expect("seq execute");
            cumulative = r.cumulative_gas_used;
            delta.apply(ws);
        }
        ClaimIndex::from_alloy(&bal.into_alloy_bal())
    }

    fn seq_delta<S: StateDatabase>(snap: &S, txs: &[BlockTx]) -> PendingDelta {
        let mut delta = PendingDelta::new();
        let mut cumulative = 0u64;
        for (i, t) in txs.iter().enumerate() {
            let (r, ws) = execute_tx(
                snap,
                None,
                &delta,
                env(),
                t.tx_idx,
                t.position,
                &t.envelope,
                i as u64,
                cumulative,
                None,
            )
            .expect("seq execute");
            cumulative = r.cumulative_gas_used;
            delta.apply(ws);
        }
        delta
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
        let txs: Vec<BlockTx> = (0..12).map(|i| tx(&signer, to, i, 1_000 + i, i)).collect();

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
        let txs: Vec<BlockTx> = (0..8).map(|i| tx(&signer, to, i, 500, i)).collect();

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
        let txs: Vec<BlockTx> = (0..47).map(|i| tx(&signer, to, i, 100 + i, i)).collect();

        // The executor's view: per-tx capture, then the SHARED quantize.
        let per_tx = honest_claims(&snap, &txs);
        let _ = &per_tx;
        let mut bal = revm::state::bal::Bal::new();
        let mut delta = PendingDelta::new();
        let mut cumulative = 0u64;
        for (i, t) in txs.iter().enumerate() {
            let (r, ws) = execute_tx(
                &snap,
                None,
                &delta,
                env(),
                t.tx_idx,
                t.position,
                &t.envelope,
                i as u64,
                cumulative,
                Some((&mut bal, (i + 1) as u64)),
            )
            .expect("seq");
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
        if let Some(w) = forged.balance.get_mut(&to) {
            if let Some(e) = w.iter_mut().find(|(i, _)| *i == 2) {
                e.1 += U256::from(999u64);
            }
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
        let b1: Vec<BlockTx> = (0..4).map(|i| tx(&signer, to, i, 100, i)).collect();
        let claims1 = honest_claims(&snap, &b1);
        let out1 =
            execute_block_parallel(&snap, None, &b1, &claims1, env(), 2, 1).expect("block 1");
        let parent = out1.delta.clone();

        // Block 2: nonces 4..7. Against the bare snapshot every tx is a
        // nonce-mismatch skip; with the parent layer they execute.
        let b2: Vec<BlockTx> = (4..8).map(|i| tx(&signer, to, i, 100, i)).collect();
        // Build block-2 claims through the same capture path, WITH parent.
        let mut bal = revm::state::bal::Bal::new();
        let mut delta = PendingDelta::new();
        let mut cumulative = 0u64;
        for (i, t) in b2.iter().enumerate() {
            let (r, ws) = execute_tx(
                &snap,
                Some(&parent),
                &delta,
                env(),
                t.tx_idx,
                t.position,
                &t.envelope,
                i as u64,
                cumulative,
                Some((&mut bal, (i + 1) as u64)),
            )
            .expect("seq block 2");
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
}

// ---------------------------------------------------------------------------
// Engine strategy: what the validator hands to the exec loop
// ---------------------------------------------------------------------------

use kardamom_engine::actor::{BlockExec, BlockExecOutput, BufferedRecord};
use kardamom_engine::executor::execute_deposit_tx;
use std::sync::Arc;
use std::time::Duration;

/// How long a block waits for its BAL claims before falling back to
/// sequential re-execution. Short: liveness never depends on the BAL.
const CLAIM_WAIT: Duration = Duration::from_millis(250);

/// Sequential re-execution of a whole block — the always-available fallback
/// (no claims yet, or the block contains deposits, which carry no
/// attribution). Identical semantics to the engine's streaming path.
pub fn execute_block_sequential<S: StateDatabase>(
    snapshot: &S,
    parent: Option<&PendingDelta>,
    records: &[BufferedRecord],
    env: ExecEnv,
) -> Result<BlockExecOutput, ExecutorError> {
    let mut delta = PendingDelta::new();
    let mut receipts = Vec::with_capacity(records.len());
    let mut cumulative = 0u64;
    for (i, rec) in records.iter().enumerate() {
        let idx_in_block = i as u64;
        let (receipt, ws) = match rec {
            BufferedRecord::Tx {
                tx_idx,
                envelope,
                position,
            } => execute_tx(
                snapshot,
                parent,
                &delta,
                env,
                *tx_idx,
                *position,
                envelope,
                idx_in_block,
                cumulative,
                None,
            )?,
            BufferedRecord::Deposit {
                tx_idx,
                deposit,
                position,
            } => execute_deposit_tx(
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
            )?,
        };
        cumulative = receipt.cumulative_gas_used;
        delta.apply(ws);
        receipts.push(receipt);
    }
    Ok(BlockExecOutput { receipts, delta })
}

/// Build the validator's whole-block execution strategy: seeded parallel
/// batches when this block's BAL claims are available and it contains only
/// transactions; sequential otherwise. Deposits fall back because EIP-7928
/// attribution covers transaction execution, not L1-derived credits.
pub fn parallel_block_exec<D: StateDatabase + Sync + 'static>(
    claims: Arc<crate::ClaimBuffer>,
    batch_size: usize,
) -> BlockExec<D> {
    Box::new(
        move |snapshot: &D,
              parent: Option<&PendingDelta>,
              records: &[BufferedRecord],
              env: ExecEnv,
              block: u64| {
            let tx_only = records
                .iter()
                .all(|r| matches!(r, BufferedRecord::Tx { .. }));
            if !tx_only || records.is_empty() {
                return execute_block_sequential(snapshot, parent, records, env);
            }
            let Some((granularity, idx)) = claims.take(block, CLAIM_WAIT) else {
                crate::metrics::counter_parallel_fallback();
                tracing::debug!(block, "no BAL claims in time; sequential re-execution");
                return execute_block_sequential(snapshot, parent, records, env);
            };
            let txs: Vec<BlockTx> = records
                .iter()
                .map(|r| match r {
                    BufferedRecord::Tx {
                        tx_idx,
                        envelope,
                        position,
                    } => BlockTx {
                        tx_idx: *tx_idx,
                        position: *position,
                        envelope: envelope.clone(),
                    },
                    BufferedRecord::Deposit { .. } => unreachable!("tx_only checked above"),
                })
                .collect();
            let out =
                execute_block_parallel(snapshot, parent, &txs, &idx, env, batch_size, granularity)?;
            crate::metrics::counter_parallel_block(out.batches);
            Ok(BlockExecOutput {
                receipts: out.receipts,
                delta: out.delta,
            })
        },
    )
}
