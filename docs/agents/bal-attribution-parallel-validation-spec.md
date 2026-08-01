# BAL access attribution + parallel validator re-execution (spec)

Status: DESIGN — not implemented. Companion to the throughput campaign notes
(`2026-07-28-throughput-campaign.md`).

## Goal

Let the validator re-execute a block's transactions **in parallel** while
*strengthening* — not weakening — the verification contract, by enriching the
existing per-block batched BAL with **per-transaction access attribution**:
which tx indices read and wrote which slots. The batched one-frame-per-block
shape stays; attribution is an additional section, dictionary-encoded so keys
are never repeated.

Secondary goals, in order:

1. **Enforced access contract**: the validator verifies each tx's *actual*
   accesses against its *declared* set during re-execution — an undeclared
   access is a divergence → fail-stop. The BAL stops being a passive
   cross-check artifact and becomes a checked claim (EIP-7928 in spirit).
2. **State prefetch**: the slot dictionary is exactly the block's touched
   set — one batched mdbx read pass warms everything before execution
   (valuable even for the sequential path).
3. **Forward reuse**: the same capture + DAG machinery is what an eventual
   parallel *executor* needs; proving it on the validator first means a
   concurrency bug surfaces as a loud divergence, never as silent state
   corruption (the validator's output is checked against the executor's).

## Non-goals

- Changing the executor's sequential execution (this spec only *captures*
  more from it).
- Trusting BAL final values instead of re-executing (self-checking a claim
  validates nothing).
- Cross-block parallelism (blocks stay sequential; the depth-K commit
  pipeline already overlaps commit with the next block).

## Wire format

Today the `tx_bal` frame is an rkyv-encoded `BlockDelta` with `receipts`
stripped (the #113 lesson: receipts dominated byte size and fat frames
collapsed the validator's lapse window on the byte-bounded term buffer).

New frame — a versioned wrapper so the validator accepts both during
rollout, and degrades gracefully forever:

```rust
enum BalFrame {
    /// Exactly today's payload (receipts-stripped BlockDelta).
    V1(BlockDelta),
    /// V1 + attribution. The merged section is UNCHANGED — consumers that
    /// only want final values (prefetch, existing write-set cross-check)
    /// read it exactly as before.
    V2 {
        delta: BlockDelta,
        attribution: Attribution,
    },
}

struct Attribution {
    /// Deduplicated slot dictionary. Account-level entries (nonce/balance/
    /// code_hash as one unit) and storage slots share one index space:
    ///   0..n_accounts           → account keys (Address)
    ///   n_accounts..            → storage keys (Address, B256)
    /// 52-byte keys appear ONCE regardless of how many txs touch them.
    accounts: Vec<Address>,
    storage: Vec<(Address, B256)>,
    /// Per tx, in block order: indices into the dictionary.
    /// `writes[i]` ⊆ dictionary; `reads[i]` ⊆ dictionary. u32 indices.
    writes: Vec<Vec<u32>>,
    reads: Vec<Vec<u32>>,
    /// Set when the encoded attribution would exceed `MAX_BAL_FRAME_BYTES`:
    /// the section is emitted EMPTY and the validator falls back to
    /// sequential re-execution for this block. Liveness never depends on
    /// attribution fitting.
    truncated: bool,
}
```

Notes:

- The merged `delta` remains the authority for final values (the existing
  write-set-hash cross-check is unchanged). Attribution adds ordering
  structure only.
- `reads` include every account/slot the tx observed (see capture below);
  `writes` are the tx's WriteSet keys — both refer to the same dictionary.
- Frames stay ONE per block. Aeron-level fragmentation now reassembles (the
  `AeronFragmentAssembler` work) up to `maxMessageLength` (term/8 = 2MB at
  the default 16MB term). Above that, `truncated` fires — no app-level
  chunking in v2 (see Size budget).

## Size budget

Per-tx cost ≈ (unique-new-slot keys × 52B amortized into the dictionary) +
(reads+writes indices × 4B). Plain transfers: ~3 dictionary entries and
~6 indices per tx ⇒ a 17,000-tx block (2s at 8.5k tps) ≈ 1.0–1.3MB — inside
the 2MB frame ceiling. Contract-heavy blocks with wide unique footprints can
exceed it; `truncated` degrades those blocks to sequential validation
rather than fatter frames (revisit chunking only if telemetry shows
truncation is common). Emit `kardamom_bal_attribution_bytes` +
`kardamom_bal_attribution_truncated_total` from day one.

## Executor-side capture

- **Writes**: already exist per tx (`WriteSet` before `delta.apply`) — free.
- **Reads**: a recording `Database` adapter wraps the per-tx `CacheDB`
  handed to revm (the outermost layer, so cache HITS are recorded too, not
  just snapshot misses): every `basic`/`storage`/`code_by_hash` records the
  key into a per-tx `HashSet`. Deterministic across replicas (same inputs,
  same access sequence). Estimated cost: a few HashSet inserts per access —
  small vs. EVM execution; measure before/after with the perf harness.
- The boundary path builds the dictionary + index lists while folding
  per-tx sets (one pass), then encodes once — same
  encode-on-exec-thread/publish-best-effort shape as today.
- Skipped txs (`invalid_skip`): empty write set; reads recorded as observed
  (they still read sender account state to fail deterministically).

## Validator-side parallel engine

1. Decode; if `V1` or `truncated` → today's sequential path (always kept).
2. **Prefetch**: batch-read every dictionary key from the state DB.
3. **DAG**: tx `j` depends on tx `i < j` iff
   `writes[i] ∩ (reads[j] ∪ writes[j]) ≠ ∅` (RAW + WAW; WAR is subsumed —
   a reader before a later writer is unordered only if the reader is in an
   earlier-or-equal wave, which the RAW edge from any earlier writer
   already forces). Waves = topological levels; within a wave, declared
   sets are pairwise non-conflicting by construction.
4. **Execute waves in parallel** over snapshot ∘ accumulated-prior-wave
   deltas. Each worker wraps its DB view in the same recording adapter and
   asserts, per tx:
   - `actual_reads ⊆ declared_reads` and `actual_writes = declared_writes`
     — any excess is a **divergence → fail-stop** (the executor executed a
     different footprint than it declared);
   - `declared \ actual` is a soft metric (over-declaration wastes
     parallelism but is sound).
5. Fold wave deltas in block order (within-wave order irrelevant — disjoint
   by contract), then the existing checks run unchanged: per-tx receipt
   comparison, merged write-set hash vs the BAL's `delta`.
6. **Determinism**: the serial-equivalence argument is that every conflict
   pair is ordered by an edge, so any wave schedule folds to the identical
   final state; a property test drives random conflict graphs through both
   engines and asserts byte-identical results.

## Rollout

1. **Capture + publish** (executor): V2 frames, attribution ignored by the
   validator; watch size/truncation/capture-overhead telemetry under the
   perf harness.
2. **Prefetch** (validator): dictionary-driven warm reads, still sequential
   execution — a latency win on its own.
3. **Parallel waves behind a flag** (`--parallel-validation`), soak +
   chain-semantics + chaos (validator-lapse, leader-kill) at parity with
   sequential before defaulting on.
4. Revisit for the executor once contract-heavy workloads make sequential
   execution the binding constraint.

## Test plan

- Property: random blocks with controlled conflict density — parallel
  result ≡ sequential result (state, receipts, write-set hashes).
- Divergence: a tampered attribution (missing declared read; forged write)
  must fail-stop, not mis-execute.
- Truncation fallback: oversized attribution → sequential path, no frame
  published above the cap.
- Determinism across two validator instances with different worker counts.
- Perf: capture overhead on the executor ≤ 2% at 8.5k tps (harness A/B);
  validator wall-clock per block vs sequential at varying conflict density.

## Open questions

- Read-set granularity for account fields: nonce/balance/code_hash are one
  dictionary unit here (matches `WriteSet`); splitting them would shrink
  false conflicts (gas-payment balance writes serialize same-sender txs
  anyway, which per-sender nonce ordering already forces) — likely not
  worth it in v2.
- Whether `code` reads (by hash) need attribution at all: code is
  append-only content-addressed — reads can never conflict with same-block
  writes except CREATE-then-CALL in one block, which the account-entry
  dependency already orders. v2 says: exclude code from attribution.
- Compression (lz4 over the rkyv frame) if truncation telemetry says
  contract workloads routinely exceed 2MB.
