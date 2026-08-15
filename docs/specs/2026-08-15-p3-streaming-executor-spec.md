# P3 — The Streaming Executor (pipelined Block-STM)

Status: DRAFT for review · Parent: `docs/agents/block-stm-executor-spec.md` (P3 §integration)
Depends on: `kardamom-stm` as of `4f3ea93` (PR #205)

## Why this exists — the measured boundary of block-at-a-time

The P2 engine, block-at-a-time, on the fully-independent yardstick
(`parcounter`, 4000-tx blocks, 4 workers, cores held hot):

    per-tx busy      15.6 µs   vs sequential 16.5 — the engine WINS per tx
    utilization      96%, dispatch perfectly balanced
    steady state     22.2 ms/block vs sequential 66.3  →  2.99×

The remaining wall per block is entirely **serial tail**, measured to the
last piece (phase timers, `KARDAMOM_STM_PHASE_TIMING`):

    span (execution)  16.4 ms      ← parallel, at per-tx parity
    commit            3.7 ms       ← hash 1.4 ∥ + fold 2.3 serial
    extract           0.9 ms       ← OnceLock → Vec<TxResult>
    validate          0.9 ms       ← parallel read-replay
    drain             0.3 ms

Two shaping facts, both measured and both binding on this design:

1. **Per-block scoped threads cannot win sub-millisecond phases.** A
   fused parallel extract+validate pass turned 1.8 ms of serial work into
   2.9–3.8 ms (spawn + affinity migration + double moves of ~500 B
   `TxResult`s) and was reverted. Tail parallelism must come from
   **persistent** threads or from **overlap**, never from per-block spawns.
2. **The 3.5× target needs ≤ 18.9 ms/block.** Shaving the tail thinner
   tops out ~19.9 ms (3.3×). Taking the tail **off the critical path** —
   overlapping block N's tail with block N+1's execution — removes up to
   5.8 ms and lands 16.4–17 ms (3.9–4.0× ceiling, 3.5× with margin).

That overlap is this spec. It is also, not coincidentally, the shape the
production executor needs anyway: the sealer emits boundaries on a
cadence, and an executor that idles its workers during every commit tail
throws away exactly the milliseconds this campaign spent recovering.

## Architecture

Five stages, each owning one resource class. Stages communicate by
channels; blocks flow left to right; at steady state stage k works on
block N while stage k−1 works on N+1.

```
 tx_data readers ──► serial feed ──► workers (4×) ──► tail (persistent) ──► writer
   decode+predict     admission        execute            validate            mdbx
   (parallel, P1's    canonical        (MvCache,          hash               commit
    Prepared API)     order, DAG       keep-hot,          fold
                      + sticky         pinned)            receipts
                                                          advance_base
```

- **Readers** already exist (`prepare()` / `push_prepared` — the P2 bench
  measures with decode off the feed precisely because P3 pays it here).
- **Feed** is the serial section: canonical-order discovery
  (`last_toucher`, sticky assignment, eager-chain classification). ~0.7
  µs/tx measured; it feeds block N+1 while N's tail runs.
- **Workers** are the P2 pool unchanged: pinned, keep-hot, per-worker
  FIFO + graph, work stealing with take-time verification.
- **Tail** is ONE persistent thread (two on wide hosts) that owns, per
  block, in order: extract → validate → wound repair (rare) → sink
  prefix fixup → parallel hash (chunks executed BY THE PARKED WORKERS —
  they are pinned to hot cores and idle during tail; the tail thread is
  their coordinator, not a spawn site) → fold → `advance_base` →
  publish outcome.
- **Writer** is the existing state writer, unchanged.

### The unsettled-delta chain

Block N+1 executes before N's delta reaches mdbx. Its reads must see
N's writes. The machinery exists and is already load-bearing:

- `BlockInput { snapshot, base: Option<&PendingDelta> }` layers a pending
  delta over the backend — probed BEFORE the pool-lifetime base cache
  (ordering fixed in `19234cb`, and the reason it was fixed that way).
- `PendingDelta::merge_from` maintains ONE merged layer over multiple
  unsettled blocks — its doc comment has described this pipeline since
  it was written.
- The pool-lifetime base cache advances by `advance_base(&delta)` only
  when a delta SETTLES (writer-committed), never for pending layers.

Rules:

1. `base(N+1) = merge(unsettled deltas ≤ N)`, maintained by the tail as
   each block folds. Pipeline depth is capped at **2** (one block
   executing, one in tail); depth 2 keeps the merged layer to a single
   `merge_from` and bounds wound-abort blast radius to one block.
2. Snapshots for N+1's workers open at the last WRITER-COMMITTED height;
   the pending layer covers the difference. This is exactly the
   `BlockInput` contract the equivalence test `base_delta_layer_is_visible`
   pins today.
3. `advance_base` moves values from "pending layer" to "cache mirror of
   backend" when the writer confirms; the pending layer shrinks by the
   same delta. At no point may a cell be visible in both with different
   values — the transfer is atomic per block (swap the merged layer
   pointer after advancing).

### Two phases, one design decision

**P3a — fold-first, non-speculative.** The tail reorders to
extract → validate → repair → fixup → fold → *release delta to feed* →
hash ∥ receipts. N+1's feed starts only after N's delta is final
(post-repair). Overlap captured: hash + receipt assembly + outcome
publication (~2.3 ms of 5.8). Expected: ~19.9 ms/block, **3.3×**. No new
correctness surface at all: N+1 never sees a delta that validation could
still change.

**P3b — speculative-validate with wound-abort.** The tail releases N's
delta to the feed IMMEDIATELY after fold, CONCURRENT with validation
(fold before validate — sound because fold is deterministic in the
already-executed write sets; only a WOUND can change them). N+1 executes
speculatively. If N's validation convicts (wounds > 0):

1. abort N+1's session (the pool's `aborted` flag + drain — the
   machinery the error path uses today);
2. repair N sequentially (existing wound path, unchanged);
3. re-derive N's delta, rebuild the merged layer, re-run N+1 from its
   Prepared set (retained until its predecessor settles).

Cost of a wound: one aborted block re-executed — bounded, loud
(`stm_wound_abort_total`), and priced only when wounds fire. Measured
wound rate across the entire campaign, every workload, every width:
**zero**. Expected steady state: ~16.4–17 ms/block, **3.9–4.0× ceiling**.

P3a ships first and is the fallback mode (`--stm-pipeline=conservative`);
P3b is the default target (`--stm-pipeline=speculative`). Both are
behind `--parallel-execution` exactly as the parent spec requires.

## API

On `PoolHandle` (the bench and the actor share it):

```rust
/// Submit a block; returns immediately once the feed has accepted it.
/// At most one block may be unsettled ahead of this one (depth 2).
pub fn submit_block(&self, …, prepared: Vec<Prepared>) -> BlockTicket;

/// Block until this block's outcome is final (validated, folded,
/// repaired if wounded). Outcomes complete in submission order.
impl BlockTicket { pub fn wait(self) -> Result<StmOutcome, ExecutorError>; }
```

`run_block_prepared` remains as `submit + wait` — the block-at-a-time
harness and every existing test keep their exact semantics.

## Invariants (additions to the parent spec's four)

5. **Outcome order is submission order**, and each outcome is
   byte-identical to sequential execution of its block over its parent's
   final state — the pipeline is invisible in every output stream.
6. **A wound anywhere unwinds everything after it.** No output derived
   from a speculatively-wrong delta can escape: outcomes publish only
   after their own validation AND their predecessor's settlement.
7. **The pending layer is single-writer** (the tail thread); the feed
   reads it only between blocks; workers read it only through
   `BlockInput`. No new shared mutable surface.

## Testing

- **Equivalence, pipelined**: the existing harness gains a mode that
  submits all blocks eagerly and waits with lag 1, asserting the same
  byte-identical receipts+delta per block. Every current test runs in
  both modes.
- **Wound-abort adversarial**: the lying-stats generator (P2's
  `wrongly_trained_stats_still_produce_identical_bytes`) extended to fire
  a wound in block N while N+1 executes speculatively; asserts abort,
  re-run, byte-identical final outputs, and `stm_wound_abort_total == 1`.
- **Depth pressure**: submit at depth 2 continuously for 200+ blocks
  (the drain/leak test shape) asserting no deadlock, no leak, monotone
  settlement.
- **The ladder**: parcounter (independent), sender-chain rung, shared
  contract rung, uniswap — pipelined vs block-at-a-time vs sequential,
  byte-identical everywhere.
- **The live oracle** (parent spec): full-pipeline-e2e with the in-stack
  validator re-executing every block — an STM bug is a loud halt with
  forensics, not a silent diff.

## Measurement gates

On the P2 bench host discipline (cores pinned + keep-hot, background
shepherded, per-core frequency verified — see the campaign memory):

- parcounter 4k×w4: **≥ 3.5×** aggregate over ≥ 20 warm blocks (P3b);
  ≥ 3.2× (P3a).
- No rung of the ladder regresses vs block-at-a-time.
- Wound-abort microbench: forced-wound block costs ≤ 2.5× its clean
  execution (abort + re-run, no cascade).
- Phase timers extended per stage; the per-block tail-on-critical-path
  time must read ~0 in steady state (that IS the design's claim).

## Milestones

- **P3a**: submit/wait API, persistent tail thread, fold-first overlap,
  pipelined equivalence mode green, ≥3.2× measured.
- **P3b**: speculative validate, wound-abort protocol, adversarial tests
  green, ≥3.5× measured.
- **P3c**: actor integration (`--parallel-execution`), determinism suite
  both modes, full-pipeline-e2e with the validator oracle, perf + alloc
  gates from the parent spec.

## Explicitly out of scope

Credit-deferral / `pending_credits` observation edges (designed earlier
in this campaign — composes with, but is not required by, this spec);
cold-barrier watermark (O(1) barriers — filed, independent); multi-node
concerns (the pipeline is within one executor).
