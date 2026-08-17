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

## P3b first measurement (2026-08-16) — the sync point is in the wrong place

Engine protocol landed and adversarially tested (streaming release at
fold, wound → corrected re-issue → consumer `abort_active` → rebuild;
the test also exposed and fixed a latent repair-path bug: layers were
dropped from the materialized prefix). Bench grew
`--pipeline-speculative`: block N+1 layered on the ENGINE's released
delta, production-shaped.

Measured (parcounter cw100, 4k-tx blocks, w=4, clean-box protocol):

    block-at-a-time      2.68x   (21.4 ms/block)
    pipeline, baseline   2.66x   (layers known in advance — mechanics only)
    pipeline, SPECULATIVE 2.08x  (26.1 ms/block)

The naive sequencing LOSES: waiting for N's fold-release before
BUILDING N+1's session puts the feed (~3 ms) and the fold (~2.5 ms)
back on the critical path — the exact milliseconds the pipeline exists
to hide. The release point is as early as it can be; the consumer's
wait is what must move.

Next unit, in order of leverage:

1. **Late-bound layers**: admission (predict + DAG + queues) is
   layer-independent — only `sink_start` and worker reads need them.
   Build and FEED N+1's session during N's execution; bind the layer
   vec when the release arrives; gate worker start on bind (the
   install/generation gate already exists). Recovers the feed (~3 ms).
2. **The mv cache IS the layer**: N's MvCache already holds every
   final value (highest version = final; read at idx MAX) the moment
   execution drains — BEFORE any fold. Let N+1's MvView probe
   predecessor caches directly; the fold then runs entirely off the
   critical path (needed only for writer settlement and
   advance_base). Removes the fold (~2.5 ms). Deeper speculation
   (pre-verdict values), same wound-abort unwind — machinery already
   tested. Requires MvCache to outlive its block (MvPool exists on
   `stash/flat-seqlock-tables`; the RwLock cache pools the same way).
3. Ceiling then ≈ span + install ≈ 15.4 ms → ~3.5x at cw100 — the
   goal number, with span-slack packing (~2.6 ms of imperfect
   dispatch) as the remaining margin.

## P3b late-bound layers (2026-08-16) — feed recovered; the fold chain is next

Landed: `begin_block_deferred` + `LayerBinder` (weak — a binder must
never stall the tail's ctx unwrap; found as a 30s STALL_TIMEOUT burn by
the never-bind test), worker late-bind gate, bench sequencing that
builds/feeds/submits fi during fi-1's execution and binds at fi-1's
release. Adversarial test upgraded to this exact sequencing.

Measured (same protocol): 2.08x → **2.20x**; block-at-a-time 2.56x on
that run. Loop split per block: feed 4ms (now hidden), bind-wait
17-18ms. The bind-wait decomposes as fi-1's remaining execution
(~13ms, the irreducible sequential dependency) + drain 0.3 + extract
~1 + phase-1 + fold ~2.5 — i.e., ~4ms of fold-chain still serial,
plus the pipeline's layer-probe tax inside the span.

Sharpened ceiling math (cw100, w=4, seq 50ms/block): ideal span =
busy/4 = 13.8ms → 3.62x; block-at-a-time span 15.2 (slack 1.4);
3.5x = 14.3ms/block. So the goal needs ALL of: (a) release before the
fold — the mv cache IS the delta (top version per cell == last
writer's write set == what the fold computes); the release then needs
only drain + extract + the sink running-sum (~1.4ms), with the fold
running off-path for writer settlement only; (b) the layer tax held
down — at steady state the writer confirms fast enough that layer
windows go empty (advance_base absorbs them into the base cache);
(c) span packing (~1.4ms of dispatch slack). Wound semantics
unchanged: mv values pre-verdict equal fold values pre-verdict by
construction, and a wound corrects/aborts through the existing
protocol.

### mv-as-layer — design decisions (pre-implementation)

- **Release**: `MvRelease { block, mv: Arc<MvCache>, sink_final: Option<AccountInfo> }`
  sent by the tail right after drain + extract + a fee-delta running
  sum (~1.4ms past exec) — before phase-1/fold. `BlockCtx.mv` becomes
  `Arc<MvCache>`; the reaper ships slots only.
- **Reads**: `BlockInput` gains `mv_layers: &[Arc<MvCache>]`, probed
  newest-first between own-mv and the delta layers: account/slot at
  `u32::MAX` (top version == last writer == exactly what the fold
  computes), `read_code` chained for CREATE-then-call across blocks.
  Recorded as `SeenVersion::None` (block-input semantics — predecessor
  caches are immutable after their drain; repair never writes mv).
- **Sink**: mv skips the fee sink, so the release carries the final
  sink account (start + fee-delta sum); `bind` requires it when mv
  layers are present, probes through delta layers otherwise.
- **Repair**: `MvCache::final_delta()` (top version per cell + code)
  materializes each mv layer; the prefix merges base ← delta layers
  (oldest first) ← mv layers (oldest first). Rare path, fold-cost.
- **Wounds**: unchanged protocol. Pre-verdict mv content equals
  pre-verdict fold content by construction; a wound corrects via the
  existing DeltaRelease and the consumer rebinds with the corrected
  DELTA layer (never the stale mv).
- **Rejected alternative — absorb into the base cache at release**
  ("advance early"): zero probe tax, but bind must wait for a
  fold-equivalent materialization (final_delta + advance_base) before
  the first read, putting the cost right back on the cadence; and a
  wound would leave stale cells in the pool-lifetime cache that the
  corrected delta does not necessarily overwrite (different execution
  path → different write set) — cache deletion of stale-minus-
  corrected keys would be required. The probe-level design keeps the
  cache backend-mirroring and wound-clean.
- **Expected**: cadence ≈ exec + ~1.4ms → ~3.0x at cw100; the
  remaining gap to 3.5x is the mv-probe tax inside the span plus
  ~1.4ms dispatch slack — both span-level work, after this lands.

## mv-as-layer landed + the span-inflation investigation (2026-08-16)

Landed and adversarially green (25/25 wound reps byte-identical):
`MvRelease` (early release at drain+extract, pre-fold, sink
materialized alongside), `submit_streaming_mv`, `bind_with` (mv layers
+ sink override), mv probes in `BlockInput` AND `MvView`'s own walks,
`MvCache::final_delta()` for the repair prefix, serial tail for the mv
pipeline (validate-first, one thread — parallel lanes are pointless
when the tail hides behind the next span).

Ladder of measurements (parcounter cw100 4k w=4, clean protocol):

    naive speculative (fold-gated build)    2.08x
    late-bound layers (feed overlapped)     2.20x   bind-wait 17.4ms
    mv-as-layer (fold off the path)         2.38x   bind-wait 15.1ms
    block-at-a-time (reference)             2.53x

**The blocker, isolated**: pipeline worker busy = 63-66ms vs 55.3
block-at-a-time (+13-19%), while the read path is FLAT (3.4 vs 3.3ms
— the mv probe tax is negligible). Facts established:

1. A pure CPU spin hog on the caller cores does NOT inflate worker
   busy (55.4 ≈ 55.3): not core stealing, not frequency.
2. An external ALLOC-STORM hog on the caller cores inflates
   block-at-a-time busy to 64-65ms — reproducing the pipeline's
   inflation exactly. The channel is allocation-related crosstalk.
3. mimalloc as global allocator: REVERTED — raised block-at-a-time
   busy (+11%) without helping the pipeline. So NOT user-space arena
   locks; prime suspect is the kernel mmap_lock / page-fault path
   (per-block ctx construction maps fresh 4096-slot arenas + MvCache
   while the reaper unmaps the predecessor's — all during the span).
4. The delta-baseline pipeline shows the same inflation (62.5-63.6):
   it is pipeline-generic, has been present since P3a, and explains
   why the pipeline never beat block-at-a-time on micro-tail
   workloads.

Next unit, first experiment: POOL the block ctx allocations (nodes
arena, slots/results OnceLock arrays, MvCache — the reverted MvPool on
`stash/flat-seqlock-tables` is the pattern) so steady-state blocks map
no new memory and the reaper unmaps none. If busy returns to ~55, the
pipeline immediately reads ~2.9-3.0x (cadence 16.9ms measured minus
the inflation), and the remaining path to 3.5x is span packing
(~1.4ms slack) plus caller-side loop slop (~2ms). Topology note for
all of this: callers 0,1,6,7 are ONLY TWO physical cores (SMT pairs
0/6 and 1/7); workers 2,3,4,5 each own a physical core with an idle
sibling.

### Span-inflation: two more negatives, one surviving hypothesis

- `mallopt(M_MMAP_THRESHOLD/M_TRIM_THRESHOLD, 1GB)` (heap recycling,
  no per-block mmap/munmap, no TLB-shootdown IPIs): NO change
  (pipeline busy 64.5, block-at-a-time 55.6). Reverted.
- Together with the mimalloc negative, everything user-space-allocator
  and kernel-mapping shaped is excluded.

The surviving hypothesis is **LLC eviction**: the pipeline's services
stream several MB through the shared L3 every block (settler
delta-clone + finalize sort, writer apply, feed prepare, ctx build)
concurrently with the span — and every established fact fits: a spin
hog (no memory traffic) does not inflate; an alloc-storm (heavy
streaming) reproduces the inflation exactly, from ANOTHER PROCESS
(shared L3, not shared locks); allocator swaps move the same bytes and
change nothing; the inflation shows in evm-not-read because L3 misses
hit the interpreter's working set, not just state reads.

To confirm and fix (next session):
1. Confirm with LLC-miss counters (no `perf` on this box — install or
   use resctrl occupancy MSRs if permitted).
2. Reduce bytes moved per block: the settler's `out.delta.clone()` is
   BENCH-ONLY pollution (outcomes retained for post-hoc asserts —
   production consumes the delta); finalize could take ownership.
   The writer's apply is production-real; its streaming footprint is
   the honest floor.
3. If the floor still inflates: L3 partitioning (resctrl/CAT) between
   worker cores and service cores is the structural answer on shared
   boxes; on production hosts with private-L3 core clusters, pinning
   services to a different CCX than workers achieves the same free.

Fourth negative: removing the settler's on-clock delta clone
(KARDAMOM_PIPE_ASSERT=0 pure-timing mode, settler consumes the
outcome) — busy unchanged (63-66ms). The inflation's remaining feeders
are production-real (writer apply, feed prepare, ctx build). Software
knobs are exhausted; the LLC hypothesis now requires hardware counters
(perf/resctrl) before further engineering — or CCX separation on a
host that has one to give.

### Counter data (perf unlocked) + allocation-diet effect (2026-08-16)

Worker-core counters (paranoid lowered; AMD generic cache events — LLC
events unsupported on this PMU): pipeline workers run at **+37%
cache-misses per instruction** vs block-at-a-time (1.36 vs 0.99 per
kilo-instruction, equal ~9-10% miss rate); the raw misses/s doubling is
duty cycle (pipeline workers execute back-to-back). Moderate,
consistent with the mv-layer probe level plus co-tenant streaming —
the LLC-pressure attribution stands at reduced magnitude after the
allocation diet.

Allocation diet (user-directed): 19.3 → 11.7 KB/tx (-39%), realloc
churn zeroed (warm mv scrub), STM now allocates less often than the
sequential engine (23.1 vs 27.8 allocs/tx). Effect on the gap: at 24
flow blocks (pools warm for 20+), block-at-a-time 2.67x vs pipeline
2.62x — TIED within noise, from a 0.3x deficit before the diet.

Remaining, in leverage order: (1) span packing (~1.4ms dispatch slack
— now the largest single item on the cadence); (2) the mv-layer probe
level (skip it when the window is empty — steady-state windows shrink
as the writer keeps up; check window occupancy stats first); (3) the
last co-tenant streams (writer apply, feed) via CCX separation on
hosts that have one.

### Allocation diet — final state (2026-08-16)

User-directed target: only tx data + state diff + receipt per tx.
Ladder, all measured on the bench's counting allocator (process-local,
immune to box noise):

    start                                19.3 KB/tx  (~77MB/4k block)
    arena recycling (slots/results/nodes/mv)   14.1
    read-record buffer recycling               13.1
    warm mv scrub (keys+buffers kept)          11.7   reallocs -> 0
    carcass vec + fold-delta shells             6.7   big bucket -90%
    pooled journal state (table hand-back)      4.9   <4K bucket -99.5%

The engine now allocates LESS than the sequential baseline (20.5 vs
27.8 allocs/tx; 4.3 vs 8.0 KB/tx) and `perf` puts malloc at **0.07% of
process cycles** — allocation is no longer a time cost. Remaining
composition: ~2.4KB/tx of <512B objects (per-account journal storage
maps, ws spills, receipt internals — attribution needs an alloc-site
tracer, not cycle sampling, and has no measurable time payoff), ~0.6KB
of receipts (the deliverable), small fry. The revm journal's state
TABLE recycles via pub `Journal.inner.state` — no JournalTr
reimplementation was needed. Seq-side equivalent (hand-back slot on
the commit-cache db) left unimplemented: it would only lower the
baseline denominator.

Shared-box note: perf -C windows caught ANOTHER Claude session's V8
"HeapHelper" threads at 50% on the worker cores (affinity 0-11).
Process-scoped `perf record -p/-- cmd` is immune; timing comparisons
on this box must check `pgrep claude` first.

## Admission-queue redesign (2026-08-16, co-designed) — claim-CAS ring, tombstone skips

Motivation (measured): for independent micro-txs the serial feed IS the
span (workers consume one 2.5µs tx per 0.6µs; admission delivers one
per ~1.0-1.5µs; utilization 58%). The queue op is only ~0.2µs of that,
but its LOCK is what prevents sharding the rest of admission later —
and the pop lock is what the stall/steal paths serialize on.

Structure (per worker, pre-allocated, O(n) memory, O(1) amortized ops,
lock-free, NO unsafe — packed AtomicU64 slots):

    slot: EMPTY(0) | VALUE(tag|tx) | TOMBSTONE(tag|skip)
    append:  tail.fetch_add + slot.store(VALUE, Release)   [feed/prune]
    claim:   verify fifo_ready(tx) THEN CAS VALUE->TOMBSTONE
             — sound WITHOUT a lock because readiness is MONOTONE
             (a completed predecessor stays completed); the CAS is the
             single consumption point, so putback ceases to exist.
    head:    owner-only (thieves claim mid-queue, never touch head);
             advance follows tombstone skip pointers with lazy
             union-find-style compression (stale skips only undershoot
             — safe, amortized O(1)).
    queued flag: stored AFTER the publish; a racing coverage check
             reads false and takes an edge (the always-correct path).
    wrap:    cap = MAX_BLOCK_TXS and <=1 push per tx per block — the
             ring never wraps mid-block; reset via the arena scrub.
    parking: tiny per-worker mutex+cv, COLD path only.

Wins: feed append ~10ns uncontended and shardable later; owner pop = 1
CAS; the stalled-head case IMPROVES (owner claims a later ready item
instead of yield-spinning); the pop/verify/putback race class (two
measured wedges in the campaign) is structurally gone.

Rejected on the way: a SHARED independent lane (no assignment at all)
— perfect balance for independents, but chain HEADS are
indistinguishable from independents at admission, so chains lost their
FIFO anchors and eager coverage collapsed to edges (hot_chain test
failed 8/8, deterministically). The ring keeps every chain semantic in
place.

Test plan: the flat-table torture methodology — packed-word oracle
tests, claim-storm races (owner vs thieves vs producers), stalled-head
chains, plus the standing equivalence + lying-stats gauntlets.

Follow-up (needed for 4x on independents): slim the remaining ~1µs of
admission — batch node init, cheaper last-toucher upserts, and (the
lock now being gone) shard admission for predicted-independent txs.

### SUPERSEDED by the bag scheduler (same session, user question: "why
### contiguous at all?")

The ring encoded order positionally; the order already lives in the
DAG. Dispatch-only-when-runnable makes the ready-set UNORDERED, and
the whole positional machinery deletes:

- ONE shared lock-free bag (ArrayQueue, preallocated, O(1)) for every
  runnable tx — no assignment, no stealing, balanced by construction.
- CHAIN-LOCAL HAND-OFF: completing link n processes its children
  INLINE (uncontended per-node child lock + indegree fetch_sub); the
  first zero-indegree child becomes the worker's OWN next job (a local
  slot, zero queue ops — chain affinity and streaming for free); the
  rest go to the bag.
- DELETED: per-worker queues + locks, queued flag, fifo_preds,
  take-time verification, stall/putback, steal, prune batching +
  completed buffers (inline completion), sticky queue assignment.
- The earlier shared-lane failure does NOT recur: coverage no longer
  exists, so chain heads need no FIFO anchor — chains anchor on DAG
  successor pointers.
- Rollout FLAG-GATED (scheduler = eager-fifo | bag): equivalence
  harness runs both; flip on measurement; delete the old machinery in
  a follow-up once the ladder confirms.

Risks: single bag CAS traffic (fine at ~200k dispatches/s vs multi-M
capability; shard the bag if ever hot), chain affinity via local-next
instead of sticky domains, fifo_covered/steal metrics + tests retire
with the machinery.

## Sharded admission (design; build when the feed binds again)

### Why it is possible at all

The feed is serial for exactly ONE reason: `last_toucher` (cell -> most
recent toucher) must be read and written in canonical order. Everything
else admission does is already concurrency-safe per node — the
registration point (`open` under the child-list lock) and the indegree
guard were built that way. So the serialization is a property of ONE
data structure, not of the algorithm.

Partition the CELL SPACE, not the tx stream: cell `c` belongs to shard
`h(c) % K`, and each shard owns its own `TouchTable`. No shard ever
writes another's table, so the shards need no locks between them.

### Protocol

Router (serial, ~0.15µs/tx — slot store + node init + enqueue):

    S_j   = { h(c) % K : c in cells(j) }      // from prepare()'s hashes
    node[j].indegree = |S_j|                  // one guard PER SHARD
    node[j].open = true; children.clear()
    for s in S_j: queue[s].push(j)            // in canonical index order

Shard s (parallel, one thread each, processes its subsequence IN INDEX
ORDER):

    for each c in cells(j) with h(c)%K == s:
        if let Some(p) = table[s].upsert(h(c), j):   register edge p->j
    drop this shard's guard: if indegree.fetch_sub(1) == 1 { dispatch j }

### Why it is correct

1. **No conflict edge can be missed.** For any i < j sharing cell c,
   the shard owning c sees both, in index order (each shard's queue is
   canonical). When it processes j it finds `last_toucher[c] == i` and
   registers — or finds i already finished, which needs no edge (its
   writes are published). Cells map to exactly one shard, so exactly
   one shard is responsible for each real dependency.
2. **No premature dispatch.** j carries |S_j| guards; every shard drops
   one only AFTER registering all of its edges for j, so j cannot
   dispatch while any shard still owes it an edge. This is today's +1
   guard, generalized.
3. **No half-initialized node.** The router initializes node j before
   publishing j to any shard, and a predecessor can only learn of j
   through a shard's table.
4. **Duplicate edges are harmless.** Two cells in different shards can
   share the same last toucher p: p's child list then holds j twice and
   decrements j's indegree twice, matching the two increments. Only
   the within-shard dedup survives; cross-shard duplicates cost two
   atomics, never correctness.
5. **Validation is untouched.** The read-set replay still convicts any
   dependency the PREDICTOR missed — sharding changes who registers a
   predicted edge, not what is checked.

### Cold (⊤) barriers need a global sync

A cold tx conflicts with everything: it must take edges from all
outstanding txs, become the barrier every later tx depends on, and
clear every shard's table. The router therefore quiesces all shards at
a cold tx (wait until each has drained past index i), performs the
barrier registration serially, then resumes. Cold txs are rare by
construction (untrained selectors; steady-state blocks measure zero),
so a stall there is simpler and cheaper than any lock-free alternative.

### What it is worth (fan-out math — read before building)

A shard's work is CELL-events, not txs. With `m` cells per tx and K
shards, each shard handles `n*m/K` cell-events, so the parallel speedup
of the expensive part is `K/m` capped by the router:

    pacing = max( router_per_tx , (m/K) * shard_cost_per_cell )

For transfers (m = 2): K=4 halves the shard portion (~0.35 -> ~0.18
µs/tx), and the router floor (~0.15-0.18) then dominates — total
admission pacing ~0.18µs/tx vs 0.51 today, i.e. ~2.8x headroom, NOT
Kx. Pushing further means shrinking the router (readers doing node init
themselves, with a monotone `prepared_upto` watermark so shards can
scan a per-shard index bitmap in order and skip the router entirely).

### When to build it

NOT NOW. The Amdahl budget is `per-tx busy / workers`; measured today
at w=4: transfers 1.18µs (feed uses 43%), contract calls 3.6µs (14%).
Admission stopped being the binding constraint when it fell to
0.51µs/tx — the binder on micro-tx blocks is now the COMMIT TAIL
(~2.7ms per 4k block, ~29% of block wall), which is what the P3b
pipeline hides. Build sharded admission when either (a) worker counts
go to 8-12 on micro-txs, where the budget falls to 0.6/0.4µs and the
feed binds again, or (b) the pipeline has hidden the tail and the feed
resurfaces as the span. Shard threads must live on the CALLER cores
(the worker cores stay dedicated), which on this host is 2 physical
cores — so K=2..3 in practice.

## The commit tail, decomposed (2026-08-16)

Measured per 4000-tx block, both workloads (the tail is TX-COUNT driven,
not gas driven — it is the same ~3.6ms for 21k transfers and for
100-round contract calls):

    extract            0.85 ms   OnceLock takes + 1.8MB of TxResult moves
    overlap scope      2.26 ms   fold ∥ (hash + validate) lanes
      lanes' own work  5.77 ms aggregate = 1.44 µs/tx
        write-set hash   ~1.1 µs/tx   <-- 70% of the tail's work
        validation       ~0.3 µs/tx
      spawn/join gap   ~0.34 ms
    delta assign       0.40 ms
    -------------------------------------------------
    tail total         ~3.6 ms  (40% of a micro-tx block's serial time)

### The hash is the tail, and it is NOT cache misses

`WriteSet::hash` microbenchmark (`exec-core/tests/hash_cost.rs`), 3
accounts, data HOT in L1: **1110 ns/tx**; cold-ish: 1247. So the cost is
the keccak permutations themselves, not the data path — a transfer's
write set encodes to 297 bytes = 3 Keccak-f permutations at ~290ns
each, which is simply what Keccak-f[1600] costs on this core.

Consequences:

1. **Both engines pay it.** Sequential hashes inline per tx (~22% of a
   transfer block's sequential time); STM pays it in parallel lanes. So
   making keccak cheaper improves ABSOLUTE throughput on both sides and
   moves the RATIO only slightly (and in either direction, depending on
   which side's share is larger).
2. `asm-keccak` (XKCP assembly, identical output — the byte-identical
   harness proves it) measured: hash 1110 -> 874 ns/tx; engine
   parcounter 2.95 -> 3.09x with stm 134 -> 125ms; partransfer stm 64 ->
   62ms with the ratio inside noise. Wired as an OPT-IN feature
   (`kardamom-stm/asm-keccak` -> `exec-core/asm-keccak`), never default:
   zkVM/RISC-V guests must keep the portable backend. Recommend enabling
   it on native node binaries.
3. **The only large remaining lever is the ENCODING, and it is a
   consensus-format decision, not a perf change.** Today each account
   costs 20 (addr) + 8 (nonce) + 32 (balance) + 32 (code_hash) = 92
   bytes, so 3 accounts + section tags = 297 bytes = 3 permutations.
   Encoding the balance and nonce as varints and omitting `code_hash`
   when it is empty/unchanged puts a transfer's write set near ~130
   bytes = ONE permutation — a ~3x cut of the single largest fixed cost
   in BOTH engines. It changes every receipt's `write_set_hash`, so it
   needs operator sign-off and a coordinated rollout; flagged, not done.

### What was done here

- The fold moved off its own spawned thread onto the TAIL thread (a
  caller core): one fewer spawn, the release point is reached without a
  join, and all four worker cores now run hash lanes (was 3 + fold).
  partransfer 2.13 -> 2.18x, parcounter 2.93 -> 2.95x, commit 18.9 ->
  18.4ms per 7 blocks.
- Hiding the tail instead of shaving it was RE-MEASURED and remains a
  loss on this host: pipeline 1.18-1.31x vs block-at-a-time 2.18x
  (partransfer), 2.56-2.62x vs 2.92x (parcounter), with a parallel tail
  and with a serial one. The pipeline's cadence is dominated by the
  span-inflation effect, not by the tail it removes.

### Compact witness encoding — DONE (2026-08-16, operator-approved)

Consensus format v2 (`WriteSet::encode`, version byte 0x02): minimal-
width balances/values with carried widths, varint nonces and counts, a
2-bit code-hash tag (KECCAK_EMPTY and ZERO cost nothing), and the
address emitted once per run of slots sharing it. Canonical and
injective; one encoder now feeds both the stack-buffer and streaming
sinks so they cannot drift.

    WriteSet::hash   1110 -> 483 ns/tx   (384 with asm-keccak)
    partransfer      seq 139 -> 120ms, stm 64 -> 58ms, tail -33%
    parcounter       seq 394 -> 389ms, stm 134 -> 127ms, 2.95 -> 3.05x

Note the RATIO barely moves while both engines get faster: sequential
pays this hash inline and serially, the engine pays it across four
lanes, so removing shared work helps the denominator more. The right
read is absolute: ~14% more sequential transfer throughput, ~9% more
parallel. Ratio-chasing would have argued against this change; product
throughput argues for it.

Tail after this change (per 4000-tx block): extract 0.85 + scope ~1.2 +
delta 0.4 ~= 2.4ms (was 3.6). Next tail items, in order: extract
(0.85ms of OnceLock takes + 1.8MB of moves — workers could place
results in a contiguous arena the tail borrows), then validation
(~0.3µs/tx of read replay).

### Tail after the witness change — two measured negatives

Per 4000-tx block the tail is now ~1.9ms (was 3.6): extract 0.04
(gone — results are read in place), scope ~1.4, delta ~0.35. Inside
the scope the lanes' own work is only ~0.85ms; the rest is SPAWN/JOIN
(~0.5ms). Two attempts to reclaim it, both REVERTED on measurement:

- 128KB lane stacks (theory: 2MB stacks mean mmap + first-touch faults
  per spawn): no change (scope 9.6 -> 10.0ms per 7 blocks).
- Giving the TAIL THREAD its own chunk (theory: it idles on a caller
  core waiting for joins): WORSE — partransfer 2.30 -> 2.09x, scope 9.5
  -> 13.4ms. The caller cores (0,1,6,7 = two physical cores, shared
  with the harness main thread and the writer) are not equivalent to a
  dedicated worker core, so the tail's chunk straggles and becomes the
  pole. Lane chunks must go only to dedicated cores.

What remains is a PERSISTENT lane pool: the spawn/join cost is
scheduling four fresh threads onto cores that keep-hot workers are
spinning on. The fix is to make the workers themselves serve tail
chunks (they are already pinned, hot, and idle at tail time), posted
through a pool-level job slot with lifetime erasure — the tail blocks
until all chunks finish, which is the same guarantee `thread::scope`
gives, so the unsafe is contained and testable. Worth ~0.5ms/block
(~7% on micro-tx blocks, ~3% at contract weight).
