# Block-STM executor with BAL-trained footprint heuristics (spec)

Status: DESIGN v1 — for review before implementation. The strategic lever
toward the 1 Ggas/s @ p95 ≤ 20ms target after allocation squashing
(#138-#140, #148) closed the memory side: the engine now executes
~806 Mgas/s on ONE core (DeFi mix, post-squash measurement) — the
remaining ~10× is parallelism, and the executor is the only sequential
stage left on the hot path.

Companions: `bal-attribution-parallel-validation-spec.md` (the validator's
seeded parallelism — claims exist there because execution already
happened; the executor has no claims and must DISCOVER conflicts),
`2026-08-03-allocation-report.md` (single-core ceiling measurement).

## Why plain Block-STM is not enough here

Classic Block-STM (Aptos) is purely optimistic: run every tx in parallel
against multi-version memory, validate read-sets afterward, abort + retry
on conflict. Its worst case is exactly our best-paying workload: DeFi
flows where most txs touch the SAME pool/vault/CLOB storage. A naive
optimistic pass over a swap-heavy block aborts most of the block once,
executing ~2× the work to serialize anyway — and the abort/retry storm
lands on the p95 we are trying to protect.

The asymmetric advantage of this codebase: **the executor already
publishes an EIP-7928 BAL for every block** (reliable, non-best-effort —
#133). That stream is a free, continuously-updating training signal for
WHAT each contract call touches. Sequenced-but-unexecuted txs reveal
`(to, selector, sender, args)` up front; historical BALs tell us, per
`(to, selector)`, which storage slots calls like it touched before, and
how those slots derive from the caller:

- **fixed** slots — same slot every call (a pool's reserves, a vault's
  total-assets): every pair of calls conflicts. Serialize up front.
- **sender-derived** slots — `keccak(sender . base)` mappings (balances,
  allowances, per-user vault shares): two calls conflict only when
  senders collide. Almost always parallel.
- **arg-derived** slots — `keccak(arg . base)` (per-market CLOB books,
  per-pair pools): conflict is decidable from calldata.
- **unpredictable** — history disagrees with itself; treat as
  conflict-with-everything (falls back to Block-STM's optimism or a
  serial lane).

**Scheduling is PESSIMISTIC** (design decision, 2026-08-07): our
transactions are highly correlated — swap/CLOB/vault flows converge on
shared state as the common case, not the exception — so the scheduler
treats predicted conflicts as authoritative ORDER, built before anything
executes. Two txs predicted to overlap never run concurrently; there are
no abort storms to absorb, wall time collapses to the critical path of
the dependency DAG, and p95 stays flat under contention (aborts are
variance, and retries steal cores other lanes need on a 12-core budget).
Parallelism comes from the workload's TRUE structure: distinct pools /
books / vaults / senders proceed independently; each hot domain is
internally sequential — which is what it actually is. Validation still
runs underneath as an invariant check (a misprediction re-executes and
retrains the stats), so a wrong heuristic degrades throughput for a
block, never state.

## The strategy stack

Each sequenced tx is assigned one strategy, evaluated top-down —
exact structural knowledge first, statistics second, pessimism as the
default when neither speaks:

1. **`System` — serial barrier lane** (exact). Deposits and epoch
   records: own commit semantics, rare, never concurrent with anything
   (same reasoning as the validator's batch path, #151).
2. **`SenderChain`** (exact, no stats needed). Same-sender txs are
   always ordered — nonce succession and gas-balance flow make this a
   hard data dependency. Free and precise: cross-sender parallelism is
   the one form of independence that needs no prediction for the
   *sender's own* accounts.
3. **`Accumulator` deferred writes** (exact by algebra). Slots that are
   only ever ADDED to and not read mid-block — the fee sink above all
   (if every tx credits the beneficiary, it is a universal serializer
   that would chain the entire block). Such writes are recorded as
   deltas and folded at commit in canonical order (the Aptos
   aggregator trick). P0 must confirm from real BALs which slots
   qualify (expected: beneficiary balance; candidates: monotone
   counters that no same-block tx reads).
4. **`DomainChain { domains }`** (stats-driven, the workhorse). The
   predictor maps the tx to the contention DOMAINS it will touch —
   fixed slots of a `(to, selector)` (a pool's reserves), arg-derived
   instances (pool-pair, CLOB market id resolved from calldata).
   Within each domain, member txs are chained in canonical order; a tx
   touching several domains joins all their chains (its ready-time is
   the max of its predecessors). The DAG is the union of per-domain
   chains — pessimistic by construction: predicted overlap ⇒ ordered.
5. **`Independent`** (stats-driven, the bounded optimism). Only when
   the ENTIRE predicted footprint is sender-derived slots of distinct
   senders (the plain-transfer / distinct-user-balance class) does a tx
   run with no incoming edges beyond its SenderChain. This is the one
   place a prediction miss can cause an abort — it is narrow by
   design, and v1 may ship with it disabled (pure pessimism) until P0
   quantifies the win.
6. **`Tail` — the pessimistic default.** Cold selectors, low-confidence
   stats, unpredictable footprints, adversarial calldata: join the
   global serial lane at the block's end, canonical order. NOT
   optimistic — "when unsure, serialize" is the inversion of classic
   Block-STM that the correlation profile demands. Bounded damage:
   sequential throughput, i.e. today's.

## The graph index — what a tx is keyed by

Every tx contributes a set of ACCESS KEYS — each names a state cell (or
semantically-merged cell group) with an access MODE (R/W) and a
confidence. The DAG is built in ONE canonical-order pass over per-key
last-access tables with standard RW semantics: readers share (no
reader-reader edges), prior-writer -> reader, prior-readers +
prior-writer -> writer. O(total keys), before anything executes.

Key tiers, most-certain first:

1. **Exact, from the envelope alone** — sender account (W: nonce +
   gas balance; same-sender chains fall out automatically), recipient
   account (W, when value > 0), created address (W, on CREATE — the
   same-block deploy-then-call key; the burst-block lesson).
2. **Computed instances** — stats supply the FORMULA, calldata the
   VALUE: arg-derived `(contract, base_slot, arg)` (CLOB market, pool
   pair — evaluate `keccak(arg ++ base)` against the tx's own
   calldata) and principal-derived `(contract, base_slot, owner)`
   (balances/allowances/shares — the principal is not always the
   sender: `allowance(owner, spender)` keys on the owner ARG).
   `base_slot` = the Solidity storage anchor of the mapping (the slot
   `p` of its DECLARATION; entries live at `keccak(pad32(key) ++
   pad32(p))`, nested mappings chain the hash). Never observed
   directly — P0 RECOVERS it by inversion: test observed slots against
   `keccak(pad(sender|arg_i) ++ pad(p))` for small `p` (contracts
   declare few variables; brute-forcing p∈0..255 is a handful of
   keccaks). A slot that solves consistently self-identifies as
   "mapping at base p keyed by X" — an algebraic identity, not a
   co-occurrence guess, which is why Tier-2 keys are trustworthy.
3. **Semantic domains** — fixed slots every call of `(to, selector)`
   touches regardless of caller/args (pool reserves, vault totals),
   MERGED by measured co-access stability: slots that always co-write
   are one contention key (reserve0/reserve1/cumulative = one domain,
   not three). Keeps key counts small while matching the true
   contention structure.
4. **Boundaries** — Accumulator slots are EXCLUDED from the graph (a
   fee sink as a key would chain every block; it bypasses via deferred
   commutative folding). Everything unpredictable maps to the wildcard
   key ⊤, which conflicts with all keys by definition — the `Tail`
   lane expressed as a key, not a special case.

Granularity is itself feedback-tuned per contract region: account-level
(coarse) < slot-level (verbose) < domain-level (default). A domain that
keeps convicting FALSE EDGES is too coarse and splits; keys that never
disagree merge.

**The objective is block wall time, never misprediction count.** A
controller that minimizes mispredictions alone has a degenerate global
optimum: chain everything — one thread, zero misses, zero speedup. The
two failure modes spend the same currency (core-seconds) from opposite
sides, and their observability is ASYMMETRIC in the dangerous
direction:

- **Optimism error** (false independence): bounded per event, LOUD — an
  abort fires, work is retried, a counter ticks.
- **Pessimism error** (false edge): silent and continuous — cores idle
  behind an edge that never needed to exist, nothing fires. Left
  ungraded, every noisy selector ratchets toward `Tail` and stays.

So the feedback is SYMMETRIC, and the BAL grades both directions for
free: after each block, (a) every miss (self-abort / validation
failure) demotes its selector toward chaining, and (b) every DAG edge
`i -> j` whose parent's ACTUAL writes did not intersect the child's
ACTUAL reads is a convicted FALSE EDGE — its rate per selector-pair
promotes back toward parallelism. The edge decision itself is expected
cost, both terms estimable from the per-selector stats + gas
histograms:

    chain  iff  P(conflict) x retry_cost  >  (1 - P(conflict)) x serialized_wait

The stats are a rolling window — the stream retrains them in minutes,
so a contract that changes behavior (proxy upgrade) self-corrects in
both directions.

**Stats footprint**: AGGREGATES ONLY — grading folds each block into
O(1) decayed counters (EWMA) per entry and drops the observations, so
memory is O(live entries), independent of uptime and tps. Per
`(to, selector)` entry ≈ 1KB: ≤8 solved Tier-2 formulas (a formula is
~20B — base_slot + derivation + mode + counters, NOT observed slots;
inversion is also a compression), ≤16 fixed slots with domain grouping
+ stability, a gas histogram, and the error-rate EWMAs. Cardinality is
a bounded LFU cache whose eviction is free BY CONSTRUCTION: a cold
selector schedules as `Tail` with or without stats, so evictees lose
nothing. Zipfian traffic ⇒ top ~1k selectors carry ~all gas; cap 16k
entries ≈ 16MB (64k ≈ 64MB as the paranoid knob; the bench-DeFi +
Uniswap working set is tens of entries). Transients: the per-block
grading buffer (DAG + BAL cross-check) peaks ~hundreds of KB on a
burst block, freed at the boundary. No persistence in v1 — retraining
takes minutes and cold-start merely means a brief Tail-conservative
window after restart (P1 measures it; the mdbx table stays an open
question until that window proves expensive).

**Failure semantics under pessimism**: a validation miss or self-abort
is not routine — the stats were wrong. Response: re-execute the tx at
its canonical position (correctness), demote the selector (learning),
count it (`stm_misprediction_total`). Health is judged on the PAIR of
error rates plus realized utilization — `stm_misprediction_total`,
`stm_false_edge_total`, and achieved-speedup vs the block's oracle
critical path (`stm_speedup_ratio`) — alert on misprediction rate
leaving ~zero AND on utilization sagging toward 1x, never on one side
alone.

## Phases

### P0 — footprint statistics (offline, zero product risk)

`kardamom-footprint`: consume BALs + their blocks' tx envelopes (offline:
re-execute recorded workloads through the engine exactly like
`parallel_defi_repro` does; later: live tail of `tx_bal`), and aggregate
per `(to, first-4-bytes-of-calldata)`:

- observed write/read slot sets, bucketed by the derivation classes above
  (classification: solve `slot == keccak(sender.pad32 ++ base)` /
  `keccak(arg_i.pad32 ++ base)` for each observed slot against the tx's
  own sender/args — mappings self-identify; what never solves and never
  repeats is unpredictable),
- per-class stability score (how often history predicted the next call's
  actual footprint),
- gas histogram (the scheduler also wants to length-balance waves).

Deliverable: a report on the bench DeFi mix + real-Uniswap workload —
**prediction hit-rate per selector**, plus the numbers pessimistic
scheduling lives or dies by:

- **critical-path ratio**: block gas ÷ longest domain-chain gas — the
  theoretical speedup under pessimism (NOT antichain width; with
  chains, wall time = critical path). If realistic mixes give < ~3×,
  stop here.
- **domain-population distribution**: how block gas spreads across
  domains (one dominant pool ⇒ its chain IS the block — Amdahl-honest).
- **over-merge cost**: parallelism a perfect oracle would find that
  pessimism gives up (predicted-conflict, actually-independent) — the
  price paid for zero aborts; if large, the domain model is too coarse.
- **join density**: fraction of txs spanning >1 domain (DAG complexity).
- **fee-sink check**: is a beneficiary/fee slot written by every tx in
  the recorded BALs (⇒ the `Accumulator` strategy is mandatory, not
  optional)?

### P1 — shadow scheduler (in the executor, measurement only)

At each boundary, run the predictor over the block's txs and emit
metrics: predicted waves, width, conflict-edge count, prediction
confidence — while execution stays sequential. Compare afterward against
the block's actual BAL (the ground truth ships in-process for free):
`footprint_prediction_hit_rate`, `footprint_false_independent_total`
(the dangerous miss class — predicted parallel, actually conflicting).
Costs a hash-map pass per block; no execution change; runs behind
`KARDAMOM_FOOTPRINT_SHADOW=1` in perf runs.

### P2 — the STM engine

- Multi-version state: `MvCache` over the snapshot∘parent view — per
  (address, slot) a small version list `(tx_index, value)`; reads record
  `(slot, version-observed)`; a read at index i sees the highest write
  below i, else the block-input view (the same layering `ExecScope`
  already encodes — MvCache is its concurrent sibling).
- Workers pull DAG-READY txs (all predecessors executed) — pessimistic
  chains mean a tx's reads of its domains see its chain predecessor's
  writes deterministically through MvCache, not speculatively.

**Conflict authority — wound-wait on the canonical order.** The
sequencer's strong total order is a free, consensus-fixed PRIORITY
relation (classical wound-wait schemes have to invent one; we inherit
it): lower canonical index = parent, higher = child, and the two legs
of the discipline fall out:

- **Children WAIT on parents** — ahead of time via the predicted DAG's
  edges, and at runtime via ESTIMATE marks: when a wounded/re-executing
  parent's write slot is read by a child, the child parks on the mark
  instead of consuming a value about to change (prevents wound
  cascades).
- **Children SELF-ABORT on a parent's conflicting write.** Parents
  never track readers and never signal anyone — tx i just writes its
  versioned slots. Each child records `(slot, version)` as it reads
  (revm's native capture); at every subsequent state access it re-checks
  its recorded versions with a cheap compare, and the moment a slot it
  read carries a newer LOWER-index version, it aborts itself and
  re-enqueues with a LEARNED edge i→j. The detection cost rides the
  CHILD's accesses — the speculator pays for its own speculation — and
  the parent's hot path stays free of reader-index maintenance and
  cross-thread flagging (a shared reader index would be touched on
  EVERY read to serve an event pessimism makes ~never happen, and is a
  contention magnet besides). Two-level cheapening: per-domain write
  epochs guard the common case (epoch unchanged ⇒ skip the per-slot
  recheck entirely). No parent ever waits on a child; deadlock-free by
  construction (priority is the fixed total order, the wait graph is
  acyclic). Detection latency is bounded by the child's next access,
  and commit-time validation backstops a child that finished before
  the parent's write landed. Learned edges RETRAIN: each self-abort is
  a prediction miss, demoting the selector exactly like a validation
  miss.

- Validation remains as the final invariant check before commit (a
  wound can only fire while the parent still executes; a child that
  finished before its parent's conflicting write is caught here).
  Under pessimistic scheduling both self-aborts and validation misses
  are expected ~never on hot flows — but their combined rate
  (`stm_misprediction_total`) is only ONE side of health; it is graded
  jointly with the false-edge rate and realized speedup (see "The
  objective is block wall time" above — misses-to-zero alone optimizes
  toward one thread). Commit is strictly in canonical order, so
  receipts, cumulative gas, the BAL, and the delta are BYTE-IDENTICAL
  to sequential execution by construction.
- Deposits and epoch records take the serial lane (rare; own commit
  semantics — same reasoning as the validator's batch path, #151).

### P3 — integration

`--parallel-execution` on the executor (default off). The determinism
suite runs both modes against identical inputs and asserts byte-identical
tx_receipts + tx_bal streams; the validator re-verifies every block
regardless (an STM bug surfaces as a loud divergence, never silent
corruption — the same safety argument the validator's parallel path rode
in on). Perf gate: the DeFi gigagas suite A/B, the alloc gate (MvCache
must not regress the −99% allocation win), and p95 under the fixed-rate
CI load.

## Invariants

1. **Byte-identical outputs** vs sequential — receipts, BAL, delta, in
   canonical order. Enforced by the determinism suite + the validator.
2. **Heuristics affect scheduling only.** A 0%-accurate predictor
   degrades to classic Block-STM (aborts), never to wrong state.
3. **Fail-safe:** any STM invariant violation inside a block →
   discard, re-execute the block sequentially, count it
   (`stm_fallback_total`).
4. **The BAL contract is unchanged** — same capture, same publisher,
   same frames; downstream consumers cannot tell which engine ran.

## Validation bench — the whole stack, one component at a time

No bespoke lab: the existing `LocalStack` harness (chain-semantics /
full-pipeline-e2e) IS the whole stack singly instanced — one ingress,
one sequencer, the sealer, one executor, one validator, real Aeron
streams, real boundary cadence, plain processes on one host. The STM
bench is that stack with **the executor as the single variable**
(`--parallel-execution` on/off; everything else byte-identical), which
buys what a synthetic tape would have had to fake:

1. **A LIVE correctness oracle**: the in-stack validator re-executes
   every block sequentially and fail-stops on ANY divergence —
   receipts, wsh, BAL cross-check, flight-ring dump on mismatch. An
   STM bug is a loud halt with forensics, not a diff someone remembers
   to run.
2. **Real block shapes**: actual cadence, burst blocks under load
   spikes, deposits/epochs interleaved in canonical positions.

**Attribution discipline**: end-to-end tps dilutes the executor behind
pipeline stages — the bench reads EXECUTOR-STAGE metrics (block-apply
elapsed, exec-thread utilization; already exported) alongside pipeline
gigagas/s and block-latency p50/p95/p99 (the 20ms target is a latency
target). Worker count is an env knob swept on the same stack (1..12).
Workload knobs ride the existing generators: contention factor, mix
ratios, senders, offered rate (block size follows rate x cadence).

**Learning dynamics** measure directly: cold-start each run, watch
hit-rate vs block index (prices the no-persistence decision), then the
steady-state health triple (misprediction rate, false-edge rate,
realized-vs-oracle speedup).

**The offline piece that stays offline — the oracle analyzer**: pure
computation over BALs captured from these same runs (or any recorded
blocks — the flight-ring dump format already carries records+claims):
the TRUE dependency graph and its critical path, the bound no predictor
beats, computable in P0 before any STM code exists. THE go/no-go
number, and later the denominator of `stm_speedup_ratio`.

**CI**: full-pipeline-e2e already runs the mini-stack; P3 adds an
STM-mode pass of the same scenarios (the validator gate makes it a
correctness test with zero new assertions) plus an
STM->=sequential-throughput bound on a parallel-friendly scenario.

## Measurement plan

- P0 report: hit-rates + implied parallelism on bench-DeFi and
  real-Uniswap (v2-core is vendored; the harness choreography lands with
  this campaign).
- P2 offline A/B: recorded blocks through both engines
  (`parallel_defi_repro` pattern) — gigagas/s per core count, abort
  rates, wall-per-block distribution.
- P3 cluster: the perf suite's DeFi ramp/soak on the dev host, target
  trajectory 100 Mgas/s (current shared-host soak) → 1 Ggas/s with the
  batcher isolated (cpuset Run E pairs naturally with this campaign —
  parallel execution needs cores the batcher currently steals).

## Open questions

- Wave commit pipelining vs the depth-4 commit pipeline (#129): the STM
  block still produces ONE delta; the writer path is untouched — but
  intra-block wave overlap with the PREVIOUS block's fsync may move the
  settle stall. Measure at P2.
- Selector stats persistence: in-memory rolling window first (the stream
  retrains it in minutes); mdbx table only if cold-start hit-rates
  matter in practice.
- `unpredictable` lane sizing under adversarial calldata (a griefing
  contract that randomizes its footprint) — the serial lane bounds the
  damage to sequential throughput; no worse than today.
