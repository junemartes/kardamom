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

The scheduler then builds a PREDICTED conflict graph before executing
anything: predicted-independent txs run optimistically in parallel
(validation still catches prediction misses — correctness never depends
on the heuristic), predicted-conflicting txs are chained as explicit
dependencies instead of discovered-by-abort. The heuristic buys
throughput; STM validation keeps it sound.

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
**prediction hit-rate per selector** and the implied wave-parallelism of
recorded blocks (how wide the predicted conflict graph's antichains are).
This number decides whether P2 is worth building: if predicted
parallelism on realistic mixes is < ~3×, stop here.

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
- Workers execute txs optimistically in canonical-index order given by
  the scheduler (predicted-independent first; predicted chains as
  explicit dependencies; unpredictable txs in a serial lane).
- Validation: after tx i's execution, its recorded read-set is checked
  against writes committed by txs < i that landed AFTER i read (the
  standard STM invalidation); revm's Bal capture already tracks read
  sets natively — the same machinery the validator's claims use.
  Aborted txs re-execute; commit is strictly in canonical order, so
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
