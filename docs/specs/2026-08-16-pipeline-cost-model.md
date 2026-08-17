# Pipeline cost model, end to end (2026-08-16)

Measured on the campaign host (6 cores, 4.1-4.2GHz): workers pinned to
dedicated cores, sequential baseline pinned to a clean core
(`KARDAMOM_SEQ_ON_THREAD=1`), neighbour workloads paused. Reproduce with
`crates/bench` (`kardamom-stm-p2`), `crates/exec-core/tests/hash_cost.rs`,
`crates/exec-core/tests/write_set_encoding.rs`,
`crates/ingress/tests/stage_costs.rs`.

## Per-tx cost and per-core capacity, by stage

| # | Stage | Cost/tx | Capacity/core | Parallelism |
|---|-------|---------|---------------|-------------|
| 1 | RPC decode (RLP) | 0.33 µs | 3.0 M tx/s | per connection |
| 2 | **ECDSA recovery + canonical hash** | **42.0 µs** | **23.8 k tx/s** | embarrassingly parallel |
| 3 | Sequencer order + dedup + contiguity guard | O(1), sub-µs | — | single writer |
| 4 | Executor feed / admission | 0.51 µs | 2.0 M tx/s | serial per pool |
| 5 | Execution | 1.6 – 13.6 µs | 74 k – 625 k tx/s | 4 workers |
| 6 | Witness hash + validate + fold | 0.87 µs | 1.1 M tx/s | 4 lanes |
| 7 | State write — receipts | 1.4 – 2.5 µs | 400 – 700 k tx/s | 1 writer |
| 7 | State write — accounts + storage | 0.03 – 0.75 µs | — | 1 writer |
| 7 | State write — commit fsync | 10 – 27 ms/block | off critical path | depth-4 pipeline |
| 8 | Validator re-execution | = stage 5 | duplicates stage 5 | separate process |

Stage 2 dominates: signature recovery is 11x the cost of executing the
transfer it authorizes (3.8 µs) and 22x the parallel engine's per-tx wall
(1.9 µs at w=4). `libsecp256k1` was measured against `k256` in the same
test — 41.2 vs 42.2 µs, i.e. the same — so ingress capacity is a
CORE-COUNT question, not a library question. Feeding the executor at its
transfer throughput needs ~22 recovery cores; at contract weight, ~9.

## End-to-end execution time per scenario

4000-tx blocks, mdbx, w=4, quiet host:

| Scenario | Sequential | Parallel | Speedup | Util | Seq tx/s | Parallel tx/s |
|----------|-----------|----------|---------|------|----------|---------------|
| transfers (chained senders) | 7.7 ms | 6.0 ms | 1.28x | 64% | 519 k | 667 k |
| partransfer (independent) | 17.4 ms | 7.6 ms | 2.29x | 87% | 230 k | 526 k |
| defi (mixed cross-contract) | 25.1 ms | 19.3 ms | 1.31x | 43% | 159 k | 207 k |
| uniswap (8 pools) | 49.9 ms | 19.0 ms | 2.63x | 95% | 80 k | 211 k |
| parcounter (contract calls) | 58.4 ms | 18.6 ms | 3.14x | 95% | 68 k | 215 k |

`defi` (43% util, 58,709 real edges) and `transfers` (same-sender chains)
are bound by their own dependency graphs, not by the engine; the decline
gate routes the latter shape to the sequential path in production. The
three parallelizable rungs land at 2.3-3.1x with 87-95% utilization.

## Where a parallel block's time sits (per block)

| Scenario | Span | Commit tail | Feed (overlapped) | Extract | State write (pipelined) |
|----------|------|-------------|-------------------|---------|--------------------------|
| transfers | 3.8 ms | 1.93 ms | 2.87 ms | 0.04 ms | ~25 ms |
| partransfer | 5.33 ms | 1.83 ms | 2.36 ms | 0.04 ms | ~35 ms |
| defi | 15.6 ms | 3.29 ms | 1.79 ms | 0.04 ms | ~30 ms |
| uniswap | 14.9 ms | 3.51 ms | 1.54 ms | 0.04 ms | ~38 ms |
| parcounter | 15.5 ms | 2.77 ms | 1.07 ms | 0.04 ms | ~33 ms |

The state write dwarfs everything and does not matter — depth-4
pipelining puts its residual wait at 0.0 ms. The commit tail is the
genuinely serial piece, still ~25% of a micro-tx block.

## System context

Deployed cluster (2026-07-28/31 campaign): 6,000 tx/s admission ceiling,
3,800 tx/s zero-loss sustained, p50 11 ms / p99 47 ms, p95 12-16 ms
through 3,000 tx/s. All services co-located on one host; the latency knee
tracks host CPU pressure 1:1.

3,800 tx/s is 0.16 of ONE ingress core's signature budget and 0.7% of the
executor's measured capacity. Today's binding constraint is orchestration
on a saturated box; scaled onto real hardware it becomes recovery-core
count; execution binds only on heavy contract workloads, where the engine
already delivers 3.1x.

## Priorities implied by the model

SYSTEM (where the headroom is):
1. Provision ingress by core count — 23.8 k tx/s per core, no library fix.
2. Egress receipt publish is still per-receipt blocking with a clone.
3. Receipt persistence is the largest writer section (1.4-2.5 µs/tx).
4. Validator duplicate execution doubles system CPU.
5. Dedicate cores per service.

EXECUTOR (diminishing, still real):
6. Persistent tail lanes — spawn/join is 0.5 ms of a 1.9 ms tail (~7% on
   micro-tx blocks, ~3% at contract weight).
7. Per-tx premium — the engine costs 23% more per transfer than
   sequential execution (versioned writes, read records).
8. Sharded admission — designed (see the P3 spec); binds only at w>=8.
9. `defi`'s ceiling — bound by its dependency graph; the P0 oracle
   analyzer can state the bound instead of guessing.
