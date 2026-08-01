# BAL phase 1 — measurement report (2026-08-01)

Implementation: EIP-7928 capture + reliable per-block publisher + validator
seeding primitives (spec:
`bal-attribution-parallel-validation-spec.md`). All numbers from the DinD
cluster on the 12-core dev host, legacy blocking submits through the tuned
proxy, 2s block tick.

## Headline

- **Batch-size verdict: keep per-tx attribution (K=1).** Chunking saves
  1.5-6.9% of frame bytes for a 5-20x coarser artifact — the slot identity
  dominates the encoding, exactly as the spec predicted.
- **Capture is free at the pipeline level.** Ramp and soak with capture
  live are at or inside pre-BAL baseline noise; the edge is unchanged (the
  6,000 tx/s harness ceiling), soak p95 improved 93 -> 79 ms (run-to-run
  variance on a shared host, i.e. no measurable regression).
- **Emission is reliable, and it is exercised**: 2 frames whose live offer
  hit the deadline during startup (subscriber not yet connected) were
  RETAINED rather than dropped, and the validator's only 2 unverified
  blocks were exactly those — with 0 divergences over the whole run.

## BAL size vs granularity K

Ramp to 4,500 tx/s, 80 loaded blocks sampled at every K per block.

| K | mean frame | peak block (~9,000 txs) | vs per-tx |
|---:|---:|---:|---:|
| 1 (per-tx) | 224 KB | **419 KB** | — |
| 5 | 218 KB | 413 KB | -1.5% |
| 10 | 211 KB | 405 KB | -3.3% |
| 20 | 199 KB | 390 KB | -6.9% |

~47 bytes/tx. Extrapolated to a 17,000-tx block (8,500 tx/s x 2s):
**~790 KB**, comfortably inside the ~2 MB Aeron frame ceiling (term/8 at
the default 16 MB term, with fragment reassembly already wired).

**Recommendation**: K=1 default. The size ladder (K=5 -> 10 -> merged-only)
stays as the oversized-frame fallback it was designed to be, and it is
reached only by contract workloads far wider than anything measured here.
NOTE: this is the ATTRIBUTION granularity. The validator's EXECUTION batch
size (5-10 txs) is a separate knob and is unaffected — under the seeded
model batches never wait on each other, so attribution K and batch size are
independent by construction.

## Cost

| metric | value |
|:--|--:|
| encode time / block (mean, 4,800 tx/s soak) | **2.44 ms** |
| encode time / block (mean, 2,000 tx/s) | 253 us |
| share of the 2s block interval | 0.12% |
| where it runs | dedicated publisher thread (never the exec thread) |
| frame bytes (mean, 4,800 tx/s soak) | 153 KB |
| BAL bandwidth | ~77 KB/s |
| publish outcomes | 281 ok, 2 deadline-retained, 0 dropped |
| retention ring | 256 blocks (~8.5 min catch-up window) |

The exec thread's only capture cost is `Bal::update_account` per touched
account (revm classifies write-vs-read from data it already produces) and
one channel send per block.

## Pipeline A/B (same ramp, same topology)

| rate (tx/s) | baseline p95 | with capture p95 |
|---:|---:|---:|
| 500-2,000 | 12-14 ms | 13 ms |
| 2,500 | 14 ms | 14 ms |
| 3,000 | 16 ms | 15 ms |
| 3,500 | 26 ms | 28 ms |
| 4,000 | 95 ms | 35 ms |
| 5,000 | 122 ms | 63 ms |
| 6,000 (ceiling) | 103 ms | 110 ms |

Soak, 4,800 tx/s: baseline p50 29 / p95 93 / p99 126 ms;
with capture **p50 21 / p95 79 / p99 121 ms**, 1,027,549 offered, PASS,
all product drop counters zero. Differences at and below 4,000 tx/s are
run-to-run variance on the shared host (both runs share the machine with
the load generator); the honest reading is **no measurable regression**.

## Correctness signals

- Validator consumed V2 frames for the whole run: **0 divergences**, 2
  unverified blocks (#1-2, startup, before its subscription attached).
- The merged `delta` section is byte-compatible with V1, so the existing
  write-set cross-check ran unchanged throughout.
- Unit tests added around the parts that broke or could break silently:
  capture through `execute_tx` (empty and seeded delta), capture through
  the actor handoff, seed selection (a batch must NOT see its own writes),
  batch-final claim collapse, and range tiling.

## Method note (measurement trap)

The first measurement pass looked like a total capture failure — every
sampled BAL was 1 byte. It was a SAMPLING artifact: `head`/`tail` of the
log both fell outside the load window, where empty blocks correctly
produce empty BALs. Always check the distribution (`loaded blocks` count)
before concluding capture is broken.

## Next

Phase 2 (validator): drive re-execution from `ClaimIndex` seeds —
partition into 5-10-tx batches, seed each from claims, execute all batches
concurrently, verify claims where produced. The primitives and their tests
are already on main; what remains is the executor-engine integration and
the parity/chaos gates before defaulting it on.
