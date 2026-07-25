# Sequencer Lag Detection + Receipt-Floor Resync — Spec

- **Date:** 2026-07-25
- **Status:** Draft for review
- **Extends:** `replicated-sequencer-shards-spec.md` (P=2 racing replicas),
  `sealer-aeron-cluster-failover-spec.md` (cluster dedup window)

## Goal

Close the **dedup-horizon hazard**: a sequencer replica that falls far enough behind its twin
can drain a backlog whose refs were first ordered more than one dedup window ago — the
cluster's first-seen window (`SealerClusteredService.DEFAULT_DEDUP_CAPACITY = 2^17`) has
evicted their ids, so the re-offers are accepted as *fresh* and the same tx is canonically
ordered twice. A duplicate in the canonical log is executor-fatal and **permanent**:
execution hits revm `NonceTooLow`, the error propagates (`crates/engine/src/actor.rs` —
per-tx results are `?`-propagated, not skipped), every executor fail-stops deterministically,
and crash-recovery replay (`crates/engine/src/replay.rs`) re-executes the same poisoned
record and wedges again. One frozen replica can halt the chain until an operator intervenes.

The horizon in wall-clock terms is load-dependent because eviction is **count-based**:
2^17 global unique records ≈ 109 s at the observed ~1.2–1.5k tps ceiling, ~13 s at the
10k tps sizing point, hours at quiet load. Window sizing (the current sole defense, plus the
executor's own 2^20 `DedupWindow`, `crates/engine/src/reader.rs`) narrows the hazard; this
spec removes it.

## The two cliffs (design constraint)

Any mechanism here sits between two failure modes that are BOTH cluster-fatal:

- **Publish too much** → past-horizon duplicate → `NonceTooLow` halt (above).
- **Skip too much** → canonical nonce gap → `NonceTooHigh` halt. This is not hypothetical:
  the F02.1 "stream-adaptive floor fast-forward" was REMOVED for exactly this — it inferred
  skippability from the *envelope stream* and could not distinguish twin-ordered gaps from
  client-abandoned ones (see the header note in `crates/sequencer/src/sequencer.rs` and
  `PartitionState`'s note).

Therefore: **every skip must be backed by proof of execution, never by inference.** All
degraded modes must fall back toward *publish* (which the layered dedup windows guard),
never toward *skip* (which nothing guards).

## Design

Two decoupled parts: a **provably-safe response** (the resync filter) and a **cheap trigger**
(lag heuristic). Because the response is safe under false positives, the trigger is allowed
to be twitchy and local — no consensus, no protocol change.

### Executed-truth floors from the receipts stream

The tx_receipts multicast already broadcasts proof of execution:
`Receipt { tx_hash, nonce, from, … }` (`crates/types/src/receipt.rs`). Each sequencer
replica gains a receipts subscription and maintains, per sender:

```
floor(sender) = max(receipt.nonce over observed receipts for sender) + 1
```

Properties:

- `floor` is a **lower bound** on the sender's executed nonce. Missed receipts (multicast
  lapse, late subscribe) make the floor LOWER → the replica skips *less* and publishes more →
  degradation lands on the guarded side. A receipt gap can never cause a canonical gap.
- `floor(sender) > tx.nonce` proves the tx (or its nonce slot) already executed — a skip
  backed by this needs **no dedup-window guarantee at all**. This is what actually removes
  the horizon race, instead of resizing it.
- Sole-survivor safety is automatic: if the twin is dead, nothing receipts, floors stay put,
  and the backlog publishes in full. No accepted tx is ever dropped.

Deployment note: the deployed binary currently wires `EmptyStateDatabase`
(`crates/sequencer/src/bin/kardamom-sequencer.rs` — floors seed at 0); the receipts
subscription is the first real executed-truth source in the sequencer process.

### Resync mode (the response)

While in resync mode, the publish loop consults the floor before each `Publish` action:

- `floor(sender) > tx.nonce` → **skip**, count `resync_skipped_executed_total`. The skip is
  final for this replica (the tx is executed; nothing downstream needs the ref).
- otherwise → **publish normally** (twin-covered-but-unexecuted refs were ordered recently,
  hence still inside the dedup window; uncovered refs MUST be published regardless of age).

Deposit refs are NOT filtered in v1 (see Non-goals).

### Lag trigger — primary: canonical-count watermark (the horizon's native units)

The sealer already tells every publisher exactly where the horizon is: egress broadcasts go
to **every** open session (`offerRelayed`/`offerBoundary` loop over
`cluster.clientSessions()` — publishers included; this is load-bearing for executors and
already flows), and every boundary carries `end_tx_idx` = the global `canonicalCount`. The
publisher-side session driver already polls and decodes these frames and then discards them
(`crates/cluster-adapter/src/live.rs` — `AppMessage` dropped once `egress_alive = false`).
The primary trigger simply stops discarding:

1. The publisher wiring keeps the egress receiver and tracks
   `watermark = latest boundary.end_tx_idx` (records may be skipped undecoded).
2. Each envelope is tagged at local arrival with `count_at_arrival = watermark`. Both
   replicas of a shard read the same multicast at ~the same time, and the healthy twin
   publishes within its small publish delay — so `count_at_arrival` approximates the
   canonical count at the record's FIRST ordering, which is exactly the quantity the
   horizon is measured against.
3. Enter resync when `watermark − count_at_arrival(oldest unpublished) >
   resync_enter_fraction × dedup_capacity` (default `0.25 × 2^17` = 32 768 records).

No wire change, no Java change, no cross-host clock comparison; the trigger measures the
count-based horizon directly, so the compound-failure residue below largely evaporates.
**Boundary silence** doubles as a trigger: no boundary for `boundary_silence_ticks`
(default 5) tick intervals ⇒ the replica is partitioned/disconnected from egress and must
assume lag ⇒ enter resync (covers the case where the watermark is unavailable).

Secondary triggers (belt-and-suspenders, all cheap and local):

- **Frontier age** (fallback when egress is silent AND the session is down): local
  monotonic arrival stamps; alarm at `θ = resync_enter_fraction × dedup_capacity /
  design_peak_tps` (≈ 3.3 s at defaults).
- **Session churn**: any re-`Connected` after the first, or a backpressure-rewind
  persisting longer than θ.
- **Startup**: always start in resync mode (subsumes part of cold rejoin; see F02.1 below).

Exit resync when the watermark gap has stayed below half the enter threshold (hysteresis)
for one full boundary interval. All triggers only *arm the safe filter* — nothing is ever
rejected on time or count evidence alone; contrast with stamp-based rejection at the
sealer, which cannot distinguish a late duplicate from a legitimately delayed first copy
(see Alternatives).

### Interaction with F02.1 (cold rejoin, RE-OPENED)

The same floors partially close F02.1: a rejoined replica whose `PartitionState` buffers
established senders against twin-ordered nonces it never observed will see those nonces
*receipt*; the floor then jumps past the missing range, the buffered refs become contiguous
relative to the floor, and P=2 coverage resumes. This is the safe version of the removed
fast-forward: it advances on **execution evidence**, so it cannot publish a gap.

Explicitly NOT closed: the cold-start sole survivor (twin dead + no receipts flowing for a
sender since subscribe). Full closure needs a committed-state reader (executor nonce
endpoint) — out of scope here.

## Config

| Knob | Default | Notes |
| --- | --- | --- |
| `--cluster-dedup-capacity` | `131072` | MUST equal `-Dkardamom.cluster.dedupCapacity`; assert at startup log level, surface in metrics for the CI contract check |
| `--resync-enter-fraction` | `0.25` | watermark-gap enter threshold, as a fraction of capacity |
| `--resync-boundary-silence-ticks` | `5` | boundary-silence trigger (× the cluster tick interval) |
| `--resync-design-peak-tps` | `10000` | fallback frontier-age trigger only; matches the window's sizing math |
| `--tx-receipts-*` | existing channel flags | reuse the validator's receipts-subscription wiring |

The capacity knob introduces a new must-agree pair with the JVM sysprop. Add it to the
yamllint/contract CI check that already guards channel config drift.

## Observability

- `kardamom_sequencer_resync_mode` (gauge 0/1) and `…_resync_entered_total`
- `kardamom_sequencer_resync_skipped_executed_total` — provable-duplicate skips
- `kardamom_sequencer_receipt_floor_senders` / `…_receipt_floor_lag_seconds`
- `kardamom_sequencer_canonical_watermark` (latest `end_tx_idx` seen) and
  `…_watermark_gap_records` — the primary trigger input, scrapable pre-trigger
- `kardamom_sequencer_frontier_age_seconds` — fallback trigger input
- stdout: `sequencer RESYNC enter/exit shard=… frontier_age=…` (chaos-suite grep-able,
  matching the cluster's stdout signal-line convention)

## Test plan

1. **Unit** (`PartitionState` + floor filter): skip-iff-floor-proves; floor monotonicity;
   missed-receipt degradation publishes; sole-survivor publishes all.
2. **Chaos case `sequencer-lapse`** (mirrors `validator-lapse`): under load, `docker pause`
   ONE replica of a shard for a window sized past a shrunken dedup capacity (set
   `dedupCapacity` low for the case so the horizon is reachable at CHAOS_TPS), resume.
   Assert: no executor halt, zero divergence, pipeline progress verdicts pass,
   `resync_skipped_executed_total > 0` on the paused replica, and the twin covered the gap
   (every accepted tx receipts).
3. **Cold-rejoin regression**: restart a replica mid-stream, assert buffered established
   senders unstick via receipt floors (F02.1 partial-closure behavior) and NO nonce gap is
   ever published (the NonceTooHigh guard the removed fix failed).

## Failure modes after this change

- Frozen replica past horizon → resync filter skips the executed prefix, publishes the
  uncovered tail → no duplicates, no loss. The count-based watermark trigger measures the
  horizon in its native units, so the old compound-failure residue (record ordered > 2^17
  ago AND receipt unseen AND executor's 2^20 window blown) now additionally requires the
  watermark trigger to have missed a 32k-record gap — i.e., egress silent, in which case the
  boundary-silence trigger fires instead.
- Receipts multicast lapse during resync → floors stall low → replica publishes more →
  dedup windows absorb, verdicts unaffected.
- Trigger misconfiguration (capacity mismatch with JVM) → θ wrong → trigger late/early;
  late trigger degrades to today's behavior (window-sized protection), early trigger costs
  floor lookups only. Contract check makes drift loud.

## Alternatives considered

- **Envelope timestamps + sealer-side staleness detection.** Tag envelopes with a clock at
  ingress, let the sealer flag/reject offers that are "too old", optionally signalling the
  publisher to resync via a new egress control kind. Rejected in favor of the watermark
  trigger:
  - *Monotonic* clocks are host-local (arbitrary epoch) and cannot be compared across
    machines at all; only an ingress-proxy **wall**-clock stamp compared against the
    leader's replicated `cluster.time()` is even deterministic, and that inherits
    ingress↔leader NTP skew.
  - Time is the wrong unit — the window evicts by **count**, so any time threshold must
    assume a worst-case rate and is simultaneously too tight at peak and too loose at idle.
  - The sealer must never *reject* on staleness (it cannot distinguish a late duplicate
    from a legitimately delayed first copy — the outage case drops accepted txs), so
    sealer-side detection is at best an advisory tripwire that fires only as/after the
    harm lands, needs a shared-framing wire change (the Java service deliberately parses
    nothing but the canonical id), and a new control frame — all to deliver a weaker
    signal than the boundary watermark the sealer already broadcasts to every publisher
    session for free.

## Non-goals / future

- **Deposit refs**: `DepositRef` dedups on `source_hash` and has no nonce chain; the M-way
  duplicate absorption already relies on the dedup window. A deposit-index watermark analog
  is future work.
- **Committed-state reader** (executor nonce endpoint) for cold-start sole-survivor
  hydration — the remaining half of F02.1.
- **Poisoned-log recovery policy**: independent of this spec, a canonical-log duplicate
  today wedges recovery replay permanently (`NonceTooLow` on every re-execution). Decide
  explicitly between skip-with-error-receipt at execution vs. operator-attended halt; filed
  separately so the decision isn't buried here.
