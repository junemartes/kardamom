# Sequencer Lag Detection + Receipt-Floor Resync — Spec

- **Date:** 2026-07-25
- **Status:** Implemented (same PR); sections marked *as implemented* record where the build
  deviated from the draft
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

### The response: floors drive the nonce state machine directly

*(As implemented — the floor advance subsumes the per-publish filter this section originally
described: floors drain at the top of every loop iteration, so the state machine's expected
nonce is always current before any publish action is computed.)*

Every raised floor is applied to `PartitionState` via `advance_floor(sender, floor)`:
`next_nonce` jumps to the floor (never regresses) and buffered entries below it are dropped —
each a receipt-**proven** duplicate, counted in `resync_skipped_executed_total`. A stale
incoming envelope below the floor then takes the ordinary `Past` path and surfaces to the
client as `DuplicatedTx` (also counted when the floor proves it). Everything unproven —
twin-covered-but-unexecuted (recently ordered, still inside the dedup window) or genuinely
uncovered — publishes normally.

The floor advance is **always on**, not gated on resync mode: it is proof-backed and
therefore unconditionally safe, and keeping it continuous minimizes duplicate re-offers into
the cluster. Resync *mode* is the trigger/observability envelope — the enter/exit signal the
chaos suite and operators watch.

Two receipt classes are excluded from floor evidence:

- **`nonce == 0` receipts** — deposit receipts stamp a filler `nonce: 0` (deposits execute
  with the nonce check disabled) and are indistinguishable on the wire from a genuine
  first-tx receipt; treating one as proof could wrongly `Past`-reject a sender's first tx.
  Floors therefore only ever prove from nonce ≥ 1 — degradation toward publish.
- **Other shards' senders** — filtered at the receipts thread, keeping the floor map bounded.

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

**Silence detection lives in the egress FEED thread, not the publish loop** *(as
implemented — the first CI run of `sequencer-lapse` failed the draft design two ways,
run 30163255470)*:

- The publish loop can be **blocked exactly when lag happens**: `LiveIngress::offer` waits
  on the session thread's reply, and after a process freeze the sealer has closed the
  session (1 s offer deadline) and the session thread is mid reconnect/backoff — the loop
  sat inside `publish_ref` for ~70 s and a loop-evaluated silence check never ran, missing
  a 30 s freeze entirely. The feed thread polls egress with a bounded wait
  (`recv_timeout(500 ms)`), measures **boundary inter-arrival gaps** (a freeze appears as
  one wall-clock gap between the last pre-freeze arrival and the first post-resume one),
  and raises a **sticky lag flag** (`SharedWatermark::flag_lag`, largest-gap-wins) plus the
  starvation-proof `resync_lag_suspected_total` metric. The controller consumes the flag on
  the loop's next turn — however late; detection cannot be missed, only the response
  delayed.
- Liveness is **boundary arrival, not count change**: idle traffic emits a boundary every
  cluster tick with an unchanged `end_tx_idx`, which a value-change tracker mistakes for
  silence (observed as enter/exit thrash every 10 s — 60 spurious enters in one shard run).

Secondary triggers (belt-and-suspenders, all cheap and local):

- **Publish stall** (as implemented, replacing the drafted frontier-age stamps): continuous
  backpressure — which a not-yet-reconnected cluster session also maps to — persisting past
  `publish_stall_ms` (default 10 s). Covers session churn and wedges the egress signals miss.
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
| `--cluster-dedup-capacity` | `131072` | `[resync] dedup_capacity`; MUST equal `-Dkardamom.cluster.dedupCapacity` — logged at startup as the contract line |
| `--resync-enter-percent` | `25` | `[resync] enter_percent` — watermark-jump enter threshold, percent of capacity (integer so the config stays `Eq`) |
| `--resync-boundary-silence-ms` | `10000` | `[resync] boundary_silence_ms` — 5 × the 2000 ms deploy tick |
| `[resync] publish_stall_ms` | `10000` | publish-stall secondary trigger (TOML only) |
| `[resync] exit_hold_ms` | `2000` | exit hysteresis (TOML only) |
| `--executor-count` | from channels config | tx_receipts MDS parity with the validator; unused on the multicast deploy |

The capacity knob introduces a new must-agree pair with the JVM sysprop. Add it to the
yamllint/contract CI check that already guards channel config drift.

## Observability

- `kardamom_sequencer_resync_lag_suspected_total` — bumped by the feed thread at detection
  time; starvation-proof (the chaos suite asserts on THIS, tightly; `entered_total` follows
  when the loop next turns, asserted with a wide window)
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
2. **Chaos case `sequencer-lapse`** (as implemented, in the `chaos-sequencer` CI shard;
   evolved substantially from this draft through five CI iterations): under shard-pinned
   load, freeze shard 0's replica A via **SIGSTOP/SIGCONT with a mid-freeze verification
   probe** (`docker pause` silently no-ops in the nested-DinD freezer, and an unverified
   injector rots into vacuity — validator-lapse shares this pattern, flagged for follow-up).
   A 30 s freeze exceeds the media driver's client-liveness timeout, so the frozen replica's
   aeron client is EVICTED and the process correctly fail-stops on thaw → Nomad restarts it
   → clean rejoin (safe since #99). Detection is therefore asserted on the **twin**: the
   frozen replica's wedged egress session stalls the sealer's service thread on the offer
   deadline, a genuine cluster-wide boundary-arrival gap every running replica's feed must
   flag (`lag_suspected` + resync engagement on the twin, scrape-failure-aware sampling).
   Safety asserts: pipeline progress, load verdict, per-replica convergence, and the frozen
   replica exporting again post-restart. (Overrunning the 2^17 horizon itself at
   CHAOS_TPS=200 would need a ~11-minute freeze — a local/manual variant, not CI.)
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

## What the chaos case surfaced (issue #99 — FIXED)

The `sequencer-lapse` CI iterations exposed a PRE-EXISTING session-lifecycle bug this
mechanism was the first thing able to observe: a hard-killed-and-restarted replica's cluster
session appeared to die every ~90 s (`cluster session failed reason=TIMEOUT` at exactly
`sessionTimeoutNs`), churning open→timeout→reconnect forever. Root cause (#99, fixed by the
foreign-session event filter in `SessionDriver::on_session_event`): the egress endpoint is
per-node static config, so the restarted process received its dead predecessor's session
corpse events on the shared channel and misattributed each `Closed(TIMEOUT)` to its own
healthy session — abandoning it, whose own timeout then killed the replacement, forever.
Publishers never consumed egress before this PR, so the state was invisible — every
load/chaos verdict passes while the shard silently runs P=1. With #99 fixed the case's
DETECTION asserts (`lag_suspected`, resync engagement) are RE-ARMED (fail-on-miss) alongside
the always-enforcing safety asserts.

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
