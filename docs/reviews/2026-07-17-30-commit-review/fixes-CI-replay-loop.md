# CI regression — cluster-e2e replay loop (all 5 shards)

Branch: `claude/commit-review-30` (PR #84). Parent commit `70d0823`
(== `origin/main`) is green on all five `container-cluster-e2e` shards; the
review-fix commit fails all five identically. Two failing runs analysed from
their full CI logs: job 88127031248 (run 1) and job 88143158686 (run 2, which
also carried the first-attempt watchdog fix, commit bbfb0e9).

## Corrected read of the evidence (both runs, cross-checked)

Session topology (settles several misreadings, including this task's own
briefs):

- `cluster REPLAY … session=N` (`SealerClusteredService.java`, the
  `handleReplayRequest` stdout line) prints `ClientSession#id()` — the Aeron
  cluster session id of the REQUESTER, and the served frames are
  `session.offer(...)` on that same session, so they carry that id on egress.
- The client driver surfaces a `SessionMessage` only when its own
  `cluster_session_id` matches (`crates/cluster-client/src/session.rs`,
  `on_egress`); everything else — other sessions' frames, unknown templates,
  undecodable frames — is dropped **silently**.
- Sessions 1–4 are the four sequencer replicas (their `cluster session opened
  cluster_session_id=1..4` lines sit inside the sequencer alloc sections of
  both dumps). Sessions 5–8 are the 3 executors + the validator — their
  `session opened` lines exist but scroll out of the 40-line stdout tails the
  CI dump captures (they are buried under warn/resend spam). Reports that
  "session 8 belongs to no client" were this observational bias; the sessions
  are stable for the whole run (ids cycle 5,6,7,8 in the sealer log — they do
  not increment), there is no session churn and no cross-session mixup.

Hard timeline (identical shape in both runs; run-2 numbers shown):

1. Cluster starts 01:36:31, leader = member 1, term 0 for the whole run, zero
   Aeron errors, boundary tick 2000ms.
2. Consumers connect ~01:37:33-35; their on-connect `REPLAY_FROM(0,1)` is
   served (`served=31/40`), cursors advance through the replayed prefix and
   then via LIVE boundary broadcasts in real time up to b53.
3. Smoke tx submitted 01:38:19.54. **Consumers' cursor last advances at
   01:38:19.78** (first 3s-resend at 01:38:22.78 − 3.000s) — a ~240ms
   coupling; run 1 shows the same sub-second coupling (22:05:37.2 submit,
   22:05:37.4 freeze). The canonical record for the tx **never appears** (the
   sealer's retained count stays 1 record + N boundaries), i.e. the
   sequencers' ref publish never lands either.
4. From then on, forever: each consumer re-requests `REPLAY_FROM(1,54)` every
   3.000s over the **healthy** ingress path (requests keep arriving and being
   served up to the moment the dump is taken); the leader's offers of the
   served frames + `REPLAY_DONE` all SUCCEED (`served=N` prints only after
   the terminal control offer was accepted); the cursor never moves.
5. The egress-silence watchdog (first-attempt fix) **never fires** — bytes
   keep arriving on the consumers' `frame_rx` — yet nothing that survives
   `driver.on_egress` + ingest can be reaching the reader, because any one of
   the drain-served frames (`r1`/`b54`) would advance the cursor
   (`crates/engine/src/reader/cluster.rs::ingest/try_deliver`: at cursor
   `(1,54)`, record index 1 or a deliverable b54 unblocks the whole buffered
   chain). No `dropping malformed cluster egress frame` warns, no
   `BoundaryMisaligned` fail-stops, no session events, no process exits.

So the first-attempt diagnosis ("egress image dead, leader unaware") was
wrong in its mechanism — traffic still arrives at the client — and the fix
aimed at it was a no-op for this failure. What IS proven:

- The freeze afflicts all 8 cluster clients' *application-level* flow
  simultaneously, at first-record time, deterministically (5/5 shards, twice),
  while session-level machinery (keep-alives, requests, serves) stays healthy.
- It does not occur on main. The only branch-side behavioral deltas in the
  steady-state cluster frame path are the F07.3/F05.3 replay redesign (below).
- The in-process regression tests cannot see it: Aeron `TestCluster` egress is
  `aeron:udp?term-length=128k|endpoint=localhost:0` — loopback unicast with
  in-process media drivers — not the CI's cross-node UDP (DinD bridge,
  per-node fixed ports, `term-length=1m`) topology.

## Decision: revert the replay-path redesign to main's semantics

Per the coordination directive: the exact client-side drop point could not be
pinned from two runs of CI logs alone, every remaining candidate mechanism
lives in the frame flow that F07.3/F05.3 changed, and main is green.
Correctness beats the optimization — the F07.3 finding was a leader-stall
performance concern, not corruption. Enumerated behavioral changes vs
`origin/main` on the replay path, and their disposition:

| Change (vs main) | Disposition |
|---|---|
| F07.3: replay served via `pendingReplays` + 1ms cluster timer in 256-frame chunks (`PendingReplay`, `drainPendingReplays`, `serveReplayChunk`, `offerOnce`, `REPLAY_TIMER_CORRELATION_ID`, re-arm in `onNewLeadershipTermEvent`, cleanup in `onSessionClose`) | **REVERTED** to main's synchronous serving inside `handleReplayRequest` (+ restored `offerControl`, `offerBytesToSession`) |
| F07.3 companion: live `offerRelayed`/`offerBoundary` broadcasts SKIP sessions in `pendingReplays` | **REVERTED** — live broadcasts reach every session again (the single most suspicious delta: it made steady-state live delivery conditional on replay state) |
| F05.3: replay-request publish moved to a `cluster-replay-pub` helper thread, single-in-flight | **REVERTED** to main's inline `publish_bytes` on the session loop (`crates/cluster-adapter/src/live.rs`); kept F05.3's one real fix — the publish `Result` is logged, not discarded |
| my DONE-latch un-latch fix (first attempt) | removed along with the drain (no longer applicable) |
| F07.1: retention floors initialized from restored snapshot state (`onStart`) | kept — replay-request-time only, no steady-state frame-flow change |
| F07.5: terminal offer results close the session | kept — folded into `offerToSession` AND the restored `offerBytesToSession` (main's version returned silently on terminal results — the zombie-session bug) |
| F12.1/F12.2: chunked snapshot write / assembled snapshot read, fail-stop on bad snapshot | kept — snapshot boundaries only; no snapshot occurs in the failing window |
| F12.6 / F02.3: dedup-window validation + 8192→131072 default | kept — deterministic state-machine sizing, symmetric across members, no frame-flow change |
| F12.12: malformed-frame counter | kept — logging only |
| F12.4 / config consolidation / `validate_config` (Rust) | kept — startup-time only |
| Egress-silence watchdog + `SessionDriver::force_reconnect` (first attempt, this task) | **kept** — run 2 proves it never false-fires (zero warns), and it bounds the historically-observed true-silence zombie (the "validator starved 30+ minutes on an intact session after a leader kill" case documented at the `retryable()` comment) to a 10s recovery via close + reconnect + replay-on-connect. It is explicitly NOT claimed to fix this CI failure. |

Post-revert, the branch's cluster steady-state frame path (live broadcast
loop, replay serving, request publishing) is byte-for-byte main's behavior;
what remains on top is startup validation, snapshot correctness, terminal-
result closes, retention floors, the dedup default, counters, and the
watchdog safety net — each argued above to be outside the steady-state flow.

## Round 3: replay revert in, CI still red — upstream audit

CI re-ran with the F07.3/F05.3 revert (commit f2c8f2c): **all 5 shards still
fail identically.** This clears the replay path (its redesign was never the
cause — consistent with the log evidence that it served correctly) and
confirms the fault is upstream: the first tx's canonical record never enters
the log, and the four fresh-start consumers' tx_ordering cursors freeze at
the same moment. Audit of the four directed suspects:

### Suspect 1 — WP-SEQ `flush_drained` rebuffer, single-tx-after-idle. VERDICT: SAFE (one real adjacent bug found and fixed)

Trace for the degenerate smoke case (one tx, then silence), from code:
`run_once` ingress path → `process()` Matched → `flush_drained` →
backpressure → `flush_drained` (`crates/sequencer/src/sequencer.rs`,
"ingress" call site) rebuffers via `reinsert_for_retry`, which REWINDS the
floor to the failed nonce and buffers the payload
(`crates/sequencer/src/state.rs::reinsert_for_retry`). The retry is driven by
`run_once`'s FIRST step — `drain_pending()`
(`state.rs::drain_pending`: `expected == lowest` after the rewind, so
`drain_consecutive_from(expected)` yields it) — which needs **no fresh
ingress**; `run()` re-enters after a 10µs sleep on `Backpressure`. The lone
tx retries every pass until accepted. Rebuffer order (reverse) is correct:
the floor ends at the lowest unpublished nonce.
**Adjacent bug (fixed):** `reinsert_for_retry` used the capacity-enforcing
`PendingBuffer::insert`, which on a FULL buffer evicts the LOWEST nonce
(`crates/sequencer/src/pending.rs::insert`, `EvictedOldest`). A full future
run (capacity items) drained by `process()` plus the in-flight ingress item
is capacity+1 rebuffered items → the final (lowest) reinsert evicted a ref
whose nonce the floor had already rewound below — a silent permanent
per-sender gap. Same failure with a capacity-0 (disabled) buffer, which
silently DROPPED the rebuffered match. Fix: new unbounded
`PendingBuffer::reinsert` used only by `reinsert_for_retry` (overshoot is
transient, bounded by one drained batch). Not the CI cause (needs a full
buffer; the smoke case has an empty one), but a real data-loss bug in the
reviewed change. Tests:
`sequencer_step.rs::single_tx_after_idle_survives_repeated_backpressure_without_new_ingress`
(4 backpressured passes with no new ingress, then publish exactly once),
`state.rs::full_buffer_backpressure_rebuffer_loses_nothing`,
`state.rs::disabled_buffer_still_rebuffers_backpressured_match`.

### Suspect 2 — `fast_forward_stalled` at first-tx. VERDICT: SAFE

`state.rs::fast_forward_stalled` iterates ONLY senders with a non-empty
pending (future-nonce) buffer and only fires when `lowest > expected` has
been unchanged for `nonce_floor_lag_ms`. A fresh smoke sender's nonce-0 tx
hydrates at floor 0 (`sequencer.rs::hydrate_if_unknown` → `Ok(None)` → 0),
matches, and never enters `pending` — so no stall mark can exist and the
floor cannot jump past nonce 0. Boundary-only traffic is invisible to
`PartitionState` entirely (it sees only tx_data envelopes), so no
"stream ahead" misread is possible. Pinned by existing test
`fast_forward_ignores_senders_without_a_gap`.

### Suspect 3 — F13.3 always-on replay-merge at first-envelope. VERDICT: cannot wedge in-process; node-level interference cannot be excluded — GATING REVERTED to main's

In-process it cannot block or divert anything shared: the merge runs on its
own `kardamom-replay-merge` thread with its own archive client
(`crates/log/src/replay.rs`, 10ms idle loop — no spin), bridged via
unbounded channels (`crates/engine/src/bin_support.rs::open_tx_data_subs` —
`std::sync::mpsc::channel` + tokio pump, no blocking sends), and the
consumers' tx_ordering runs on a DEDICATED cluster runtime
(`kardamom-executor.rs`: `cluster_rt`). A hard merge failure would error-log
and exit non-zero (F13.4b) — no such logs; no failure occurred.
What cannot be excluded from CI logs is node/driver-level interference: the
always-on path holds 12 archive replay sessions (4 processes × 3 streams)
against the ingress-node archives from boot, and the freeze lands at a
deterministic ~60s (= `MERGE_PROGRESS_TIMEOUT_MS` horizon) offset from merge
open in BOTH analysed runs, on EXACTLY the four processes running this path,
on a branch where main (resume-only gating) is green. Per the
correctness-beats rule the gating is reverted to main's `resume.is_some()`
in both bins (`kardamom-executor.rs`, `kardamom-validator.rs`): fresh starts
use live multicast again; crash-recovery resume keeps the replay-merge (and
keeps F13.1's hard coverage failures, F13.2's recorder barrier, F13.4b's
non-zero exit — all resume-path correctness fixes). Known reverted cost, as
on main: a crash before the first commit rejoins live mid-stream and relies
on the bounded join timeout to fail loudly.

### Suspect 4 — ingress `tx_error_dedup` / `PendingReceipts` grace. VERDICT: SAFE

`crates/ingress/src/pending.rs::on_tx_error`: the 500ms grace only DELAYS
releasing a parked client with a sequencer REJECTION, and suppresses that
rejection when a receipt exists (`e.receipt.is_some()`). The receipt/success
path (`on_receipt` → responder) is untouched. For the smoke tx no sequencer
ever emitted a `TxError` (no rejection, no record), so the whole path is a
no-op; the RPC failed through the plain `pending_receipt_timeout` arm. Also
confirmed from `proxy.rs`: the error was the TIMEOUT arm, which means
`publish_tx_data` returned Ok (a publish failure returns
"partition-unavailable" instead) — the envelope reached the tx_data
publication with a connected subscriber (at minimum the recorder); the
sequencers show no trace of ever receiving it (no ingest effects, no
backpressure warns, no record, no rejection).

## Root-cause statement (honest form)

The all-shards failure is a consumer-side canonical-stream freeze at
first-record time introduced between `70d0823` and the review-fix commit; the
replay protocol itself functions (requests arrive, frames + `REPLAY_DONE` are
served on the correct sessions). Round 3 (replay revert in, still 5/5 red)
proves the fault is OUTSIDE the replay path. The remaining branch delta on
the frozen processes' data plane was F13.3's always-on tx_data replay-merge
— now reverted to main's resume-only gating (see the Suspect 3 verdict:
in-process it is provably clean; the node-level archive-session load and the
~60s merge-horizon alignment of the freeze instant in both runs make it the
strongest remaining candidate). Also fixed on the way: a real data-loss bug
in WP-SEQ's backpressure rebuffer (Suspect 1). The smoke-time "coincidence"
in both runs is explained without causation: the deploy pipeline reaches the
smoke test at a near-deterministic offset from consumer start, the same
clock that drives the merge horizon.

## Tests

- `SealerReplayTest.replayDuringLiveTrafficReachesLiveCursorAndCompletes`
  (kept from the first attempt, passes against the synchronous path): a
  reconnected session requests replay from genesis while live records keep
  publishing; asserts full in-order canonical coverage + `REPLAY_DONE`,
  never `REPLAY_UNAVAILABLE`.
- `SnapshotRestoreTest` updated: the `drain()` timer-pump helper is gone
  (replay is synchronous again); assertions unchanged.
- `session.rs`: `force_reconnect_closes_old_session_and_reestablishes`,
  `force_reconnect_is_noop_unless_connected` (watchdog primitive).

## Verification

- `./gradlew :service:test :core:test` — BUILD SUCCESSFUL: service 13/13
  (ClusterNodeTest 6, SealerClusterFailoverTest 1, SealerReplayTest 3,
  SnapshotRestoreTest 3), core 12/12.
- `cargo check --workspace --all-targets` — clean.
- `cargo test -p kardamom-cluster-adapter -p kardamom-cluster-client
  -p kardamom-engine -p kardamom-log` — all pass (cluster-adapter 18+2,
  cluster-client 23, engine, log).
- Docker-gated `kardamom-log` suites (`aeron_live_e2e`, `offer_starvation`,
  `offer_connect_race`) — 3/3 pass.
- `cargo clippy` on touched crates — clean; `cargo fmt --all` applied.

## Round 4: F13.3 gating revert FIXED the freeze — new failure is a real executor kill, not scrape wiring

CI run 29687514869 (with fd85769: F13.3 resume-only gating + the reinsert
fix): the smoke gate PASSES and the pipeline processes ~23k canonical
records. **The first-record freeze is resolved — root cause confirmed as the
F13.3 always-on tx_data replay-merge on fresh-start consumers.**

The remaining all-shard failure is NOT the metrics/scrape wiring (that was
audited and is correct: executors bind `0.0.0.0:9004` via
`executor.nomad.hcl` `--metrics-addr` with `network_mode = "host"`, and the
harness's `docker exec <node> curl 127.0.0.1:9004/metrics`
(`crates/bench/src/load/scrape.rs::fetch`) shares the node netns). The
checks that fired — `accounting.rs:132` "block metric unreachable" and
`:187` `kardamom_service_up=0` — correctly flagged executors that were
**dead**:

- Load shard: at the 600tps overload step (accept ratio 0.346 — ingress
  drops), all three executors crashed with
  `revm execution failure at tx TxIndex(23002): Transaction(NonceTooHigh
  { tx: 3836, state: 3833 })` — with 6 senders, 23001/6 ≈ 3833: the canonical
  stream itself contained a per-sender NONCE GAP.
- Chaos-cluster shard: same signature after the leader-kill outage window
  (`NonceTooHigh { tx: 4098, state: 1818 }` — 2280 nonces skipped), then
  `CHAOS FAIL: pipeline NOT progressing`.
- Both then crash-looped: crash-recovery `join timeout: TxRef … not found
  within 30000 ms` and a reconnect storm of `cluster session failed
  reason=concurrent session limit` (leaked 90s sessions from the crash loop
  exhaust the CM's session cap). The load shard's `base=None` keep-pace
  values are explained by the crash landing BEFORE the soak baseline
  snapshot (base is taken after the ramp).

### Root cause: the F02.1/F02.2 nonce-floor fast-forward poisons the canonical stream

`PartitionState::fast_forward_stalled` adopted the lowest buffered nonce
after a 5s stall. It was designed for the rejoin case ("the twin already
ordered the gap — first-seen dedup absorbs re-offers"), but a sequencer
cannot locally distinguish that from a CLIENT-ABANDONED hole: under ingress
overload or a chaos outage, some nonces are dropped before tx_data, so
NEITHER replica ever sees them — both replicas observe the identical stall
and both adopt the identical post-hole run, publishing a canonical stream
with a real nonce gap. Executors treat an invalid canonical tx as fatal
(`revm NonceTooHigh` → pipeline exit), so one overloaded sender kills every
executor replica simultaneously — the exact designed-in corner the WP-SEQ
doc waved through as "surfaces the gap at the executor". On main (no
fast-forward) the same overload merely stalls those senders at the
sequencer (their post-hole txs never ack and time out client-side), which
is why main was green under the identical load profile.

### Fix (this round, working tree)

- **REVERTED the fast-forward wholesale** (`crates/sequencer/src/state.rs`:
  `fast_forward_stalled` + stall marks removed; `sequencer.rs` sweep
  removed; `metrics.rs` counter removed; `config.rs` keeps
  `nonce_floor_lag_ms` parsing for TOML compat, documented as unused; bin
  startup log rewritten). A stalled sender now stalls at the sequencer —
  recoverable — never in the canonical stream. **F02.1 is consciously
  RE-OPENED** (rejoined replica does not regain coverage of established
  senders); a sound fix needs a global signal (committed-state reader or a
  canonical/receipt-stream hydration), noted in code and here.
- Regression tests pinning the safe semantics:
  `replicated_shard_racing.rs::client_abandoned_nonce_hole_is_never_published_past`
  (the round-4 killer shape: a hole in tx_data → every published per-sender
  nonce run stays dense, victim stalls at the hole) and
  `rejoining_replica_with_empty_db_stalls_but_never_corrupts` (pins the
  re-opened limitation AND canonical integrity, so a future fix must flip
  the first assertion deliberately). The two tests that pinned fast-forward
  behavior are removed with the mechanism.

### Follow-ups surfaced by the cascade (not fixed here)

1. Executor fatality on an invalid canonical tx: any nonce gap that DOES
   reach the canonical stream kills all replicas at once. The L2-standard
   alternative is a deterministic skip of invalid txs. Systemic decision,
   out of scope.
2. Crash-recovery `join timeout: TxRef not found` crash-loop after a
   mid-load executor death (resume-path replay-merge) deserves its own
   investigation.
3. Cluster `concurrent session limit` during crash loops: dead clients'
   sessions linger the full 90s timeout and exhaust the CM session cap;
   `LiveCluster::drop` could send a best-effort `SessionCloseRequest`.

### Round-4 verification

- `cargo test -p kardamom-sequencer` — 12/12 suites pass (43 lib incl. the
  reinsert no-loss tests; racing suite 5/5 incl. the two new regression
  tests; sequencer_step 9/9 incl. single-tx-after-idle).
- `cargo test -p kardamom-bench` — pass (scrape/accounting untouched — the
  checks were right).
- `cargo check --workspace --all-targets` — clean;
  `cargo clippy -p kardamom-sequencer --all-targets` — clean;
  `cargo fmt --all` applied; `bash -n` on ci-cluster.sh / smoke-load.sh /
  chaos.sh — OK (unmodified).
