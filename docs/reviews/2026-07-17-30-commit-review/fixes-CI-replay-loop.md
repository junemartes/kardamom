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

## Root-cause statement (honest form)

The all-shards failure is a consumer-side canonical-stream freeze at
first-record time introduced between `70d0823` and the review-fix commit; the
replay protocol itself functions (requests arrive, frames + `REPLAY_DONE` are
served on the correct sessions). The freeze's final drop point (which of:
driver-level silent drop, transport interaction with the redesigned
serve/skip pattern, or an engine-side consumer stall) could not be uniquely
pinned from CI logs; the F07.3/F05.3 redesign is the only steady-state
behavioral delta in that path and is reverted wholesale rather than patched.
If the next CI run still fails with the revert in place, the fault is
provably OUTSIDE the replay path (prime remaining suspect: the F13.3
always-on tx_data replay-merge, which activates on all four affected
consumers and whose first-envelope transition coincides with the freeze
instant; the sequencers' simultaneous publish failure would then need its own
explanation — e.g. their shared-runtime interaction at envelope time).

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
