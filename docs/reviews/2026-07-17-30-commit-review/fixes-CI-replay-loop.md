# CI regression — cluster-e2e replay loop (all 5 shards)

Branch: `claude/commit-review-30` (PR #84). Parent commit `70d0823` was green on
all five `container-cluster-e2e` shards; `a0023e9` fails all five identically:
cluster deploys healthy, first smoke tx times out, every executor loops
`cluster replay requested next_index=1 next_block=51` every 3s forever while
the sealer leader logs `cluster REPLAY … from=(1,51) served=N` cycles.

## What the CI logs actually show (job 88127031248, load shard)

Facts recovered from the real failure dump (timestamps re-anchored against
`ClusterTool` output: cluster start 22:03:54, tick 2000ms, leader = member 1,
term 0 for the whole run, zero Aeron errors on all members):

- The four `cluster REPLAY … session=5|6|7|8` lines are the **3 executors +
  the validator** on STABLE sessions. The "incrementing session ids" reading
  in the task brief was wrong — the ids cycle (5,6,7,8, 5,6,7,8, …), one
  completed serve per client re-request, right up to the moment the dump was
  taken (`retained=71` ≈ b70 ≈ 22:06:15). Sessions never churned.
- Sessions 1–4 are the four sequencer replicas (their `cluster session opened
  cluster_session_id=1..4` lines are visible in their alloc sections).
- The consumers' initial replays (`from=(0,1) served=29/38`) **worked**: their
  cursors advanced from genesis to `(1,51)` via the served frames + live
  boundaries b29..b50, delivered in real time until exactly ~22:05:37.4.
- From b51 (~22:05:38) onward the consumers received **zero egress frames,
  ever again** — while the leader's `ClientSession.offer` for both live
  broadcasts and every drain-served replay round **kept succeeding** for the
  next 40+ seconds (each `served=N` line prints only after the terminal
  control frame was accepted, i.e. offers returned ≥ 0).
- The consumers' **ingress direction stayed healthy the whole time**: every
  3s re-request kept arriving at the cluster and being served (the cycling
  lines track `retained` growth 53→71 in lockstep with the resend cadence).
  No `STALLED`, no malformed-frame drops, no session closes, no elections.
- The smoke tx (submitted 22:05:37.23) never became a canonical record
  (`retained` = 1 record + N boundaries throughout), so no receipt/watermark
  ever existed → the ingress RPC timeout is downstream fallout.

## Root cause

Two layered defects; the second is what turns a transient into a permanent
all-shard failure:

1. **Trigger (egress image death, client side).** At ~22:05:37 — sub-second
   coincident with the first tx entering `tx_data` — the egress subscription
   of all four canonical-stream consumers went permanently silent while the
   leader's offers kept succeeding. This is the failure mode already
   documented in `crates/log/src/aeron_live.rs` (`PendingPublish` doc): a >2s
   poll stall lets Aeron's min flow control drop the receiver, the image
   develops an unfillable gap and goes end-of-stream, and with
   `no_unavailable_image_handler` it is never replaced — the driver keeps
   acking at the transport level, so the sender never sees an error. The
   precise >2s stall source could not be pinned from the dump (it is new on
   this branch and correlated with the first envelope flowing through the
   F13.3 always-on replay-merge path on exactly these four processes), but
   note it is a *transient* any CI CPU spike can also produce.

2. **The livelock (the actual regression-critical defect).** Nothing in the
   client stack can detect or recover a dead egress path:
   `SessionDriver` stays `Connected` forever (session-close/`SessionEvent`
   frames would arrive on the — dead — egress), keep-alives keep the zombie
   session alive server-side, and the consumer's only recovery reflex — the
   3s `REPLAY_FROM(cursor)` resend — goes out over the *healthy* ingress
   path and is served by the sealer **into the same dead egress image**. The
   loop is therefore self-sustaining and permanent: cursor frozen at
   `(1,51)`, `cluster replay requested` every 3.000s, `cluster REPLAY …
   served=N` cycling on the sealer, forever. All five shards fail identically
   because the trigger fires deterministically at first-envelope time.

The task brief's prime hypothesis (F05.3 helper thread ⇒ request arrives on a
different cluster session than the one polled) is **disproven by the logs**:
requests demonstrably arrived on the consumers' own sessions and were served
on them. The F07.3 drain also demonstrably emits `REPLAY_DONE` under live
traffic (the `served=N` lines *are* the successful DONE offers).

## Fixes

### 1. Egress-liveness watchdog → forced session re-establishment (Rust)

- `crates/cluster-client/src/session.rs`: new
  `SessionDriver::force_reconnect(reason) -> Option<Vec<u8>>` — when
  `Connected`, returns a `SessionCloseRequest` frame for the old session and
  moves the driver to `Failed(reason)`, which flows through the existing
  self-heal machinery (backoff → fresh connect with rotate hint → new
  session). No-op in `Connecting`/`Failed` (those states already own their
  retry paths).
- `crates/cluster-adapter/src/live.rs` (`run_session`): for canonical-stream
  consumers (`replay.is_some()`) that are `Connected`, if **no egress frame
  at all** arrives for `EGRESS_SILENCE_RESET_MS = 10s` (the sealer broadcasts
  a boundary every tick ≤2s to every session, so a healthy connected consumer
  can never see 10s of silence), the session is declared egress-dead:
  the close frame is sent best-effort on the (healthy) ingress so the cluster
  reaps the zombie, and the driver re-establishes. The new session makes the
  leader open a **new egress publication ⇒ a fresh image end-to-end**, and the
  existing replay-on-connect (`REPLAY_FROM(cursor)`) closes the canonical gap
  — the exact recovery the retained-replay protocol exists to make gapless.
  The silence clock restarts on every frame AND on every session
  establishment (otherwise a fresh session would be reset before its first
  frame could arrive). Publisher-only clients (sequencer) legitimately
  receive almost no egress and are exempt.

This converts the permanent livelock into a bounded (~10–15s) self-heal,
regardless of which transient kills an egress image.

### 2. `REPLAY_DONE` latch could skip frames forever (Java)

`SealerClusteredService.serveReplayChunk`: once the drain scan reached the
retention tail it latched `controlKind = REPLAY_DONE`; if the DONE control
offer then back-pressured, the session stayed in `pendingReplays` (live
broadcasts keep skipping it) while the latched `controlKind` **skipped all
future rescans** — any frame retained between the completed scan and the
eventual control send was lost to that session forever (a silent canonical
gap; this window did not fire in the CI trace but is a real correctness bug
in consensus-critical replay). Fix: un-latch `controlKind` on a
back-pressured DONE so the next drain event rescans from the replay cursor
(already-served frames sit below the cursor — nothing duplicates).
`REPLAY_UNAVAILABLE` stays latched (floor-derived, only becomes more true).

## Tests

- `cluster/sealer-service … SealerReplayTest.replayDuringLiveTrafficReachesLiveCursorAndCompletes`
  (new, 3-member `TestCluster`): a reconnected session requests replay from
  genesis and live records keep being published while the replay is in
  flight (mid-replay sessions are skipped by live broadcasts, so those frames
  must arrive via the drain). Asserts the session converges to the live
  cursor — the full canonical prefix `0..2K-1` exactly once, in order — with
  `replayDoneCount > 0` and zero `REPLAY_UNAVAILABLE`.
- `crates/cluster-client/src/session.rs`:
  `force_reconnect_closes_old_session_and_reestablishes` (close frame targets
  the old `(term, session)`, `wrap_app` goes dead, self-heal re-emits a
  connect with a rotate hint, fresh session connects) and
  `force_reconnect_is_noop_unless_connected`.

## Verification

- `cluster/sealer-service`: `./gradlew :service:test :core:test` — BUILD
  SUCCESSFUL; service 13/13 (SealerReplayTest 3/3 incl. the new test),
  core 12/12.
- `cargo check --workspace --all-targets` — pass (pre-existing
  proc-macro-error2 future-incompat note only).
- `cargo test -p kardamom-cluster-adapter -p kardamom-cluster-client
  -p kardamom-engine -p kardamom-log` — all pass (cluster-client 23 incl. the
  2 new; cluster-adapter 18 lib + 2 integration; engine 55+; log 29+).
- Docker-gated: `cargo test -p kardamom-log --features docker-e2e --test
  aeron_live_e2e --test offer_starvation --test offer_connect_race --
  --ignored` — 3/3 pass on this host.
- `cargo clippy -p kardamom-cluster-adapter -p kardamom-cluster-client
  --all-targets` — clean; `cargo fmt --all` applied.

## Open follow-ups (not done here)

- The >2s poll-stall trigger at first-envelope time deserves its own hunt
  (suspect: the F13.3 always-on replay-merge's archive/live-merge transition
  on the consumer nodes). The watchdog makes it non-fatal either way.
- A dead egress image on a **publisher-only** client (sequencer) still goes
  undetected (it would miss `NewLeaderEvent`s); pre-existing behavior,
  exempted from the watchdog by design here.
- `crates/log` subscriptions still use `no_unavailable_image_handler`;
  surfacing image loss as an event would allow faster, precise recovery
  instead of a timeout-based one.
