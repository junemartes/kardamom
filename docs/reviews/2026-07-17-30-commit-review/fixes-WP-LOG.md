# Fixes — WP-LOG (aeron log, replay/recovery, ingress/da-watcher bins)

Findings F13.1, F13.2, F13.4-log-side, F13.6, F16.5, F16.7, F21.1, F22.1/F28.1,
F12.12-rust-ingress, plus the F02.6 hand-off from WP-SEQ (consumer-side tx_errors
dedup in ingress).

## Per-finding status

### F13.1 [M] — FIXED
Files: `crates/log/src/replay.rs`.
- `AeronArchiveReplayMerge::new` now receives `desc.start_position` instead of a
  hardcoded `0`.
- The wrong warn heuristic is replaced by two **hard failures** (recovery
  correctness requires gapless coverage from stream origin — the executor
  skip-counts from record 0):
  1. **>1 recording matches the stream** → `Err`. This is the case the old
     heuristic missed entirely: a restarted publisher creates a NEW session
     whose recording starts at position 0 again, so `start_position != 0` never
     fired while every pre-restart record was silently skipped. `resolve_recording`
     now returns the total match count and `run_replay_merge` refuses to run an
     incomplete recovery.
  2. **latest recording's `start_position != 0`** → `Err` (recorder attached
     after the publication had offered fragments; Aeron would also reject the
     replay).
- Stitching multiple recordings/sessions remains future work (a design change);
  until then a broken-coverage restart surfaces as a clear fatal error (which,
  with F13.4's fix, is now also observable by the consumer) instead of a silent
  record loss + opaque BoundaryMisaligned crash-loop.

### F13.2 [M] — FIXED
Files: `crates/ingress/src/bin/kardamom-ingress.rs`,
`crates/da_watcher/src/bin/kardamom-da-watcher.rs`.
- (a) Barrier: each recorder thread now reports its startup outcome (recording
  id or error) on an mpsc channel. The ingress binary blocks on all shard
  recorders **after the tx_data publications are open and before
  `proxy.start()`** (so no transaction can be accepted before its shard's
  recording is active); the da-watcher blocks before entering the watcher loop
  (so no deposit is published unrecorded). 60 s timeout bounds a
  wedged/unreachable archive (normal materialisation is one 500 ms catalog-poll
  tick since the publications are already open).
- (b) Fatal: any recorder startup failure (archive connect, start_recording,
  stopped-before-materialised) now fails the process with a contextual error
  when `--archive-durability` was requested, instead of an error!-log while the
  service keeps serving with durability silently ineffective. (After the ready
  signal the recorder thread only sleeps holding the session, so startup is the
  only failure window.)

### F13.4 [L] (log side) — FIXED
Files: `crates/log/src/replay.rs`.
- Thread-body failure log raised warn → **error**.
- `ReplayMergeSubscriber` now records the fatal error in a shared slot and
  exposes `take_failure() -> Option<LogError>`: after `recv()` returns `None`,
  `Some(_)` means the replay terminated abnormally (failed crash recovery) vs a
  clean stop. **Hand-off to WP-VAL (F13.4b)**: the engine/executor consumer
  should call `take_failure()` when the replay channel closes and exit non-zero
  on `Some(_)` instead of mapping the closed channel to `Ok(())`.

### F13.6 [L] — FIXED
Files: `crates/log/src/replay.rs`, `crates/log/src/recorder.rs`.
- `resolve_recording` now pages through the whole archive catalog
  (`from_record_id` advanced past the highest matching id while a page comes
  back full) instead of reading only the first 100 entries, so the true latest
  recording is resolved regardless of how many archive-global recording ids
  precede it. The paging loop also feeds the F13.1 match count.
- Fixed the **identical first-100 defect** in `recorder.rs::active_recording_for_stream`
  (recording adoption on restart) — same bug class, same crate, same silent
  stale-resolution consequence (durable watermark polled off a dead recording).

### F16.5 [L] — FIXED
Files: `crates/log/src/aeron_live.rs`.
- `drain_pending_inner` now evaluates the deadline of **every retained entry on
  every pass** — a frame parked behind a blocked head expires at its own
  deadline (acked as an error / warned for best-effort) instead of only when it
  reaches the head. Combined with the documented invariant
  `OFFER_TIMEOUT (5 s) < PubHandle::ACK_TIMEOUT (10 s)` (new named const with
  the rationale), every ack resolves before its caller's timeout, so a publish
  the caller reported as failed can never be delivered late (the double-publish
  hazard the finding described). The separate "drop when ack receiver
  disconnected" suggestion is thereby unnecessary — a caller can no longer give
  up while its frame is still queued (crossbeam senders also expose no probe).
- Regression tests: `queued_frame_behind_blocked_head_expires_at_its_own_deadline`
  (expired tail is acked-with-error and never offered, unexpired head retained)
  and `queued_frame_behind_blocked_head_with_live_deadline_is_retained`
  (no premature error / reorder).

### F16.7 [N] — FIXED
Files: `crates/log/src/aeron_live.rs`.
- The duplicated decode/forward closures in `open_subscription_merged` and
  `open_subscription_with_id` are factored into one `typed_deliver<T>` helper
  (the tx_data variant keeps its own closure — different location type).

### F21.1 [L] — FIXED
Files: `crates/log/tests/aeron_live_e2e.rs`, `crates/log/tests/offer_connect_race.rs`,
`crates/log/tests/offer_starvation.rs`.
- All three `if !docker_available() { eprintln!("skipping"); return; }` silent
  false-pass guards converted to `assert!(docker_available().await, ...)`,
  matching the e2e-crate conversion (commit 21's rationale): these tests only
  run when explicitly opted in (`--features docker-e2e -- --ignored`, one of
  them in CI), where missing Docker is an error, not a skip.

### F22.1 / F28.1 [N] — FIXED
Files: `crates/da_watcher/src/lib.rs`, `crates/da_watcher/src/publisher.rs`.
- Both doc comments now name the real live adapter type
  `kardamom_log::aeron_live::TxDepositsPublisherHandle` (the referenced
  `TxDepositsPublisher` was deleted in 239e632).

### F12.12 [N] (Rust ingress side) — FIXED
Files: `crates/ingress/src/cluster.rs`, `crates/ingress/src/metrics.rs`.
- Malformed cluster-egress frame drops in the watermark observer are now
  metered: new counter `kardamom_ingress_cluster_frames_dropped_total`
  (described; should stay 0 — sustained non-zero flags a Java/Rust envelope
  framing mismatch) in addition to the existing warn. (The executor-side decode
  path and the Java short-frame path belong to WP-VAL / WP-J.)

### F02.6 [L] (hand-off from WP-SEQ) — FIXED
Files: `crates/ingress/src/tx_error_dedup.rs` (new), `crates/ingress/src/lib.rs`,
`crates/ingress/src/proxy.rs`, `crates/ingress/src/pending.rs`,
`crates/ingress/src/metrics.rs`, `crates/ingress/tests/end_to_end_test.rs`.
Consumer-side dedup where tx_errors are consumed (the proxy watcher pipeline
— the same layer that already dedups MDS receipt copies):
- **`TxErrorDedup`** (new module): bounded, TTL-windowed (default 10 s,
  capacity 1<<16) terminal-outcome tracker keyed on `{sender, nonce, reason
  CLASS}` (enum discriminant — racing replicas can disagree on payload details
  like `expected_nonce`). A TTL window rather than first-wins-forever because
  `(sender, nonce)` rejections legitimately recur (client resubmits the same
  duplicate later and must get a prompt error again); replica copies arrive
  within milliseconds. Dropped duplicates increment the new
  `kardamom_ingress_tx_error_duplicate_total` counter.
- **Success overrides rejection**, both orderings:
  - receipt first: the receipt watcher calls `record_success(sender, nonce)`;
    a later twin rejection is dropped at the dedup. A receipt stored but still
    watermark-gated also suppresses the error inside `PendingReceipts`.
  - rejection first (the common ordering — rejections are emitted at ordering
    time, receipts after execution): `PendingReceipts::on_tx_error` now holds
    the error release for a grace window (`DEFAULT_TX_ERROR_GRACE` = 500 ms,
    `with_error_grace` for tests) and releases the error only if no receipt won
    meanwhile. Genuine duplicates (both replicas reject, no receipt) are merely
    delayed by the grace.
- Tests: 6 unit tests on `TxErrorDedup` (twin-copy drop, TTL re-arm, success
  override + expiry, capacity bound), 3 on the pending grace
  (success-within-grace wins; rejection-without-success still delivered;
  gated-receipt suppresses rejection), and 2 full-pipeline integration tests in
  `end_to_end_test.rs` (`racing_replica_rejection_is_overridden_by_twin_success`,
  `genuine_rejection_from_both_replicas_reaches_the_client_once`) driving the
  real broadcast-bus watcher path.

## Verification
- `cargo check -p kardamom-log -p kardamom-ingress -p kardamom-da-watcher --all-targets` — pass.
- `cargo check -p kardamom-log --all-targets --features docker-e2e` — pass (covers the F21.1 test edits).
- `cargo test -p kardamom-log -p kardamom-ingress -p kardamom-da-watcher` — pass:
  all suites green (log lib 26 incl. 8 drain_pending tests; ingress lib 45 incl.
  6 new tx_error_dedup + 3 new pending tests; end_to_end_test 5/5 incl. the 2 new
  F02.6 integration tests; da-watcher 15). Docker-gated tests not run (no
  media-driver container in this pass; they are `#[ignore]` + feature-gated).
- `cargo clippy -p kardamom-log -p kardamom-ingress -p kardamom-da-watcher --all-targets`
  (and log with `--features docker-e2e`) — no warnings in owned crates (only the
  pre-existing workspace-wide `proc-macro-error2` future-compat note).
- No `Cargo.toml` changes.

## Hand-off notes
- **WP-VAL**: `ReplayMergeSubscriber::take_failure()` now exists for F13.4b —
  the engine actor / executor bin should consult it when the replay channel
  closes and exit non-zero on `Some(_)`.
- F13.1's multi-recording refusal means an ingress/da-watcher restart makes a
  *subsequent executor crash-recovery* fail fast with a clear error (previously:
  silent record loss). Operationally that recovery needs the archive catalog
  reset (or the future stitching work) — same data reality as before, now
  visible.
