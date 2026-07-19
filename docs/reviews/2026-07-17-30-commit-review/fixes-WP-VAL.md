# Fixes — WP-VAL (validator + engine + executor bins)

Findings F10.1, F10.3, F10.4, F10.5, F10.6 (incl. F01.4), F10.7, F10.8,
F10.9-crate-parts, F13.3, F13.5, F13.4b, F05.4, F11.1-port-default, F07.2,
F08.2, plus the F06.3 attester-wiring hand-off from WP-WD.

## Per-finding status

### F10.1 [H] — receipt-mismatch fail-stop defeated by must-deliver retry — FIXED
Files: `crates/engine/src/error.rs`, `crates/engine/src/actor.rs`,
`crates/validator/src/lib.rs`.
- New `ExecutorError::Divergence(String)` variant: fatal + non-retryable by
  contract. `ValidatorWriterQueue` (BAL mismatch) and `ValidatorReceiptSink`
  (receipt mismatch) now return it instead of `State`.
- The commit thread's must-deliver retry loop propagates `Divergence`
  immediately (`return Err`) instead of retrying — retrying consumed the
  buffered receipt and slid into the "unverified" `Ok` arm, so the pipeline
  kept committing past a proven mismatch and the process never exited 2.
- Belt-and-braces latch: `ValidatorReceiptSink::publish` fails every call once
  `Divergence::is_halted()`, so no retry path can ever absorb the fail-stop.
- The error propagates commit→`Executor::run`→validator main → `exit(2)`
  (the existing select on engine completion already surfaces it).
- Regression tests: `commit_thread_fail_stops_on_divergence` (engine),
  retry-after-mismatch latch assertion in
  `consistent_receipt_passes_inconsistent_fails` (validator).

### F10.3 [M] — no aged-out skip in ReceiptBuffer → 5s/receipt cold-start crawl — FIXED
Files: `crates/validator/src/lib.rs`.
- `ReceiptBuffer::take` now mirrors the #78 BAL catch-up heuristic: when the
  highest buffered `tx_idx` is more than `BACKLOG_LOOKBEHIND` (4096 canonical
  records ≈ the BAL heuristic's 16 blocks at a few hundred tx/block) ahead of
  the requested one, the receipt has aged out of the live stream and `take`
  returns `None` immediately ("unverified") instead of blocking the commit
  thread for the full 5s window per historical tx.
- Test: `receipt_take_skips_aged_out_backlog_immediately`.

### F10.4 [M] — ~200 lines of binary wiring duplicated executor↔validator — FIXED
Files: `crates/engine/src/bin_support.rs` (new), `crates/engine/src/lib.rs`,
`crates/engine/Cargo.toml`, `crates/executor/src/bin/kardamom-executor.rs`,
`crates/validator/src/bin/kardamom-validator.rs`, `crates/engine/src/actor.rs`.
- New `kardamom_engine::bin_support` module holds the single copy of:
  `StateDurabilityArg` (+`From<_> for Durability`), `load_genesis`,
  `resolve_genesis` (chain-id conflict check), `build_genesis_alloc`,
  `LiveTxDataSub`/`LiveTxDepositsSub` adapters, the per-shard tx_data +
  tx_deposits async→sync bridge blocks (live AND replay-merge variants),
  `bounded_join_timeout`, `init_tracing`, `wait_for_shutdown`, and the new
  `ReplayFailure` (F13.4b). Both binaries now call it; only role-specific seam
  construction remains in each.
- The drift the finding predicted was already fixing-relevant: unifying the
  shutdown/select structure is what let the executor gain the validator's
  exit-on-engine-completion semantics (see F13.4b).
- Added `impl StateWriterQueue for Box<dyn StateWriterQueue>` (engine) so a
  binary can pick between queue wrappers at runtime (used by the attester tee).
- **Cargo.toml note (allowed per instructions)**: `crates/engine/Cargo.toml`
  gains workspace deps `anyhow`, `clap`, `tokio`, `toml`, `tracing-subscriber`
  for bin_support. Engine's only dependents are the two binaries' crates, both
  of which already use all five.

### F10.5 [L] — receipt cross-check ignores `logs` — FIXED
Files: `crates/validator/src/lib.rs`.
- `receipt_consistent` now also compares `logs` (execution output carried on
  the wire; `write_set_hash` covers state writes but not events). The doc
  comment now explicitly lists the deliberately out-of-scope fields (the RPC
  enrichment set) and why: they derive deterministically from already-checked
  inputs, so a divergence there implies a checked-field divergence.
- Mismatch reason string includes log counts. Test:
  `log_only_divergence_fail_stops`.

### F10.6 [L] (incl. F01.4) — unbounded buffers; late arrivals leak — FIXED
Files: `crates/validator/src/lib.rs`.
- `BalBuffer`/`ReceiptBuffer` are now thin wrappers over a shared
  `KeyedBuffer` core (also de-duplicates the deadline-wait + catch-up logic):
  - **Cursor pruning**: `take(key)` records the (monotone) request cursor and
    prunes everything below it; `insert` drops keys strictly below the cursor.
    An artifact arriving after its `take` gave up (the F01.4 5s-race leak, and
    replayed BALs for skipped blocks) can no longer accrete for the process
    lifetime.
  - **Size cap**: BAL 1024 entries, receipts 65536; overflow evicts the OLDEST
    entry — by the "missing ⇒ unverified, never false-positive" discipline an
    eviction can only cost an unverified block/tx.
- F01.4(a) (skip inert while buffer empty until first insert) is inherent to
  the head-based heuristic and bounded (ends at the first live insert);
  seeding the head from the subscriber image position would need a
  kardamom-log API change — left as is, per the finding's "bounded in
  practice" assessment.
- Tests: `late_arrival_below_cursor_does_not_leak`,
  `buffer_is_bounded_evicting_oldest` (+ existing suite unchanged/green).

### F10.7 [L] — dead `BalPublisher` / `Subscribers::bal()` — FIXED (ownership deviation)
Files: `crates/log/src/publisher.rs`, `crates/log/src/subscriber.rs`.
- Deleted `BalPublisher`, `Subscribers::bal()` and the `BalSubscriber` alias
  (plus now-unused `BlockDelta` imports). Zero callers in the workspace; the
  blessed path is the `aeron_live` one the binaries use (isolated publication
  runtime + `publish_best_effort`), so switching the binaries onto the legacy
  blocking client — the alternative fix inside my Owns list — would have been
  a regression.
- **Deviation note**: these files are under `crates/log/**` (WP-LOG's Owns),
  but the finding is assigned to WP-VAL and WP-LOG had already completed; the
  edit is a pure deletion of dead code plus import trims, verified against
  WP-LOG's finished tree (`cargo check/test -p kardamom-log` green).

### F10.8 [N] — docs describe never-built `BlockSink` seam — FIXED
Files: `crates/engine/src/lib.rs`, `crates/engine/Cargo.toml`.
- Both now name the real seams: `StateWriterQueue` + `TxReceiptsPublication`.

### F10.9 [N] — stale comments + state-root gauge mirrors block gauge (crate parts) — FIXED
Files: `crates/validator/src/metrics.rs`,
`crates/validator/src/bin/kardamom-validator.rs`, `crates/log/src/config.rs`.
- `validator_state_root_block` is no longer set unconditionally alongside
  `validator_committed_block`: new `set_state_root_block` is called by the
  snapshot poller only when `snap.state_root()` actually yields a root, so the
  CI "state root advancing" line is now an independent measurement.
- `crates/log/src/config.rs` tx_bal comment no longer claims "single publisher
  (the executor)" — it documents that every executor replica publishes on the
  group and why duplicates are harmless. (Same ownership deviation as F10.7 —
  one comment block.)
- The `validator.nomad.hcl` / `ci-cluster.sh` comment parts belong to WP-OPS.

### F13.3 [M] — recovery gated on `last_committed_block > 0` → crash-before-first-commit crash-loop — FIXED
Files: `crates/executor/src/bin/kardamom-executor.rs`,
`crates/validator/src/bin/kardamom-validator.rs`.
- tx_data / tx_deposits now use the archive replay-merge whenever
  `--replay-destination-endpoint` is configured (both cluster nomad jobs
  always pass it), not only when the DB shows a committed block. A restart at
  ANY point — including a crash before block 1 committed, or a node started
  behind a chain with history — replays the streams from origin and the exec
  thread's skip-count handles any already-committed prefix. This also fixes
  the fresh-validator-behind-a-loaded-chain case (live multicast no longer
  carries historical envelopes).
- Flag absent (local/IPC runs) keeps the live path; resuming without the flag
  still hard-fails with the same message as before. Fresh cluster boots are
  safe: `resolve_recording` waits for the recording to materialize, and
  WP-LOG's F13.2 barrier guarantees no record predates its recording.

### F13.5 [L] — receipts/Boundary published before durable ack — FIXED
Files: `crates/engine/src/actor.rs`.
- The sealed `Boundary` is now forwarded to the tx_receipts publisher AFTER
  `wait_committed` returns — downstream can no longer observe a boundary for
  state a crash could still un-commit (no latency cost: the wait happened
  before the next block either way).
- Per-tx receipts intentionally still stream at execute time; the
  at-least-once contract is now documented at the reorder site: recovery
  re-publishes byte-identical receipts, consumers must dedup on `tx_idx`
  (ingress does).

### F13.4b [L] — failed replay-merge recovery exits 0 — FIXED
Files: `crates/engine/src/bin_support.rs`, both bins,
(consumes WP-LOG's `ReplayMergeSubscriber::take_failure()`).
- The bridge pump tasks call `take_failure()` when the replay channel closes
  and record any fatal error on the shared `ReplayFailure` slot (error-logged).
- Both binaries `select!` on `replay_failure.failed()` alongside the shutdown
  signal and engine completion; a recorded failure drives shutdown and a
  **non-zero exit** (`bail!`) instead of the old warn + exit 0.
- Note on the hand-off wording: the engine actor itself never sees the
  replay subscriber (the binaries bridge it through sync channels), so the
  consumption point is the binaries' pump tasks + main select — the exec
  loop's `Err(_) => Ok(())` on its internal channel stays correct because the
  reader-thread results carry the true status through `Executor::run`.
- The executor main also no longer parks solely on SIGTERM: it exits (non-zero)
  when the engine loop dies on its own — previously an errored executor
  lingered alive-but-frozen until the orchestrator noticed, and `main`
  returned 0 even after an engine error. Clean SIGTERM shutdown still exits 0.
- Unit test: `replay_failure_recorded_before_wait_is_not_lost`.

### F05.4 [N] — fresh(60s) > resume(30s) join timeout reads backwards — FIXED
Files: `crates/engine/src/bin_support.rs` (+ pointer comments in both bins).
- The shared `bounded_join_timeout` doc explains the deliberate inversion:
  fresh starts must ride out full bring-up races (images forming, deploy
  ordering, joining mid-burst), while a resume reads locally-materialized
  archive streams that merely catch up at different rates.

### F11.1 [M] — validator default metrics port collides with ingress (9006) — FIXED (bin part)
Files: `crates/validator/src/bin/kardamom-validator.rs`.
- Default is now `127.0.0.1:9007` with a comment naming the collision. The
  cluster deploy is unaffected (validator.nomad.hcl sets
  `KARDAMOM_METRICS_ADDR` explicitly). Docs port table → WP-OPS.

### F07.2 [M] — boundary-only gap across reconnect → silent canonical-order inversion — FIXED
Files: `crates/engine/src/reader/cluster.rs`.
- The suggested fix (enter catch-up on every session re-establishment) needs a
  reconnect signal from the session thread, which lives in
  `crates/cluster-adapter/**` (owned by WP-SEQ) — not implementable in this WP.
- Implemented the engine-side guard instead: `ingest` now fail-stops
  (`BoundaryMisaligned`) on any still-owed boundary (`block_number >=
  next_block`) sealing at `end_tx_idx < next_index`. Within one Aeron session
  frames are ordered, so that condition is reachable ONLY via the
  reconnect-inversion window — no false positives — and it converts the silent
  replica divergence into the designed fail-stop: the partial block never
  commits, and the restart's cursor-based REPLAY_FROM re-delivers the window
  in order (gapless). Preventing rather than detecting the inversion remains
  available to WP-SEQ via a session-establishment flag.
- Test: `late_boundary_sealing_below_cursor_is_fatal` (reproduces the exact
  scenario from the finding).

### F08.2 [L] — kardamom_sealer_* also emitted by validators — FIXED
Files: `crates/engine/src/reader/cluster.rs`, `crates/engine/src/metrics.rs`,
both bins.
- `ClusterTxOrderingSubscription` gains an emission toggle
  (`suppress_sealer_metrics()`, default on for back-compat); the validator
  binary suppresses it, the executor stays the blessed emitter. metrics.rs
  comment updated to match reality (per-replica emission, max()-based queries).

### F06.3 hand-off (WP-WD) — attester wiring into the validator binary — DONE
Files: `crates/validator/src/bin/kardamom-validator.rs`
(attester.rs untouched, per instructions).
- All 5 steps implemented: (1) `--l1-rpc-url` / `--output-oracle` /
  `--attester-key` (raw hex or `env:VAR`, resolved before building
  `AttesterConfig`) / `--attester-post-interval` (default 1), each with the
  specified env vars; (2) `spawn_attester` after the writer-queue build, inside
  the tokio runtime, handle held for the process lifetime; (3) writer queue
  wrapped in `AttestingWriterQueue` when enabled (via
  `Box<dyn StateWriterQueue>`, both arms feed the same `Executor::run`);
  (4) snapshot poller hoists `state_root()` out of the debug branch and calls
  `handle.submit_root(block, root)` on each observed root (shared with the
  F10.9 gauge fix); (5) bin module docs state that without the three flags no
  automatic attestation happens and the key must be the oracle's permissioned
  attester. A PARTIAL flag set fails startup with a clear error rather than
  silently disabling attestation.

## Verification
- `cargo check -p kardamom-engine -p kardamom-validator -p kardamom-executor
  -p kardamom-log --all-targets` — pass; `cargo check --workspace` — pass
  (no reverse-dep breakage from the log deletions or engine API additions).
- `cargo test -p kardamom-engine --lib` — **55 passed** (new:
  `commit_thread_fail_stops_on_divergence`,
  `late_boundary_sealing_below_cursor_is_fatal`,
  `replay_failure_recorded_before_wait_is_not_lost`; all pre-existing
  reader/cluster/recovery tests green against the F13.5 reorder).
- `cargo test -p kardamom-validator --lib` — **15 passed** (new/updated:
  log-only divergence, receipt aged-out skip, below-cursor leak, bounded
  eviction, divergence-latch retry).
- `cargo test -p kardamom-executor` — all suites green (10 result lines, 0
  failures). `cargo test -p kardamom-log --lib` — 26 passed.
- `cargo clippy -p kardamom-engine -p kardamom-validator -p kardamom-executor
  --all-targets` — no warnings (only the pre-existing workspace
  `proc-macro-error2` future-incompat note).
- `cargo fmt --check` on the four touched crates — clean.
- CLI smoke: `kardamom-validator --help` / `kardamom-executor --help` show the
  shared `--state-durability` and the new attester flags.
- Not run: `withdrawal_e2e` (needs anvil; exercises attester.rs, which this WP
  did not modify — WP-WD ran it green), docker-gated Aeron e2e.

## Behavior changes to flag for reviewers/ops
- Executor process now exits non-zero when its pipeline dies or a replay-merge
  recovery fails (previously: lingered until SIGTERM, then exit 0). Clean
  SIGTERM shutdown still exits 0.
- With `--replay-destination-endpoint` set (all cluster deploys), tx_data /
  tx_deposits are ALWAYS read via archive replay-merge, fresh starts included.
- Validator default metrics port moved 9006 → 9007 (cluster deploy pins the
  addr explicitly and is unaffected; docs table update is WP-OPS).
- `Subscribers::bal()` / `BalPublisher` / `BalSubscriber` removed from
  kardamom-log (dead code; no workspace callers).
