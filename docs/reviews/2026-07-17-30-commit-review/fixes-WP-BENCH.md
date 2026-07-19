# Fixes — WP-BENCH (load/chaos bench harness)

Findings: F15.1, F15.2, F15.4, F15.5, F15.7-plan.rs, F14.1, F14.2
Detail files: 15-af98ffa.md, 14-b00b3e0.md. All fixes confined to `crates/bench/**`. No Cargo.toml changes needed (jsonrpsee's workspace dep already carries the `server` feature used by the new tests).

## F15.1 [M] — vacuous must-deliver gate — FIXED (flagship)

Files: `crates/bench/src/load/engine.rs`, `crates/bench/src/load/mod.rs`

- `submit_task` now takes a `verify_receipts` flag (wired as `!chaos_mode` from `LoadConfig`). In non-chaos soak, an accepted submit whose post-accept `eth_getTransactionReceipt` re-fetch returns nothing is inserted as `Pending { accepted: true }` instead of being silently confirmed via `unwrap_or(1)`. The drain re-polls it, and a never-confirmed leftover surfaces as `missing`, making the `assert_all_delivered` "must-deliver violated" gate actually fireable. The ingress receipt cache is only volatile across restarts, which don't happen in soak — so a failed re-fetch there is a real durability signal, exactly per the suggested fix.
- Chaos mode keeps the previous on-offer-trusting behavior (a restart wipes the cache; re-fetch failure there would false-flag delivered txs), now as an explicit, commented branch.
- Regression tests (engine.rs): `accepted_but_unreceipted_tx_counts_missing_and_fires_must_deliver` drives the real `submit_task` → `drain` → `remaining_pending` → `evaluate` path against an in-process mock jsonrpsee ingress that acks submits but never serves a receipt, and asserts `missing == 1` and the must-deliver failure fires; `chaos_mode_trusts_on_offer_ack_when_receipt_refetch_fails` pins the chaos contrast.

## F15.2 [M] — indiscriminate retry + hard seq_dropped gate — FIXED

Files: `crates/bench/src/load/engine.rs`, `crates/bench/src/load/accounting.rs`

- Retry loop: before each retry, `submit_task` checks `eth_getTransactionReceipt` for the locally-known hash; if the errored attempt actually landed, it confirms and returns instead of resubmitting a duplicate (which the sequencer counts as a past-nonce drop).
- Verdict: `seq_dropped > 0` is no longer a hard failure in `chaos_mode` (retry noise across an ingress restart with a volatile dedup cache is expected); the delta stays in the verdict as a diagnostic, and remains a hard failure in soak. Both halves of the suggested fix implemented.
- Tests: `landed_tx_is_not_resubmitted_after_submit_error` (asserts exactly 1 send RPC against a mock that errors submits but serves the receipt) and `sequencer_drop_is_soft_in_chaos`; existing `sequencer_drop_fails` still covers soak.

## F15.4 [M] — submit tasks never joined; drain exits early — FIXED

Files: `crates/bench/src/load/engine.rs`, `crates/bench/src/load/mod.rs`

- `pacer` now spawns submit tasks into a caller-owned `tokio::task::JoinSet` (threaded through `ramp_to_max` and the soak call). New `join_submit_tasks(tasks, deadline)` awaits them all, bounded by `drain_timeout`, warning if the deadline passes with tasks still in flight. `run()` joins before `drain`, so all pending insertions happen before the drain snapshot and the final `counts()`/`remaining_pending()` read classifies the whole tail instead of leaving up to `max_in_flight` txs counted only as `offered`.
- Tests: `join_submit_tasks_waits_for_in_flight_tasks`, `join_submit_tasks_gives_up_at_deadline`.

## F15.5 [L] — FROZEN false-positive on restarted executor gauge reset — FIXED

Files: `crates/bench/src/load/accounting.rs`, `crates/bench/src/load/mod.rs`

- Per the suggested fix: in chaos mode `run()` takes a second "recheck" snapshot ~3s after `fin` and passes it via new `EvalInput::recheck`. In `evaluate`, an executor with `advanced <= 0` while the sealer advanced is classified `RECOVERING` (informational, no failure) if the recheck shows its gauge moving past `fin` — i.e. a hard-killed executor still replaying past its reset gauge — and `FROZEN` (failure) only if the recheck shows no movement. Soak passes `recheck: None`, preserving the strict behavior.
- Tests: `restarted_executor_with_moving_recheck_is_recovering_not_frozen`, `restarted_executor_with_stalled_recheck_is_frozen`; existing `frozen_executor_fails_even_in_chaos` (no recheck movement) still passes.

## F15.7 [N] — "strict nonce order" doc claim (plan.rs part) — FIXED

File: `crates/bench/src/load/plan.rs`

- Module doc reworded: the engine pops each sender's txs in per-sender FIFO nonce order; submits are spawned as concurrent tasks, so wire/arrival order is explicitly noted as not strict. (chaos.sh / shell-helper parts → WP-OPS, per SUMMARY.)

## F14.1 [L] — harness bin advertises removed node; calls/mixed fail at runtime — FIXED

File: `crates/bench/src/bin/harness.rs`

- `about` and module doc repointed from "in-process kardamom node" to "in-process ingress stand-in (write path only)". `calls`/`mixed` subcommands are kept for discoverability but now `bail!` immediately with a clear error (no `eth_call` on the write-only stand-in; use `transfers` here or `kardamom-bench` against a full node) instead of failing deep inside `CallsWorkflow::prepare`; their help text says the same. Unused `CallsWorkflow`/`MixedWorkflow` imports removed.

## F14.2 [N] — stale genesis_alloc validation claim — FIXED

Files: `crates/bench/src/harness.rs`, `crates/bench/src/workflow.rs`

- `Harness::run`'s `# Errors` doc no longer claims genesis validation; it now states `genesis_alloc` is not consulted by the ingress stand-in. `BenchWorkflow::genesis_alloc` (and the module-doc bullet) redocumented as descriptive — the funding/contracts a workflow expects on an external chain, usable by external harnesses and the future full-pipeline harness. The trait method is kept (dropping it would break `examples/custom_workflow.rs` and external implementors, and the smoke test still covers its error path).

## Verification

- `cargo check -p kardamom-bench --all-targets` — PASS (only pre-existing workspace future-incompat note for `proc-macro-error2`, unrelated).
- `cargo test -p kardamom-bench --lib` — PASS: 43 passed, 0 failed (includes all 8 new regression tests listed above).
- `cargo clippy -p kardamom-bench --all-targets` — no warnings from this crate.
