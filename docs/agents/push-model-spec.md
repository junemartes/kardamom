# Push-Based Waiting — Replacing Manual Polls and Sleeps — Spec

- **Date:** 2026-08-07
- **Status:** PROPOSED. Push-1a in progress.
- **Goal (definition of done):** every place the pipeline waits by sleeping-and-rechecking is either converted to an event-driven wait (notify/recv/subscription) or explicitly classified as a deliberate timer / transport duty cycle and documented as such. Latency wins: the 50µs-per-tx engine join spin, the 200ms validator snapshot staleness, and the ≤12s L1 ingest delay all go to event-time.
- **Non-goals:** the Aeron duty cycle (poll-by-design; its `IdleBackoff`s carry hard-won tuning — a fixed 100µs wake was once 66% of sequencer CPU, an unconditional 1ms sleep once capped shards at ~2k tps); deliberate timers (250ms boundary clock = block cadence, snapshot/checkpoint intervals, keep-alives, sig-verify batch window, 500ms tx-error grace); bounded loud publish retries (`offer_with_deadline`, must-deliver loops); bash chaos drills.

## Background: the audit (2026-08-07)

Full inventory in the session record; the short version: **the push infrastructure already exists — polling is a consumption-site choice.** Every Aeron subscriber handle exposes both `async recv()` and `try_recv()`; all hot-path pumps pick `try_recv` + backoff for sync-loop/core-pinning reasons. `validator::KeyedBuffer` (Condvar notify) and `SnapshotReceiver::recv()` (bounded(1) notify, published by the state writer after every commit) sit unused next to code that sleeps and re-checks. No `tokio::sync::watch` exists in the workspace; no WS/pubsub L1 connection exists in the product.

Key sites (class → representative):

| Class | Site | Today | Push source |
|---|---|---|---|
| hot path | `engine/src/reader.rs:651` join `wait_for_envelope` | 50µs spin per tx | per-key Condvar (KeyedBuffer pattern); writer is in-process (`bin_support.rs:190`) |
| hot path | sequencer pumps ×3 (`sequencer.rs:659`, bin `:419`, `:550`) | `try_recv` + 1µs→100–500µs backoff | same channels' `recv()`; needs Select (see decision below) |
| background | `validator/bin:649` snapshot feeder | `current()` + 200ms sleep | `SnapshotReceiver::recv()` — drop-in |
| background | recorder parks (`ingress/bin:428`, `da-watcher/bin:241`) | 500ms sleep forever | shutdown Notify/oneshot |
| L1 ingest | `da_watcher/src/watcher.rs:212` | HTTP poll of `finalized` every 12s | alloy `connect("ws://…")` → `newHeads` subscription as trigger + `finalized` read; HTTP poll as reconnect fallback |
| startup | `recorder.rs:358` recording-materialize wait | 500ms poll | archive `poll_for_recording_signals` (`RECORDING_STARTED`), already used in `replay.rs` |
| dead | `recorder.rs:400` `run_durable_watermark_loop` | polls archive position | **no caller in-tree** — delete |
| harness | `proc.rs` log-file re-reads, `poll_until` ×30 | 100–200ms polls | stream child stdout (supervisor already pipes it); `kardamom_subscribeReceipts` WS; Prometheus gauges stay pull |

## Design decision: blocking `recv` on the Aeron bridge channels

Considered: replacing pump `try_recv` loops with full blocking `recv`.

Scoping fact: these sites consume the **bridge channel** filled by the Aeron duty-cycle thread — the duty cycle stays a poller either way, so blocking recv removes the second polling stage (consumer backoff, 100–500µs caps), not the first (duty-cycle `IdleBackoff`, 1ms cap after quiet).

- Pro: exact wakeup on first-tx-after-idle; idle CPU ~0 (pumps currently wake ~10k/s at cap even idle — relevant to the proven >3.5k tps host-CPU ceiling, #124/#129 campaign); simpler code; channel-close shutdown for free.
- Con: futex park/unpark per message regresses sustained throughput if done naively — must be **recv-then-drain** (block for first item, `try_recv` until empty; in-repo precedent: the commit thread, `engine/src/actor.rs:1039`). The sequencer publish loop is multi-source (tx_data + floor updates + contiguity rejects + backpressure retry) — parking on one channel starves the rest, so it needs a `Select` over all sources (in-repo template: the cluster-adapter session loop, `cluster-adapter/src/live.rs:660`). Parked threads can see ms-class unpark jitter on contended hosts; under load recv-then-drain rarely parks, but the p95 12–16ms load-shard SLO and the perf harness arbitrate, not intuition.

**Decision:** use tokio's `recv_many`/`blocking_recv_many` (parks until non-empty, then drains up to `limit` in one call — recv-then-drain as a single primitive; available in workspace tokio 1.52). The `limit` is deliberate: an unbounded batch would let a burst starve the loop's other duties. Single-source pumps (deposits, receipts-floors) need only `recv_many`; the multi-source sequencer main loop still needs the `Select` design with `recv_many` per ready source. Refinement (user question, 2026-08-07): the park/unpark cost does NOT appear at high load — a backlogged channel returns immediately without parking (and in async contexts tokio never parks the worker while work exists). It concentrates in the KEEPING-UP regime (drain-to-empty between arrivals ⇒ one futex pair per arrival ≈ <1% core at 3k tps — comparable to what the backoff cap already burns while idle). The historical 66%-CPU finding was crossbeam pre-park SPIN in the Aeron runtime's own channel, a different primitive, already fixed by IdleBackoff. `recv_many`'s real value is wake amortization for bursty arrivals — and arrivals are structurally bursty because the Aeron duty cycle pushes up to 64 fragments per poll into the bridge. Residual risk = unpark jitter on contended hosts vs the p95 SLO, which is what the perf gate measures. Its own measured PR (Push-1c), after the zero-risk swaps. Perf gate: `kardamom-load` p50/p95 and the allocation regression gate, before/after, plus `replicated_shard_racing`/resync tests and both chaos-sequencer shards.

## Phases

- **Push-1a (zero-risk swaps, this PR):** validator snapshot feeder → `SnapshotReceiver::recv()`; recorder park loops → shutdown-notified waits; delete `run_durable_watermark_loop`; `supervisor::wait_for_path` unchanged (startup-only, fine).
- **Push-1b:** `JoinBuffer` grows per-key notify (Condvar, KeyedBuffer pattern) killing the 50µs/tx join spin. Hot path ⇒ perf-gated like 1c.
- **Push-1c:** sequencer pumps → `recv_many` (+ `Select` for the multi-source main loop) per the decision above.
- **Push-2:** L1 over WS — da-watcher `newHeads` trigger + `finalized` read with HTTP-poll fallback; batcher/attester receipt confirmation over the same pubsub provider. Zero dependency changes (alloy `connect` dispatches on scheme).
- **Push-3:** harness — Target-L child stdout streamed into a broadcast (replacing log-file re-read polls), receipt waits over `kardamom_subscribeReceipts`; `poll_until` stays for Prometheus gauges. Feeds the W4 `kardamom-checker` design (checker = subscriber, not another interval scraper).
