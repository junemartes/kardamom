# Observability

Kardamom exposes the cost of every RPC call from a single instrumentation
point that feeds both Prometheus metrics and `tracing-flame` flamegraphs. The
local stack — Prometheus + Grafana — is wired up with `docker compose`, and a
`kardamom-bench` binary drives load so the dashboard panels move.

## Metrics

Every kardamom service binary exports Prometheus metrics on its own HTTP listener.
Defaults (override with `--metrics-addr` or `KARDAMOM_METRICS_ADDR`):

| Service | Default address | Dashboard UID |
| --- | --- | --- |
| `kardamom` (RPC node) | `127.0.0.1:9000` | `kardamom-node` |
| `kardamom-sequencer` | `127.0.0.1:9001` | `kardamom-sequencer` |
| `kardamom-batcher` | `127.0.0.1:9002` | `kardamom-batcher` |
| `kardamom-sealer` | `127.0.0.1:9003` | `kardamom-sealer` |
| `kardamom-executor` | `127.0.0.1:9004` | `kardamom-executor` |
| `kardamom-da-watcher` | `127.0.0.1:9005` | `kardamom-da-watcher` |
| `kardamom-ingress` | `127.0.0.1:9006` | `kardamom-ingress` |

Every binary also takes `--host-id <STRING>` (env `KARDAMOM_HOST_ID`, default `local`).
It's stamped on every emitted metric as the `host_id` label, alongside an automatic
`service` label set by `kardamom_obs::init`. The top-level `Kardamom Overview`
dashboard exposes a `host` template variable; per-service dashboards inherit it.

### Naming convention

`kardamom_<service>_<subsystem>_<name>_<unit>` (e.g. `kardamom_sequencer_tx_ingested_total`,
`kardamom_executor_block_apply_duration_seconds`). The RPC node uses
`kardamom_rpc_*` for handler-level metrics and `kardamom_block_number` for the
chain head (predates the convention).

### Scaling to multiple hosts

Each scrape job in `deploy/prometheus.yml` is a static-targets list. To add a
second host running every service, append `host-2:<port>` to each of the seven
target lists. Every metric is already labelled with `host_id`, so dashboards
group by host without relabel rules.

### Rename map (from before this PR)

| Old | New |
| --- | --- |
| `batcher.blocks_observed_total` | `kardamom_batcher_blocks_observed_total` |
| `batcher.batches_posted_total` | `kardamom_batcher_batches_posted_total` |
| `batcher.blobs_posted_total` | `kardamom_batcher_blobs_posted_total` |
| `sealer_boundaries_emitted_total` | `kardamom_sealer_boundaries_emitted_total` |
| `sealer_block_number` | `kardamom_sealer_block_number` |
| `sealer_tick_skipped_total` | `kardamom_sealer_tick_skipped_total` |

The sealer's per-emission `host_id` label is gone — `host_id` is now a recorder-level global.

## What is instrumented

Per-stage spans + histograms (`kardamom_rpc_stage_duration_seconds`):

- `eth_sendRawTransaction`: `decode`, `recover_signer`, `acquire_write_lock`,
  `build_tx_env`, `execute`, `build_receipt`, `store_receipt`.
- `eth_call`: `acquire_read_lock`, `build_tx_env`, `execute`.

Handler-level: every RPC method gets a histogram
(`kardamom_rpc_duration_seconds`), a counter
(`kardamom_rpc_requests_total`), **and a tracing span named after the
method** (so the flamegraph nests the per-stage spans under e.g.
`eth_sendRawTransaction`). All three are wired from the same
`instrument_method!` site in `crates/node/src/metrics.rs`. Labels:
`method`, `outcome` (`ok` or `err`).

Plus `kardamom_block_number` (gauge) and `kardamom_build_info` (gauge, always
1, labeled with `version` and `git_sha`).

## Quick start

For Grafana exploration against a long-running node:

```sh
# 1. Observability stack.
cd deploy && docker compose up

# 2. Start kardamom. `chains/dev.toml` only prefunds Anvil account #0 —
#    fine for hitting the node by hand, but the bench wants N derived
#    signers prefunded. See "Workflows + signer prefunding" below before
#    pointing the bench at it.
cargo run --release --bin kardamom -- --chain chains/dev.toml

# 3. Drive load. `mixed` is the 1:4 transfers:calls built-in workflow.
cargo run --release --bin kardamom-bench -- \
  --rpc http://127.0.0.1:8545 \
  --concurrency 16 --timeout 30s \
  mixed
```

Open <http://localhost:3000> (Grafana, `admin` / `kardamom`) → the
provisioned **Kardamom RPC** dashboard populates within a few seconds.

For flame/pprof inspection that doesn't need an external Grafana or a
hand-tuned genesis, prefer the single-process harness — it builds the
genesis it needs from the workflow itself. See "Option B" below.

The bench prints percentile latencies to stdout and (with `--output` set)
writes a JSON report.

## Flamegraph workflow

`tracing-flame` records a sample any time a span is entered. It does **not**
sample on-CPU time the way `perf` does — anything outside an instrumented
span (jsonrpsee dispatch, hyper framing, tokio scheduling, revm internals)
is collapsed onto the bare `ThreadId(N)-tokio-rt-worker` root frame. For a
deep view past the spans, see "CPU profiling with pprof" below.

There are two flame-recording paths, for different jobs.

### Option A — long-running node, `KARDAMOM_FLAME`

For Grafana exploration where the node stays up for minutes and you Ctrl-C it
when you've seen enough:

```sh
KARDAMOM_FLAME=./flame.folded \
  cargo run --release --bin kardamom -- --chain chains/dev.toml

# Drive load (or hit the node by hand)…
cargo run --release --bin kardamom-bench -- --rpc http://127.0.0.1:8545 mixed
# Ctrl-C the node when done.

cargo install inferno          # one-time
grep ';' flame.folded | inferno-flamegraph --minwidth 0 --width 2000 > flame.svg
open flame.svg
```

The `grep ';'` filter drops bare-root samples (`ThreadId(N)-tokio-rt-worker`
with nothing after) so the spans aren't sub-pixel. The unfiltered SVG is
~99% root frame because the node spends most of its lifetime not inside any
span (idle workers, RPC framing, async-machinery, etc.).

The `FlushGuard` returned by `FlameLayer::with_file` is held in `main` until
shutdown completes, so the folded file is not truncated.

### Option B — single-process harness, `kardamom-bench-harness`

For a tight flamegraph scoped exactly to the dispatch window, use the
embedded harness. It boots a kardamom node in-process, derives the
workflow's signer set, prefunds them in the in-process genesis
automatically, drives load, and gates `tracing-flame` recording to the
dispatch phase only (signer derivation, presigning, and the workflow's
own sequential warmup queue are all excluded via an `AtomicBool`-driven
`FilterFn`).

```sh
cargo build --release --bin kardamom-bench-harness

./target/release/kardamom-bench-harness \
  --timeout 10s --concurrency 32 \
  --flame-out /tmp/flame.svg \
  transfers

open /tmp/flame.svg
```

The harness merges the per-tokio-worker stacks in memory and renders the
SVG via inferno itself — no external `inferno-flamegraph` post-processing
step needed.

The three built-in workloads are subcommands: `transfers`, `calls`,
`mixed`. Each uses hardcoded defaults (Anvil test mnemonic, deterministic
`eth_call` target, 1:4 mix ratio).

## CPU profiling with pprof

`tracing-flame` only sees our spans. To see what the *CPU* actually does
(hyper, jsonrpsee, serde_json, revm internals, ECDSA), pass `--pprof-out`
to the harness. It uses [`pprof-rs`](https://crates.io/crates/pprof) to
sample on-CPU time at 999Hz via `SIGPROF`, scoped to the same dispatch
window as the `tracing-flame` recording.

```sh
./target/release/kardamom-bench-harness \
  --timeout 10s --concurrency 128 \
  --max-in-flight 30 \
  --flame-out /tmp/flame.svg \
  --pprof-out /tmp/cpu.svg \
  transfers

open /tmp/flame.svg /tmp/cpu.svg
```

Both `--flame-out` and `--pprof-out` are written as ready-to-view SVGs;
the harness uses inferno internally for both, with shared options
(`min_width = 0`, `image_width = 2000`).

**The pprof report is filtered to stacks containing at least one
`kardamom_node::*` frame before rendering.** The in-process harness runs
the node server and the bench client on the same tokio runtime, so the
raw report mixes node and client work — without the filter the SVG would
be mostly jsonrpsee client, hyper, and our `send_loop` noise. The harness
logs the kept/dropped counts (typically ~5% kept against the closed-loop
write workload — most CPU is on the bench side) so you can see how
aggressive the filter was.

Both outputs are restricted to the dispatch window: neither sees prepare-
phase ECDSA presigning, the workflow's sequential warmup queue, server
startup, or shutdown.
Symbolication is handled in-process by `pprof-rs`, so no `dsymutil` /
`.dSYM` dance is needed.

## Workflows + signer prefunding

Workloads are Rust `BenchWorkflow` impls (in `crates/bench/src/workflows/`),
not TOML scenarios. Three are built in and exposed as subcommands of both
binaries:

| Subcommand | Workflow type        | Notes |
|------------|---------------------|-------|
| `transfers`| `TransfersWorkflow` | `eth_sendRawTransaction` only — stresses the write path. The single `RwLock` serializes writes; expect `acquire_write_lock` to dominate at high `--concurrency`. |
| `calls`    | `CallsWorkflow`     | `eth_call` only against the `PUSH1 0x42 ... RETURN` contract. |
| `mixed`    | `MixedWorkflow`     | 1:4 transfers:calls — mirrors a typical RPC traffic shape. Default for casual runs. |

All three default to the **Anvil test mnemonic**. Signers are derived
deterministically (`m/44'/60'/0'/0/i` for `i = 0..concurrency`).

- **`kardamom-bench-harness`** (in-process): the harness asks the workflow
  for its genesis allocs (`workflow.genesis_alloc(concurrency)`), builds
  the in-process `Genesis` from them, and starts the node against it. No
  manual chain config needed — just pick a subcommand.

- **`kardamom-bench`** (standalone, against a remote node): the remote
  node's chain config must already prefund the signers the workflow will
  use. For the built-in workflows that means the first N Anvil accounts.
  `chains/dev.toml` prefunds only account #0 — to bench transfers/mixed
  against it, extend it with one `[[alloc]]` per Anvil account `0..N` and
  one `[[alloc]]` for the call target contract:

  ```toml
  chain_id = 412346

  [[alloc]]
  address = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"  # Anvil #0
  balance = "1000000000000000000000"

  # ... one per signer 1..N-1 ...

  [[alloc]]
  address = "0x0000000000000000000000000000000000001234"
  code    = "0x604260005260206000f3"
  ```

  Then run kardamom against that file before pointing the bench at it.

### Custom workloads

Implement `BenchWorkflow` for your own type and drive it via the generic
`Benchmark<W>` / `Harness<W>`. See
`crates/bench/examples/custom_workflow.rs` for an ~70-line example that
benches `eth_blockNumber` from outside this crate — no changes to the
bench crate needed.

```sh
cargo run --release --example custom_workflow -p kardamom-bench
```

`cargo run --example gen_mnemonic -p kardamom-bench` prints a fresh
BIP-39 phrase if your custom workflow wants its own signer set.

#### Workflow API at a glance

`BenchWorkflow::prepare` returns a `Prepared<Item> { warmup, main }`:

- `warmup: Vec<Item>` — a single flat queue the harness drains
  **sequentially** with one in-flight request, unmetered, with the
  flame/pprof gates off. Bounded by `--timeout`. Use this to JIT hot
  paths, warm jsonrpsee/hyper buffers, and stabilize chain state before
  the metered window. Built-in workflows produce `WARMUP_PER_TASK = 100`
  items per task; pick whatever volume suits yours.
- `main: Vec<Vec<Item>>` — per-task metered work (`n_tasks ×
  txs_per_task`). Dispatched concurrently in the recording window.

Workflows that align tx state across phases (e.g. transfers consume
nonces) must lay the warmup queue out so the main per-task chunks pick
up at the right nonce. See `TransfersWorkflow::prepare` for the pattern:
round-robin presign across signers for warmup nonces `0..WARMUP`, then
per-signer presign for main starting at nonce `WARMUP`.

## Dashboard panels

The provisioned dashboard (`deploy/grafana/dashboards/kardamom-rpc.json`) has
six panels in a 2×3 grid plus a footer:

1. **Request rate** — `sum by (method) (rate(kardamom_rpc_requests_total[30s]))`
2. **Error rate** — same, filtered on `outcome="err"`
3. **Handler latency P50/P90/P99** — `histogram_quantile(…)` over
   `kardamom_rpc_duration_seconds_bucket`
4. **`sendRawTransaction` stage breakdown** — mean per stage, stacked
5. **`eth_call` stage breakdown** — mean per stage, stacked
6. **Block number** — gauge

Build-info appears in a small text panel; the raw values are at
`http://localhost:9000/metrics`.

## Histogram buckets

Shared across all three histograms:

```
0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025,
0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0
```

(100 µs → 5 s, exponential.) Configured once via `PrometheusBuilder::set_buckets`
before the recorder is installed; the same boundaries apply to handler-level
and per-stage histograms, so quantile math is consistent.

## Cardinality

Roughly:

- handler: 7 methods × 2 outcomes = 14 series for each of `requests_total`
  and `rpc_duration_seconds`.
- stage: ~10 (method, stage) pairs.

Order of 30 series total. Safe for a `metrics-exporter-prometheus` HTTP
listener at 1 s scrape cadence.
