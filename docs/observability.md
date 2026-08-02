# Observability

Kardamom's cluster services each export Prometheus metrics, and the
`kardamom-bench` load generator drives traffic so the dashboard panels move.
The local stack — Prometheus + Grafana — is wired up with `docker compose`.

## Metrics

Every kardamom service binary exports Prometheus metrics on its own HTTP listener.
Defaults (override with `--metrics-addr` or `KARDAMOM_METRICS_ADDR`):

| Service | Default address | Dashboard UID |
| --- | --- | --- |
| `kardamom-sequencer` | `127.0.0.1:9001` (replica group seq-b: `:9011`) | `kardamom-sequencer` |
| `kardamom-batcher` | `127.0.0.1:9002` | `kardamom-batcher` |
| `kardamom-executor` | `127.0.0.1:9004` (cluster deploy binds `0.0.0.0`) | `kardamom-executor` |
| `kardamom-da-watcher` | `127.0.0.1:9005` | `kardamom-da-watcher` |
| `kardamom-ingress` | `127.0.0.1:9006` | `kardamom-ingress` |
| `kardamom-validator` | `127.0.0.1:9007` | — (no dashboard yet) |

The validator's default is `9007` precisely because `9006` is the ingress
default: running both locally with defaults used to race for one socket (the
loser's exporter only complains from a background thread — see the shared-addr
warning below). The cluster deploy pins the validator to `0.0.0.0:9006` on the
aux node instead (`validator.nomad.hcl`; no ingress runs there), so scrapes
and `ci-cluster.sh`'s validator verdict still read `:9006` **in the cluster**.

One more port-map footnote: with P=2 racing sequencer replicas per shard, the
second Nomad group (`seq-b`) moves to `:9011` on the same nodes; scraping
`:9001` across the sequencer nodes still yields exactly one replica per shard,
so per-shard totals keep their pre-replication meaning.

The sealer no longer appears here: canonical ordering runs as a Java
clustered service inside an Aeron Cluster (`cluster/sealer-service/`), which
has no Prometheus endpoint of its own — there is no `kardamom-sealer` binary,
no `:9003` listener, and no sealer scrape job in `deploy/prometheus.yml`.
Instead, each **executor re-exports the sealer's output** as it decodes
cluster egress: every boundary frame bumps
`kardamom_sealer_boundaries_emitted_total` and sets
`kardamom_sealer_block_number` to the sealer's declared block. These series
appear on the executor's `:9004` endpoint (labelled with the executor's
`host_id`) and ride the existing executor scrape job, so they measure the
boundary stream *as observed at each executor's subscription*, not
JVM-internal state.

All binaries read the same `KARDAMOM_METRICS_ADDR` env var, so a value
shared across colocated services makes them race for one socket — prefer the
per-service `--metrics-addr` flag when overriding more than one service.

The services bind loopback by default. The compose-managed Prometheus scrapes
them through `host.docker.internal`, which works as-is on Docker Desktop
(macOS/Windows); on Linux that name resolves to the bridge gateway, so each
service must be started with `--metrics-addr 0.0.0.0:<port>` (or another
non-loopback bind) to be reachable.

In the **cluster deploy** the executor job binds `0.0.0.0:9004` deliberately:
the chaos suite probes executors directly over the cluster bridge
(`http://<node_ip>:9004/metrics`) instead of through `docker exec`, because
killing a privileged DinD node can stall host-dockerd exec runner-wide for
minutes — which used to black out every exec-based probe at once and
masquerade as total pipeline failure (issue #76). The exporter itself also
runs on a dedicated thread inside every service (`kardamom_obs::init`), so
`/metrics` keeps answering even if the service's own runtime wedges:
"wedged but alive" reads as `kardamom_service_up == 1` with stale gauges,
not as node loss.

Every binary also takes `--host-id <STRING>` (env `KARDAMOM_HOST_ID`, default
`local`). It's
stamped on every emitted metric as the `host_id` label, alongside an automatic
`service` label set by `kardamom_obs::init`. The top-level `Kardamom Overview`
dashboard exposes a `host` template variable; per-service dashboards inherit it.

### Naming convention

`kardamom_<service>_<subsystem>_<name>_<unit>` (e.g. `kardamom_sequencer_tx_ingested_total`,
`kardamom_executor_block_apply_duration_seconds`). One deviation: the
validator's metrics are unprefixed `validator_*` —
`validator_committed_block`, `validator_blocks_verified_total`,
`validator_bal_missing_total`, `validator_receipt_missing_total`,
`validator_divergence_total`, `validator_state_root_block` — consumed by the
ci-cluster validator verdict and the `validator-lapse` chaos case rather than
a dashboard. The executor exposes a
`kardamom_executor_block_number` gauge for the chain head, plus the
re-exported `kardamom_sealer_block_number` / `kardamom_sealer_boundaries_emitted_total`
observed from cluster egress (see above).

### Scaling to multiple hosts

Each scrape job in `deploy/prometheus.yml` is a static-targets list. To add a
second host running every service, append `host-2:<port>` to each of the
target lists. Every metric is already labelled with `host_id`, so dashboards
group by host without relabel rules.

### Rename map (historical — the metrics-namespace migration)

| Old | New |
| --- | --- |
| `batcher.blocks_observed_total` | `kardamom_batcher_blocks_observed_total` |
| `batcher.batches_posted_total` | `kardamom_batcher_batches_posted_total` |
| `batcher.blobs_posted_total` | `kardamom_batcher_blobs_posted_total` |
| `sealer_boundaries_emitted_total` | `kardamom_sealer_boundaries_emitted_total` |
| `sealer_block_number` | `kardamom_sealer_block_number` |
| `sealer_tick_skipped_total` | `kardamom_sealer_tick_skipped_total` |

Per-emission `host_id` labels are gone — `host_id` is a recorder-level global,
sourced from `--host-id`/`KARDAMOM_HOST_ID`. (The `kardamom_sealer_*` rows
above date from the standalone Rust sealer; the boundaries/block-number names
live on, re-exported by the executors, while `tick_skipped` died with it.)

## What is instrumented

Each service instruments its own hot path; the metric set for a service is
whatever its provisioned Grafana dashboard
(`deploy/grafana/provisioning/dashboards-json/kardamom-<service>.json`) queries.
Names follow the `kardamom_<service>_*` convention above, and the `Kardamom
Overview` dashboard (`kardamom-overview`) stitches the cross-service signals
(liveness, block height from the sealer's boundary stream and the executor)
into one view.

Every service also emits `kardamom_build_info` (gauge, always 1, labeled with
`version` and `sha`) via `kardamom_obs::init` — set `KARDAMOM_GIT_SHA` at build
time to populate `sha`.

Two low-rate counters deserve standing alerts:

- `kardamom_executor_resync_total` (labeled `outcome=peer-checkpoint |
  unrecoverable`) counts full-resync fallbacks after a cluster replay-window
  overrun (`REPLAY_UNAVAILABLE`). Any non-zero rate means a node fell behind
  the retention window and repaired itself from a peer checkpoint —
  `unrecoverable` means it could not and is waiting for an operator (see
  `docs/failure-modes.md`, "Replay-window overrun").
- `kardamom_executor_invalid_tx_skipped_total` counts deterministically-invalid
  canonical txs skipped with a marker receipt (`status=false, gas_used=0` —
  `Receipt::is_invalid_skip`; issue #92). The skip keeps the chain live and is
  identical across live execution, recovery replay, and validator re-execution
  — but any occurrence means an upstream guard (sequencer nonce fence, cluster
  dedup, receipt-floor resync) let an invalid record into the canonical log:
  investigate the source, the chain itself is fine.

The live batcher (#39) adds a settlement-health group on port 9002: in live
mode `kardamom_batcher_batches_posted_total` / `_blobs_posted_total` count
**confirmed L1 posts** (not packed batches), `kardamom_batcher_last_posted_block`
vs `kardamom_executor_block_number` is the DA-freshness lag to watch,
`_l1_post_retries_total` flags a flaky L1, and `_skipped_posted_blocks_total`
counts stale-cursor re-observations after a restart (bounded and expected —
sustained growth means the cursor file is not being persisted).

## Quick start

Bring up the observability stack:

```sh
cd deploy && docker compose up
```

Start the cluster (see `deploy/cluster/` — canonical ordering lives in the
Aeron Cluster sealer, which only runs there, so that is the full-pipeline
path; individual Rust services can still be run locally against a native
Aeron media driver — `just aeron-driver-up`).
Then drive load at the ingress JSON-RPC endpoint with the bench. `transfers` is
the write-path workload (`eth_sendRawTransaction`); the chain it targets must
already prefund the signer EOAs the workflow uses (see "Workflows + signer
prefunding" below):

```sh
cargo run --release --bin kardamom-bench -- \
  --rpc http://127.0.0.1:8545 \
  --concurrency 16 --timeout 30s \
  transfers
```

Open <http://localhost:3000> (Grafana, `admin` / `kardamom`) → the provisioned
per-service dashboards populate within a few seconds.

The bench prints percentile latencies to stdout and (with `--output` set)
writes a JSON report.

For a tight flame/pprof recording scoped to the measurement window — without an
external Grafana or a hand-tuned chain — use the in-process harness below.

## Flamegraph + CPU profiling: the in-process harness

`kardamom-bench-harness` runs the bench against an **in-process `IngressProxy`**
stand-in: a real ingress (batched secp256k1 recovery, sender routing, jsonrpsee
framing, parked-receipt release) backed by in-memory `MockChannels` plus a
trivial "fake executor" that reflects every submitted tx as a success receipt.
No live Aeron media driver, sequencer, executor, or sealer is involved, so the
recording stays tight around the dispatch window:

```sh
cargo build --release --bin kardamom-bench-harness

./target/release/kardamom-bench-harness \
  --timeout 10s --concurrency 128 --max-in-flight 30 \
  --pprof-out /tmp/cpu.svg \
  transfers

open /tmp/cpu.svg
```

`--pprof-out` uses [`pprof-rs`](https://crates.io/crates/pprof) to sample
on-CPU time at 999Hz via `SIGPROF`, scoped to the dispatch window, and filters
the report to stacks containing at least one `kardamom_ingress::*` frame — the
harness runs the ingress proxy and the bench client on the same runtime, so the
raw report mixes ingress and client work and the filter keeps the SVG to the
ingress hot path (sig recovery, routing). The harness logs the kept/dropped
sample counts.

`--flame-out` records `tracing-flame` spans over the same window, but the
ingress stand-in emits **no** `tracing` spans, so that SVG is skipped when
empty (the harness logs a warning). The `pprof` output is the useful one for
this stand-in. Restoring span-based flames that show real ordering + revm
execution is the job of the **full in-process Aeron pipeline harness**, tracked
as a follow-up: it will run the real sequencer/executor/ingress in one process
over Aeron IPC (the sealer is now a JVM Aeron Cluster service, so it stays
external).

Symbolication is handled in-process by `pprof-rs`, so no `dsymutil` / `.dSYM`
dance is needed. Both outputs are restricted to the dispatch window: neither
sees prepare-phase ECDSA presigning, the workflow's sequential warmup queue,
server startup, or shutdown.

## Workflows + signer prefunding

Workloads are Rust `BenchWorkflow` impls (in `crates/bench/src/workflows/`),
not TOML scenarios. Three are built in and exposed as subcommands of both
binaries:

| Subcommand | Workflow type        | Notes |
|------------|---------------------|-------|
| `transfers`| `TransfersWorkflow` | `eth_sendRawTransaction` only — stresses the write path. Served by ingress today. |
| `calls`    | `CallsWorkflow`     | `eth_call` only against the `PUSH1 0x42 ... RETURN` contract. Needs the read-path RPCs ingress does not yet serve. |
| `mixed`    | `MixedWorkflow`     | 1:4 transfers:calls — mirrors a typical RPC traffic shape. |

All three default to the **Anvil test mnemonic**. Signers are derived
deterministically (`m/44'/60'/0'/0/i` for `i = 0..concurrency`).

- **`kardamom-bench-harness`** (in-process ingress stand-in): the fake executor
  reflects a success receipt for any signed tx, so no genesis/prefunding step
  is needed — just pick a write-path subcommand (`transfers`).

- **`kardamom-bench`** (standalone, against a running cluster): the target
  chain must already prefund the signer EOAs the workflow uses. For the built-in
  workflows that means the first N Anvil accounts. `chains/dev.toml` prefunds
  only account #0 — to bench transfers against a cluster seeded from it, extend
  it with one `[[alloc]]` per Anvil account `0..N`:

  ```toml
  chain_id = 412346

  [[alloc]]
  address = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"  # Anvil #0
  balance = "1000000000000000000000"

  # ... one per signer 1..N-1 ...
  ```

  Then seed the cluster's executor genesis from that file before pointing the
  bench at the ingress.

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

Each service has a provisioned dashboard under
`deploy/grafana/provisioning/dashboards-json/` (`kardamom-ingress`,
`kardamom-sequencer`, `kardamom-executor`,
`kardamom-batcher`, `kardamom-da-watcher`), plus the cross-service
`kardamom-overview`. The `kardamom-sealer` dashboard is fed by the
executor-re-exported `kardamom_sealer_*` series described above: boundary
rate, the sealer's declared block number per observing executor host, and
each executor's commit lag behind the sealer.
`crates/obs/tests/dashboards.rs` statically validates that
every shipped dashboard parses, is schema-38, and only queries `kardamom`-scoped
metrics — keep the `EXPECTED_DASHBOARDS` list in that test in sync when adding
or removing a dashboard.

## Histogram buckets

`kardamom_obs::init` installs a shared bucket set on the Prometheus recorder, so
every service's latency histograms share the same boundaries:

```
0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025,
0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0
```

(100 µs → 5 s, exponential.) Configured once via `PrometheusBuilder::set_buckets`
before the recorder is installed, so quantile math is consistent across
services.
