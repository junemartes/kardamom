# Prometheus + Grafana wiring for all Kardamom services

Status: approved
Author: Claude (autonomous)
Date: 2026-05-29

## Goal

Today only `kardamom-node` (RPC) exports metrics to Prometheus. `kardamom-sequencer`, `kardamom-batcher`, and `kardamom-sealer` emit metrics via `metrics::*!()` but have no HTTP exporter, so the data is silently dropped. `kardamom-executor`, `kardamom-da-watcher`, and `kardamom-ingress` emit nothing.

This PR:

1. Adds a shared `crates/obs` crate that every service binary uses to install the Prometheus exporter and stamp recorder-level global labels (`host_id`, `service`).
2. Wires the exporter into all 7 service binaries, each on a distinct default port.
3. Adds a small baseline of metrics to the 3 currently-silent binaries so their dashboards aren't empty.
4. Normalises metric names to `kardamom_<service>_<subsystem>_<name>_<unit>` (renames batcher's period-separated names and sealer's prefixless names; node + sequencer are already compliant).
5. Extends `deploy/prometheus.yml` with one scrape job per service.
6. Ships 8 Grafana dashboards: a top-level Kardamom overview plus one detailed dashboard per service.
7. Updates `docs/observability.md`.

## Non-goals

- Alertmanager / alert rules. (Separate cycle once SLOs land.)
- Tracing exporter (Jaeger, OTLP). Tracing setup stays as it is.
- Service discovery beyond static-targets. The scrape config is templated for N hosts with a comment, not wired to Consul / k8s.
- Pre-prod metric stability guarantees. Renames are deliberate and breaking.

## Architecture

### `crates/obs` — shared observability init

New workspace crate. Public surface:

```rust
// crates/obs/src/lib.rs

pub use metrics_exporter_prometheus; // re-export for binaries that need raw access
pub const DURATION_BUCKETS: &[f64];  // moved here from crates/node/src/metrics.rs

/// Install the Prometheus exporter for this service.
///
/// `service` is a stable, short identifier (e.g. `"sequencer"`, `"node"`) —
/// it's stamped on every metric as the `service` label.
///
/// `host_id` is a per-host identifier (e.g. `"local"`, `"seq-01"`) — it's
/// stamped on every metric as `host_id` so Prometheus can group by replica.
/// Empty strings are rejected.
pub fn init(service: &'static str, metrics_addr: SocketAddr, host_id: &str)
    -> anyhow::Result<()>;
```

`init()`:

1. Builds `PrometheusBuilder::new()`, binds `with_http_listener(metrics_addr)`, applies `DURATION_BUCKETS` via `set_buckets`, and `install()`s the recorder.
2. Sets recorder-level global labels: `host_id`, `service` (so every metric every binary emits is automatically labelled — emission sites stay clean).
3. Registers `kardamom_build_info{version,sha,host_id}` (gauge = 1) using `metrics::describe_gauge!` + `metrics::gauge!`.
4. Registers `kardamom_service_up` (gauge = 1).

`crates/obs` deliberately does **not** export a `clap::Args` struct. Each binary already owns its own CLI; it adds two flags (`--metrics-addr <SocketAddr>` with a service-specific default, `--host-id <String>` defaulting to `"local"`, env vars `KARDAMOM_METRICS_ADDR` / `KARDAMOM_HOST_ID`) and then calls `kardamom_obs::init("sequencer", args.metrics_addr, &args.host_id)?` early in `main`.

### Per-binary integration

| Binary | Crate | Default port | Status today | Action |
| --- | --- | --- | --- | --- |
| `kardamom` (node) | `crates/kardamom` | 9000 | Has its own `PrometheusBuilder::install()` in `main.rs:81` | Swap to `kardamom_obs::init`. Remove duplicated buckets constant. |
| `kardamom-sequencer` | `crates/sequencer` | 9001 | Emits, no exporter | Wire `obs::init`. No rename. |
| `kardamom-batcher` | `crates/batcher` | 9002 | Emits dotted names, no exporter | Wire `obs::init`. Rename 3 metrics. |
| `kardamom-sealer` | `crates/sealer` | 9003 | Emits prefixless names, no exporter | Wire `obs::init`. Rename 3 metrics; drop `host_id` *label* from emission (now a global). |
| `kardamom-executor` | `crates/executor` | 9004 | Silent | Wire `obs::init` + add baseline emission (below). |
| `kardamom-da-watcher` | `crates/da_watcher` | 9005 | Silent | Wire `obs::init` + add baseline emission (below). |
| `kardamom-ingress` | `crates/ingress` | 9006 | Silent | Wire `obs::init` + add baseline emission (below). |

### Baseline metrics for the silent services

Bounded scope — only what's already trivial to wire from existing log spans / observable signals. Heavier instrumentation is a follow-up.

**`kardamom-executor`** (`crates/executor`):

| Metric | Kind | Labels | Where emitted |
| --- | --- | --- | --- |
| `kardamom_executor_tx_applied_total` | counter | `outcome` (`ok` / `revert` / `error`) | after each tx applies in the reader's exec path |
| `kardamom_executor_block_apply_duration_seconds` | histogram | (none) | wrap the block-apply call site |
| `kardamom_executor_state_commit_duration_seconds` | histogram | (none) | wrap the state-DB commit call |
| `kardamom_executor_block_number` | gauge | (none) | set on every committed block |

**`kardamom-da-watcher`** (`crates/da_watcher`):

| Metric | Kind | Labels |
| --- | --- | --- |
| `kardamom_da_watcher_l1_head_block_number` | gauge | (none) |
| `kardamom_da_watcher_l1_finalized_block_number` | gauge | (none) |
| `kardamom_da_watcher_deposits_detected_total` | counter | (none) |
| `kardamom_da_watcher_tick_total` | counter | `outcome` (`ok` / `rpc_error` / `parse_error`) |

**`kardamom-ingress`** (`crates/ingress`):

| Metric | Kind | Labels |
| --- | --- | --- |
| `kardamom_ingress_tx_received_total` | counter | (none) |
| `kardamom_ingress_tx_accepted_total` | counter | (none) |
| `kardamom_ingress_tx_rejected_total` | counter | `reason` (one of the existing `TxError` enum variants, kebab-cased) |
| `kardamom_ingress_queue_depth` | gauge | (none) |

### Metric renames

| Before | After |
| --- | --- |
| `batcher.blocks_observed_total` | `kardamom_batcher_blocks_observed_total` |
| `batcher.batches_posted_total` | `kardamom_batcher_batches_posted_total` |
| `batcher.blobs_posted_total` | `kardamom_batcher_blobs_posted_total` |
| `sealer_boundaries_emitted_total` | `kardamom_sealer_boundaries_emitted_total` |
| `sealer_block_number` | `kardamom_sealer_block_number` |
| `sealer_tick_skipped_total` | `kardamom_sealer_tick_skipped_total` |

Sealer's per-emission `host_id` label is dropped (now a recorder-level global). The `reason` label on `tick_skipped_total` stays.

### Prometheus scrape config (`deploy/prometheus.yml`)

```yaml
global:
  scrape_interval: 1s
  evaluation_interval: 5s

# One scrape job per service. Each `static_configs.targets` is the list of
# hosts that run that service. To add a host, append `host-N:<port>` to the
# matching job — every metric is already labelled with `host_id` (set via
# `--host-id` on the binary), so Prometheus can group by host without
# relabel rules.
scrape_configs:
  - job_name: kardamom-node
    metrics_path: /metrics
    static_configs:
      - targets: ["host.docker.internal:9000"]
  - job_name: kardamom-sequencer
    metrics_path: /metrics
    static_configs:
      - targets: ["host.docker.internal:9001"]
  - job_name: kardamom-batcher
    metrics_path: /metrics
    static_configs:
      - targets: ["host.docker.internal:9002"]
  - job_name: kardamom-sealer
    metrics_path: /metrics
    static_configs:
      - targets: ["host.docker.internal:9003"]
  - job_name: kardamom-executor
    metrics_path: /metrics
    static_configs:
      - targets: ["host.docker.internal:9004"]
  - job_name: kardamom-da-watcher
    metrics_path: /metrics
    static_configs:
      - targets: ["host.docker.internal:9005"]
  - job_name: kardamom-ingress
    metrics_path: /metrics
    static_configs:
      - targets: ["host.docker.internal:9006"]
```

### Grafana dashboards (`deploy/grafana/provisioning/dashboards-json/`)

All dashboards target the existing Prometheus datasource provisioning (compose stack), use schema 38 (same as the existing `kardamom-rpc.json`), and define a `host` template variable bound to `label_values(kardamom_service_up, host_id)`.

**`kardamom-overview.json` (new top-level)** — golden signals across services. Panels:

1. *Services up* — stat panel: `sum by (service) (kardamom_service_up)`. Red if 0, green if ≥1.
2. *Build info* — table: `kardamom_build_info`.
3. *Request rate (per service)* — timeseries: derived rates by service, e.g. `sum by (service) (rate(kardamom_*_*_total[1m]))`. Coarse — for "is anything moving" not for diagnosis.
4. *Latency P99 (per service)* — heatmap-ish: top P99s by service.
5. *Block height* — timeseries with three series: `kardamom_block_number`, `kardamom_sealer_block_number`, `kardamom_executor_block_number`. Drift between them is the headline health indicator.
6. *L1 / L2 lag* — `kardamom_da_watcher_l1_head_block_number - kardamom_da_watcher_l1_finalized_block_number`.

**`kardamom-node.json`** — keep the existing `kardamom-rpc.json` content but rename the file + bump uid to `kardamom-node`, add the `host` template var, and update its panel queries to filter `{host_id=~"$host"}` everywhere.

**`kardamom-sequencer.json`** — panels:

1. Ingest rate by partition (`rate(kardamom_sequencer_tx_ingested_total[1m])`).
2. Publish-to-B rate by partition.
3. Drops + buffers (future + past tx rates) by partition — anomaly indicators.
4. Backpressure events by partition.
5. Nonce check P50 / P90 / P99 by partition (histogram_quantile).
6. Pending evictions rate by partition.
7. Standby replay lag — gauge.
8. Build info — table.

**`kardamom-batcher.json`** — panels:

1. Blocks observed rate.
2. Batches posted rate.
3. Blobs posted rate.
4. Throughput ratio: blobs / batches over time (mini-stat).
5. Build info — table.

**`kardamom-sealer.json`** — panels:

1. Boundaries emitted rate per host.
2. Block number per host — timeseries.
3. Tick-skipped rate by reason (stacked).
4. Build info — table.

**`kardamom-executor.json`** — panels:

1. Tx applied rate by outcome (stacked).
2. Block-apply P50 / P90 / P99 (histogram_quantile).
3. State-commit P50 / P90 / P99.
4. Block number — stat.
5. Build info — table.

**`kardamom-da-watcher.json`** — panels:

1. L1 head + finalized block number — timeseries.
2. L1 lag (head − finalized) — stat.
3. Deposits detected — rate timeseries.
4. Tick outcome rate — stacked by `outcome`.
5. Build info — table.

**`kardamom-ingress.json`** — panels:

1. Tx received / accepted / rejected rates — stacked.
2. Reject reasons — bar gauge by `reason`.
3. Queue depth — timeseries.
4. Build info — table.

### Compose stack

No new services. Existing `deploy/compose.yaml` continues to run Prometheus + Grafana. The dashboards-provisioning volume mount picks up the new JSON files automatically.

## Data flow

```
binary main()
    │
    ├── parse CLI (per-binary --metrics-addr + --host-id flags)
    ├── kardamom_obs::init("<service>", args.metrics_addr, &args.host_id)?
    │       └── PrometheusBuilder bound to args.metrics_addr (default 127.0.0.1:<port>)
    │           ├── recorder-level global labels: host_id, service
    │           ├── histograms use DURATION_BUCKETS
    │           └── gauges kardamom_build_info / kardamom_service_up = 1
    ├── runtime spawn …
    └── (service work) → emits metrics via metrics::*!() macros
                          ↓
                   Prometheus recorder
                          ↓
                   http://host:port/metrics  ← Prometheus scrapes per scrape_configs
                                                          ↓
                                                    Grafana dashboards
```

## Error handling

- **`metrics_addr` already in use** — `PrometheusBuilder::install()` returns an `Err`; binaries `?` it from `main` and exit non-zero with a clear message.
- **Empty `host_id`** — clap's default kicks in (`local`). If a user passes `--host-id ""` explicitly, `obs::init` returns an error.
- **`obs` crate compile/runtime regressions** — caught by the binary-level integration tests below (every binary's smoke test exercises `init`).

## Testing

Three layers, none of which require Docker or external infra:

1. **`crates/obs` unit tests** — `init` succeeds on a free port; `init` fails on a port already bound; build_info gauge is present in the scrape output; histogram buckets match `DURATION_BUCKETS`.
2. **Per-binary `/metrics` integration test** — for each of the 7 binaries, a `tests/metrics.rs` test that:
   - Spawns the binary's `main`-equivalent (or a thin re-implementation in test code that reuses `kardamom_obs::init`) on an ephemeral `127.0.0.1:0` port.
   - HTTP-GETs `/metrics` and asserts the expected metric names appear and `kardamom_service_up` is `1`.
   - Mirrors the shape of the existing `crates/node/tests/metrics.rs`.
3. **Dashboard JSON validity** — a `crates/obs/tests/dashboards.rs` test that parses every JSON file in `deploy/grafana/provisioning/dashboards-json/` and asserts: schema version is 38, every panel has a non-empty title, every PromQL expression references at least one `kardamom_*` metric (string contains check — guards against typos at dashboard authoring time).

Existing tests stay green. The metric renames mean the `crates/batcher` and `crates/sealer` tests that string-match metric names need updating — those updates land in the same commit as the renames.

## Documentation

Update `docs/observability.md` to:

- Replace the single-service section with a table of services + their default ports + dashboard UIDs.
- Document `--metrics-addr` and `--host-id` flags + their env-var equivalents.
- Add a "Scaling to multiple hosts" subsection with a copy-pastable `prometheus.yml` snippet showing how to add a new host to all 7 scrape jobs.
- Note the rename: link to this spec for old → new metric name mapping.

## Rollout / risk

- **Breaking renames.** `batcher.*` and `sealer_*` → `kardamom_batcher_*` / `kardamom_sealer_*`. Anyone running queries or alerts against the old names will see them go to zero. Mitigation: pre-prod, no alerts exist yet. The PR description and observability.md call this out explicitly.
- **Port collisions on dev machines.** Defaults are `127.0.0.1` so a dev running multiple service replicas locally must override `--metrics-addr`. Documented in observability.md.
- **Recorder-level global labels** affect *every* metric the binary records, including third-party crates' metrics (e.g. tokio runtime, hyper). That's intentional — easier to filter by service in Grafana — but slightly increases cardinality of any vendored libraries that emit.

## Out of scope (explicit non-goals, restated)

- Alert rules.
- Tracing exporters.
- Real service discovery (Consul / k8s).
- Heavy instrumentation of executor / da-watcher / ingress beyond the baseline.
- OpenTelemetry semantic conventions; we stay on a custom `kardamom_*` namespace.
