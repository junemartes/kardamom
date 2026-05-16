# Observability

Kardamom exposes the cost of every RPC call from a single instrumentation
point that feeds both Prometheus metrics and `tracing-flame` flamegraphs. The
local stack — Prometheus + Grafana — is wired up with `docker compose`, and a
`kardamom-bench` binary drives load so the dashboard panels move.

## What is instrumented

Per-stage spans + histograms (`kardamom_rpc_stage_duration_seconds`):

- `eth_sendRawTransaction`: `decode`, `recover_signer`, `acquire_write_lock`,
  `build_tx_env`, `execute`, `build_receipt`, `store_receipt`.
- `eth_call`: `acquire_read_lock`, `build_tx_env`, `execute`.

Handler-level (`kardamom_rpc_duration_seconds`,
`kardamom_rpc_requests_total`): every RPC method (`chainId`, `blockNumber`,
`getBalance`, `getTransactionCount`, `call`, `sendRawTransaction`,
`getTransactionReceipt`). Labels: `method`, `outcome` (`ok` or `err`).

Plus `kardamom_block_number` (gauge) and `kardamom_build_info` (gauge, always
1, labeled with `version` and `git_sha`).

## Quick start

In three terminals:

```sh
# 1. Start the observability stack.
cd deploy && docker compose up

# 2. Start kardamom. Pre-fund the bench signers and install the
#    `eth_call`-workload target contract.
cargo run --release --bin kardamom -- \
  --prefund 0xC8B1F2C2C45A8FF93E94B7C2FB91D75D8B8B0D5C=10000000000000000000 \
  --insert-code 0x0000000000000000000000000000000000001234=0x604260005260206000f3

# 3. Drive load.
cargo run --release --bin kardamom-bench -- \
  --config scenarios/mixed.toml
```

Open <http://localhost:3000> (Grafana, `admin` / `kardamom`) → the
provisioned **Kardamom RPC** dashboard populates within a few seconds.

The bench prints percentile latencies to stdout and (with `output =` set)
writes a JSON report.

## Flamegraph workflow

`tracing-flame` only sees the spans that we instrument, so revm internals show
up as one fat `execute` block. (For CPU sampling, use `cargo flamegraph` or
`perf` separately — out of scope for v1.)

```sh
# Run kardamom with the flame layer enabled.
KARDAMOM_FLAME=./flame.folded \
  cargo run --release --bin kardamom -- \
    --prefund 0xC8B1F2C2C45A8FF93E94B7C2FB91D75D8B8B0D5C=10000000000000000000

# Drive some load against it, then Ctrl-C the node.
cargo run --release --bin kardamom-bench -- --config scenarios/mixed.toml

# Render the folded file with inferno.
cargo install inferno          # one-time
inferno-flamegraph < flame.folded > flame.svg
open flame.svg                  # or: xdg-open
```

The `FlushGuard` returned by `FlameLayer::with_file` is held in `main` until
shutdown completes, so the folded file is not truncated.

## Bench scenarios

Starter scenarios live in `scenarios/`:

| File | Workload | Notes |
|------|----------|-------|
| `transfers.toml` | `eth_sendRawTransaction` only | Stresses the write path. The single `RwLock` serializes writes; expect `acquire_write_lock` to dominate at higher `concurrency`. |
| `calls.toml`     | `eth_call` only               | Read-only workload against the `PUSH1 0x42 … RETURN` contract. |
| `mixed.toml`     | 1:4 transfers:calls           | Default. Mirrors a typical RPC traffic shape. |

CLI flags override individual fields in the config file:

```sh
cargo run --release --bin kardamom-bench -- \
  --config scenarios/mixed.toml \
  --rate 1000 --duration 1m --concurrency 32
```

### Pre-funding the bench signers

The bench derives `--concurrency` signers deterministically from `--seed`
(`keccak(seed || u64_le(i))`). To run transfers, pass one `--prefund` to
kardamom per derived signer:

```sh
cargo run --bin kardamom-bench -- --config scenarios/mixed.toml --print-signers
# (planned helper, not yet implemented; for now derive addresses manually)
```

A quick hack: start kardamom with a generously prefunded "wildcard" address
that none of the signers will use, then run the bench. Failed transfers show
up as `outcome="err"` in `kardamom_rpc_requests_total`. To actually exercise
the write path, ensure each derived signer is in `--prefund`.

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
