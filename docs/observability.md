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

# 2. Start kardamom with a TOML genesis. `chains/dev.toml` is the
#    checked-in dev chain (chain_id 412346; prefunds Anvil account #0
#    with 1000 ETH). For the bench's transfers/mixed workloads you'll
#    want a genesis that also prefunds the derived signer addresses
#    and deploys the `eth_call` target contract — see "Pre-funding the
#    bench signers" below.
cargo run --release --bin kardamom -- --chain chains/dev.toml

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
  cargo run --release --bin kardamom -- --chain chains/dev.toml

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
(`keccak(seed || u64_le(i))`). For transfers/mixed workloads to actually
exercise the write path, each derived signer needs balance in the chain
state. That means writing a custom genesis TOML whose `[[alloc]]` entries
list every derived address. Today this is manual — write a small Rust
program that calls `kardamom_bench::signers::derive(seed, concurrency)`
to enumerate the addresses, then build a `chains/<bench>.toml` with one
`[[alloc]]` per signer plus an entry for the `eth_call` target contract:

```toml
chain_id = 412346

[[alloc]]
address = "0x..."   # derived signer 0
balance = "10000000000000000000"

# ... one per signer ...

[[alloc]]
address = "0x0000000000000000000000000000000000001234"
code    = "0x604260005260206000f3"
```

Then start kardamom against that file:

```sh
cargo run --release --bin kardamom -- --chain chains/your-bench.toml
```

A quick hack to see the dashboard panels move without writing a custom
genesis: run the calls-only scenario (`scenarios/calls.toml`) against the
dev chain plus a one-line edit to deploy the call target. Failed
transfers in the mixed/transfers scenarios show up as `outcome="err"` in
`kardamom_rpc_requests_total`.

A library helper that turns a bench config into a Genesis (deriving
signers from a mnemonic instead of a u64 seed) is on the way — see PR
#5.

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
