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

`tracing-flame` records a sample any time a span is entered. It does **not**
sample on-CPU time the way `perf` or `samply` do — anything outside an
instrumented span (jsonrpsee dispatch, hyper framing, tokio scheduling, revm
internals) is collapsed onto the bare `ThreadId(N)-tokio-rt-worker` root
frame. For a deep view past the spans, see "CPU profiling with samply" below.

There are two flame-recording paths, for different jobs.

### Option A — long-running node, `KARDAMOM_FLAME`

For Grafana exploration where the node stays up for minutes and you Ctrl-C it
when you've seen enough:

```sh
KARDAMOM_FLAME=./flame.folded \
  cargo run --release --bin kardamom -- --chain chains/dev.toml

# Drive load (or hit the node by hand)…
cargo run --release --bin kardamom-bench -- --config scenarios/mixed.toml
# Ctrl-C the node when done.

cargo install inferno          # one-time
grep ';' flame.folded | inferno-flamegraph > flame.svg
open flame.svg
```

The `grep ';'` filter drops bare-root samples (`ThreadId(N)-tokio-rt-worker`
with nothing after) so the spans aren't sub-pixel. The unfiltered SVG is
~99% root frame because the node spends most of its lifetime not inside any
span (idle workers, RPC framing, async-machinery, etc.).

The `FlushGuard` returned by `FlameLayer::with_file` is held in `main` until
shutdown completes, so the folded file is not truncated.

### Option B — single-process harness, `kardamom-bench-harness`

For a tight flamegraph scoped exactly to a measurement window, use the
embedded harness. It boots the node in-process, drives load with the bench
generator, and gates `tracing-flame` recording to the measurement phase only
(warmup is excluded via an `AtomicBool`-driven `FilterFn`). No idle node
lifetime, no warmup, no teardown in the data.

```sh
cargo build --release --bin kardamom-bench-harness

./target/release/kardamom-bench-harness \
  --config bench.toml \
  --workload transfers --rate 2000 --duration 10s \
  --warmup 2s --concurrency 32 \
  --flame-out /tmp/flame.folded

grep ';' /tmp/flame.folded | inferno-flamegraph > /tmp/flame.svg
open /tmp/flame.svg
```

The bench config's `[mnemonic]` section prefunds the derived signers, so
`transfers` and `mixed` workloads just work end-to-end. For `mixed`/`calls`,
pass `--calls-contract <address>` and add the contract to `[[contracts]]` in
the config so it lands in the in-process genesis alloc.

## CPU profiling with samply

`tracing-flame` only sees our spans. To see what the *CPU* actually does
(hyper, jsonrpsee, serde_json, revm internals, ECDSA), record the harness
under `samply` — a sampling profiler that needs no kernel privileges on
macOS.

```sh
cargo install --locked samply       # one-time
cargo build --release --bin kardamom-bench-harness
dsymutil ./target/release/kardamom-bench-harness   # generate .dSYM for symbols

samply record --save-only -o /tmp/profile.json.gz -- \
  ./target/release/kardamom-bench-harness \
    --workload transfers --rate 10000 --duration 10s \
    --warmup 1s --concurrency 128 \
    --flame-out /tmp/flame.folded

# View interactively in the Firefox Profiler UI:
samply load /tmp/profile.json.gz
```

Run `samply load` from the repo root so it can find the `.dSYM` bundle next
to the binary. If our own frames still show as hex offsets in the UI, the
dSYM isn't being picked up — check `./target/release/kardamom-bench-harness.dSYM`
exists. System dylibs (`libsystem_kernel.dylib`, etc.) will remain
unsymbolicated; their syscall stubs (`__psynch_cvwait`, `mach_msg_trap`)
are what tokio workers park in when there's no work to do.

Quick post-hoc symbolication (if you don't want to open the UI):

```sh
# Look up a hex offset from the profile against the dSYM. PIE base is 0x100000000.
atos -o ./target/release/kardamom-bench-harness.dSYM/Contents/Resources/DWARF/kardamom-bench-harness \
     -arch arm64 -l 0x100000000 0x1000f6c34
# → k256::arithmetic::field::FieldElement::square (field.rs:160)
```

## Profiling experiment: which ECDSA backend?

The harness + samply combo makes a concrete cost-vs-benefit experiment
reproducible in ~5 minutes. The example below identifies signature
recovery as the dominant on-CPU cost and confirms a fix by re-measuring.

1. **Baseline.** Build and record:

   ```sh
   cargo build --release --bin kardamom-bench-harness
   dsymutil ./target/release/kardamom-bench-harness

   samply record --save-only -o /tmp/profile-baseline.json.gz -- \
     ./target/release/kardamom-bench-harness \
       --workload transfers --rate 10000 --duration 10s \
       --warmup 1s --concurrency 128 \
       --flame-out /tmp/flame-baseline.folded
   ```

   Note the `ok=<n>` count printed at the end and load
   `/tmp/profile-baseline.json.gz` in samply. The top non-park leaf frames
   on the worker threads will be a mix of `k256::*` functions
   (`FieldElement::square`, `Scalar::mul`, `WideScalar::reduce_impl`,
   `Scalar::invert_vartime`, etc.) — that's the pure-Rust secp256k1
   implementation doing signature recovery.

2. **Swap the ECDSA backend.** In `Cargo.toml` (workspace root), change the
   `alloy-consensus` features:

   ```diff
   - alloy-consensus = { version = "2.0", features = ["k256"] }
   + alloy-consensus = { version = "2.0", features = ["secp256k1"] }
   ```

   This swaps the pure-Rust `k256` crate for the libsecp256k1 C library
   bindings, used for both signing (bench-side) and recovery (node-side).
   No code changes needed; `recover_signer` dispatches through
   alloy-consensus.

3. **Verify and remeasure.**

   ```sh
   cargo test -p kardamom-node -p kardamom-bench       # everything still passes
   cargo build --release --bin kardamom-bench-harness
   dsymutil ./target/release/kardamom-bench-harness    # symbols changed; regenerate

   samply record --save-only -o /tmp/profile-secp.json.gz -- \
     ./target/release/kardamom-bench-harness \
       --workload transfers --rate 10000 --duration 10s \
       --warmup 1s --concurrency 128 \
       --flame-out /tmp/flame-secp.folded
   ```

   Compare `ok=<n>` to the baseline. Worker park-share (computed from the
   profile JSON, see `scripts/` or the snippet in step 4) should rise
   noticeably as each request finishes faster. The remaining top non-park
   frames should be `rustsecp256k1_v0_*_*` C-library symbols.

4. **Quick stats from the JSON** (no UI needed):

   ```sh
   gunzip -c /tmp/profile-secp.json.gz | python3 -c "
   import json, sys
   from collections import Counter
   p = json.load(sys.stdin)
   libs = [l['name'] for l in p['libs']]
   park, total = 0, 0
   for t in p['threads']:
       if 'tokio-rt-worker' not in t.get('name',''): continue
       fr=t['frameTable']; fn=t['funcTable']; st=t['stackTable']; res=t['resourceTable']
       for s_idx in t['samples']['stack']:
           if s_idx is None: continue
           total += 1
           lib_idx = res['lib'][fn['resource'][fr['func'][st['frame'][s_idx]]]]
           if 'libsystem' in libs[lib_idx]: park += 1
   print(f'workers: {total} samples, {park} parked = {100*park/total:.1f}%')"
   ```

   For the harness at 10 000 rps × 128 concurrency on an 8-core M-series
   machine, the swap typically delivers ~2× more successful tx/s and a
   ~10-percentage-point increase in worker park-share. Per-request latency
   is queue-bound at this load, so it does *not* change much; to see
   per-request latency drop, also run at a sustainable rps where the
   in-flight semaphore (`concurrency × 4`) doesn't fill.

The same recipe applies for any other backend-swap or hot-path change:
record before, change one thing, record after, compare `ok` count and the
top non-park frames.

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
