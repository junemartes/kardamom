# Prometheus + Grafana Wiring — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire every kardamom service binary to export Prometheus metrics, add per-service Grafana dashboards, and add a top-level overview dashboard.

**Architecture:** New `crates/obs` crate centralises the Prometheus exporter install + shared global labels (`service`, `host_id`). Every service binary calls `kardamom_obs::init(...)` at the top of `main`. Metric names normalise to `kardamom_<service>_<subsystem>_<name>_<unit>`. Dashboards live under `deploy/grafana/provisioning/dashboards-json/`.

**Tech Stack:** `metrics = "0.24"` (already in workspace deps), `metrics-exporter-prometheus = "0.18"` (already in workspace deps), `clap` (already in workspace deps), Grafana schemaVersion 38 (matches existing `kardamom-rpc.json`).

**Spec:** `docs/specs/2026-05-29-prometheus-grafana-design.md`.

---

## Task 1: Scaffold `crates/obs`

**Files:**
- Create: `crates/obs/Cargo.toml`
- Create: `crates/obs/src/lib.rs`
- Modify: `Cargo.toml` (add `crates/obs` to workspace members glob — already covered by `crates/*`, verify only)

- [ ] **Step 1: Create the crate manifest**

Write `crates/obs/Cargo.toml`:

```toml
[package]
name = "kardamom-obs"
version = { workspace = true }
edition = { workspace = true }
license = "MIT"
publish = false

[lib]
name = "kardamom_obs"
path = "src/lib.rs"

[dependencies]
metrics = { workspace = true }
metrics-exporter-prometheus = { workspace = true }
anyhow = { workspace = true }
tracing = { workspace = true }

[dev-dependencies]
reqwest = { workspace = true }
tokio = { workspace = true, features = ["macros", "rt-multi-thread"] }
```

- [ ] **Step 2: Create a placeholder lib.rs**

Write `crates/obs/src/lib.rs`:

```rust
//! Shared Prometheus exporter init for every kardamom service binary.
//!
//! See `docs/specs/2026-05-29-prometheus-grafana-design.md`.

use std::net::SocketAddr;

use anyhow::{Context, Result, anyhow};
use metrics_exporter_prometheus::PrometheusBuilder;

/// Shared histogram buckets (seconds). 100 µs → 5 s, exponential.
/// Promoted out of `crates/node/src/metrics.rs` so every service histogram
/// uses the same boundaries.
pub const DURATION_BUCKETS: &[f64] = &[
    0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
];

/// Build-info gauge name. Stays `kardamom_build_info` for compatibility with
/// the existing RPC dashboard.
pub const BUILD_INFO: &str = "kardamom_build_info";

/// Heartbeat gauge: set to 1 once init succeeds. Used by the overview
/// dashboard's "services up" panel.
pub const SERVICE_UP: &str = "kardamom_service_up";

/// Install the Prometheus exporter for this service.
pub fn init(
    service: &'static str,
    metrics_addr: SocketAddr,
    host_id: &str,
    version: &'static str,
    git_sha: &'static str,
) -> Result<()> {
    if host_id.is_empty() {
        return Err(anyhow!("host_id must be non-empty"));
    }

    PrometheusBuilder::new()
        .with_http_listener(metrics_addr)
        .set_buckets(DURATION_BUCKETS)
        .context("set_buckets")?
        .add_global_label("service", service)
        .add_global_label("host_id", host_id)
        .install()
        .context("PrometheusBuilder::install")?;

    metrics::describe_gauge!(BUILD_INFO, "Build info; value is always 1.");
    metrics::gauge!(
        BUILD_INFO,
        "version" => version,
        "sha" => git_sha,
    )
    .set(1.0);

    metrics::describe_gauge!(SERVICE_UP, "1 while the service's exporter is live.");
    metrics::gauge!(SERVICE_UP).set(1.0);

    tracing::info!(
        service = service,
        host_id = host_id,
        addr = %metrics_addr,
        "kardamom_obs: prometheus exporter installed"
    );
    Ok(())
}
```

- [ ] **Step 3: Verify workspace picks up the new crate**

Run: `cargo metadata --no-deps --format-version 1 | grep -o 'kardamom-obs' | head -1`
Expected output: `kardamom-obs`

If absent, check `Cargo.toml` at the root — `members = ["crates/*"]` should already match.

- [ ] **Step 4: Build the crate**

Run: `cargo build -p kardamom-obs --locked`
Expected: success.

- [ ] **Step 5: Commit**

```bash
git add crates/obs/Cargo.toml crates/obs/src/lib.rs Cargo.lock
git commit -m "feat(obs): scaffold shared Prometheus exporter init crate"
```

---

## Task 2: Test `kardamom_obs::init`

**Files:**
- Create: `crates/obs/tests/init.rs`

- [ ] **Step 1: Write failing test**

Write `crates/obs/tests/init.rs`:

```rust
//! End-to-end smoke test for `kardamom_obs::init`: spin up the exporter on an
//! ephemeral port, scrape `/metrics`, and assert the heartbeat + build_info
//! show up with the correct global labels.

use std::net::{SocketAddr, TcpListener};

fn free_port() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = l.local_addr().expect("local_addr");
    // Dropping the listener releases the port before the exporter binds it.
    drop(l);
    addr
}

#[tokio::test]
async fn init_exposes_build_info_and_service_up() {
    let addr = free_port();
    kardamom_obs::init("test-service", addr, "test-host", "0.0.0", "deadbeef")
        .expect("init succeeds on a free port");

    // The exporter binds asynchronously — give it a short retry budget.
    let url = format!("http://{}/metrics", addr);
    let body = scrape_with_retry(&url).await;

    assert!(
        body.contains("kardamom_service_up{"),
        "expected kardamom_service_up in:\n{body}"
    );
    assert!(
        body.contains("service=\"test-service\""),
        "expected service label in:\n{body}"
    );
    assert!(
        body.contains("host_id=\"test-host\""),
        "expected host_id label in:\n{body}"
    );
    assert!(
        body.contains("kardamom_build_info"),
        "expected kardamom_build_info in:\n{body}"
    );
    assert!(
        body.contains("version=\"0.0.0\""),
        "expected version label in:\n{body}"
    );
}

#[tokio::test]
async fn init_rejects_empty_host_id() {
    let addr = free_port();
    let err = kardamom_obs::init("svc", addr, "", "0.0.0", "sha").expect_err("must fail");
    assert!(err.to_string().contains("host_id"));
}

async fn scrape_with_retry(url: &str) -> String {
    for _ in 0..40 {
        match reqwest::get(url).await {
            Ok(r) if r.status().is_success() => return r.text().await.expect("text"),
            _ => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
        }
    }
    panic!("exporter did not become ready at {url}");
}
```

- [ ] **Step 2: Run test (expects failure first — no reqwest in workspace yet)**

Run: `cargo test -p kardamom-obs --locked --test init 2>&1 | tail -10`

If it fails with "unresolved import reqwest", confirm the workspace deps already include reqwest (`Cargo.toml` line "reqwest = { version = …}") — they do. Re-check `crates/obs/Cargo.toml` dev-deps.

- [ ] **Step 3: Run test for real**

Run: `cargo test -p kardamom-obs --locked --test init -- --test-threads=1`
Expected: PASS — both tests succeed. (Use `--test-threads=1` because both tests install a global recorder; a second install on the same process is a no-op or fails.)

If the second test fails because the recorder is already installed from the first, that's expected — each test must run in its own process. Add `// each test must run in a fresh process; integration tests already get one process per file.` comment above the `mod` line OR split into two separate test files.

Actually metrics 0.24 allows only **one** recorder per process. Since both tests are in the same integration test binary, the second `init` call will fail. **Refactor:** put the second test in `crates/obs/tests/init_rejects_empty.rs`:

```rust
use std::net::SocketAddr;

#[test]
fn init_rejects_empty_host_id() {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let err = kardamom_obs::init("svc", addr, "", "0.0.0", "sha").expect_err("must fail");
    assert!(err.to_string().contains("host_id"));
}
```

And remove that test from `crates/obs/tests/init.rs`. Re-run:

Run: `cargo test -p kardamom-obs --locked`
Expected: 2 test binaries pass, 1 test each.

- [ ] **Step 4: Commit**

```bash
git add crates/obs/tests/
git commit -m "test(obs): exporter scrape + empty host_id rejection"
```

---

## Task 3: Refactor `kardamom-node` (binary `kardamom`) to use `kardamom_obs::init`

**Files:**
- Modify: `crates/kardamom/Cargo.toml` (add `kardamom-obs` dep)
- Modify: `crates/kardamom/src/main.rs` (lines around 39-40 for CLI flags; lines 81-84 for the Prometheus install)
- Modify: `crates/node/src/metrics.rs` (drop `DURATION_BUCKETS` const since it now lives in `kardamom-obs`)

- [ ] **Step 1: Add the dep**

Add to `crates/kardamom/Cargo.toml` `[dependencies]`:

```toml
kardamom-obs = { path = "../obs" }
```

- [ ] **Step 2: Wire `obs::init` in main.rs**

Read `crates/kardamom/src/main.rs` lines 30-90. Replace the existing `PrometheusBuilder` install block (around lines 81-84) with:

```rust
let git_sha = option_env!("KARDAMOM_GIT_SHA").unwrap_or("unknown");
kardamom_obs::init(
    "node",
    args.metrics_addr,
    &args.host_id,
    env!("CARGO_PKG_VERSION"),
    git_sha,
)?;
```

Add `host_id` to the CLI struct (next to `metrics_addr`):

```rust
/// Host identifier; stamped on every metric.
#[arg(long, env = "KARDAMOM_HOST_ID", default_value = "local")]
host_id: String,
```

- [ ] **Step 3: Drop the duplicate `DURATION_BUCKETS` from `crates/node/src/metrics.rs`**

Remove lines 22-25 (the const definition + its doc comment lines 21-22). In `crates/kardamom/src/main.rs` and anywhere else `kardamom_node::metrics::DURATION_BUCKETS` is referenced, switch to `kardamom_obs::DURATION_BUCKETS`. Update those imports.

Grep for references first:

Run: `rg 'DURATION_BUCKETS' crates/ --type rust -n`
For each result outside `crates/obs`, switch to `kardamom_obs::DURATION_BUCKETS`.

- [ ] **Step 4: Build + run existing node tests**

Run: `cargo build -p kardamom --locked`
Expected: success.

Run: `cargo test -p kardamom-node --locked`
Expected: all green (the existing `crates/node/tests/metrics.rs` still installs its own recorder — that test stays as-is).

- [ ] **Step 5: Commit**

```bash
git add crates/kardamom crates/node Cargo.lock
git commit -m "refactor(kardamom): use kardamom-obs for prometheus exporter init"
```

---

## Task 4: Wire `obs::init` into `kardamom-sequencer`

**Files:**
- Modify: `crates/sequencer/Cargo.toml` (add `kardamom-obs` dep)
- Modify: `crates/sequencer/src/bin/kardamom-sequencer.rs` (CLI struct + `main`)

- [ ] **Step 1: Add the dep**

Append to `crates/sequencer/Cargo.toml` `[dependencies]`:

```toml
kardamom-obs = { path = "../obs" }
```

- [ ] **Step 2: Add CLI flags + init call**

Read the existing CLI struct in `crates/sequencer/src/bin/kardamom-sequencer.rs` (it uses clap derive). Add to it:

```rust
/// Address for the Prometheus /metrics HTTP listener.
#[arg(long, env = "KARDAMOM_METRICS_ADDR", default_value = "127.0.0.1:9001")]
metrics_addr: std::net::SocketAddr,

/// Host identifier; stamped on every metric.
#[arg(long, env = "KARDAMOM_HOST_ID", default_value = "local")]
host_id: String,
```

In `main` (or `tokio::main` entry), right after CLI parse and tracing init, add:

```rust
kardamom_obs::init(
    "sequencer",
    args.metrics_addr,
    &args.host_id,
    env!("CARGO_PKG_VERSION"),
    option_env!("KARDAMOM_GIT_SHA").unwrap_or("unknown"),
)?;
```

- [ ] **Step 3: Build**

Run: `cargo build -p kardamom-sequencer --locked`
Expected: success.

- [ ] **Step 4: Add per-binary `/metrics` smoke test**

Create `crates/sequencer/tests/metrics_endpoint.rs`:

```rust
//! Smoke test: calling `kardamom_obs::init("sequencer", ...)` exposes
//! `/metrics` with the expected counters.

use std::net::{SocketAddr, TcpListener};

#[tokio::test]
async fn sequencer_metrics_endpoint_serves_expected_counters() {
    let addr = free_port();
    kardamom_obs::init("sequencer", addr, "local", "test", "test")
        .expect("init");

    // Touch every counter the sequencer crate is expected to publish so
    // describe_counter calls don't require us to also drive the binary.
    metrics::counter!("kardamom_sequencer_tx_ingested_total", "partition" => "0")
        .increment(0);

    let body = scrape(&format!("http://{addr}/metrics")).await;
    assert!(
        body.contains("kardamom_sequencer_tx_ingested_total"),
        "missing sequencer counter; got:\n{body}"
    );
    assert!(body.contains("service=\"sequencer\""), "missing service label");
}

fn free_port() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}

async fn scrape(url: &str) -> String {
    for _ in 0..40 {
        if let Ok(r) = reqwest::get(url).await
            && r.status().is_success()
        {
            return r.text().await.unwrap();
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("exporter not ready at {url}");
}
```

Add to `crates/sequencer/Cargo.toml` `[dev-dependencies]`:

```toml
reqwest = { workspace = true }
tokio = { workspace = true, features = ["macros", "rt-multi-thread"] }
```

- [ ] **Step 5: Run the test**

Run: `cargo test -p kardamom-sequencer --locked --test metrics_endpoint`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/sequencer Cargo.lock
git commit -m "feat(sequencer): wire kardamom-obs prometheus exporter on :9001"
```

---

## Task 5: Wire `obs::init` into `kardamom-batcher` and rename metrics

**Files:**
- Modify: `crates/batcher/Cargo.toml`
- Modify: `crates/batcher/src/bin/kardamom-batcher.rs`
- Modify: `crates/batcher/src/batcher.rs` (lines 14-28: metric names)
- Create: `crates/batcher/tests/metrics_endpoint.rs`

- [ ] **Step 1: Rename batcher metrics**

In `crates/batcher/src/batcher.rs`:

| Old name | New name |
| --- | --- |
| `batcher.blocks_observed_total` | `kardamom_batcher_blocks_observed_total` |
| `batcher.batches_posted_total` | `kardamom_batcher_batches_posted_total` |
| `batcher.blobs_posted_total` | `kardamom_batcher_blobs_posted_total` |

Use search-and-replace across `crates/batcher/`. Also grep tests:

Run: `rg 'batcher\.(blocks|batches|blobs)' crates/batcher/ -n`
Replace every match with the dotted-form's `kardamom_batcher_*` equivalent.

- [ ] **Step 2: Add dep and wire init (same pattern as Task 4)**

Same diff structure as the sequencer task, with `"batcher"` and default port `127.0.0.1:9002`.

- [ ] **Step 3: Build**

Run: `cargo build -p kardamom-batcher --locked`
Expected: success.

- [ ] **Step 4: Run batcher tests**

Run: `cargo test -p kardamom-batcher --locked`
Expected: green. If any test asserts the old metric name, update it.

- [ ] **Step 5: Add the `metrics_endpoint.rs` smoke test**

Mirror Task 4 Step 4 with `"batcher"` and the three renamed counter names. Add dev-deps to `Cargo.toml`.

- [ ] **Step 6: Run smoke test**

Run: `cargo test -p kardamom-batcher --locked --test metrics_endpoint`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/batcher Cargo.lock
git commit -m "feat(batcher): wire kardamom-obs on :9002; normalise metric names to kardamom_batcher_*"
```

---

## Task 6: Wire `obs::init` into `kardamom-sealer`, rename metrics, drop `host_id` label

**Files:**
- Modify: `crates/sealer/Cargo.toml`
- Modify: `crates/sealer/src/bin/kardamom-sealer.rs`
- Modify: `crates/sealer/src/emitter.rs` (lines 112-130: metric names + label)
- Create: `crates/sealer/tests/metrics_endpoint.rs`

- [ ] **Step 1: Rename sealer metrics and drop per-emission `host_id`**

In `crates/sealer/src/emitter.rs` lines 112-130:

| Old name | New name |
| --- | --- |
| `sealer_boundaries_emitted_total` | `kardamom_sealer_boundaries_emitted_total` |
| `sealer_block_number` | `kardamom_sealer_block_number` |
| `sealer_tick_skipped_total` | `kardamom_sealer_tick_skipped_total` |

Also remove the `"host_id" => self.host_id.as_str()` (or similar) label from every emission site. The host identifier becomes a recorder-level global via `obs::init`, so each emission just drops that label.

Grep first:

Run: `rg '(sealer_boundaries|sealer_block_number|sealer_tick_skipped)' crates/sealer/ -n`
Replace each metric name. Then:

Run: `rg '"host_id"\s*=>' crates/sealer/ -n`
Drop that label argument from every site that's emitting a sealer metric. Keep the `reason` label on `tick_skipped_total`.

- [ ] **Step 2: Add dep + wire init**

Same pattern as Task 4, with `"sealer"` and default port `127.0.0.1:9003`.

- [ ] **Step 3: Build**

Run: `cargo build -p kardamom-sealer --locked`
Expected: success.

- [ ] **Step 4: Run sealer tests**

Run: `cargo test -p kardamom-sealer --locked`
Expected: green. Update any test assertions that referenced the old names or the dropped `host_id` label.

- [ ] **Step 5: Add metrics_endpoint smoke test**

Mirror Task 4 Step 4 with `"sealer"` and the renamed counter `kardamom_sealer_boundaries_emitted_total`.

- [ ] **Step 6: Run smoke test**

Run: `cargo test -p kardamom-sealer --locked --test metrics_endpoint`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/sealer Cargo.lock
git commit -m "feat(sealer): wire kardamom-obs on :9003; normalise metric names; promote host_id to global label"
```

---

## Task 7: Add baseline metrics + `obs::init` to `kardamom-executor`

**Files:**
- Modify: `crates/executor/Cargo.toml`
- Modify: `crates/executor/src/bin/kardamom-executor.rs`
- Modify: `crates/executor/src/reader.rs` (likely site of tx-apply hot path)
- Create: `crates/executor/src/metrics.rs`
- Create: `crates/executor/tests/metrics_endpoint.rs`

- [ ] **Step 1: Create `crates/executor/src/metrics.rs`**

```rust
//! Metric name constants for the executor. Emission sites live in
//! `reader.rs`.

pub const TX_APPLIED_TOTAL: &str = "kardamom_executor_tx_applied_total";
pub const BLOCK_APPLY_DURATION_SECONDS: &str = "kardamom_executor_block_apply_duration_seconds";
pub const STATE_COMMIT_DURATION_SECONDS: &str = "kardamom_executor_state_commit_duration_seconds";
pub const BLOCK_NUMBER: &str = "kardamom_executor_block_number";

pub fn describe() {
    metrics::describe_counter!(TX_APPLIED_TOTAL, "tx executions, labelled by outcome");
    metrics::describe_histogram!(
        BLOCK_APPLY_DURATION_SECONDS,
        "wall time spent applying a block's tx batch"
    );
    metrics::describe_histogram!(
        STATE_COMMIT_DURATION_SECONDS,
        "wall time spent committing state to the backing DB"
    );
    metrics::describe_gauge!(BLOCK_NUMBER, "most recently committed block number");
}
```

Add `pub mod metrics;` to `crates/executor/src/lib.rs`.

- [ ] **Step 2: Emit at the obvious sites**

Read `crates/executor/src/reader.rs` and locate (a) the per-tx apply call and (b) the state commit boundary. Wrap them like:

```rust
// at tx-apply:
let outcome = match exec_result { Ok(_) => "ok", Err(_) => "error" };
metrics::counter!(crate::metrics::TX_APPLIED_TOTAL, "outcome" => outcome).increment(1);

// at block-apply (around the whole batch):
let start = std::time::Instant::now();
// ... existing block-apply work ...
metrics::histogram!(crate::metrics::BLOCK_APPLY_DURATION_SECONDS).record(start.elapsed().as_secs_f64());

// at state commit:
let start = std::time::Instant::now();
// ... existing commit ...
metrics::histogram!(crate::metrics::STATE_COMMIT_DURATION_SECONDS).record(start.elapsed().as_secs_f64());

// at block-applied:
metrics::gauge!(crate::metrics::BLOCK_NUMBER).set(block_number as f64);
```

The exact line numbers depend on the current reader.rs shape — locate the existing block-apply and commit call sites and instrument around them.

- [ ] **Step 3: Wire `obs::init` in the binary**

Same pattern as Task 4, with `"executor"` + default port `127.0.0.1:9004`. Call `crate::metrics::describe()` immediately after `obs::init` so `/metrics` shows describes even before traffic arrives.

- [ ] **Step 4: Build**

Run: `cargo build -p kardamom-executor --locked`
Expected: success.

- [ ] **Step 5: Add metrics_endpoint smoke test**

Mirror Task 4. Assert `kardamom_executor_tx_applied_total` is present.

- [ ] **Step 6: Run all executor tests**

Run: `cargo test -p kardamom-executor --locked`
Expected: green.

- [ ] **Step 7: Commit**

```bash
git add crates/executor Cargo.lock
git commit -m "feat(executor): wire kardamom-obs on :9004; baseline tx/block/commit metrics"
```

---

## Task 8: Add baseline metrics + `obs::init` to `kardamom-da-watcher`

**Files:**
- Modify: `crates/da_watcher/Cargo.toml`
- Modify: `crates/da_watcher/src/bin/kardamom-da-watcher.rs`
- Modify: `crates/da_watcher/src/<watcher loop>.rs` (locate via `rg fn poll crates/da_watcher`)
- Create: `crates/da_watcher/src/metrics.rs`
- Create: `crates/da_watcher/tests/metrics_endpoint.rs`

- [ ] **Step 1: Create `crates/da_watcher/src/metrics.rs`**

```rust
pub const L1_HEAD: &str = "kardamom_da_watcher_l1_head_block_number";
pub const L1_FINALIZED: &str = "kardamom_da_watcher_l1_finalized_block_number";
pub const DEPOSITS_DETECTED_TOTAL: &str = "kardamom_da_watcher_deposits_detected_total";
pub const TICK_TOTAL: &str = "kardamom_da_watcher_tick_total";

pub fn describe() {
    metrics::describe_gauge!(L1_HEAD, "latest L1 block number observed");
    metrics::describe_gauge!(L1_FINALIZED, "latest finalised L1 block number observed");
    metrics::describe_counter!(DEPOSITS_DETECTED_TOTAL, "deposits detected from L1");
    metrics::describe_counter!(TICK_TOTAL, "watcher loop ticks, labelled by outcome");
}
```

Add `pub mod metrics;` to `crates/da_watcher/src/lib.rs`.

- [ ] **Step 2: Emit at the obvious sites**

Locate the watcher's poll loop and per-tick branches:

```rust
// per successful poll:
metrics::gauge!(crate::metrics::L1_HEAD).set(head as f64);
metrics::gauge!(crate::metrics::L1_FINALIZED).set(finalized as f64);
metrics::counter!(crate::metrics::TICK_TOTAL, "outcome" => "ok").increment(1);

// per RPC error:
metrics::counter!(crate::metrics::TICK_TOTAL, "outcome" => "rpc_error").increment(1);

// per parse error:
metrics::counter!(crate::metrics::TICK_TOTAL, "outcome" => "parse_error").increment(1);

// when a deposit is decoded:
metrics::counter!(crate::metrics::DEPOSITS_DETECTED_TOTAL).increment(1);
```

- [ ] **Step 3: Wire `obs::init` in the binary**

Same pattern as Task 4, with `"da-watcher"` and default port `127.0.0.1:9005`. Call `crate::metrics::describe()`.

- [ ] **Step 4: Build + test**

Run: `cargo build -p kardamom-da-watcher --locked`
Run: `cargo test -p kardamom-da-watcher --locked`
Expected: green.

- [ ] **Step 5: Add metrics_endpoint smoke test**

Mirror Task 4 with `"da-watcher"` and assert `kardamom_da_watcher_tick_total`.

- [ ] **Step 6: Commit**

```bash
git add crates/da_watcher Cargo.lock
git commit -m "feat(da-watcher): wire kardamom-obs on :9005; baseline L1 + deposit metrics"
```

---

## Task 9: Add baseline metrics + `obs::init` to `kardamom-ingress`

**Files:**
- Modify: `crates/ingress/Cargo.toml`
- Modify: `crates/ingress/src/bin/kardamom-ingress.rs`
- Modify: `crates/ingress/src/<handler>.rs` (locate via `rg fn submit_raw crates/ingress`)
- Create: `crates/ingress/src/metrics.rs`
- Create: `crates/ingress/tests/metrics_endpoint.rs`

- [ ] **Step 1: Create `crates/ingress/src/metrics.rs`**

```rust
pub const TX_RECEIVED_TOTAL: &str = "kardamom_ingress_tx_received_total";
pub const TX_ACCEPTED_TOTAL: &str = "kardamom_ingress_tx_accepted_total";
pub const TX_REJECTED_TOTAL: &str = "kardamom_ingress_tx_rejected_total";
pub const QUEUE_DEPTH: &str = "kardamom_ingress_queue_depth";

pub fn describe() {
    metrics::describe_counter!(TX_RECEIVED_TOTAL, "tx submissions received");
    metrics::describe_counter!(TX_ACCEPTED_TOTAL, "tx submissions accepted into the pool");
    metrics::describe_counter!(TX_REJECTED_TOTAL, "tx submissions rejected, labelled by reason");
    metrics::describe_gauge!(QUEUE_DEPTH, "current pending-tx queue depth");
}
```

Add `pub mod metrics;` to `crates/ingress/src/lib.rs`.

- [ ] **Step 2: Emit at the obvious sites**

```rust
// at the top of submit_raw_transaction (or whatever the handler is):
metrics::counter!(crate::metrics::TX_RECEIVED_TOTAL).increment(1);

// on successful enqueue:
metrics::counter!(crate::metrics::TX_ACCEPTED_TOTAL).increment(1);

// on rejection, with a reason derived from the existing TxError enum:
let reason = match err {
    TxError::InvalidSignature => "invalid_signature",
    TxError::NonceTooLow => "nonce_too_low",
    TxError::DuplicateHash => "duplicate_hash",
    // ... add other variants explicitly
};
metrics::counter!(crate::metrics::TX_REJECTED_TOTAL, "reason" => reason).increment(1);

// when the queue depth changes (after each enqueue/dequeue):
metrics::gauge!(crate::metrics::QUEUE_DEPTH).set(queue.len() as f64);
```

The actual `TxError` variant list lives in the ingress (or shared types) crate — read it and add every variant explicitly. Avoid a catch-all `_ => "other"`; that hides interesting failure modes from the dashboard.

- [ ] **Step 3: Wire `obs::init` in the binary**

Same pattern as Task 4, with `"ingress"` and default port `127.0.0.1:9006`. Call `crate::metrics::describe()`.

- [ ] **Step 4: Build + test**

Run: `cargo build -p kardamom-ingress --locked`
Run: `cargo test -p kardamom-ingress --locked`
Expected: green.

- [ ] **Step 5: Add metrics_endpoint smoke test**

Mirror Task 4 with `"ingress"` and assert `kardamom_ingress_tx_received_total`.

- [ ] **Step 6: Commit**

```bash
git add crates/ingress Cargo.lock
git commit -m "feat(ingress): wire kardamom-obs on :9006; baseline tx accept/reject + queue depth metrics"
```

---

## Task 10: Update `deploy/prometheus.yml` with 7 scrape jobs

**Files:**
- Modify: `deploy/prometheus.yml`

- [ ] **Step 1: Replace the file contents**

Overwrite `deploy/prometheus.yml`:

```yaml
global:
  scrape_interval: 1s
  evaluation_interval: 5s

# One scrape job per kardamom service. Each `static_configs.targets` is the
# list of host:port pairs running that service. To add a host, append
# `host-N:<port>` to the matching job's `targets` — every metric is already
# labelled with `host_id` (set via `--host-id` on the binary), so Prometheus
# can group by host without relabel rules.
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

- [ ] **Step 2: Validate YAML**

Run: `python3 -c "import yaml,sys; yaml.safe_load(open('deploy/prometheus.yml'))" && echo OK`
Expected: `OK`.

- [ ] **Step 3: Commit**

```bash
git add deploy/prometheus.yml
git commit -m "build(deploy): scrape every kardamom service (7 jobs, ports 9000-9006)"
```

---

## Task 11: Create the overview dashboard template + `kardamom-overview.json`

**Files:**
- Create: `deploy/grafana/provisioning/dashboards-json/kardamom-overview.json`

- [ ] **Step 1: Write the dashboard JSON**

Write the file with this structure (full JSON; this dashboard becomes the template for all other dashboards in subsequent tasks):

```json
{
  "annotations": {"list": []},
  "editable": true,
  "fiscalYearStartMonth": 0,
  "graphTooltip": 1,
  "id": null,
  "links": [],
  "liveNow": false,
  "panels": [
    {
      "datasource": {"type": "prometheus", "uid": "prometheus"},
      "fieldConfig": {"defaults": {"color": {"mode": "thresholds"}, "thresholds": {"mode": "absolute", "steps": [{"color": "red", "value": null}, {"color": "green", "value": 1}]}}, "overrides": []},
      "gridPos": {"h": 4, "w": 24, "x": 0, "y": 0},
      "id": 1,
      "options": {"colorMode": "background", "graphMode": "none", "justifyMode": "auto", "orientation": "horizontal", "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": false}, "textMode": "value_and_name"},
      "pluginVersion": "10.4.0",
      "targets": [{"datasource": {"type": "prometheus", "uid": "prometheus"}, "expr": "max by (service) (kardamom_service_up{host_id=~\"$host\"})", "legendFormat": "{{service}}", "range": true, "refId": "A"}],
      "title": "Services up",
      "type": "stat"
    },
    {
      "datasource": {"type": "prometheus", "uid": "prometheus"},
      "fieldConfig": {"defaults": {"custom": {"lineWidth": 1, "fillOpacity": 10}, "unit": "ops"}, "overrides": []},
      "gridPos": {"h": 8, "w": 12, "x": 0, "y": 4},
      "id": 2,
      "options": {"legend": {"calcs": [], "displayMode": "list", "placement": "bottom", "showLegend": true}, "tooltip": {"mode": "multi", "sort": "none"}},
      "targets": [{"datasource": {"type": "prometheus", "uid": "prometheus"}, "expr": "sum by (service) (rate({__name__=~\"kardamom_.+_total\", host_id=~\"$host\"}[1m]))", "legendFormat": "{{service}}", "range": true, "refId": "A"}],
      "title": "Counter rate by service (1m)",
      "type": "timeseries"
    },
    {
      "datasource": {"type": "prometheus", "uid": "prometheus"},
      "fieldConfig": {"defaults": {"custom": {"lineWidth": 1, "fillOpacity": 0}, "unit": "short"}, "overrides": []},
      "gridPos": {"h": 8, "w": 12, "x": 12, "y": 4},
      "id": 3,
      "options": {"legend": {"calcs": [], "displayMode": "list", "placement": "bottom", "showLegend": true}, "tooltip": {"mode": "multi", "sort": "none"}},
      "targets": [
        {"datasource": {"type": "prometheus", "uid": "prometheus"}, "expr": "kardamom_block_number{host_id=~\"$host\"}", "legendFormat": "node", "range": true, "refId": "A"},
        {"datasource": {"type": "prometheus", "uid": "prometheus"}, "expr": "kardamom_sealer_block_number{host_id=~\"$host\"}", "legendFormat": "sealer", "range": true, "refId": "B"},
        {"datasource": {"type": "prometheus", "uid": "prometheus"}, "expr": "kardamom_executor_block_number{host_id=~\"$host\"}", "legendFormat": "executor", "range": true, "refId": "C"}
      ],
      "title": "Block height (node / sealer / executor)",
      "type": "timeseries"
    },
    {
      "datasource": {"type": "prometheus", "uid": "prometheus"},
      "fieldConfig": {"defaults": {"unit": "short"}, "overrides": []},
      "gridPos": {"h": 5, "w": 12, "x": 0, "y": 12},
      "id": 4,
      "options": {"colorMode": "value", "graphMode": "area", "justifyMode": "auto", "orientation": "auto", "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": false}, "textMode": "auto"},
      "targets": [{"datasource": {"type": "prometheus", "uid": "prometheus"}, "expr": "kardamom_da_watcher_l1_head_block_number{host_id=~\"$host\"} - kardamom_da_watcher_l1_finalized_block_number{host_id=~\"$host\"}", "legendFormat": "L1 lag", "range": true, "refId": "A"}],
      "title": "L1 head − finalized",
      "type": "stat"
    },
    {
      "datasource": {"type": "prometheus", "uid": "prometheus"},
      "gridPos": {"h": 8, "w": 24, "x": 0, "y": 17},
      "id": 5,
      "options": {"showHeader": true},
      "targets": [{"datasource": {"type": "prometheus", "uid": "prometheus"}, "expr": "kardamom_build_info{host_id=~\"$host\"}", "format": "table", "instant": true, "range": false, "refId": "A"}],
      "title": "Build info",
      "transformations": [{"id": "organize", "options": {"excludeByName": {"Time": true, "Value": true, "__name__": true, "instance": true, "job": true}, "indexByName": {}, "renameByName": {}}}],
      "type": "table"
    }
  ],
  "refresh": "5s",
  "schemaVersion": 38,
  "tags": ["kardamom", "overview"],
  "templating": {
    "list": [
      {
        "current": {"selected": false, "text": "All", "value": "$__all"},
        "datasource": {"type": "prometheus", "uid": "prometheus"},
        "definition": "label_values(kardamom_service_up, host_id)",
        "hide": 0,
        "includeAll": true,
        "label": "host",
        "multi": true,
        "name": "host",
        "options": [],
        "query": {"query": "label_values(kardamom_service_up, host_id)", "refId": "StandardVariableQuery"},
        "refresh": 1,
        "regex": "",
        "skipUrlSync": false,
        "sort": 0,
        "type": "query"
      }
    ]
  },
  "time": {"from": "now-15m", "to": "now"},
  "timepicker": {},
  "timezone": "",
  "title": "Kardamom Overview",
  "uid": "kardamom-overview",
  "version": 1,
  "weekStart": ""
}
```

- [ ] **Step 2: Validate**

Run: `python3 -c "import json,sys; d=json.load(open('deploy/grafana/provisioning/dashboards-json/kardamom-overview.json')); assert d['schemaVersion']==38; print('OK,', len(d['panels']), 'panels')"`
Expected: `OK, 5 panels`.

- [ ] **Step 3: Commit**

```bash
git add deploy/grafana/provisioning/dashboards-json/kardamom-overview.json
git commit -m "feat(grafana): top-level kardamom-overview dashboard (services up / counter rate / block heights / L1 lag / build info)"
```

---

## Task 12: Rename `kardamom-rpc.json` → `kardamom-node.json` and add host template var

**Files:**
- Rename: `deploy/grafana/provisioning/dashboards-json/kardamom-rpc.json` → `kardamom-node.json`
- Modify: the renamed file (uid, title, host template, query filters)

- [ ] **Step 1: Move the file**

Run: `git mv deploy/grafana/provisioning/dashboards-json/kardamom-rpc.json deploy/grafana/provisioning/dashboards-json/kardamom-node.json`

- [ ] **Step 2: Update uid + title + templating**

In the JSON, set:

```json
"uid": "kardamom-node",
"title": "Kardamom Node (RPC)",
```

Add the `host` template var to `templating.list` (use the same JSON block from Task 11 Step 1's `templating` section).

For each panel's target `expr`, append `{host_id=~"$host"}` to every `kardamom_*` metric reference. For metric *family* selectors (e.g. `rate(kardamom_rpc_requests_total[1m])`), rewrite to `rate(kardamom_rpc_requests_total{host_id=~"$host"}[1m])`.

- [ ] **Step 3: Validate**

Run: `python3 -c "import json; d=json.load(open('deploy/grafana/provisioning/dashboards-json/kardamom-node.json')); assert d['uid']=='kardamom-node'; assert any(t['name']=='host' for t in d['templating']['list']); print('OK')"`
Expected: `OK`.

- [ ] **Step 4: Commit**

```bash
git add deploy/grafana/provisioning/dashboards-json/
git commit -m "feat(grafana): rename kardamom-rpc dashboard to kardamom-node + host template var"
```

---

## Task 13: Create `kardamom-sequencer.json`

**Files:**
- Create: `deploy/grafana/provisioning/dashboards-json/kardamom-sequencer.json`

- [ ] **Step 1: Write the dashboard**

Use the Task 11 JSON as the template. Replace the `panels` array with the following 8 panels (`gridPos` laid out 12-wide × 8-tall in pairs; `id` runs 1..8; `templating` includes the same `host` var as Task 11 plus a `partition` var: `definition: "label_values(kardamom_sequencer_tx_ingested_total{host_id=~\"$host\"}, partition)"`, `multi: true`, `includeAll: true`):

1. **Ingest rate (rps by partition)** — timeseries — `sum by (partition) (rate(kardamom_sequencer_tx_ingested_total{host_id=~"$host", partition=~"$partition"}[1m]))`
2. **Publish-to-B rate (rps by partition)** — timeseries — `sum by (partition) (rate(kardamom_sequencer_tx_published_to_b_total{host_id=~"$host", partition=~"$partition"}[1m]))`
3. **Tx buffered (future) rate** — timeseries — `sum by (partition) (rate(kardamom_sequencer_tx_buffered_future_total{host_id=~"$host", partition=~"$partition"}[1m]))`
4. **Tx dropped (past) rate** — timeseries — `sum by (partition) (rate(kardamom_sequencer_tx_dropped_past_total{host_id=~"$host", partition=~"$partition"}[1m]))`
5. **Backpressure events** — timeseries — `sum by (partition) (rate(kardamom_sequencer_backpressure_total{host_id=~"$host", partition=~"$partition"}[1m]))`
6. **Nonce check P50/P90/P99 (by partition)** — timeseries — three targets: `histogram_quantile(0.5, sum by (le, partition) (rate(kardamom_sequencer_nonce_check_microseconds_bucket{host_id=~"$host", partition=~"$partition"}[1m])))` (and 0.9, 0.99)
7. **Pending evictions rate** — timeseries — `sum by (partition) (rate(kardamom_sequencer_pending_evictions_total{host_id=~"$host", partition=~"$partition"}[1m]))`
8. **Standby replay lag** — timeseries — `kardamom_sequencer_standby_replay_lag{host_id=~"$host", partition=~"$partition"}`

Plus a Build info table panel (same shape as Task 11 panel 5), id 9, at the bottom.

Set `"uid": "kardamom-sequencer"`, `"title": "Kardamom Sequencer"`, `"tags": ["kardamom", "sequencer"]`.

- [ ] **Step 2: Validate**

Run: `python3 -c "import json; d=json.load(open('deploy/grafana/provisioning/dashboards-json/kardamom-sequencer.json')); assert d['uid']=='kardamom-sequencer'; assert len(d['panels'])==9; print('OK')"`
Expected: `OK`.

- [ ] **Step 3: Commit**

```bash
git add deploy/grafana/provisioning/dashboards-json/kardamom-sequencer.json
git commit -m "feat(grafana): kardamom-sequencer dashboard (8 panels + build info)"
```

---

## Task 14: Create `kardamom-batcher.json` and `kardamom-sealer.json`

**Files:**
- Create: `deploy/grafana/provisioning/dashboards-json/kardamom-batcher.json`
- Create: `deploy/grafana/provisioning/dashboards-json/kardamom-sealer.json`

- [ ] **Step 1: Write the batcher dashboard**

Same template as Task 11. Panels:

1. **Blocks observed (1m rate)** — timeseries — `rate(kardamom_batcher_blocks_observed_total{host_id=~"$host"}[1m])`
2. **Batches posted (1m rate)** — timeseries — `rate(kardamom_batcher_batches_posted_total{host_id=~"$host"}[1m])`
3. **Blobs posted (1m rate)** — timeseries — `rate(kardamom_batcher_blobs_posted_total{host_id=~"$host"}[1m])`
4. **Blobs / batch (instantaneous ratio)** — stat — `kardamom_batcher_blobs_posted_total{host_id=~"$host"} / clamp_min(kardamom_batcher_batches_posted_total{host_id=~"$host"}, 1)`
5. **Build info** — same table as Task 11 panel 5.

UID: `kardamom-batcher`. Title: `Kardamom Batcher`. Tags: `["kardamom", "batcher"]`.

- [ ] **Step 2: Write the sealer dashboard**

Panels:

1. **Boundaries emitted (1m rate per host)** — timeseries — `sum by (host_id) (rate(kardamom_sealer_boundaries_emitted_total{host_id=~"$host"}[1m]))`
2. **Block number per host** — timeseries — `kardamom_sealer_block_number{host_id=~"$host"}`
3. **Tick-skipped rate by reason** — timeseries (stacked: `fillOpacity: 40`) — `sum by (reason) (rate(kardamom_sealer_tick_skipped_total{host_id=~"$host"}[1m]))`
4. **Build info** — table.

UID: `kardamom-sealer`. Title: `Kardamom Sealer`. Tags: `["kardamom", "sealer"]`.

- [ ] **Step 3: Validate both**

Run: `python3 -c "import json; [print(f, json.load(open(f))['uid']) for f in ['deploy/grafana/provisioning/dashboards-json/kardamom-batcher.json', 'deploy/grafana/provisioning/dashboards-json/kardamom-sealer.json']]"`
Expected: each line prints its filename and the matching uid.

- [ ] **Step 4: Commit**

```bash
git add deploy/grafana/provisioning/dashboards-json/kardamom-batcher.json deploy/grafana/provisioning/dashboards-json/kardamom-sealer.json
git commit -m "feat(grafana): kardamom-batcher + kardamom-sealer dashboards"
```

---

## Task 15: Create the three silent-service dashboards

**Files:**
- Create: `deploy/grafana/provisioning/dashboards-json/kardamom-executor.json`
- Create: `deploy/grafana/provisioning/dashboards-json/kardamom-da-watcher.json`
- Create: `deploy/grafana/provisioning/dashboards-json/kardamom-ingress.json`

- [ ] **Step 1: Executor dashboard**

Panels:

1. **Tx applied (rps by outcome, stacked)** — timeseries — `sum by (outcome) (rate(kardamom_executor_tx_applied_total{host_id=~"$host"}[1m]))`
2. **Block-apply P50/P90/P99** — timeseries — three targets via `histogram_quantile(...) on kardamom_executor_block_apply_duration_seconds_bucket{host_id=~"$host"}`
3. **State-commit P50/P90/P99** — timeseries — same shape on `kardamom_executor_state_commit_duration_seconds_bucket`
4. **Block number** — stat — `kardamom_executor_block_number{host_id=~"$host"}`
5. **Build info** — table.

UID: `kardamom-executor`. Title: `Kardamom Executor`.

- [ ] **Step 2: DA-watcher dashboard**

Panels:

1. **L1 head + finalized** — timeseries — two targets: `kardamom_da_watcher_l1_head_block_number{host_id=~"$host"}` (legend: head), `kardamom_da_watcher_l1_finalized_block_number{host_id=~"$host"}` (legend: finalized)
2. **L1 lag (head − finalized)** — stat — `kardamom_da_watcher_l1_head_block_number{host_id=~"$host"} - kardamom_da_watcher_l1_finalized_block_number{host_id=~"$host"}`
3. **Deposits detected (rps)** — timeseries — `rate(kardamom_da_watcher_deposits_detected_total{host_id=~"$host"}[1m])`
4. **Tick outcomes (stacked rps by outcome)** — timeseries — `sum by (outcome) (rate(kardamom_da_watcher_tick_total{host_id=~"$host"}[1m]))`
5. **Build info** — table.

UID: `kardamom-da-watcher`. Title: `Kardamom DA Watcher`.

- [ ] **Step 3: Ingress dashboard**

Panels:

1. **Tx received / accepted / rejected (rps, stacked)** — timeseries — three targets: rates of the three counters
2. **Reject reasons** — bargauge — `sum by (reason) (rate(kardamom_ingress_tx_rejected_total{host_id=~"$host"}[1m]))`
3. **Queue depth** — timeseries — `kardamom_ingress_queue_depth{host_id=~"$host"}`
4. **Build info** — table.

UID: `kardamom-ingress`. Title: `Kardamom Ingress`.

- [ ] **Step 4: Validate all three**

Run: `for f in deploy/grafana/provisioning/dashboards-json/kardamom-executor.json deploy/grafana/provisioning/dashboards-json/kardamom-da-watcher.json deploy/grafana/provisioning/dashboards-json/kardamom-ingress.json; do python3 -c "import json,sys; d=json.load(open('$f')); assert d['schemaVersion']==38; print('$f', 'OK', len(d['panels']), 'panels')"; done`

- [ ] **Step 5: Commit**

```bash
git add deploy/grafana/provisioning/dashboards-json/kardamom-executor.json deploy/grafana/provisioning/dashboards-json/kardamom-da-watcher.json deploy/grafana/provisioning/dashboards-json/kardamom-ingress.json
git commit -m "feat(grafana): kardamom-executor, kardamom-da-watcher, kardamom-ingress dashboards"
```

---

## Task 16: Dashboard JSON validity test

**Files:**
- Create: `crates/obs/tests/dashboards.rs`

- [ ] **Step 1: Add the test**

```rust
//! Static validity check for every dashboard JSON shipped in
//! `deploy/grafana/provisioning/dashboards-json/`. Cheap to run, catches
//! typos at authoring time (mis-spelled metric names, missing `host` filter,
//! drift from schema 38).

use std::path::{Path, PathBuf};

const EXPECTED_DASHBOARDS: &[&str] = &[
    "kardamom-overview",
    "kardamom-node",
    "kardamom-sequencer",
    "kardamom-batcher",
    "kardamom-sealer",
    "kardamom-executor",
    "kardamom-da-watcher",
    "kardamom-ingress",
];

fn dashboards_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_WORKSPACE_DIR"))
        .join("deploy/grafana/provisioning/dashboards-json")
}

#[test]
fn every_dashboard_is_present_valid_and_schema_38() {
    let dir = dashboards_dir();
    for stem in EXPECTED_DASHBOARDS {
        let path = dir.join(format!("{stem}.json"));
        let body = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        let v: serde_json::Value = serde_json::from_str(&body)
            .unwrap_or_else(|e| panic!("parse {}: {e}", path.display()));

        assert_eq!(v["schemaVersion"], 38, "{path:?} schemaVersion");
        assert_eq!(v["uid"].as_str(), Some(*stem), "{path:?} uid != {stem}");
        let panels = v["panels"].as_array().expect("panels array");
        assert!(!panels.is_empty(), "{path:?} has no panels");
        for (i, p) in panels.iter().enumerate() {
            assert!(
                p["title"].as_str().is_some_and(|s| !s.is_empty()),
                "{path:?} panel[{i}] missing title"
            );
            // Every PromQL target must reference at least one kardamom_*
            // metric (or the literal `kardamom_service_up` — also matches).
            let targets = p["targets"].as_array().cloned().unwrap_or_default();
            for (j, t) in targets.iter().enumerate() {
                let expr = t["expr"].as_str().unwrap_or("");
                assert!(
                    expr.contains("kardamom_"),
                    "{path:?} panel[{i}] target[{j}] expr does not reference any kardamom_* metric: {expr}"
                );
            }
        }
    }
}
```

Add `serde_json = { workspace = true }` to `crates/obs/Cargo.toml` `[dev-dependencies]` (workspace already has serde_json).

- [ ] **Step 2: Run the test**

Run: `cargo test -p kardamom-obs --locked --test dashboards`
Expected: PASS — all 8 dashboards validate.

- [ ] **Step 3: Commit**

```bash
git add crates/obs/Cargo.toml crates/obs/tests/dashboards.rs Cargo.lock
git commit -m "test(obs): static validity check for all 8 grafana dashboards"
```

---

## Task 17: Update `docs/observability.md`

**Files:**
- Modify: `docs/observability.md`

- [ ] **Step 1: Read the current file**

Run: `wc -l docs/observability.md && head -50 docs/observability.md`

- [ ] **Step 2: Rewrite the "Metrics" section**

Replace the single-service narrative with:

```markdown
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

```

Leave the flame-graph + bench-harness sections of `docs/observability.md` alone.

- [ ] **Step 3: Verify file still has its existing flame section**

Run: `grep -c 'flame' docs/observability.md`
Expected: ≥ 1 (the existing flame section is preserved).

- [ ] **Step 4: Commit**

```bash
git add docs/observability.md
git commit -m "docs(observability): per-service ports + dashboard table; rename map"
```

---

## Task 18: Final verification + open PR

**Files:**
- None — verification only.

- [ ] **Step 1: Full build**

Run: `PATH="$(just java-shim):$PATH" JAVA_HOME="$(just java-home)" cargo build --workspace --all-targets --locked`
Expected: success.

- [ ] **Step 2: Full test**

Run: `sg docker -c 'just test 2>&1 | tee /tmp/just-test-prom.log'`
Then: `grep -cE 'FAILED|^thread.*panicked|^error: test failed' /tmp/just-test-prom.log` — expect `0`.
Then: `grep -E '^test result' /tmp/just-test-prom.log | awk -F'[. ;]+' '{ ok+=$4; fail+=$6 } END { print "passed:", ok, "failed:", fail }'` — expect failed `0`.

- [ ] **Step 3: Clippy + rustfmt**

Run: `PATH="$(just java-shim):$PATH" JAVA_HOME="$(just java-home)" cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
Run: `PATH="$(just java-shim):$PATH" cargo fmt --check`
Expected: both clean.

- [ ] **Step 4: Push**

```bash
git push -u origin claude/wire-prometheus-metrics
```

- [ ] **Step 5: Open PR**

```bash
gh pr create --base claude/kardamom --head claude/wire-prometheus-metrics \
  --title "feat(obs): Prometheus exporter on every service + 8 Grafana dashboards" \
  --body "$(cat <<'EOF'
## Summary
- New `crates/obs` crate: shared `kardamom_obs::init(service, addr, host_id, version, sha)` for every service binary.
- All 7 service binaries now serve `/metrics`. Default ports 9000-9006; per-binary `--metrics-addr` + `--host-id` flags (env `KARDAMOM_METRICS_ADDR` / `KARDAMOM_HOST_ID`).
- Metric names normalised to `kardamom_<service>_<subsystem>_<name>_<unit>`. Renames called out in `docs/observability.md` rename map.
- 8 Grafana dashboards (1 overview + 7 per-service) live in `deploy/grafana/provisioning/dashboards-json/`.
- 7 new scrape jobs in `deploy/prometheus.yml` (one per service).

## Design
See `docs/specs/2026-05-29-prometheus-grafana-design.md`.

## Breaking
Sealer's per-emission `host_id` label is dropped (it's now a recorder-level global label). Batcher's dotted metric names are renamed to the `kardamom_batcher_*` form. No alerts yet exist on either, so blast radius is limited to ad-hoc queries.

## Test plan
- [x] `cargo test --workspace --all-targets --all-features --locked` (full just test)
- [x] `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
- [x] `cargo fmt --check`
- [x] Per-binary `/metrics` smoke tests (one per service) pass
- [x] `crates/obs/tests/dashboards.rs` validates every dashboard JSON

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 6: Wait for CI; fix anything that fails**

Run: `gh pr checks <pr-number>` and address failures before reporting back. Per project memory: every step must be green before this plan is considered complete.

---

## Self-review checklist

- [x] Spec coverage: every section of the spec maps to at least one task (obs crate → T1-T2; per-binary wiring → T3-T9; scrape config → T10; dashboards → T11-T15; tests → embedded in each + T16; docs → T17; PR → T18).
- [x] Placeholder scan: no TBD/TODO/"implement later". The per-service emission sites in T7-T9 are described with concrete file paths to grep and concrete code snippets; line numbers are not pinned because the reader/handler shapes evolve — the plan tells the implementer where to look.
- [x] Type consistency: every metric name appears identically across the binary task (T4-T9), the prometheus scrape job (T10), the dashboard panel (T11-T15), and the rename map in docs (T17).
