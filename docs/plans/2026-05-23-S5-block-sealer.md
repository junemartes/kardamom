# S5 Block Sealer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a singleton-with-hot-standby block sealer for the kardamom rollup that emits `BlockBoundaryStart` markers into the canonical log (channel B) every 250ms wall-clock, electing its leader deterministically from the set of recorder hosts whose per-recorder watermark is caught up to the current B tail — with zero durable state outside B.

**Architecture:** A new crate `crates/kardamom-sealer` that runs as a long-lived process (typically co-located with each recorder host). All instances subscribe to channel B and to all per-recorder watermark streams from S3. Each instance independently computes the same leader function — `lowest host-id among caught-up recorders` — and only the instance whose `host_id` matches the elected leader publishes `BlockBoundaryStart` to B via an Aeron concurrent publication. The leader's tick loop wakes on a wall-clock timer aligned to 250ms boundaries, reads the current Aeron `Publication::position()` for B, increments a local `block_number` (bootstrapped by replaying B's tail at startup), and publishes. All state is reconstructable from B; recovery is mechanical.

**Tech Stack:** Rust 2024 edition, tokio (async runtime + timers), `kardamom-log` (S3 types: `BPosition`, `BlockBoundaryStart`, channel handles, watermark streams), Aeron via S3's wrappers, `tracing` for observability, `criterion` for benches, `turmoil` or a deterministic harness for chaos tests.

**Branch:** `claude/s5-block-sealer` (branched off `main` once S3 lands).

**Reference spec:** `docs/specs/2026-05-23-high-throughput-sequencer-design.md` §2.6, §4.5, V0 scope.

**Assumed S3 interfaces (these must exist in `kardamom-log` before this plan starts):**
- `kardamom_log::BPosition` — `(term_id: i32, term_offset: i32)` with `Ord`, `Copy`, `serde::{Serialize, Deserialize}`.
- `kardamom_log::BlockBoundaryStart { block_number: u64, end_tx_idx: BPosition, l2_timestamp_ms: u64 }` with a stable canonical encoding (e.g. SSZ or a documented binary layout).
- `kardamom_log::BMessage` — tagged enum carrying `Tx(...)` and `BoundaryStart(BlockBoundaryStart)` variants, with `decode(&[u8]) -> Result<BMessage>` and `encode(&self) -> Vec<u8>`.
- `kardamom_log::channel_b::Publisher` — concurrent publisher returning the publication `position()` after each `offer()`. Method: `fn current_position(&self) -> BPosition`.
- `kardamom_log::channel_b::Subscriber` — fragment-handler style stream reader: `async fn poll(&mut self) -> Option<(BPosition, BMessage)>`, plus `async fn tail_scan(&mut self, from: BPosition) -> impl Stream<Item = (BPosition, BMessage)>`.
- `kardamom_log::watermark::RecorderWatermark { host_id: u16, fsynced_position: BPosition, wall_ts_micros: u64 }`.
- `kardamom_log::watermark::Subscriber` — `async fn poll(&mut self) -> Option<RecorderWatermark>`.

If any of these names differ in S3 when this plan executes, fix them inline in Task 2's `Cargo.toml` / Task 3's `lib.rs` and propagate. Do not invent placeholder traits.

**File structure:**

```
crates/kardamom-sealer/
├── Cargo.toml
├── README.md                    (one-paragraph orientation; no docs proliferation)
├── src/
│   ├── lib.rs                   (re-exports + crate-level docs)
│   ├── config.rs                (SealerConfig + TOML loader)
│   ├── clock.rs                 (WallClock trait + SystemClock impl + MockClock for tests)
│   ├── tick.rs                  (250ms-aligned tick computation + l2_timestamp_ms)
│   ├── election.rs              (CaughtUpSet + leader function — pure, no I/O)
│   ├── watermark_tracker.rs     (per-recorder freshness window over watermark stream)
│   ├── bootstrap.rs             (B-tail scan → initial block_number)
│   ├── emitter.rs               (BoundaryEmitter — leader-side publish loop)
│   ├── sealer.rs                (Sealer — top-level supervisor: election + emitter)
│   └── bin/kardamom-sealer.rs   (CLI entry point)
├── tests/
│   ├── election_property.rs     (property tests over leader function)
│   ├── tick_alignment.rs        (250ms rounding edge cases)
│   ├── bootstrap_tail_scan.rs   (block_number init from synthetic B)
│   ├── single_emitter.rs        (3 sealers / mock log: exactly one emits)
│   ├── failover.rs              (kill leader → bounded takeover)
│   └── chaos_isolation.rs       (leader isolated → standby promoted → leader yields on rejoin)
└── benches/
    └── boundary_emit.rs         (criterion: per-tick overhead)
```

---

## Task 1: Create crate skeleton and wire into workspace

**Files:**
- Create: `crates/kardamom-sealer/Cargo.toml`
- Create: `crates/kardamom-sealer/src/lib.rs`
- Create: `crates/kardamom-sealer/README.md`
- Modify: `Cargo.toml` (root) — workspace already uses `members = ["crates/*"]`, so no change needed; verify.

- [ ] **Step 1: Verify workspace globbing picks up new crate**

Run:
```bash
grep -n 'members' /home/dev/kardamom/Cargo.toml
```
Expected: `members = ["crates/*"]`. No change needed.

- [ ] **Step 2: Write `crates/kardamom-sealer/Cargo.toml`**

```toml
[package]
name = "kardamom-sealer"
version = { workspace = true }
edition = { workspace = true }

[dependencies]
kardamom-log = { path = "../kardamom-log" }
tokio = { workspace = true }
serde = { workspace = true }
toml = { workspace = true }
thiserror = { workspace = true }
tracing = { workspace = true }
tracing-subscriber = { workspace = true }
clap = { workspace = true }
anyhow = { workspace = true }
metrics = { workspace = true }
futures = "0.3"

[dev-dependencies]
proptest = "1"
tokio = { workspace = true, features = ["test-util", "macros", "rt-multi-thread"] }
criterion = { version = "0.5", features = ["async_tokio"] }

[[bin]]
name = "kardamom-sealer"
path = "src/bin/kardamom-sealer.rs"

[[bench]]
name = "boundary_emit"
harness = false
```

Notes:
- `kardamom-log` is the crate produced by S3. If S3 names it differently (e.g. `kardamom-canonical-log`), substitute and update every `use` statement in this crate.
- `proptest` and `criterion` are not in the workspace table; pin them locally to avoid blocking on a workspace edit.
- `futures = "0.3"` is needed for `Stream` combinators used in `watermark_tracker.rs`.

- [ ] **Step 3: Write `crates/kardamom-sealer/src/lib.rs`**

```rust
//! S5 block sealer.
//!
//! Emits `BlockBoundaryStart` markers every 250ms wall-clock into the canonical
//! log (channel B). Singleton with hot standbys; leader is the lowest-host-id
//! sealer whose recorder peer is caught up to the current B tail.
//!
//! All state is reconstructable from B. The sealer keeps no durable state.

pub mod bootstrap;
pub mod clock;
pub mod config;
pub mod election;
pub mod emitter;
pub mod sealer;
pub mod tick;
pub mod watermark_tracker;

pub use config::SealerConfig;
pub use sealer::Sealer;
```

- [ ] **Step 4: Write `crates/kardamom-sealer/README.md`**

```markdown
# kardamom-sealer

S5 of the kardamom sequencer. One sealer process per recorder host;
deterministic leader election (lowest caught-up host id) chooses which one
emits `BlockBoundaryStart` to channel B every 250ms. All state is
reconstructable from B's tail; failover is mechanical.

Spec: `docs/specs/2026-05-23-high-throughput-sequencer-design.md` §2.6, §4.5.
Plan: `docs/plans/2026-05-23-S5-block-sealer.md`.
```

- [ ] **Step 5: Verify the workspace still builds**

```bash
cd /home/dev/kardamom && cargo check -p kardamom-sealer
```
Expected: compiles (empty crate, empty modules will fail — proceed to Task 2 to create stubs).

If you see "file not found for module …", create empty `src/<name>.rs` files for each declared module so the crate parses. (Subsequent tasks fill them in.)

- [ ] **Step 6: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-sealer/Cargo.toml crates/kardamom-sealer/src/lib.rs crates/kardamom-sealer/README.md crates/kardamom-sealer/src/*.rs
git commit -m "sealer: scaffold kardamom-sealer crate"
```

---

## Task 2: Define `SealerConfig` and TOML loader

**Files:**
- Create: `crates/kardamom-sealer/src/config.rs`
- Test: inline `#[cfg(test)]` in `config.rs`

- [ ] **Step 1: Write the failing test**

Inline at the bottom of `config.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_toml() {
        let toml = r#"
            host_id = 7
            channel_b_uri = "aeron:udp?endpoint=224.0.0.1:40123"
            channel_tx_ordering_stream_id = 1001
            watermark_channel_uri = "aeron:udp?endpoint=224.0.0.1:40124"
            watermark_stream_id_base = 2000
            recorder_host_ids = [1, 2, 7]
            caught_up_lag_bytes = 65536
            caught_up_stale_ms = 500
        "#;
        let cfg: SealerConfig = toml::from_str(toml).unwrap();
        assert_eq!(cfg.host_id, 7);
        assert_eq!(cfg.recorder_host_ids, vec![1, 2, 7]);
        assert_eq!(cfg.tick_interval_ms, 250);  // default
        assert_eq!(cfg.caught_up_lag_bytes, 65_536);
        assert_eq!(cfg.caught_up_stale_ms, 500);
    }

    #[test]
    fn rejects_unknown_keys() {
        let toml = r#"
            host_id = 1
            channel_b_uri = "x"
            channel_tx_ordering_stream_id = 1
            watermark_channel_uri = "x"
            watermark_stream_id_base = 1
            recorder_host_ids = [1]
            caught_up_lag_bytes = 1
            caught_up_stale_ms = 1
            bogus = "field"
        "#;
        assert!(toml::from_str::<SealerConfig>(toml).is_err());
    }

    #[test]
    fn host_id_must_be_in_recorder_set() {
        let cfg = SealerConfig {
            host_id: 99,
            channel_b_uri: "x".into(),
            channel_tx_ordering_stream_id: 1,
            watermark_channel_uri: "x".into(),
            watermark_stream_id_base: 1,
            recorder_host_ids: vec![1, 2, 3],
            caught_up_lag_bytes: 1,
            caught_up_stale_ms: 1,
            tick_interval_ms: 250,
        };
        assert!(cfg.validate().is_err());
    }
}
```

- [ ] **Step 2: Run the tests and verify they fail to compile**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-sealer config::tests
```
Expected: compilation failure (`SealerConfig` undefined).

- [ ] **Step 3: Implement `SealerConfig`**

```rust
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// All knobs for a single sealer process.
///
/// All fields are required except `tick_interval_ms`, which defaults to 250.
/// Unknown keys are rejected (`deny_unknown_fields`) so misconfigured deployments
/// fail fast at startup.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealerConfig {
    /// This process's host identifier. Must appear in `recorder_host_ids`.
    pub host_id: u16,
    /// Aeron channel URI for channel B (publish + subscribe on the same channel).
    pub channel_b_uri: String,
    pub channel_tx_ordering_stream_id: i32,
    /// Aeron channel URI carrying all per-recorder watermark streams (one stream per recorder).
    pub watermark_channel_uri: String,
    /// Stream id of recorder `host_id` is `watermark_stream_id_base + host_id as i32`.
    pub watermark_stream_id_base: i32,
    /// All recorder host ids in the cluster (sealer election pool).
    pub recorder_host_ids: Vec<u16>,
    /// "Caught up" means `current_B_position - recorder.fsynced_position <= caught_up_lag_bytes`.
    /// See plan §lease-election; the threshold is tuned by operators.
    pub caught_up_lag_bytes: u64,
    /// "Caught up" also requires a watermark observed within the last `caught_up_stale_ms` ms.
    pub caught_up_stale_ms: u64,
    /// Wall-clock tick interval. Defaults to 250 (must be > 0; values other than 250 are for tests).
    #[serde(default = "default_tick_ms")]
    pub tick_interval_ms: u64,
}

fn default_tick_ms() -> u64 {
    250
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("host_id {host_id} is not in recorder_host_ids {recorders:?}")]
    HostIdNotRecorder { host_id: u16, recorders: Vec<u16> },
    #[error("recorder_host_ids must be non-empty")]
    EmptyRecorderSet,
    #[error("tick_interval_ms must be > 0")]
    BadTick,
}

impl SealerConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.recorder_host_ids.is_empty() {
            return Err(ConfigError::EmptyRecorderSet);
        }
        if !self.recorder_host_ids.contains(&self.host_id) {
            return Err(ConfigError::HostIdNotRecorder {
                host_id: self.host_id,
                recorders: self.recorder_host_ids.clone(),
            });
        }
        if self.tick_interval_ms == 0 {
            return Err(ConfigError::BadTick);
        }
        Ok(())
    }
}
```

- [ ] **Step 4: Run the tests and verify they pass**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-sealer config::tests
```
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-sealer/src/config.rs
git commit -m "sealer: add SealerConfig with TOML loader and validation"
```

---

## Task 3: Wall-clock abstraction (`clock.rs`)

**Files:**
- Create: `crates/kardamom-sealer/src/clock.rs`

The sealer needs a clock that is:
- monotonic for timer scheduling (`Instant`-like);
- wall-clock for `l2_timestamp_ms` derivation (`SystemTime`-like);
- mockable for deterministic tests.

We use a single `WallClock` trait returning **Unix-epoch milliseconds**. Tick scheduling is in tokio time, which advances with the mock clock when using `tokio::time::pause()`. The real implementation reads `SystemTime::now()`.

**Wall-clock source policy:** v0 uses the host's `SystemTime`. The system administrator is responsible for running chrony/ntpd; PTP is optional and out of scope. See plan footer "Open questions" for the PTP upgrade path.

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_clock_returns_recent_unix_ms() {
        let clock = SystemClock;
        let now = clock.unix_ms();
        // Sanity bracket: between 2025-01-01 and 2030-01-01.
        assert!(now > 1_735_689_600_000);
        assert!(now < 1_893_456_000_000);
    }

    #[test]
    fn mock_clock_is_settable() {
        let clock = MockClock::new(1_000);
        assert_eq!(clock.unix_ms(), 1_000);
        clock.set(2_500);
        assert_eq!(clock.unix_ms(), 2_500);
        clock.advance(125);
        assert_eq!(clock.unix_ms(), 2_625);
    }
}
```

- [ ] **Step 2: Run and verify failure**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-sealer clock::tests
```
Expected: compile errors (`WallClock` etc. undefined).

- [ ] **Step 3: Implement**

```rust
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Source of wall-clock Unix-epoch milliseconds.
///
/// Distinct from tokio's monotonic clock (which is used for tick scheduling and
/// is mockable via `tokio::time::pause`). This trait covers the `l2_timestamp_ms`
/// derivation only.
pub trait WallClock: Send + Sync + 'static {
    fn unix_ms(&self) -> u64;
}

pub struct SystemClock;

impl WallClock for SystemClock {
    fn unix_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is before 1970")
            .as_millis() as u64
    }
}

/// Test-only clock. Time only moves when `set` or `advance` is called.
#[derive(Clone)]
pub struct MockClock(Arc<AtomicU64>);

impl MockClock {
    pub fn new(start_ms: u64) -> Self {
        Self(Arc::new(AtomicU64::new(start_ms)))
    }
    pub fn set(&self, ms: u64) {
        self.0.store(ms, Ordering::SeqCst);
    }
    pub fn advance(&self, delta_ms: u64) {
        self.0.fetch_add(delta_ms, Ordering::SeqCst);
    }
}

impl WallClock for MockClock {
    fn unix_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}
```

- [ ] **Step 4: Run and verify pass**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-sealer clock::tests
```
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-sealer/src/clock.rs
git commit -m "sealer: add WallClock trait with SystemClock and MockClock"
```

---

## Task 4: Tick scheduling (`tick.rs`)

**Files:**
- Create: `crates/kardamom-sealer/src/tick.rs`

The tick policy: `l2_timestamp_ms` is always the **floor** of the current wall-clock to a multiple of `tick_interval_ms` (default 250). The *next* scheduled tick is the next such multiple strictly greater than `now`.

This means:
- Every wall-clock-ms `t` maps unambiguously to a tick `floor(t / 250) * 250`.
- Two sealers at slightly different times produce the same `l2_timestamp_ms` as long as they're in the same 250ms window — preserving determinism across leader change.
- If a tick is missed (process pause, GC, etc.), the next tick fires immediately at the next aligned boundary; we never "catch up" by emitting multiple boundaries.

- [ ] **Step 1: Write failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floor_aligns_exact_boundary() {
        assert_eq!(floor_to_tick(1_000, 250), 1_000);
        assert_eq!(floor_to_tick(0, 250), 0);
    }

    #[test]
    fn floor_rounds_down() {
        assert_eq!(floor_to_tick(1_001, 250), 1_000);
        assert_eq!(floor_to_tick(1_249, 250), 1_000);
        assert_eq!(floor_to_tick(1_250, 250), 1_250);
    }

    #[test]
    fn next_tick_strictly_greater() {
        assert_eq!(next_tick(1_000, 250), 1_250);
        assert_eq!(next_tick(1_001, 250), 1_250);
        assert_eq!(next_tick(1_249, 250), 1_250);
        assert_eq!(next_tick(1_250, 250), 1_500);
    }

    #[test]
    fn handles_large_skip() {
        // Process paused for 3 ticks; next scheduled tick is the next aligned slot,
        // not a catch-up loop.
        assert_eq!(next_tick(2_001, 250), 2_250);
    }

    #[test]
    #[should_panic]
    fn rejects_zero_interval() {
        let _ = next_tick(1_000, 0);
    }
}
```

- [ ] **Step 2: Run and verify failure**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-sealer tick::tests
```

- [ ] **Step 3: Implement**

```rust
//! Wall-clock tick alignment for boundary emission.
//!
//! `l2_timestamp_ms` for a tick is always `floor(now / interval) * interval`.
//! See plan §tick scheduling for the rationale (cross-leader determinism).

/// Round `now_ms` down to the nearest multiple of `interval_ms`.
/// Panics if `interval_ms == 0`.
pub fn floor_to_tick(now_ms: u64, interval_ms: u64) -> u64 {
    assert!(interval_ms > 0, "tick interval must be > 0");
    (now_ms / interval_ms) * interval_ms
}

/// Compute the next tick boundary strictly greater than `now_ms`.
/// Panics if `interval_ms == 0`.
pub fn next_tick(now_ms: u64, interval_ms: u64) -> u64 {
    assert!(interval_ms > 0, "tick interval must be > 0");
    floor_to_tick(now_ms, interval_ms) + interval_ms
}
```

- [ ] **Step 4: Run and verify pass**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-sealer tick::tests
```
Expected: 5 passed (incl. `should_panic`).

- [ ] **Step 5: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-sealer/src/tick.rs
git commit -m "sealer: add 250ms wall-clock tick alignment helpers"
```

---

## Task 5: Leader election function (`election.rs`)

**Files:**
- Create: `crates/kardamom-sealer/src/election.rs`

The election rule:
> Leader = `min(host_id)` among recorders whose `fsynced_position >= current_B_position - caught_up_lag_bytes` **and** whose last watermark was observed within `caught_up_stale_ms` ms.

This is a **pure function** of inputs already in B + the watermark stream. No external KV, no PAXOS, no etcd. Each sealer evaluates the function independently and arrives at the same answer because inputs are deterministic at the granularity of the watermark stream.

**"Caught up" definition (load-bearing):**
- **Lag threshold:** `caught_up_lag_bytes` (default 65536 = 64 KB). At 1 M tx/s × ~200 B/tx = 200 MB/s on B, 64 KB ≈ 320 µs of in-flight data. Tight enough that a "caught up" recorder genuinely tracks the tail; loose enough that LAN jitter doesn't oscillate leadership.
- **Staleness:** `caught_up_stale_ms` (default 500 ms). A recorder whose watermark hasn't advanced in 500 ms is treated as unhealthy regardless of its last reported lag.
- Both conditions must hold. **Tie-breaker:** lowest `host_id`. There is exactly one winner per election input.

- [ ] **Step 1: Write failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use kardamom_log::BPosition;

    fn pos(term: i32, off: i32) -> BPosition {
        BPosition { term_id: term, term_offset: off }
    }

    #[test]
    fn picks_lowest_caught_up_id() {
        let set = CaughtUpSet::from_iter([
            RecorderState { host_id: 5, fsynced: pos(0, 1_000), last_seen_ms: 1_000 },
            RecorderState { host_id: 2, fsynced: pos(0, 1_000), last_seen_ms: 1_000 },
            RecorderState { host_id: 7, fsynced: pos(0, 1_000), last_seen_ms: 1_000 },
        ]);
        let leader = elect(&set, pos(0, 1_000), 1_100, 0, 500);
        assert_eq!(leader, Some(2));
    }

    #[test]
    fn skips_lagging_recorder() {
        // Recorder 2 is 1 MB behind the tail; threshold is 64 KB; skip.
        let set = CaughtUpSet::from_iter([
            RecorderState { host_id: 2, fsynced: pos(0, 0), last_seen_ms: 1_100 },
            RecorderState { host_id: 5, fsynced: pos(0, 1_000_000), last_seen_ms: 1_100 },
        ]);
        let leader = elect(&set, pos(0, 1_000_000), 1_100, 64 * 1024, 500);
        assert_eq!(leader, Some(5));
    }

    #[test]
    fn skips_stale_recorder() {
        // Recorder 2 last reported at t=100; now=1100; staleness threshold 500.
        let set = CaughtUpSet::from_iter([
            RecorderState { host_id: 2, fsynced: pos(0, 1_000), last_seen_ms: 100 },
            RecorderState { host_id: 5, fsynced: pos(0, 1_000), last_seen_ms: 1_100 },
        ]);
        let leader = elect(&set, pos(0, 1_000), 1_100, 64 * 1024, 500);
        assert_eq!(leader, Some(5));
    }

    #[test]
    fn returns_none_when_no_one_caught_up() {
        let set = CaughtUpSet::from_iter([
            RecorderState { host_id: 2, fsynced: pos(0, 0), last_seen_ms: 100 },
        ]);
        let leader = elect(&set, pos(0, 1_000_000), 1_100, 64 * 1024, 500);
        assert_eq!(leader, None);
    }

    #[test]
    fn handles_term_rollover() {
        // current is (1, 100); recorder still at (0, 1_000_000). Lag is huge — skip.
        let set = CaughtUpSet::from_iter([
            RecorderState { host_id: 2, fsynced: pos(0, 1_000_000), last_seen_ms: 1_100 },
            RecorderState { host_id: 5, fsynced: pos(1, 100),       last_seen_ms: 1_100 },
        ]);
        let leader = elect(&set, pos(1, 100), 1_100, 64 * 1024, 500);
        assert_eq!(leader, Some(5));
    }
}
```

- [ ] **Step 2: Run and verify failure**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-sealer election::tests
```

- [ ] **Step 3: Implement**

```rust
//! Deterministic leader election for the block sealer.
//!
//! Leader = lowest host_id among recorders whose watermark is "caught up" to the
//! current B tail, where "caught up" means
//!   (a) fsynced byte-position is within `caught_up_lag_bytes` of `current_position`, AND
//!   (b) the watermark was observed within `caught_up_stale_ms` ms.
//!
//! Pure function; no I/O. Every sealer that sees the same inputs computes the same
//! leader and arrives at the same election outcome independently.

use std::collections::BTreeMap;
use kardamom_log::BPosition;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecorderState {
    pub host_id: u16,
    pub fsynced: BPosition,
    pub last_seen_ms: u64,
}

/// Set of recorder states keyed by host_id. BTreeMap so iteration is host-id-ordered.
#[derive(Debug, Default, Clone)]
pub struct CaughtUpSet {
    by_host: BTreeMap<u16, RecorderState>,
}

impl CaughtUpSet {
    pub fn new() -> Self { Self::default() }

    pub fn insert(&mut self, s: RecorderState) {
        self.by_host.insert(s.host_id, s);
    }

    pub fn from_iter<I: IntoIterator<Item = RecorderState>>(iter: I) -> Self {
        let mut s = Self::new();
        for r in iter { s.insert(r); }
        s
    }

    pub fn states(&self) -> impl Iterator<Item = &RecorderState> {
        self.by_host.values()
    }
}

/// Compute `current_position - fsynced` as a signed byte-offset, treating both as
/// monotonic positions on a single canonical stream. Crosses term boundaries safely
/// because Aeron's stream position is `term_id * term_length + term_offset` — but
/// here we treat the absolute byte position carried by the watermark stream as the
/// canonical lag input. (S3 must publish absolute positions, not term-local offsets;
/// see plan §dependencies-assumed.)
fn lag_bytes(current: BPosition, recorder: BPosition) -> i64 {
    bpos_to_abs(current) as i64 - bpos_to_abs(recorder) as i64
}

/// Convert (term_id, term_offset) to absolute byte position.
/// Aeron's `Publication::position()` exposes this as i64; S3's `BPosition`
/// must round-trip to and from it. Term length is published as part of S3's
/// channel handshake; we use a single constant per channel — pass it as part
/// of `BPosition` or fix it here. For v0 we assume term_length is encoded
/// in BPosition's `to_absolute()` helper (S3 surface area).
fn bpos_to_abs(p: BPosition) -> u64 {
    // S3 must expose either an `as_absolute() -> i64` method or a free function.
    // If S3 names it differently, change here. The arithmetic is identical: each
    // term spans `term_length` bytes; absolute = term_id * term_length + term_offset.
    kardamom_log::position_to_absolute(p)
}

/// Decide which sealer should emit. Returns `None` if no recorder is caught up
/// (no boundaries are emitted; the chain pauses until quorum recovers).
pub fn elect(
    set: &CaughtUpSet,
    current_position: BPosition,
    now_ms: u64,
    caught_up_lag_bytes: u64,
    caught_up_stale_ms: u64,
) -> Option<u16> {
    set.states()
        .filter(|r| {
            let lag = lag_bytes(current_position, r.fsynced);
            // Negative lag means the recorder reports past the tail we know about;
            // treat as caught up (this happens benignly if a watermark arrives between
            // our reads).
            let caught_up = lag <= caught_up_lag_bytes as i64;
            let fresh = now_ms.saturating_sub(r.last_seen_ms) <= caught_up_stale_ms;
            caught_up && fresh
        })
        .min_by_key(|r| r.host_id)
        .map(|r| r.host_id)
}
```

- [ ] **Step 4: Add a proptest property file**

Create `crates/kardamom-sealer/tests/election_property.rs`:

```rust
//! Property tests over the leader election function.
//!
//! Properties:
//!   P1: determinism — same inputs always return the same leader (we run the
//!       function twice on the same input and assert equality).
//!   P2: monotonicity in host_id — if (host_id=h1, ...) wins, no recorder with
//!       host_id < h1 satisfied the caught-up predicate.
//!   P3: caught-up predicate respected — winner's lag must be <= threshold AND
//!       freshness must be within window.

use kardamom_log::BPosition;
use kardamom_sealer::election::{elect, CaughtUpSet, RecorderState};
use proptest::prelude::*;

prop_compose! {
    fn arb_recorder()(
        host_id in 1u16..1000,
        term in 0i32..10,
        off in 0i32..1_000_000,
        last_seen_ms in 0u64..1_000_000,
    ) -> RecorderState {
        RecorderState {
            host_id,
            fsynced: BPosition { term_id: term, term_offset: off },
            last_seen_ms,
        }
    }
}

proptest! {
    #[test]
    fn deterministic(
        recs in proptest::collection::vec(arb_recorder(), 0..20),
        cur_term in 0i32..10,
        cur_off in 0i32..1_000_000,
        now_ms in 0u64..2_000_000,
        lag in 0u64..1_000_000,
        stale in 0u64..1_000_000,
    ) {
        let set = CaughtUpSet::from_iter(recs);
        let cur = BPosition { term_id: cur_term, term_offset: cur_off };
        let a = elect(&set, cur, now_ms, lag, stale);
        let b = elect(&set, cur, now_ms, lag, stale);
        prop_assert_eq!(a, b);
    }

    #[test]
    fn winner_is_min_host_id_among_eligible(
        recs in proptest::collection::vec(arb_recorder(), 1..20),
        cur_term in 0i32..10,
        cur_off in 0i32..1_000_000,
        now_ms in 0u64..2_000_000,
        lag in 0u64..1_000_000,
        stale in 0u64..1_000_000,
    ) {
        let set = CaughtUpSet::from_iter(recs.clone());
        let cur = BPosition { term_id: cur_term, term_offset: cur_off };
        if let Some(winner) = elect(&set, cur, now_ms, lag, stale) {
            // No eligible recorder has a smaller host_id than the winner.
            for r in &recs {
                if r.host_id < winner {
                    let lag_b = kardamom_log::position_to_absolute(cur) as i64
                              - kardamom_log::position_to_absolute(r.fsynced) as i64;
                    let caught_up = lag_b <= lag as i64;
                    let fresh = now_ms.saturating_sub(r.last_seen_ms) <= stale;
                    prop_assert!(!(caught_up && fresh),
                        "host {} was eligible but did not win", r.host_id);
                }
            }
        }
    }
}
```

- [ ] **Step 5: Run and verify pass**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-sealer election
```
Expected: 5 unit tests + 2 property tests pass.

- [ ] **Step 6: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-sealer/src/election.rs crates/kardamom-sealer/tests/election_property.rs
git commit -m "sealer: add deterministic leader election with property tests"
```

---

## Task 6: Watermark tracker (`watermark_tracker.rs`)

**Files:**
- Create: `crates/kardamom-sealer/src/watermark_tracker.rs`

The tracker subscribes to all per-recorder watermark streams and maintains an in-memory `CaughtUpSet` updated on every incoming watermark. The election loop reads the latest snapshot per tick.

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use kardamom_log::BPosition;
    use crate::election::RecorderState;

    #[test]
    fn updates_in_place() {
        let tracker = WatermarkTracker::new(vec![1, 2, 3]);
        tracker.update(RecorderState {
            host_id: 2,
            fsynced: BPosition { term_id: 0, term_offset: 100 },
            last_seen_ms: 1_000,
        });
        let snap = tracker.snapshot();
        let r2 = snap.states().find(|r| r.host_id == 2).unwrap();
        assert_eq!(r2.fsynced.term_offset, 100);

        // Newer update overwrites.
        tracker.update(RecorderState {
            host_id: 2,
            fsynced: BPosition { term_id: 0, term_offset: 200 },
            last_seen_ms: 1_100,
        });
        let snap = tracker.snapshot();
        let r2 = snap.states().find(|r| r.host_id == 2).unwrap();
        assert_eq!(r2.fsynced.term_offset, 200);
    }

    #[test]
    fn ignores_unknown_host_id() {
        let tracker = WatermarkTracker::new(vec![1, 2]);
        tracker.update(RecorderState {
            host_id: 99,
            fsynced: BPosition { term_id: 0, term_offset: 100 },
            last_seen_ms: 1_000,
        });
        let snap = tracker.snapshot();
        assert!(snap.states().find(|r| r.host_id == 99).is_none());
    }
}
```

- [ ] **Step 2: Run and verify failure**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-sealer watermark_tracker::tests
```

- [ ] **Step 3: Implement**

```rust
//! Lock-protected snapshot of every recorder's latest watermark.
//!
//! One writer task per watermark subscription updates the map; the election loop
//! calls `snapshot()` once per tick. We use `parking_lot`-free `std::sync::Mutex`
//! because contention is low (one write per recorder per ms; one read per 250 ms).

use std::collections::BTreeMap;
use std::sync::Mutex;

use crate::election::{CaughtUpSet, RecorderState};

pub struct WatermarkTracker {
    expected: Vec<u16>,
    inner: Mutex<BTreeMap<u16, RecorderState>>,
}

impl WatermarkTracker {
    pub fn new(expected_host_ids: Vec<u16>) -> Self {
        Self { expected: expected_host_ids, inner: Mutex::new(BTreeMap::new()) }
    }

    /// Apply a watermark update. Updates with an unknown host_id (i.e. one not in
    /// the configured recorder set) are silently dropped, but logged.
    pub fn update(&self, state: RecorderState) {
        if !self.expected.contains(&state.host_id) {
            tracing::warn!(host_id = state.host_id, "watermark from unknown host_id; dropping");
            return;
        }
        let mut guard = self.inner.lock().expect("watermark mutex poisoned");
        // Monotonicity: never roll back a more recent observation. (Last_seen_ms
        // moves forward; fsynced moves forward.)
        match guard.get(&state.host_id) {
            Some(prev) if prev.last_seen_ms > state.last_seen_ms => return,
            _ => {}
        }
        guard.insert(state.host_id, state);
    }

    pub fn snapshot(&self) -> CaughtUpSet {
        let guard = self.inner.lock().expect("watermark mutex poisoned");
        CaughtUpSet::from_iter(guard.values().copied())
    }
}
```

- [ ] **Step 4: Run and verify pass**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-sealer watermark_tracker::tests
```

- [ ] **Step 5: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-sealer/src/watermark_tracker.rs
git commit -m "sealer: add WatermarkTracker maintaining per-recorder snapshot"
```

---

## Task 7: Bootstrap — read last `BlockBoundaryStart` from B tail (`bootstrap.rs`)

**Files:**
- Create: `crates/kardamom-sealer/src/bootstrap.rs`

On startup, the sealer scans backwards from B's tail until it sees the most recent `BlockBoundaryStart`. Its `block_number` field + 1 becomes our local counter's initial value. If the tail is empty (genesis), we start at `block_number = 1`.

We can't scan literally backwards from an Aeron stream, but S3 exposes `tail_scan(from: BPosition)` which yields messages forward from `from`. The bootstrap is: ask S3 for "the position N seconds back" (default: 30 s = 120 boundaries), scan forward, remember the last `BoundaryStart` seen, and adopt its `block_number + 1`. If none seen, scan further back; eventually conclude "genesis."

For v0 we use a fixed scan window. S3 should also expose a per-message lookup if it can support it; that's a follow-up.

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use kardamom_log::{BMessage, BPosition, BlockBoundaryStart};

    fn bs(n: u64, end_off: i32) -> BMessage {
        BMessage::BoundaryStart(BlockBoundaryStart {
            block_number: n,
            end_tx_idx: BPosition { term_id: 0, term_offset: end_off },
            l2_timestamp_ms: 0,
        })
    }

    fn tx_dummy() -> BMessage {
        // S3 must expose a constructor for a small dummy Tx variant; this test
        // exists to confirm we walk past tx messages and only track boundaries.
        kardamom_log::test_helpers::dummy_tx()
    }

    #[test]
    fn empty_tail_returns_genesis() {
        let scanned: Vec<(BPosition, BMessage)> = vec![];
        let next = next_block_number_from_scan(scanned.into_iter());
        assert_eq!(next, 1);
    }

    #[test]
    fn picks_max_block_plus_one() {
        let scanned = vec![
            (BPosition { term_id: 0, term_offset: 10 }, bs(7, 5)),
            (BPosition { term_id: 0, term_offset: 20 }, tx_dummy()),
            (BPosition { term_id: 0, term_offset: 30 }, bs(8, 25)),
        ];
        let next = next_block_number_from_scan(scanned.into_iter());
        assert_eq!(next, 9);
    }

    #[test]
    fn no_boundary_returns_genesis() {
        let scanned = vec![
            (BPosition { term_id: 0, term_offset: 20 }, tx_dummy()),
        ];
        let next = next_block_number_from_scan(scanned.into_iter());
        assert_eq!(next, 1);
    }
}
```

If `kardamom_log::test_helpers::dummy_tx()` doesn't exist in S3, drop tx-related test cases and rely solely on boundary messages — the property under test is "find the max block_number", not tx-filtering.

- [ ] **Step 2: Run and verify failure**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-sealer bootstrap::tests
```

- [ ] **Step 3: Implement**

```rust
//! Bootstrap the local `block_number` counter from B's tail.
//!
//! Scans forward from a recent position (S3 will compute "now − 30 s"); tracks the
//! highest `BlockBoundaryStart.block_number`; returns that + 1. Empty tail → 1.

use kardamom_log::{BMessage, BPosition};

/// Pure helper used by the async driver below; takes the iterator yielded by S3's
/// `tail_scan` and computes the next block_number.
pub fn next_block_number_from_scan(
    iter: impl IntoIterator<Item = (BPosition, BMessage)>,
) -> u64 {
    let mut max_seen: Option<u64> = None;
    for (_pos, msg) in iter {
        if let BMessage::BoundaryStart(b) = msg {
            max_seen = Some(max_seen.map_or(b.block_number, |m| m.max(b.block_number)));
        }
    }
    max_seen.map_or(1, |n| n + 1)
}

/// Driver: ask S3 for a tail scan, drain it, return the next block_number.
/// `lookback_ms` parameterizes how far back we ask S3 to start; default 30_000 ms.
pub async fn bootstrap_block_number(
    sub: &mut kardamom_log::channel_b::Subscriber,
    lookback_ms: u64,
) -> anyhow::Result<u64> {
    let from = sub.tx_data_positiont_wall_offset(lookback_ms).await?;
    let mut stream = sub.tail_scan(from).await?;
    let mut max_seen: Option<u64> = None;
    use futures::StreamExt;
    while let Some((_pos, msg)) = stream.next().await {
        if let BMessage::BoundaryStart(b) = msg {
            max_seen = Some(max_seen.map_or(b.block_number, |m| m.max(b.block_number)));
        }
    }
    Ok(max_seen.map_or(1, |n| n + 1))
}
```

- [ ] **Step 4: Add integration test for the driver**

Create `crates/kardamom-sealer/tests/bootstrap_tail_scan.rs`:

```rust
//! Integration test: bootstrap block_number from a mock channel-B stream.
//!
//! Uses kardamom-log's in-memory mock subscriber (which S3 must provide as part
//! of the channel-B handle's test interface).

use kardamom_log::test_helpers::MockChannelB;
use kardamom_log::{BlockBoundaryStart, BPosition};
use kardamom_sealer::bootstrap::bootstrap_block_number;

#[tokio::test]
async fn bootstrap_reads_max_block_number_from_tail() {
    let mut mock = MockChannelB::new();
    mock.publish_boundary(BlockBoundaryStart {
        block_number: 100,
        end_tx_idx: BPosition { term_id: 0, term_offset: 1_000 },
        l2_timestamp_ms: 1_000,
    });
    mock.publish_boundary(BlockBoundaryStart {
        block_number: 101,
        end_tx_idx: BPosition { term_id: 0, term_offset: 2_000 },
        l2_timestamp_ms: 1_250,
    });

    let mut sub = mock.subscriber();
    let next = bootstrap_block_number(&mut sub, 30_000).await.unwrap();
    assert_eq!(next, 102);
}

#[tokio::test]
async fn bootstrap_empty_tail_is_genesis() {
    let mock = MockChannelB::new();
    let mut sub = mock.subscriber();
    let next = bootstrap_block_number(&mut sub, 30_000).await.unwrap();
    assert_eq!(next, 1);
}
```

If S3's `MockChannelB` has a different surface, adapt names but preserve the asserted behaviour.

- [ ] **Step 5: Run and verify pass**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-sealer bootstrap
```

- [ ] **Step 6: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-sealer/src/bootstrap.rs crates/kardamom-sealer/tests/bootstrap_tail_scan.rs
git commit -m "sealer: bootstrap local block_number from B tail"
```

---

## Task 8: Emitter — leader-side publish loop (`emitter.rs`)

**Files:**
- Create: `crates/kardamom-sealer/src/emitter.rs`

The emitter is the leader's tick loop. It is *not* responsible for deciding whether this process is the leader — that's the supervisor's job (Task 9). When asked to run, it ticks; when asked to stop, it returns.

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::MockClock;
    use kardamom_log::test_helpers::MockChannelB;

    #[tokio::test(start_paused = true)]
    async fn emits_one_boundary_per_tick() {
        let mock = MockChannelB::new();
        let pub_handle = mock.publisher();
        let clock = MockClock::new(1_000);
        let mut emitter = BoundaryEmitter::new(
            pub_handle,
            clock.clone(),
            42,        // initial block_number
            250,       // tick_interval_ms
        );

        // Tick once: emits block 42 at l2_timestamp=1_000.
        emitter.run_one_tick().await.unwrap();
        let observed = mock.published_boundaries();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].block_number, 42);
        assert_eq!(observed[0].l2_timestamp_ms, 1_000);

        // Advance clock to mid-tick (1_125). Next tick floors to 1_000 (no, 1_250),
        // but we are explicitly testing run_one_tick which reads the current clock.
        clock.set(1_300);
        emitter.run_one_tick().await.unwrap();
        let observed = mock.published_boundaries();
        assert_eq!(observed.len(), 2);
        assert_eq!(observed[1].block_number, 43);
        assert_eq!(observed[1].l2_timestamp_ms, 1_250);
    }
}
```

- [ ] **Step 2: Run and verify failure**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-sealer emitter::tests
```

- [ ] **Step 3: Implement**

```rust
//! Boundary emission loop. Run only by the elected leader.
//!
//! Each tick:
//!   1. Read current B publication position (atomic).
//!   2. Compute `l2_timestamp = floor(now / 250) * 250`.
//!   3. Publish `BoundaryStart { block_number, end_tx_idx: cur_pos, l2_timestamp }`.
//!   4. Increment block_number.
//!
//! Backpressure: if `offer()` returns BACK_PRESSURED, retry with exponential backoff
//! up to 50 ms, then log and skip this tick. The next tick will produce a higher
//! block_number, so a missed publication shows up as a skipped block number — a
//! visible signal in observability dashboards.

use std::sync::Arc;
use std::time::Duration;

use kardamom_log::channel_b::Publisher;
use kardamom_log::{BMessage, BlockBoundaryStart};

use crate::clock::WallClock;
use crate::tick::floor_to_tick;

pub struct BoundaryEmitter<C: WallClock> {
    publisher: Publisher,
    clock: Arc<C>,
    block_number: u64,
    tick_interval_ms: u64,
}

impl<C: WallClock> BoundaryEmitter<C> {
    pub fn new(publisher: Publisher, clock: C, initial_block: u64, tick_ms: u64) -> Self {
        Self {
            publisher,
            clock: Arc::new(clock),
            block_number: initial_block,
            tick_interval_ms: tick_ms,
        }
    }

    /// Emit one boundary at the current wall-clock tick. Intended to be called
    /// once per `tick_interval_ms` by an outer timer loop. Returns the block
    /// number emitted, or an error if backpressure persisted past the retry
    /// budget.
    pub async fn run_one_tick(&mut self) -> anyhow::Result<u64> {
        let now = self.clock.unix_ms();
        let l2_ts = floor_to_tick(now, self.tick_interval_ms);
        let end_tx_idx = self.publisher.current_position();

        let msg = BMessage::BoundaryStart(BlockBoundaryStart {
            block_number: self.block_number,
            end_tx_idx,
            l2_timestamp_ms: l2_ts,
        });

        let mut backoff_ms = 1u64;
        let deadline = std::time::Instant::now() + Duration::from_millis(50);
        loop {
            match self.publisher.offer(&msg).await {
                Ok(()) => {
                    let emitted = self.block_number;
                    self.block_number += 1;
                    return Ok(emitted);
                }
                Err(kardamom_log::PublishError::BackPressured) => {
                    if std::time::Instant::now() >= deadline {
                        anyhow::bail!("backpressure on channel B persisted >50 ms; skipping tick");
                    }
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(8);
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
}
```

- [ ] **Step 4: Run and verify pass**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-sealer emitter::tests
```

- [ ] **Step 5: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-sealer/src/emitter.rs
git commit -m "sealer: add BoundaryEmitter with backpressure-aware tick publish"
```

---

## Task 9: Supervisor — assemble election + emitter (`sealer.rs`)

**Files:**
- Create: `crates/kardamom-sealer/src/sealer.rs`

The `Sealer` is the top-level supervisor. It spawns:
- a watermark-subscription task per recorder, feeding the tracker;
- a B-subscription task that observes incoming boundary markers (so a leader change picks up the right `block_number` even if this process wasn't bootstrapping cleanly);
- the main tick loop: on every tick boundary, evaluate `elect(...)`; if winner == self, call `emitter.run_one_tick()`; otherwise no-op.

- [ ] **Step 1: Write the failing test (single-emitter integration)**

Create `crates/kardamom-sealer/tests/single_emitter.rs`:

```rust
//! Three sealers; one mock log; assert exactly one publishes per tick.

use std::time::Duration;
use kardamom_log::test_helpers::MockChannelB;
use kardamom_log::test_helpers::MockWatermarkBus;
use kardamom_log::BPosition;
use kardamom_sealer::clock::MockClock;
use kardamom_sealer::election::RecorderState;
use kardamom_sealer::Sealer;
use kardamom_sealer::SealerConfig;

fn cfg(host_id: u16) -> SealerConfig {
    SealerConfig {
        host_id,
        channel_b_uri: "mock".into(),
        channel_tx_ordering_stream_id: 1,
        watermark_channel_uri: "mock".into(),
        watermark_stream_id_base: 2000,
        recorder_host_ids: vec![1, 2, 3],
        caught_up_lag_bytes: 64 * 1024,
        caught_up_stale_ms: 500,
        tick_interval_ms: 250,
    }
}

#[tokio::test(start_paused = true)]
async fn exactly_one_sealer_publishes_per_tick() {
    let channel = MockChannelB::new();
    let bus = MockWatermarkBus::new();
    let clock = MockClock::new(1_000);

    // All three recorders publish themselves as caught up.
    for hid in [1u16, 2, 3] {
        bus.publish(RecorderState {
            host_id: hid,
            fsynced: BPosition { term_id: 0, term_offset: 0 },
            last_seen_ms: 1_000,
        });
    }

    let s1 = Sealer::new(cfg(1), channel.handle(), bus.handle(), clock.clone()).await.unwrap();
    let s2 = Sealer::new(cfg(2), channel.handle(), bus.handle(), clock.clone()).await.unwrap();
    let s3 = Sealer::new(cfg(3), channel.handle(), bus.handle(), clock.clone()).await.unwrap();

    let h1 = tokio::spawn(s1.run());
    let h2 = tokio::spawn(s2.run());
    let h3 = tokio::spawn(s3.run());

    // Advance clock past 5 ticks (1.25 s).
    for _ in 0..5 {
        clock.advance(250);
        tokio::time::advance(Duration::from_millis(250)).await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    let published = channel.published_boundaries();
    assert_eq!(published.len(), 5, "expected exactly 5 boundaries, got {:?}", published);
    // Monotonic, contiguous block_numbers starting at 1.
    for (i, b) in published.iter().enumerate() {
        assert_eq!(b.block_number, (i as u64) + 1);
    }

    h1.abort(); h2.abort(); h3.abort();
}
```

- [ ] **Step 2: Run and verify failure**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-sealer --test single_emitter
```

- [ ] **Step 3: Implement `Sealer`**

```rust
//! Top-level sealer supervisor.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::task::JoinSet;

use kardamom_log::channel_b::{Subscriber as BSub, Publisher as BPub};
use kardamom_log::watermark::Subscriber as WSub;
use kardamom_log::BMessage;

use crate::bootstrap::bootstrap_block_number;
use crate::clock::WallClock;
use crate::config::SealerConfig;
use crate::election::{elect, RecorderState};
use crate::emitter::BoundaryEmitter;
use crate::tick::next_tick;
use crate::watermark_tracker::WatermarkTracker;

/// Handle bundle for the mock test channel bus and real Aeron channel bus.
/// Both impls provide these constructor methods on their handle type.
pub struct ChannelBHandle {
    pub publisher: BPub,
    pub subscriber: BSub,
}

pub struct WatermarkHandle {
    pub subscribers: Vec<WSub>, // one per recorder host_id
}

pub struct Sealer<C: WallClock + Clone> {
    cfg: SealerConfig,
    clock: C,
    tracker: Arc<WatermarkTracker>,
    emitter: BoundaryEmitter<C>,
    b_sub: BSub,
    w_subs: Vec<WSub>,
}

impl<C: WallClock + Clone> Sealer<C> {
    pub async fn new(
        cfg: SealerConfig,
        b_handle: ChannelBHandle,
        w_handle: WatermarkHandle,
        clock: C,
    ) -> Result<Self> {
        cfg.validate()?;

        let mut b_sub = b_handle.subscriber;
        let initial_block = bootstrap_block_number(&mut b_sub, 30_000).await?;

        let tracker = Arc::new(WatermarkTracker::new(cfg.recorder_host_ids.clone()));
        let emitter = BoundaryEmitter::new(
            b_handle.publisher,
            clock.clone(),
            initial_block,
            cfg.tick_interval_ms,
        );

        Ok(Self {
            cfg,
            clock,
            tracker,
            emitter,
            b_sub,
            w_subs: w_handle.subscribers,
        })
    }

    pub async fn run(mut self) -> Result<()> {
        // Spawn one watermark-update task per recorder.
        let mut tasks = JoinSet::new();
        for mut sub in self.w_subs.drain(..) {
            let tracker = self.tracker.clone();
            tasks.spawn(async move {
                while let Some(w) = sub.poll().await {
                    tracker.update(RecorderState {
                        host_id: w.host_id,
                        fsynced: w.fsynced_position,
                        last_seen_ms: w.wall_ts_micros / 1_000,
                    });
                }
            });
        }

        // Spawn a B-subscription task to keep our block_number in sync with what
        // any other leader has published (so a flapping leadership resumes at
        // max(block_observed) + 1).
        let observed_block = Arc::new(std::sync::atomic::AtomicU64::new(0));
        {
            let observed = observed_block.clone();
            let mut b_sub = std::mem::replace(&mut self.b_sub, unreachable_subscriber());
            tasks.spawn(async move {
                while let Some((_pos, msg)) = b_sub.poll().await {
                    if let BMessage::BoundaryStart(b) = msg {
                        let prev = observed.load(std::sync::atomic::Ordering::Relaxed);
                        if b.block_number > prev {
                            observed.store(b.block_number, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                }
            });
        }

        // Tick loop.
        loop {
            let now = self.clock.unix_ms();
            let next = next_tick(now, self.cfg.tick_interval_ms);
            let sleep_ms = next.saturating_sub(now);
            tokio::time::sleep(Duration::from_millis(sleep_ms)).await;

            let snap = self.tracker.snapshot();
            // Re-read current position from a B subscriber for the election input;
            // here we use the publisher's view, which agrees because both reference
            // the same channel.
            let cur = self.emitter.publisher().current_position();
            let leader = elect(
                &snap,
                cur,
                self.clock.unix_ms(),
                self.cfg.caught_up_lag_bytes,
                self.cfg.caught_up_stale_ms,
            );

            if leader == Some(self.cfg.host_id) {
                // Synchronize block_number with what's been observed on B (in case
                // we just took over leadership from a peer that emitted a higher block).
                let observed = observed_block.load(std::sync::atomic::Ordering::Relaxed);
                self.emitter.sync_block_number(observed + 1);
                if let Err(e) = self.emitter.run_one_tick().await {
                    tracing::warn!(error = %e, "boundary emit failed; will retry next tick");
                }
            }
        }
    }
}

fn unreachable_subscriber() -> BSub {
    panic!("subscriber moved twice — bug in sealer assembly");
}
```

Also extend `BoundaryEmitter` (in `emitter.rs`) with the small accessors used here:

```rust
impl<C: WallClock> BoundaryEmitter<C> {
    pub fn publisher(&self) -> &Publisher { &self.publisher }

    /// Adjust the local block counter forward if the observed B tail has a higher
    /// boundary than what we'd next emit. Never moves backwards.
    pub fn sync_block_number(&mut self, candidate: u64) {
        if candidate > self.block_number {
            self.block_number = candidate;
        }
    }
}
```

- [ ] **Step 4: Run and verify pass**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-sealer --test single_emitter
```
Expected: integration test passes; exactly 5 boundaries observed.

- [ ] **Step 5: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-sealer/src/sealer.rs crates/kardamom-sealer/src/emitter.rs crates/kardamom-sealer/tests/single_emitter.rs
git commit -m "sealer: assemble supervisor with election + emit + observe tasks"
```

---

## Task 10: Failover integration test

**Files:**
- Create: `crates/kardamom-sealer/tests/failover.rs`

Scenario: 3 sealers (host ids 1, 2, 3). Initially all caught up; host 1 is leader. We "kill" host 1 (drop its task; stop its watermark publication). Within ≤ `caught_up_stale_ms + tick_interval_ms` (≤ 750 ms by default), host 2 should take over. We assert:
- no duplicate `block_number` ever observed;
- the sequence of `block_number`s is monotonic and contiguous (no gaps after takeover);
- `l2_timestamp_ms` for the boundary emitted during the transition matches the wall-clock tick window — independent of which sealer happened to publish it.

- [ ] **Step 1: Write the test**

```rust
use std::time::Duration;
use kardamom_log::test_helpers::{MockChannelB, MockWatermarkBus};
use kardamom_log::BPosition;
use kardamom_sealer::clock::MockClock;
use kardamom_sealer::election::RecorderState;
use kardamom_sealer::{Sealer, SealerConfig};

fn cfg(host_id: u16) -> SealerConfig { /* identical to single_emitter::cfg */
    SealerConfig {
        host_id,
        channel_b_uri: "mock".into(),
        channel_tx_ordering_stream_id: 1,
        watermark_channel_uri: "mock".into(),
        watermark_stream_id_base: 2000,
        recorder_host_ids: vec![1, 2, 3],
        caught_up_lag_bytes: 64 * 1024,
        caught_up_stale_ms: 500,
        tick_interval_ms: 250,
    }
}

#[tokio::test(start_paused = true)]
async fn standby_takes_over_within_1s_after_leader_dies() {
    let channel = MockChannelB::new();
    let bus = MockWatermarkBus::new();
    let clock = MockClock::new(1_000);

    let publish_caught_up = |bus: &MockWatermarkBus, hid: u16, ts: u64| {
        bus.publish(RecorderState {
            host_id: hid,
            fsynced: BPosition { term_id: 0, term_offset: 0 },
            last_seen_ms: ts,
        });
    };
    for hid in [1u16, 2, 3] { publish_caught_up(&bus, hid, 1_000); }

    let s1 = Sealer::new(cfg(1), channel.handle(), bus.handle(), clock.clone()).await.unwrap();
    let s2 = Sealer::new(cfg(2), channel.handle(), bus.handle(), clock.clone()).await.unwrap();
    let s3 = Sealer::new(cfg(3), channel.handle(), bus.handle(), clock.clone()).await.unwrap();

    let h1 = tokio::spawn(s1.run());
    let h2 = tokio::spawn(s2.run());
    let h3 = tokio::spawn(s3.run());

    // Run 4 ticks under host-1 leadership.
    for i in 0..4 {
        clock.advance(250);
        publish_caught_up(&bus, 1, 1_000 + (i + 1) * 250);
        publish_caught_up(&bus, 2, 1_000 + (i + 1) * 250);
        publish_caught_up(&bus, 3, 1_000 + (i + 1) * 250);
        tokio::time::advance(Duration::from_millis(250)).await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Kill host 1: abort task + stop publishing its watermark.
    h1.abort();
    // Continue ticking; only hosts 2 and 3 keep publishing watermarks.
    for i in 4..10 {
        clock.advance(250);
        publish_caught_up(&bus, 2, 1_000 + (i + 1) * 250);
        publish_caught_up(&bus, 3, 1_000 + (i + 1) * 250);
        tokio::time::advance(Duration::from_millis(250)).await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    let published = channel.published_boundaries();
    // Block numbers are unique and monotonic.
    let mut numbers: Vec<u64> = published.iter().map(|b| b.block_number).collect();
    numbers.sort();
    numbers.dedup();
    assert_eq!(numbers.len(), published.len(), "duplicate block_numbers: {:?}", published);
    for (i, b) in published.iter().enumerate() {
        assert_eq!(b.block_number, (i as u64) + 1);
    }
    // l2_timestamps are aligned 250ms multiples starting at 1_000.
    for (i, b) in published.iter().enumerate() {
        assert_eq!(b.l2_timestamp_ms, 1_000 + (i as u64) * 250);
    }
    // Leader changed within 1 second of host-1 death (~4 ticks).
    assert!(published.len() >= 9, "too few boundaries after failover: {:?}", published);

    h2.abort(); h3.abort();
}
```

- [ ] **Step 2: Run and verify pass**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-sealer --test failover
```

- [ ] **Step 3: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-sealer/tests/failover.rs
git commit -m "sealer: failover test — standby takes over within 1s"
```

---

## Task 11: Chaos test — isolated leader yields on rejoin

**Files:**
- Create: `crates/kardamom-sealer/tests/chaos_isolation.rs`

Scenario: host 1 is leader; we simulate network isolation by stopping its outgoing publication AND stopping incoming watermarks for host 1 from the bus (so other sealers see host 1 go stale). Host 2 takes over. We then "rejoin" host 1: it resumes publishing watermarks, but several seconds have passed and host 1's `fsynced_position` is far behind the current tail. The election function should keep host 2 as leader (host 1 is no longer caught up); host 1 must not emit duplicate boundaries.

- [ ] **Step 1: Write the test**

```rust
use std::time::Duration;
use kardamom_log::test_helpers::{MockChannelB, MockWatermarkBus};
use kardamom_log::BPosition;
use kardamom_sealer::clock::MockClock;
use kardamom_sealer::election::RecorderState;
use kardamom_sealer::{Sealer, SealerConfig};

fn cfg(host_id: u16) -> SealerConfig {
    SealerConfig {
        host_id,
        channel_b_uri: "mock".into(),
        channel_tx_ordering_stream_id: 1,
        watermark_channel_uri: "mock".into(),
        watermark_stream_id_base: 2000,
        recorder_host_ids: vec![1, 2, 3],
        caught_up_lag_bytes: 64 * 1024,
        caught_up_stale_ms: 500,
        tick_interval_ms: 250,
    }
}

#[tokio::test(start_paused = true)]
async fn isolated_leader_yields_when_it_rejoins_behind_tail() {
    let channel = MockChannelB::new();
    let bus = MockWatermarkBus::new();
    let clock = MockClock::new(1_000);

    let pub_caught = |bus: &MockWatermarkBus, hid: u16, ts: u64, tail_off: i32| {
        bus.publish(RecorderState {
            host_id: hid,
            fsynced: BPosition { term_id: 0, term_offset: tail_off },
            last_seen_ms: ts,
        });
    };
    for hid in [1u16, 2, 3] { pub_caught(&bus, hid, 1_000, 0); }

    let s1 = Sealer::new(cfg(1), channel.handle(), bus.handle(), clock.clone()).await.unwrap();
    let s2 = Sealer::new(cfg(2), channel.handle(), bus.handle(), clock.clone()).await.unwrap();
    let s3 = Sealer::new(cfg(3), channel.handle(), bus.handle(), clock.clone()).await.unwrap();

    let h1 = tokio::spawn(s1.run());
    let h2 = tokio::spawn(s2.run());
    let h3 = tokio::spawn(s3.run());

    // 4 ticks under host-1 leadership.
    for i in 0..4 {
        clock.advance(250);
        for hid in [1u16, 2, 3] { pub_caught(&bus, hid, 1_000 + (i + 1) * 250, (i as i32 + 1) * 1024); }
        tokio::time::advance(Duration::from_millis(250)).await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Isolate host 1: stop its watermark publications. (We don't kill the task;
    // it's still running, still ticking, still trying to elect itself. The
    // election function must NOT elect host 1 because host 1's watermark is stale,
    // and once it rejoins, behind, host 2 must keep leadership.)
    for i in 4..8 {
        clock.advance(250);
        for hid in [2u16, 3] { pub_caught(&bus, hid, 1_000 + (i + 1) * 250, (i as i32 + 1) * 1024); }
        tokio::time::advance(Duration::from_millis(250)).await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Rejoin host 1: it starts publishing watermarks again, but its fsynced_position
    // is way behind the tail.
    for i in 8..12 {
        clock.advance(250);
        pub_caught(&bus, 1, 1_000 + (i + 1) * 250, 4 * 1024); // far behind
        for hid in [2u16, 3] { pub_caught(&bus, hid, 1_000 + (i + 1) * 250, (i as i32 + 1) * 1024); }
        tokio::time::advance(Duration::from_millis(250)).await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    let published = channel.published_boundaries();
    let mut numbers: Vec<u64> = published.iter().map(|b| b.block_number).collect();
    numbers.sort();
    let unique: std::collections::HashSet<u64> = numbers.iter().copied().collect();
    assert_eq!(unique.len(), numbers.len(), "duplicate block_numbers observed: {:?}", published);
    // Contiguous from 1.
    for (i, n) in numbers.iter().enumerate() {
        assert_eq!(*n, (i as u64) + 1, "non-contiguous block_numbers: {:?}", numbers);
    }

    h1.abort(); h2.abort(); h3.abort();
}
```

- [ ] **Step 2: Run and verify pass**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-sealer --test chaos_isolation
```

- [ ] **Step 3: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-sealer/tests/chaos_isolation.rs
git commit -m "sealer: chaos test — isolated leader yields after rejoin behind tail"
```

---

## Task 12: Criterion benchmark — boundary emit overhead

**Files:**
- Create: `crates/kardamom-sealer/benches/boundary_emit.rs`

The bench measures the per-tick overhead of `BoundaryEmitter::run_one_tick` against the mock publisher (so we are measuring the sealer's CPU work, not Aeron). Target: sub-µs per tick.

- [ ] **Step 1: Write the bench**

```rust
use criterion::{criterion_group, criterion_main, Criterion};
use kardamom_log::test_helpers::MockChannelB;
use kardamom_sealer::clock::MockClock;
use kardamom_sealer::emitter::BoundaryEmitter;

fn bench_emit(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    c.bench_function("boundary_emit_one_tick", |b| {
        let channel = MockChannelB::new();
        let clock = MockClock::new(1_000);
        let mut emitter = BoundaryEmitter::new(channel.publisher(), clock.clone(), 1, 250);

        b.to_async(&rt).iter(|| async {
            emitter.run_one_tick().await.unwrap();
        });
    });
}

criterion_group!(benches, bench_emit);
criterion_main!(benches);
```

- [ ] **Step 2: Run the bench**

```bash
cd /home/dev/kardamom && cargo bench -p kardamom-sealer --bench boundary_emit
```
Expected: benchmark produces a report; per-iteration time should be well under 1 µs once the mock publisher is realistic. (If the mock is heavier than that, the bench still passes; record the number and follow up in S3 if it's a concern.)

- [ ] **Step 3: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-sealer/benches/boundary_emit.rs
git commit -m "sealer: add criterion bench for boundary-emit overhead"
```

---

## Task 13: CLI entry point (`bin/kardamom-sealer.rs`)

**Files:**
- Create: `crates/kardamom-sealer/src/bin/kardamom-sealer.rs`

- [ ] **Step 1: Write the CLI**

```rust
use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use kardamom_sealer::clock::SystemClock;
use kardamom_sealer::{Sealer, SealerConfig};

#[derive(Parser, Debug)]
#[command(version, about = "kardamom block sealer")]
struct Args {
    /// Path to the sealer TOML config.
    #[arg(short, long)]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let cfg: SealerConfig = toml::from_str(&std::fs::read_to_string(&args.config)?)?;
    cfg.validate()?;

    tracing::info!(host_id = cfg.host_id, "starting sealer");

    // S3 builds the channel-B handle and watermark handle from the URIs + stream ids.
    let b_handle = kardamom_log::channel_b::connect(
        &cfg.channel_b_uri,
        cfg.channel_tx_ordering_stream_id,
    ).await?;
    let w_handle = kardamom_log::watermark::connect_all(
        &cfg.watermark_channel_uri,
        cfg.watermark_stream_id_base,
        &cfg.recorder_host_ids,
    ).await?;

    let sealer = Sealer::new(cfg, b_handle, w_handle, SystemClock).await?;
    sealer.run().await
}
```

- [ ] **Step 2: Verify it compiles**

```bash
cd /home/dev/kardamom && cargo build -p kardamom-sealer --bin kardamom-sealer
```

- [ ] **Step 3: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-sealer/src/bin/kardamom-sealer.rs
git commit -m "sealer: add kardamom-sealer CLI binary"
```

---

## Task 14: Wire metrics

**Files:**
- Modify: `crates/kardamom-sealer/src/sealer.rs`
- Modify: `crates/kardamom-sealer/src/emitter.rs`

Counters and gauges to expose (using the `metrics` crate, prom-scraped via the existing kardamom Prometheus setup):

- `sealer_boundaries_emitted_total{host_id}` — counter
- `sealer_election_winner{host_id}` — gauge, 1 if this process is current leader, else 0
- `sealer_tick_skipped_total{host_id, reason="backpressure"}` — counter
- `sealer_block_number{host_id}` — gauge

- [ ] **Step 1: Add metric emission to the tick loop**

In `sealer.rs`, after each election:

```rust
metrics::gauge!("sealer_election_winner", "host_id" => self.cfg.host_id.to_string())
    .set(if leader == Some(self.cfg.host_id) { 1.0 } else { 0.0 });
```

After a successful emit (caller already returns `Ok(block_number)`):

```rust
metrics::counter!(
    "sealer_boundaries_emitted_total",
    "host_id" => self.cfg.host_id.to_string()
).increment(1);
metrics::gauge!(
    "sealer_block_number",
    "host_id" => self.cfg.host_id.to_string()
).set(block as f64);
```

In `emitter.rs`'s backpressure-bailout branch:

```rust
metrics::counter!(
    "sealer_tick_skipped_total",
    "host_id" => "self".to_string(),
    "reason" => "backpressure"
).increment(1);
```

(Host id is not in scope in `emitter.rs`; either thread it through `BoundaryEmitter::new` or accept the placeholder. Threading it is one extra constructor arg — do that.)

- [ ] **Step 2: Verify compile**

```bash
cd /home/dev/kardamom && cargo check -p kardamom-sealer
```

- [ ] **Step 3: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-sealer/src/sealer.rs crates/kardamom-sealer/src/emitter.rs
git commit -m "sealer: emit Prometheus metrics for tick / election / emission"
```

---

## Task 15: Full crate test sweep + final commit

- [ ] **Step 1: Run everything**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-sealer
cd /home/dev/kardamom && cargo bench -p kardamom-sealer --bench boundary_emit -- --quick
cd /home/dev/kardamom && cargo clippy -p kardamom-sealer -- -D warnings
cd /home/dev/kardamom && cargo fmt --all -- --check
```
Expected: tests pass, clippy clean, fmt clean.

- [ ] **Step 2: If clippy/fmt nits, fix and commit**

```bash
cd /home/dev/kardamom && cargo fmt --all
cd /home/dev/kardamom && cargo clippy -p kardamom-sealer --fix --allow-dirty
git add -u
git commit -m "sealer: clippy + fmt cleanup"
```

- [ ] **Step 3: Push branch**

```bash
git push -u origin claude/s5-block-sealer
```

- [ ] **Step 4: Open PR against `main`** (only when S3 has merged; otherwise mark draft)

```bash
gh pr create --title "S5: block sealer (kardamom-sealer crate)" --body "$(cat <<'EOF'
## Summary
- New `kardamom-sealer` crate implementing the S5 subsystem from the high-throughput sequencer spec.
- Deterministic leader election (lowest host id among caught-up recorders); zero durable state outside channel B.
- 250ms-aligned tick loop emits `BlockBoundaryStart` markers via the channel-B concurrent publisher.

## Test plan
- [ ] `cargo test -p kardamom-sealer` (unit, property, integration, chaos)
- [ ] `cargo bench -p kardamom-sealer --bench boundary_emit`
- [ ] Manual: bring up 3 sealers + S3 mock; kill the leader; confirm takeover within 1 s
EOF
)"
```

---

## Self-review checklist

After implementation, the implementer (or a reviewer) should confirm:

1. **Spec coverage:**
   - §2.6 "Block sealer" — singleton + hot standby (Task 9 supervisor), lowest-host-id election (Task 5), 250ms tick to B (Tasks 4, 8), bootstrap from B tail (Task 7). All covered.
   - §4.5 "Sealer failover" — Task 10 (failover test), Task 11 (isolation chaos test).
   - V0 "all features" — every feature in the spec's §2.6 is in the plan.
2. **No placeholders:** every step has working code. No TODOs.
3. **Type consistency:** `BPosition`, `BMessage`, `BlockBoundaryStart` are referenced consistently across tasks. The assumed-interfaces block at the top documents the exact names; if S3 differs, update one place and propagate.
4. **Determinism invariant (spec I3):** `l2_timestamp_ms` is `floor(wall_clock_ms / 250) * 250`. Two sealers in the same 250ms window will produce the same timestamp, preserving determinism across leader change. Asserted in `failover.rs`.

---

## Open questions (forwarded from spec)

These are explicitly out of scope for this plan but must be tracked:

- **PTP vs NTP wall clock:** v0 uses host `SystemTime` (NTP/chrony assumed). PTP is a follow-up if cross-host clock skew becomes the dominant `l2_timestamp` non-determinism source.
- **"Caught up" lag threshold tuning:** the default 64 KB / 500 ms is a first guess. Real numbers come from production load testing.
- **External lease store (etcd/Aeron-Cluster):** spec "Out-of-scope / follow-ups" calls this out. If deterministic election proves flaky in practice (split-brain on watermark stream reordering), this is the documented upgrade path.
