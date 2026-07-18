# Fixes — WP-CFG (cross-cutting cluster config; ran last, alone)

Findings: F12.8, F12.9, F12.10, F12.4, F05.3 — plus two WP-OPS hand-offs
(F16.6 remainder: log-config port validation; F15.7b remainder: scrape
visibility).

Ownership note: edits stay inside the parent-granted surface —
`crates/cluster-adapter/**`, the ClusterConfig definitions in
engine/sequencer/ingress, `deploy/cluster/scripts/check-contract.py`, plus the
hand-off files `crates/log/src/config.rs` and `crates/bench/src/load/scrape.rs`
and the deploy templates under `deploy/cluster/config/` (explicitly directed
for F12.9). Two adjacent files were touched as strictly-required fallout:
- `crates/executor/src/config.rs` — it is the re-export shim of the shared
  ClusterConfig definition; its doc comment and three unit tests referenced the
  removed `enabled` field and would not compile otherwise.
- `crates/cluster-adapter/Cargo.toml` — added `serde.workspace = true` (the
  shared `[cluster]` section type now lives in this crate) and `toml` as a
  dev-dependency (for the new config parse tests). Noted per the shared rules.

New file: `crates/cluster-adapter/src/config.rs` (the single shared
`ClusterConfig`).

## Per-finding status

### F12.8 [L] — ClusterConfig + defaults_applied + to_live defined 3× — FIXED
One definition now lives in `kardamom-cluster-adapter`
(`crates/cluster-adapter/src/config.rs`, re-exported from the crate root),
next to the `LiveClusterConfig` that `to_live()` maps onto — so the magic
stream-id/keepalive defaults (101/102/1000) exist in exactly one place. The
three former copies are replaced by re-exports, keeping every existing path
resolving:
- `crates/engine/src/reader/cluster.rs` — `pub use kardamom_cluster_adapter::ClusterConfig;`
  (executor's `kardamom_executor::config::ClusterConfig` re-export and the
  validator's import both continue to work through it).
- `crates/sequencer/src/config.rs` — same re-export (the shared struct derives
  `Serialize, PartialEq, Eq`, preserving `SequencerConfig`'s derives and its
  `toml_round_trip` test).
- `crates/ingress/src/config.rs` — same re-export (`defaults_applied` was
  private there; the shared one is pub — behaviour unchanged).
Unit tests for defaults/`to_live` mapping/legacy-key tolerance added in the new
module. No other restructuring.

### F12.9 [L] — `enabled` knob parsed+deployed but ignored; empty [cluster] fails only at runtime — FIXED
Removed, both sides, and made the empty-section failure a clean startup error:
- The `enabled: bool` field is gone from the shared `ClusterConfig`. Old config
  files carrying `enabled = true` still parse (the struct is `#[serde(default)]`
  without `deny_unknown_fields`, so the legacy key is ignored) — covered by
  `legacy_enabled_key_is_tolerated` and the executor's `cluster_section_parses`
  test which deliberately keeps the key in its TOML fixture.
- `enabled = true` removed from all three deploy templates:
  `deploy/cluster/config/{sequencer.toml.tpl,executor.toml,ingress.toml}`
  (executor.toml's "when enabled …" comment reworded to cluster-only).
- Validation: `crates/cluster-adapter/src/live.rs` gained `validate_config()`,
  called by `connect()`/`connect_with_replay()` — empty `egress_channel` or an
  `ingress_endpoints` with no parseable `memberId=host:port` entries now fails
  the connect call, which every binary already propagates with context
  (`connect cluster …`), i.e. a hard startup error instead of a dead session
  thread. Ingress is only affected when `--ack-policy` actually requires the
  quorum gate — exactly the scope that needs the section. Unit-tested
  (`validate_rejects_empty_egress_channel`,
  `validate_rejects_empty_or_unparseable_ingress_endpoints`).

### F12.10 [L] — check-contract.py skips the ingress side of the cluster wiring — FIXED
`deploy/cluster/scripts/check-contract.py`: `"ingress.toml"` added to the
config-template loop (ingress_endpoints + both stream ids now asserted there
too) and `"ingress"` added to the `--cluster-egress-endpoint
${meta.node_ip}:<port>` job loop; the "mirrored in three places" comment now
lists four. Verified: clean run passes, and a seeded drift
(`ingress_stream_id = 999` in ingress.toml) is caught with the expected
one-line violation, then restored.

### F12.4 [M] — initial open_leader_pub failure only logs and kills the session thread — FIXED
`crates/cluster-adapter/src/live.rs`: the initial ingress publication is now
opened in `connect_inner` (i.e. on the caller, before the session thread is
spawned) and its failure returns `LiveError` — the owning binary fails startup
via its existing `.context("connect cluster …")?` instead of running with a
silently dead cluster session. The open also falls through dead member ids
via the same `open_next_member_pub` rotation the reconnect path uses (any live
member redirects to the leader), so a merely-down initial leader doesn't fail
startup. `run_session` takes the `(member, PubHandle)` pair and no longer has
a log-and-return path.

### F05.3 [L] — blocking publish_bytes (10s ack) on the session loop; Result discarded — FIXED
`crates/cluster-adapter/src/live.rs`: the retrying replay-request publish now
runs on a short-lived named helper thread (`cluster-replay-pub`), so the
Aeron-ack wait (up to 10s under exactly the reconnect-churn backpressure this
send fires in) can never delay the session loop's keep-alives or egress
draining. At most one send is in flight (`replay_send_inflight`, checked via
`JoinHandle::is_finished`; the 3s resend cadence simply re-checks), and the
`Result` is no longer discarded — a failed/timed-out publish logs
`cluster replay request publish failed (will resend)`. The retrying (non-
best-effort) semantics the original fix wanted are preserved.

### F16.6 remainder [hand-off] — tx_receipts_endpoint_base_port silently wraps via `as u32` — FIXED
`crates/log/src/config.rs`: new `ChannelsConfig::validate()` — when receipts
MDS is enabled (`tx_receipts_control_channel` set), the base port must satisfy
`0 < base` and `base + 2*tx_receipts_executor_count + 1 <= 65535` (the highest
per-replica boundary endpoint must stay a valid port). Called from
`LogConfig::from_toml_path` (the single loader behind `--log-config`), so a
negative/zero/overflowing port now fails at load time with a path-carrying
`LogError::Config` instead of wrapping into a nonsense endpoint. The field
stays `i32` (TOML-friendly signed parsing; changing the public type was not
needed once the loader validates). Regression tests:
`mds_nonpositive_base_port_rejected`, `mds_base_port_overflowing_u16_rejected`,
`mds_valid_base_port_accepted_and_non_mds_port_unchecked`.

### F15.7b remainder [hand-off] — failed scrapes silently missing from service_up — FIXED
`crates/bench/src/load/scrape.rs`: every attempted scrape now yields a
`service_up` entry. A failed fetch (exporter unreachable) records an explicit
`Some(0)` (down) for executor, ingress AND sequencer nodes — previously
executor/ingress recorded `None` (which the non-chaos end-of-run liveness gate
in `accounting.rs` ignores) and failed sequencer nodes were omitted entirely.
Semantics now documented on the field: `Some(1)` up, `Some(0)` down or scrape
failed, `None` scraped-but-metric-absent, absent-from-list never scraped — so
chaos assertions can distinguish "down" from "never scraped".

## Verification

- `cargo check --workspace --all-targets` — PASS (only the pre-existing
  proc-macro-error2 future-incompat note).
- `cargo test -p kardamom-cluster-adapter` — PASS (18 lib + 2 integration).
- `cargo test -p kardamom-log --lib` — PASS (29, incl. 3 new).
- `cargo test -p kardamom-sequencer --lib` — PASS (45).
- `cargo test -p kardamom-executor --lib` — PASS (3 config tests, updated).
- `cargo test -p kardamom-ingress --lib` — PASS (45).
- `cargo test -p kardamom-engine --lib` — PASS (55).
- `cargo test -p kardamom-validator --lib` — PASS (15).
- `cargo test -p kardamom-bench --lib` — PASS (43).
- `cargo clippy -p <all touched crates> --all-targets` — clean (one new
  `too_many_arguments` on `run_session` allowed with the repo's usual
  annotation).
- `python3 -m py_compile deploy/cluster/scripts/check-contract.py` — PASS;
  full run exits 0 ("all mirrored values agree"); negative test (seeded
  ingress.toml stream-id drift) exits 1 with the expected violation, then
  restored to a passing state. The transient `__pycache__` was removed.
