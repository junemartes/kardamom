# Spec: `--log-config` plumbing (#36) + deployable recorder/quorum process (#38)

- **Date:** 2026-06-12
- **Status:** Historical — **superseded** by archive-at-the-sealer durability.
  The recorder/quorum machinery this spec designed (`kardamom-recorder`,
  `run_watermark_loop`, `WatermarkPublisher`, `Recorder::start_a/start_b`,
  Q-of-N aggregation) was later removed; the sealer's Aeron Cluster members
  fold ordering + durability into the Raft log + archive, and the durable
  watermark is the sealer archive's position. The `--log-config` plumbing
  (#36) lives on. Kept as a point-in-time design record — do not implement
  from it.
- **Branch:** `claude/nomad-ansible-deploy` (PR #35)
- **Issues:** #36 (channels config plumbing), #38 (recorder process), resolves #37
  (per-node rendering) by design; #39 (live batcher) stays out of scope.

## Goal

The kardamom service binaries hardcode `LogConfig::default()` (IPC channel
URIs), so the multi-host Nomad/Ansible cluster (PR #35) cannot pass a
transaction across VMs; and the recorder/quorum durability machinery
(`Recorder`, `run_watermark_loop`, `QuorumState`/aggregation, ingress
`--ack-policy on-quorum` gating) exists only as library code with **zero live
call sites** — no deployable process publishes fsync or quorum watermarks.
This spec adds (1) a uniform `--log-config <toml>` flag that injects a
`LogConfig` into every channel-using binary, and (2) a `kardamom-recorder`
binary that records `tx_ordering` on each recorder node, publishes its fsync
watermark, and (in aggregator mode) publishes the Q-of-N quorum watermark —
so a 3-recorder cluster satisfies `on-quorum` acks, and the whole stack can be
exercised end-to-end in CI.

## Non-Goals

- **Batcher live L1 broadcast (#39).** The batcher opens no Aeron channels;
  it keeps its offline/dry-run shape and does not grow `--log-config`.
- **tx_data (A-channel) quorum.** Per the existing design comments, tx_data is
  single-host durability; only `tx_ordering` (B) is quorum-recorded. The
  recorder binary *can* record A (`--kind tx-data`), but no A-watermark
  aggregation is added and the cluster deploys B-recording only.
- **`on-local-fsync` in the cluster topology.** Ingress has no co-located
  recorder in the cluster; the shared-multicast watermark channel makes
  "local" indistinguishable anyway. Documented; IPC single-host behavior
  unchanged.
- **Production hardening**: no TLS/auth on archive control, no cloud
  (non-multicast) fabric support, no metrics additions.

## Design

### Part 1 — `LogConfig` loading (#36)

1. `LogConfig`, `AeronConfig`, `ChannelsConfig`, `QuorumConfig` gain
   `#[serde(default, deny_unknown_fields)]` and standalone `Default` impls
   (factored out of today's monolithic `LogConfig::default()`). A TOML file
   may then specify any subset — e.g. only `[channels]` — and every missing
   field falls back to the current defaults. `deny_unknown_fields` catches
   typos in operator-rendered configs.
2. New helper `LogConfig::from_toml_path(&Path) -> Result<LogConfig, LogError>`
   (read + `toml::from_str`, errors carry the path).
3. Every channel-using binary (`ingress`, `sequencer`, `executor`, `sealer`,
   `da_watcher`, and the new `recorder`) gains
   `#[arg(long, env = "KARDAMOM_LOG_CONFIG")] log_config: Option<PathBuf>`
   (the workspace clap already enables the `env` feature). Resolution:
   `log_config.map(LogConfig::from_toml_path).transpose()?.unwrap_or_default()`.
   Flag unset ⇒ behavior is bit-for-bit today's (multiprocess_e2e unchanged).
4. Precedence (most- to least-specific): existing CLI flags
   (`--aeron-dir`, sealer's `SealerConfig` channel overrides, ingress
   `--recorder-id`) > `--log-config` file > built-in defaults. The sealer
   continues to overwrite `tx_ordering_channel`/`stream_id` from its own
   config — one source for the B-channel URI, now consistent with the loaded
   base.

### Part 2 — `kardamom-recorder` (#38)

A new binary target in `kardamom-log` (`crates/log/src/bin/kardamom-recorder.rs`)
— the recorder is log-subsystem machinery, and all its building blocks
(`Recorder`, `run_watermark_loop`, `WatermarkPublisher`, `QuorumState`,
`QuorumPublisher`) already live there.

CLI:

```
kardamom-recorder
  --log-config <toml>          # KARDAMOM_LOG_CONFIG; channels/quorum/aeron
  --recorder-id <u8>           # required; quorum identity (node meta in cluster)
  --aeron-dir <path>           # override aeron.dir (same as other services)
  --kind tx-ordering|tx-data   # default tx-ordering
  --sequencer-id <u8>          # required iff --kind tx-data
  --record / --no-record       # default --record
  --aggregate                  # also run the quorum aggregation loop
  --poll-interval-ms <u64>     # watermark poll cadence, default 1
```

Single OS thread (rusteron archive handles are `!Send + !Sync`):

1. Connect an Aeron client to the node-local media driver (`set_dir`).
2. Connect `AeronArchive` via `AeronArchiveContext` +
   `set_control_request_channel`/`set_control_response_channel`. Two new
   `AeronConfig` fields carry these URIs:
   `archive_control_request_channel` (default
   `aeron:udp?endpoint=localhost:8010` — matches the aeron image) and
   `archive_control_response_channel` (default
   `aeron:udp?endpoint=localhost:0`).
3. **Idempotent recording start**: `find_last_matching_recording` for
   (channel, stream); adopt it if present (recordings outlive the client that
   started them — `auto_stop=false`), else `Recorder::start_b()`/`start_a()`.
   A small `Recorder::adopt(...)` constructor is added for this.
4. Run `run_watermark_loop` — polls `get_recording_position` (byte-durable
   under the archive's `fileSyncLevel=1`) and publishes monotonic
   `FsyncWatermark{recorder_id, position}`.
5. `--aggregate`: a sibling loop on the same thread subscribes the fsync
   watermark channel(s), feeds `QuorumState::observe`, and publishes
   `QuorumWatermark` on change — same semantics as the existing
   `QuorumAggregator`, factored into a sync `run_quorum_loop` both can share.
   Ingress consumes it through its existing, untouched gating path.
6. SIGTERM/SIGINT → clean shutdown (recording continues archive-side).

Topology: every recorder node runs `kardamom-recorder` (record + watermark);
**one** instance cluster-wide runs `--aggregate --no-record` (the quorum job,
pinned to a recorder node by Nomad). The aggregator is a liveness-only
singleton: if it dies, acks stall until Nomad restarts it — safety (Q durable
copies) never depends on it.

### Part 3 — cluster channel plan: multicast (resolves #37)

The B channel has multiple publishers (2 sequencers + sealer, on two hosts)
and multiple subscribers (executor, sealer tail, 3 recorder archives) — UDP
unicast cannot express this and MDC requires one control endpoint per
publisher. **UDP multicast** expresses every kardamom channel with a single
URI valid on every node, which also dissolves issue #37 (no per-node
rendering; one shared `channels.toml` everywhere):

| channel | group:port (even ports; +1 reserved for control) | streams |
|---|---|---|
| tx_data | `239.192.56.10:40000` | 2000+sid |
| tx_ordering | `239.192.56.11:40010` | 1001 |
| tx_receipts | `239.192.56.12:40020` | 1002 (+1003 boundaries) |
| tx_errors | `239.192.56.13:40030` | 1015 |
| tx_deposits | `239.192.56.14:40040` | 1016 |
| fsync watermark (B) | `239.192.56.15:40050` | 1010 |
| fsync watermark (A) | `239.192.56.16:40060` | 1030+sid |
| quorum watermark | `239.192.56.17:40070` | 1020 |

All URIs carry `|interface=192.168.56.0/24` (the cluster subnet from
`group_vars/all.yml`) and `|ttl=1`. Templates keep `{sid}`/`{rid}` only in the
`alias` label — same group, distinct stream ids — so the existing template
expansion code is untouched. Multicast works on the deployment fabrics this
cluster targets (libvirt/VirtualBox host-only networks, Linux docker bridges
in CI); cloud fabrics without multicast are explicitly out of scope here and
would swap the rendered URIs, not the code.

Cluster wiring: `config/channels.toml.tpl` is rewritten to the new
`[channels]`/`[quorum]` schema with the table above; every service job passes
`--log-config /local/channels.toml`; new `recorder.system.nomad.hcl`
(constraint `role=recorder`, `--recorder-id ${meta.recorder_id}`) and
`quorum.nomad.hcl` (count=1, `--aggregate --no-record`); `deploy.sh` gains a
recorder/quorum phase; the ingress job's `ack_policy` default flips to
`on-quorum`; README "Required service changes" prunes items 1/3/4.

### Part 4 — cluster e2e in CI

A new gated workflow (`cluster-e2e.yml`, label `run-cluster-e2e` or main,
mirroring `docker-e2e.yml`'s gating) runs the real Ansible → Nomad → smoke
path on a GitHub runner using **containers as nodes**: five privileged
systemd+DinD Ubuntu containers on a `192.168.56.0/24` docker bridge with the
contract IPs, provisioned by the unmodified `site.yml` (host-global sysctls
applied once on the runner; the role's sysctl task gated behind a var).
Service images are assembled from binaries **prebuilt once on the runner**
(`cargo build --release`, Swatinem cache — the in-image workspace build is
too slow for CI) via a thin CI-only Dockerfile. Then: `deploy.sh` against
r1's Nomad, the transaction smoke test with `on-quorum` acks, and the
redundancy scenario — stop one recorder alloc, submit again, assert acks
still flow (Q=2 of N=3), restart, assert recovery.

## Interfaces

- `LogConfig::from_toml_path(path: &Path) -> Result<LogConfig, LogError>`
- `AeronConfig` += `archive_control_request_channel: String`,
  `archive_control_response_channel: String` (serde-defaulted)
- `Recorder::adopt(archive, kind, recorder_id, recording_id, archive_dir)`
- `run_quorum_loop(subs, &QuorumState, &QuorumPublisher, should_stop)` (sync;
  `QuorumAggregator` becomes a thin tokio wrapper around it)
- `--log-config` / `KARDAMOM_LOG_CONFIG` on six binaries; `kardamom-recorder`
  CLI as above
- TOML schema: optional top-level `recorder_id`, `[aeron]`, `[channels]`,
  `[quorum]` — any subset, unknown fields rejected

## Ethereum / external spec references

- Receipts asserted by the smoke path carry `status` per **EIP-658**; no
  EVM-visible behavior changes in this work.
- Aeron: multicast channel URIs and even-port/control-port convention, MDS/MDC
  semantics, and Archive `fileSyncLevel` durability are per the Aeron
  documentation (aeron.io docs; `fileSyncLevel=1` ⇒ `fdatasync` per recorded
  frame, making `get_recording_position` byte-durable — already relied on by
  `crates/log/src/recorder.rs`).

## Testing Strategy

Unit (deterministic, no Aeron): config loading matrix (empty file, partial
`[channels]`, full file, unknown field rejection, env-var fallback, missing
file error), per-binary precedence (CLI > file > default), recorder CLI
validation (`--kind tx-data` requires `--sequencer-id`; `--no-record`
without `--aggregate` rejected), `run_quorum_loop` over the existing fake bus
(quorum advance, duplicate-rid dedup on a shared channel, monotonicity,
recorder-death liveness — extends `watermark_quorum.rs`).

Integration (docker-gated, same tier as existing `docker-e2e`):
`multiprocess_quorum_e2e` — one Aeron container, three `kardamom-recorder`
processes (distinct rids) + one `--aggregate` instance + the four pipeline
services launched with a rendered `--log-config`, ingress at
`--ack-policy on-quorum`; assert transfer receipts ack only after quorum, then
SIGKILL one recorder and assert acks continue (Q=2). Determinism per the
existing multiprocess harness conventions (single test thread, bounded polls
with deadlines, no bare sleeps for correctness — only for liveness waits).

System (gated CI workflow): the Part-4 cluster e2e — Ansible converge, jobs
running, on-quorum smoke, recorder-kill redundancy.

## Alternatives Considered

1. **Quorum aggregation embedded in ingress** (no aggregator process):
   simplest process count, but it changes the sensitive ack-gating data flow,
   makes future quorum consumers re-implement aggregation, and diverges from
   the already-designed `quorum_watermark` channel contract. Rejected: the
   singleton `--aggregate` mode reuses the existing contract with zero ingress
   changes.
2. **MDC (control-mode=dynamic) + per-node channel rendering** for cross-host
   fan-out (the original PR #35 sketch): requires a control endpoint per
   publisher host — unworkable for multi-publisher `tx_ordering` — and forces
   per-node config rendering (#37). Rejected: multicast is one URI for all
   nodes and deletes an entire rendering mechanism.
3. **Per-rid/per-sid explicit channel lists in `ChannelsConfig`**: handles
   per-host IPs without multicast but adds schema surface and still can't
   express multi-publisher B over unicast. Rejected.
4. **Full in-image cargo builds for CI cluster images** (reuse
   `service.Dockerfile`): correct but ~30–60 min on a 4-vCPU runner; prebuilt
   binaries + thin images keep the workflow under ~25 min. The production
   Dockerfile remains the canonical build; the CI Dockerfile is explicitly a
   test-only artifact.
