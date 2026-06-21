# cluster-e2e Integration of the Clustered Aeron Sealer + Resilience Suite — Spec

- **Date:** 2026-06-21
- **Status:** Approved design; ready for implementation plan
- **Parent spec:** `docs/agents/sealer-aeron-cluster-failover-spec.md` (this is its "Remaining integration", items 1–3, plus the resilience tests)
- **Goal (definition of done):** the `cluster-e2e` CI job exercises the **3-member Aeron Cluster (Raft) sealer** end-to-end and **passes green**, including new failover resilience cases.

## Background / current state

The fault-tolerant sealer (PR #62, `claude/sealer-aeron-cluster-failover`) replaced the single-`kardamom-sealer` SPOF with an Aeron Cluster behind the existing Rust transport trait seams. **Landed & deterministically tested:** `crates/cluster-client` (Rust-native session protocol + `SessionDriver`), `crates/cluster-adapter` (the `ClusterRefPublisher` / `ClusterTxOrderingSubscription` / `ClusterWatermark` trait impls, the `live` gateway, and the `cluster_ref_publisher` / `cluster_tx_ordering_subscription` factories), and `cluster/sealer-service` (Java `SealerClusteredService` + `CanonicalSealerState`, JUnit-tested).

**Missing (this spec):**
1. No Java launcher — only the `ClusteredService` class exists; nothing starts a `ConsensusModule` + `ClusteredServiceContainer`.
2. No `[cluster]` config wiring in the `kardamom-sequencer` / `kardamom-executor` / `kardamom-ingress` binaries.
3. No deploy artifacts (cluster Docker image, `cluster.nomad.hcl`, contract entries) and no CI wiring.
4. The chaos framework (`kardamom-load` + `chaos.sh` + `cluster-e2e` sharding) lives only on `origin/claude/cluster-chaos-tests`; its `sealer-hard` case is a documented SPOF gap (issue #58) — the gap this clustered sealer eliminates.

## Confirmed design decisions

- **Dedicated cluster nodes.** Scale the `sealer` node-class `count: 1 → 3` (members at `192.168.56.51/.52/.53`). The standalone `sealer` job is **not deployed in cluster mode**; a new `cluster.nomad.hcl` runs `count=3`, `constraint ${meta.role}==sealer`, `distinct_hosts`, `memberId = NOMAD_ALLOC_INDEX`.
- **Port the full chaos suite wholesale** from `origin/claude/cluster-chaos-tests` (`kardamom-load` = `crates/bench/src/load/` + `bin/load.rs`; `chaos.sh`; the `RUN_LOAD`/`RUN_CHAOS`/`CHAOS_CASES` wiring in `ci-cluster.sh` + the `cluster-e2e.yml` shard matrix).
- **Done = green**, not experimental. Drive the real `cluster-e2e` to pass.

## Architecture

### A. Cluster topology & deploy
- **`group_vars/all.yml`:** `node_classes.sealer.count: 3`; add `cluster_member_count: 3` and a `cluster_*` port block on the `aeron_channel_base` lane — per-member endpoints: `ingress` (client-facing, the Rust client connects here), `consensus` (Raft), `log` (log replication), `catchup` (replay/transfer), `archive_control`. Uniform ports per role, unique by member node IP (same pattern as `tx_ordering_mdc_port`).
- **`deploy/cluster/nomad/cluster.nomad.hcl`** (new): host-network; persistent per-member `cluster/` + `archive/` dirs (so a restarted member catches up from snapshot/log — required by the quorum-loss-recover case); renders the full `clusterMembers` endpoint string + this member's `memberId`/`memberEndpoints` from the contract; force_pull the cluster image.
- **`deploy/cluster/docker/cluster.Dockerfile`** (new): JRE 17 + the `:service` shadow jar. Pure-JVM Aeron Cluster — **no native libs** (unlike the Rust services). The cluster node embeds its own media driver via `ClusteredMediaDriver`.
- **`deploy.sh`:** in cluster mode deploy `cluster.nomad.hcl` instead of `sealer.nomad.hcl`.
- **`check-contract.py`:** extend to assert the cluster stream-ids / endpoint contract stays in sync between the Java launcher and the Rust `LiveClusterConfig`.

### B. Java cluster launcher (the core missing piece)
- New `ClusterNode` `main` in `cluster/sealer-service/service`: boots `ClusteredMediaDriver` (MediaDriver + Archive + ConsensusModule) + `ClusteredServiceContainer(new SealerClusteredService(dedupCapacity, tickIntervalMs))`. Config from env/sysprops: `memberId`, `clusterMembers`, `aeron.dir` / cluster-dir / archive-dir, the ingress/egress **stream ids that must match the Rust `LiveClusterConfig`**, and `tickIntervalMs` (default 2000 for the container cluster, per `sealer.toml.tpl`).
- It logs its Raft role on change (`onRoleChange`) as a parseable marker (e.g. `cluster role=LEADER memberId=N`) so the chaos cases can identify the leader.
- **Gradle:** add `application` + `shadow` plugins and `aeron-archive` / `aeron-driver` deps to `:service`; produce a runnable fat jar via `./gradlew :service:shadowJar`.
- **New JUnit `TestCluster` failover test** (in-JVM, deterministic, position-await — no sleeps): egress continuity across an explicit leader stop/start + snapshot catch-up. Proves invariants O2/O3 before any container run.

### C. Rust binary wiring (cluster mode, default-OFF)
- Add a `[cluster]` config section (default disabled) to `kardamom-sequencer`, `kardamom-executor`, `kardamom-ingress`: `ingress_endpoints` (`memberId=host:port,…`), `initial_leader_member_id`, `ingress_stream_id`, `egress_channel`, `egress_stream_id`, `keep_alive_interval_ms` → `LiveClusterConfig`.
- At each binary's single trait construction site, when enabled, build via the factories — `cluster_ref_publisher` (sequencer), `cluster_tx_ordering_subscription` (executor), and a `ClusterWatermark` for ingress's `on-quorum` gate (add a `cluster_watermark` factory mirroring the others) — instead of the `kardamom_log` MDC handle, **holding the returned `LiveCluster` guard alive** for the binary's lifetime.
- **The IPC/MDC path is untouched when disabled** → the existing `docker-e2e` multiprocess suite and local dev are unaffected (non-goal: replacing the Rust sealer in IPC/dev mode).
- Add `cluster-adapter` / `cluster-client` to the workspace members and to the `cargo build --release` service list in CI.

### D. CI + chaos harness
- **`cluster-e2e.yml`:** set up JDK 17; run `./gradlew :service:shadowJar` before the image build; build + push the cluster image; add a **`chaos-cluster`** matrix shard. Adopt the chaos branch's sharding (`load`, `chaos-executor`, `chaos-ingress`, `chaos-sequencer`, `chaos-sealer`, `chaos-cluster`).
- **Port** `kardamom-load` + `chaos.sh` + the `RUN_LOAD`/`RUN_CHAOS`/`CHAOS_CASES` env wiring in `ci-cluster.sh`. In cluster mode, `ci-cluster.sh` deploys `cluster.nomad.hcl` instead of `sealer.nomad.hcl`; the existing single-tx `smoke.sh` gate still runs.

### E. The three resilience cases (`chaos.sh`)
Each runs steady `kardamom-load` across the window (`--assert-all-delivered --completeness accepted`), injects a failure, asserts the SLO + pipeline progress, then asserts the load verdict PASS (no lost tx). A `cluster_leader()` helper parses the `role=LEADER` marker from the cluster allocs' logs (fallback: Aeron cluster role counter).

- **`cluster-leader-kill`** — hard-kill the **leader** member's inner container; assert a new leader is elected and `kardamom_sealer_boundaries_emitted_total` / block_number keeps advancing within RTO, load verdict PASS. **Replaces the `sealer-hard` SPOF gap (#58).**
- **`cluster-follower-kill`** — kill a **follower**; assert 2/3 quorum holds and the pipeline is *unaffected* (no progress dip), load PASS.
- **`cluster-quorum-loss-recover`** — kill **2 of 3** members (lose quorum); assert the pipeline **stalls** (boundaries flat, no phantom receipts — no false progress); restore one member; assert it rejoins from snapshot/log and the load **drains to completion gaplessly**.

## Verification strategy

- **Local (deterministic, this environment):** `./gradlew test` (`:core` + `:service`, incl. the new `TestCluster` failover JUnit); `cargo test`/`cargo build --release` for `cluster-adapter` / `cluster-client` + the wired binaries. These prove the logic without Docker.
- **Full `cluster-e2e` green (remote only):** this box has **no Docker access** (no nomad/ansible either), so the DinD cluster runs only on GH Actions. Loop: materialize edits onto a git branch **based on `claude/sealer-aeron-cluster-failover`** (PR #62, which carries the dependency code) → push → `gh workflow run cluster-e2e.yml --ref <branch>` → `gh run watch` → `gh run view --log-failed` → fix → repeat (~30 min/iteration). `cluster-e2e` already passes green on `main` and PR #62, so green is attainable.

## Risks & mitigations

- **CPU/resource contention (top risk).** Dedicated cluster nodes add 2 privileged DinD containers (9 → 11 nodes) and 3 JVM media-driver+archive+consensus members on the shared GH runner. Mitigations: keep the 2s tick; modest `kardamom-load` TPS on the `chaos-cluster` shard; per-shard cluster bring-up (sharding already isolates this); small JVM heaps.
- **Leader identification across a kill** — rely on the launcher's logged role marker + re-scan after election; tolerate transient "no leader" during election in the helper.
- **Snapshot/catch-up timing** for quorum-loss-recover — persistent cluster/archive dirs + position-await rather than fixed sleeps.
- **Contract drift** between the Java launcher stream-ids/endpoints and the Rust `LiveClusterConfig` — guarded by `check-contract.py`.
- **Aeron Cluster UDP across the bridge** — the cluster's consensus/log/ingress/egress are UDP; the existing `ci-cluster.sh` already disables IGMP snooping + bridge-nf-call-iptables for Aeron multicast, but cluster consensus is mostly unicast UDP between member IPs (should traverse the bridge fine); verify on first CI run.

## Phased implementation (for the plan)

1. **Java launcher + packaging + JUnit `TestCluster`** — independently verifiable locally.
2. **Rust binary wiring** (`[cluster]` config + factory construction; `cluster_watermark` factory) + workspace membership — verifiable via existing fake-backed adapter tests + `cargo build`.
3. **Deploy artifacts** — contract (`group_vars`), `cluster.nomad.hcl`, `cluster.Dockerfile`, `deploy.sh`, `check-contract.py`.
4. **CI + chaos** — port `kardamom-load` + `chaos.sh`; `cluster-e2e.yml` JDK/jar/image/shard; the 3 cluster cases.
5. **Drive to green** — push branch off PR #62, iterate `cluster-e2e` via `gh` until the `chaos-cluster` + other shards pass.

## Open items to resolve in the plan

- Exact `cluster_*` port assignments + the `clusterMembers` / `memberEndpoints` string format.
- The ingress `ClusterWatermark` factory shape (no factory exists yet; `ClusterWatermark` type is exported).
- Whether the `sealer` node-class is renamed to `cluster` for clarity or kept as `sealer` (reuse the role/IP lane). Default: keep `sealer` to minimize churn.

## Verification-loop authorization (confirmed)

The user authorized **autonomous push + CI iteration**: create a branch off `claude/sealer-aeron-cluster-failover` (PR #62), push to `junemartes/kardamom`, trigger `cluster-e2e` via `gh workflow run`, watch, read `--log-failed`, fix, and repeat until green — spending Actions minutes as needed, reporting progress each iteration.
