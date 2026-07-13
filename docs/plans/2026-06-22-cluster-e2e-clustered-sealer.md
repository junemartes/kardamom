# Clustered Aeron Sealer in cluster-e2e + Resilience Suite — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the `cluster-e2e` CI job bring up the 3-member Aeron Cluster (Raft) sealer, drive the pipeline through it, and pass green — including new leader-kill / follower-kill / quorum-loss-recover resilience cases.

**Architecture:** A new Java `ClusterNode` launcher boots `ClusteredMediaDriver` + `ConsensusModule` + `ClusteredServiceContainer(SealerClusteredService)` on 3 dedicated `sealer`-role nodes. The `kardamom-sequencer`/`-executor`/`-ingress` binaries gain a default-off `[cluster]` config that swaps their tx_ordering trait impls to the existing `cluster-adapter` factories. A new `cluster.nomad.hcl` + `cluster.Dockerfile` deploy it; `ci-cluster.sh`/`cluster-e2e.yml` (with the ported `kardamom-load` + `chaos.sh`) exercise it.

**Tech Stack:** Rust (cargo workspace), Java 17 + Gradle (Aeron Cluster 1.44/1.45), Aeron C client (rusteron), Nomad + Consul + Ansible, Docker-in-Docker, GitHub Actions.

**Spec:** `docs/agents/cluster-e2e-clustered-sealer-integration-spec.md`. **Parent:** `docs/agents/sealer-aeron-cluster-failover-spec.md`.

**Reference branch (port from):** `origin/claude/cluster-chaos-tests` — `kardamom-load` (`crates/bench/src/load/`, `crates/bench/src/bin/load.rs`, `crates/bench/src/lib.rs` line), `deploy/cluster/scripts/chaos.sh`, and the `cluster-e2e.yml` + `ci-cluster.sh` diffs. Query git from `/home/dev/kardamom` (the working tree `/home/dev/kardamom-2` is jj, not git).

---

## VCS / environment notes (read first)

- Edit files in `/home/dev/kardamom-2` (jj workspace). Run `git` from `/home/dev/kardamom` (`origin = github.com/junemartes/kardamom`, `gh` authed as `junemartes`).
- The feature branch must be based on **`origin/claude/sealer-aeron-cluster-failover`** (PR #62) — it carries `crates/cluster-client`, `crates/cluster-adapter`, and `cluster/sealer-service`, which this plan depends on.
- No local Docker/nomad/ansible: the full `cluster-e2e` is verified **only** on GH Actions. Deterministic layers (Gradle JUnit, `cargo test/build`) run locally.
- Commits: use the project's VCS. "Commit" steps below are logical commits; map them to `jj` (describe/new) or the sibling git per the workspace mechanics resolved in Phase 0.

---

## Phase 0 — Branch + workspace setup

### Task 0.1: Establish the feature branch off PR #62

**Files:** none (VCS only).

- [ ] **Step 1:** In `/home/dev/kardamom`, fetch and confirm the base: `git fetch origin && git log --oneline -1 origin/claude/sealer-aeron-cluster-failover`. Expected: `0200333 feat(cluster): fault-tolerant sealer via Aeron Cluster (Raft)`.
- [ ] **Step 2:** Confirm the kardamom-2 working tree already contains the dependency code: `ls crates/cluster-adapter crates/cluster-client cluster/sealer-service`. Expected: all present.
- [ ] **Step 3:** Resolve how kardamom-2's jj working copy maps to a pushable git branch. Inspect `cat /home/dev/kardamom-2/.jj/repo/repo` and `jj --repository /home/dev/kardamom-2 root`. If kardamom-2 shares the kardamom git store (jj colocated/multi-workspace), use `jj git export` + a bookmark; otherwise plan to apply the kardamom-2 diff onto a `git` branch in `/home/dev/kardamom`. Record the chosen mechanism here before proceeding.
- [ ] **Step 4:** Create branch `claude/cluster-e2e-clustered-sealer` tracking the PR #62 head via the chosen mechanism. Verify `git branch --show-current` (or `jj bookmark list`) shows it.

---

## Phase 1 — Java cluster launcher + packaging + in-JVM failover test

### Task 1.1: Make `:service` produce a runnable fat jar

**Files:**
- Modify: `cluster/sealer-service/service/build.gradle`
- Modify: `cluster/sealer-service/settings.gradle` (only if a shadow plugin pluginManagement block is needed)

- [ ] **Step 1:** Add the `application` + shadow plugins and the runtime Aeron deps. Replace `service/build.gradle` with:

```groovy
// :service — the Aeron Cluster ClusteredService adapter + the ClusterNode launcher.
plugins {
    id 'application'
    id 'com.gradleup.shadow' version '8.3.5'
}

dependencies {
    implementation project(':core')
    implementation 'io.aeron:aeron-cluster:1.44.0'
    implementation 'io.aeron:aeron-archive:1.44.0'
    implementation 'io.aeron:aeron-driver:1.44.0'
    testImplementation 'io.aeron:aeron-test-support:1.44.0'
    testImplementation 'org.junit.jupiter:junit-jupiter:5.10.2'
}

application {
    mainClass = 'io.kardamom.sealer.cluster.ClusterNode'
}

tasks.named('shadowJar') {
    archiveBaseName = 'kardamom-cluster-node'
    archiveClassifier = ''
    archiveVersion = ''
    mergeServiceFiles()
}
```

- [ ] **Step 2:** Verify the shadow plugin resolves: `cd cluster/sealer-service && ./gradlew :service:dependencies --configuration runtimeClasspath -q | head`. Expected: resolves `aeron-cluster`, `aeron-archive`, `aeron-driver` (no errors). If `com.gradleup.shadow` version is unavailable, fall back to `io.github.goooler.shadow` 8.1.x — pick whichever resolves on the toolchain.
- [ ] **Step 3:** Commit: "build(cluster): package :service as a runnable cluster-node fat jar".

### Task 1.2: `ClusterNode` launcher

**Files:**
- Create: `cluster/sealer-service/service/src/main/java/io/kardamom/sealer/cluster/ClusterNode.java`

- [ ] **Step 1:** Write the launcher. It derives this member's endpoints from `clusterMembers`, boots the all-in-one driver, and blocks on a shutdown barrier. **Verify the ingress stream id constant against `crates/cluster-client/src/protocol.rs` and `crates/cluster-adapter/src/live.rs` `LiveClusterConfig` before finalizing the default.**

```java
package io.kardamom.sealer.cluster;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import java.io.File;
import org.agrona.concurrent.ShutdownSignalBarrier;

/** Boots an all-in-one Aeron Cluster member (media driver + archive +
 *  consensus module) running {@link SealerClusteredService}. Config via -D sysprops. */
public final class ClusterNode {
    public static void main(final String[] args) {
        final int memberId = Integer.getInteger("kardamom.cluster.memberId", 0);
        // "0,ingressHost:port,consensusHost:port,logHost:port,catchupHost:port,archiveHost:port|1,...|2,..."
        final String clusterMembers = System.getProperty("kardamom.cluster.members");
        if (clusterMembers == null) throw new IllegalStateException("kardamom.cluster.members not set");
        final String aeronDir = System.getProperty("aeron.dir", "/opt/kardamom/aeron-mount/dir");
        final String clusterDir = System.getProperty("kardamom.cluster.dir", "/opt/kardamom/cluster");
        final String archiveDir = System.getProperty("kardamom.archive.dir", "/opt/kardamom/archive");
        final int ingressStreamId = Integer.getInteger("kardamom.cluster.ingressStreamId", 101);
        final long tickMs = Long.getLong("kardamom.cluster.tickMs", 2000L);
        final int dedupCapacity = Integer.getInteger("kardamom.cluster.dedupCapacity", 8192);

        final String[] me = memberEndpoints(clusterMembers, memberId); // [ingress,consensus,log,catchup,archive]

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .aeronDirectoryName(aeronDir)
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(false);

        final Archive.Context archiveCtx = new Archive.Context()
            .aeronDirectoryName(aeronDir)
            .archiveDir(new File(archiveDir))
            .controlChannel("aeron:udp?endpoint=" + me[4])
            .localControlChannel("aeron:ipc?term-length=64k")
            .recordingEventsEnabled(false)
            .threadingMode(ArchiveThreadingMode.SHARED);

        final ConsensusModule.Context consensusCtx = new ConsensusModule.Context()
            .clusterMemberId(memberId)
            .clusterMembers(clusterMembers)
            .aeronDirectoryName(aeronDir)
            .clusterDir(new File(clusterDir))
            .ingressChannel("aeron:udp")
            .ingressStreamId(ingressStreamId)
            .replicationChannel("aeron:udp?endpoint=" + me[3]);

        final ClusteredServiceContainer.Context serviceCtx = new ClusteredServiceContainer.Context()
            .aeronDirectoryName(aeronDir)
            .clusterDir(new File(clusterDir))
            .clusteredService(new SealerClusteredService(dedupCapacity, tickMs));

        try (ClusteredMediaDriver ignored = ClusteredMediaDriver.launch(driverCtx, archiveCtx, consensusCtx);
             ClusteredServiceContainer ignored2 = ClusteredServiceContainer.launch(serviceCtx)) {
            System.out.println("cluster node up memberId=" + memberId + " endpoints=" + String.join(",", me));
            new ShutdownSignalBarrier().await();
        }
    }

    /** Extract this member's 5 endpoints from the pipe/comma clusterMembers string. */
    static String[] memberEndpoints(final String clusterMembers, final int memberId) {
        for (final String member : clusterMembers.split("\\|")) {
            final String[] f = member.split(",");
            if (Integer.parseInt(f[0].trim()) == memberId) {
                return new String[] { f[1], f[2], f[3], f[4], f[5] };
            }
        }
        throw new IllegalArgumentException("memberId " + memberId + " not in " + clusterMembers);
    }
}
```

- [ ] **Step 2:** Add a role marker the chaos suite can grep. In `SealerClusteredService.onRoleChange`, replace the no-op body with `System.out.println("cluster role=" + newRole + " memberId=-1");` — but prefer logging the memberId: pass `memberId` into the service constructor OR have `ClusterNode` log role via a `Cluster.Role` poll. Simplest: keep the service Aeron-free and log role from `ClusterNode` is not possible (role lives in the service). So extend `SealerClusteredService` to accept an optional `int memberId` and emit `cluster role=LEADER memberId=N` from `onRoleChange`. Update the existing constructors accordingly (keep the no-arg + 2-arg constructors; add a 3-arg `(dedupCapacity, tickIntervalMs, memberId)`).
- [ ] **Step 3:** Run the existing JUnit to ensure nothing regressed: `cd cluster/sealer-service && ./gradlew :core:test :service:test`. Expected: existing 10 tests pass.
- [ ] **Step 4:** Commit: "feat(cluster): ClusterNode launcher (ClusteredMediaDriver + service container)".

### Task 1.3: In-JVM `TestCluster` failover test (proves O2/O3)

**Files:**
- Create: `cluster/sealer-service/service/src/test/java/io/kardamom/sealer/cluster/SealerClusterFailoverTest.java`

- [ ] **Step 1: Write the failing test.** Use `io.aeron.test.cluster.TestCluster` (from `aeron-test-support`). Start a 3-node cluster, connect a client, send N app envelopes, await egress, **stop the leader**, await new-leader election, send M more, assert all N+M relayed records arrive in order with no gap and no block-number regress (read the boundary frames). Skeleton:

```java
package io.kardamom.sealer.cluster;

import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SealerClusterFailoverTest {
    @Test
    void egressContinuesAcrossLeaderKill() {
        try (TestCluster cluster = TestCluster.start(3 /* members */, ctx -> ctx
                .clusteredService(new SealerClusteredService(8192, 2000L)))) {
            cluster.awaitLeader();
            // ... connect a client session, offer K canonical-id envelopes,
            // await K relayed egress frames (monotonic 0-based index) ...
            final TestNode leader = cluster.awaitLeader();
            cluster.stopNode(leader);
            cluster.awaitLeader(); // new leader elected
            // ... offer K more envelopes; assert indexes continue K..2K-1 (no gap)
            //     and the boundary block_number never regresses ...
            assertTrue(true, "replace with concrete egress assertions");
        }
    }
}
```

> NOTE for the implementer: the exact `TestCluster` API (`start`, `connectClient`, `awaitLeader`, `stopNode`, `pollUntilMessageSent`/egress capture) varies by Aeron version. Read the `aeron-test-support` jar's `TestCluster`/`TestNode` signatures for `1.44.0` and wire the offer/await against them. The assertions (gapless index continuation across the kill, no block-number regress) are the contract — do not weaken them to `assertTrue(true)`.

- [ ] **Step 2:** Run it, expect FAIL (assertions unimplemented / API mismatch): `./gradlew :service:test --tests '*SealerClusterFailoverTest'`.
- [ ] **Step 3:** Implement the concrete client offer + egress capture + assertions against the real `TestCluster` API until it passes.
- [ ] **Step 4:** Run: `./gradlew :service:test --tests '*SealerClusterFailoverTest'`. Expected: PASS (egress gapless across leader kill).
- [ ] **Step 5:** Commit: "test(cluster): in-JVM TestCluster leader-kill egress-continuity".

---

## Phase 2 — Rust binary wiring (cluster mode, default-OFF)

### Task 2.1: Add `cluster-adapter`/`cluster-client` to the workspace + service build list

**Files:**
- Modify: `Cargo.toml` (workspace `members`)
- Modify (later, Phase 4): `.github/workflows/cluster-e2e.yml` build list

- [ ] **Step 1:** Confirm whether `crates/cluster-adapter` and `crates/cluster-client` are already workspace members: `grep -n 'members' -A30 Cargo.toml`. If absent, add them.
- [ ] **Step 2:** `cargo build -p kardamom-cluster-adapter -p kardamom-cluster-client`. Expected: builds (deterministic tests already exist).
- [ ] **Step 3:** Commit if changed: "chore(workspace): include cluster-adapter/cluster-client as members".

### Task 2.2: Add a `cluster_watermark` factory

**Files:**
- Modify: `crates/cluster-adapter/src/lib.rs`
- Test: `crates/cluster-adapter/tests/end_to_end.rs` (extend) or a new unit test

- [ ] **Step 1: Write the failing test** asserting a `cluster_watermark(rt, cfg)` factory returns a `(LiveCluster, ClusterWatermark<LiveEgress>)` and that the watermark advances as egress boundary positions arrive (mirror the `dedup_order_and_boundary_alignment_end_to_end` style with the fake egress).
- [ ] **Step 2:** Run it, expect FAIL (`cluster_watermark` not defined).
- [ ] **Step 3:** Implement, mirroring `cluster_tx_ordering_subscription` (it keeps the egress half):

```rust
/// Wire ingress's durable watermark source from cluster egress progress.
pub fn cluster_watermark(
    rt: AeronRuntime,
    cfg: LiveClusterConfig,
) -> Result<(LiveCluster, ClusterWatermark<LiveEgress>), LiveError> {
    let (cluster, _ingress, egress) = live::connect(rt, cfg)?;
    Ok((cluster, ClusterWatermark::new(egress)))
}
```

> Check `ClusterWatermark`'s actual constructor in `crates/cluster-adapter/src/watermark.rs`; adjust `::new` args to match.

- [ ] **Step 4:** Run: `cargo test -p kardamom-cluster-adapter`. Expected: PASS.
- [ ] **Step 5:** Commit: "feat(cluster-adapter): cluster_watermark factory".

### Task 2.3: `[cluster]` config + construction swap in `kardamom-sequencer`

**Files:**
- Modify: `crates/sequencer/src/config.rs` (add `ClusterConfig`, default off)
- Modify: `crates/sequencer/src/bin/kardamom-sequencer.rs:118-138` (the tx_ordering publisher construction site)

- [ ] **Step 1:** Add a config struct (TOML, default disabled):

```rust
#[derive(Debug, Clone, serde::Deserialize, Default)]
pub struct ClusterConfig {
    #[serde(default)]
    pub enabled: bool,
    /// "memberId=host:port,…" cluster ingress endpoints.
    #[serde(default)]
    pub ingress_endpoints: String,
    #[serde(default)]
    pub initial_leader_member_id: i32,
    #[serde(default = "default_ingress_stream_id")]
    pub ingress_stream_id: i32,
    /// This client's egress (response) channel URI, e.g. "aeron:udp?endpoint=<node_ip>:<port>".
    #[serde(default)]
    pub egress_channel: String,
    #[serde(default = "default_egress_stream_id")]
    pub egress_stream_id: i32,
    #[serde(default = "default_keep_alive_ms")]
    pub keep_alive_interval_ms: u64,
}
fn default_ingress_stream_id() -> i32 { 101 }
fn default_egress_stream_id() -> i32 { 102 }
fn default_keep_alive_ms() -> u64 { 1000 }
```
Add `#[serde(default)] pub cluster: ClusterConfig` to `SequencerConfig`. (Stream-id defaults MUST match `ClusterNode`'s `ingressStreamId` and the cluster-client protocol.)

- [ ] **Step 2:** At the construction site, branch on `cfg.cluster.enabled`. When enabled, build the cluster publisher via `cluster_ref_publisher` and use `ClusterRefPublisher` as the `TxOrderingRefPublisher` for BOTH the main loop and the deposit pump (clone the `LiveIngress` — its `req_tx: Sender` is `Clone`; if the factory does not expose a clone, add a `ClusterRefPublisher::clone_ingress()` or construct two publishers from one `connect`). Keep the `LiveCluster` guard alive until shutdown (`drop(rt)` is replaced by `drop(cluster)`). Resolve the `AeronRuntime` ownership: the cluster factory takes `rt` by value — check whether `AeronRuntime` is `Clone`/`Arc`-backed (`crates/log/src/aeron_live.rs`); if not, spawn a second `AeronRuntime` for the cluster client or refactor to share. Document the resolution inline.

```rust
// pseudo at the construction site:
let (tx_ordering_box, _cluster_guard): (Box<dyn TxOrderingRefPublisher + Send>, Option<LiveCluster>) =
    if cfg.cluster.enabled {
        let (guard, pubr) = kardamom_cluster_adapter::cluster_ref_publisher(rt_for_cluster, cfg.cluster.clone().into())?;
        (Box::new(pubr), Some(guard))
    } else {
        (Box::new(LiveTxOrderingRefPub::new(tx_ordering_pub)), None)
    };
```
Add a `From<ClusterConfig> for LiveClusterConfig` conversion (in the bin or config module).

- [ ] **Step 3:** `cargo build -p kardamom-sequencer`. Expected: builds. `cargo test -p kardamom-sequencer`. Expected: existing tests pass (cluster disabled by default).
- [ ] **Step 4:** Commit: "feat(sequencer): optional [cluster] tx_ordering publisher (default off)".

### Task 2.4: `[cluster]` config + construction swap in `kardamom-executor`

**Files:**
- Modify: `crates/executor/src/config.rs` (or wherever `ExecutorConfig` lives) — add the same `ClusterConfig` (factor it into a shared place if practical; otherwise duplicate the struct — keep field names identical)
- Modify: `crates/executor/src/bin/kardamom-executor.rs` (the `TxOrderingSubscription` construction site)

- [ ] **Step 1:** Add `[cluster]` config (same shape as Task 2.3).
- [ ] **Step 2:** At the executor's tx_ordering subscription construction, when `cluster.enabled`, build via `cluster_tx_ordering_subscription` and use `ClusterTxOrderingSubscription` as the `TxOrderingSubscription`; keep the `LiveCluster` guard alive. The executor's `DedupWindow` and reader thread are unchanged (per spec O3).
- [ ] **Step 3:** `cargo build -p kardamom-executor && cargo test -p kardamom-executor`. Expected: pass (cluster off by default).
- [ ] **Step 4:** Commit: "feat(executor): optional [cluster] tx_ordering subscription (default off)".

### Task 2.5: `[cluster]` watermark in `kardamom-ingress`

**Files:**
- Modify: `crates/ingress/src/config.rs` — add `ClusterConfig`
- Modify: `crates/ingress/src/bin/kardamom-ingress.rs` — the `on-quorum` ack-gate watermark source construction

- [ ] **Step 1:** Locate the watermark/ack-gate construction in ingress (`grep -rn 'watermark\|on-quorum\|on_quorum\|ack_policy\|AckPolicy' crates/ingress/src`).
- [ ] **Step 2:** Add `[cluster]` config; when enabled, build the watermark via `cluster_watermark` (Task 2.2) and feed the ingress `on-quorum` gate from it; keep the guard alive.
- [ ] **Step 3:** `cargo build -p kardamom-ingress && cargo test -p kardamom-ingress`. Expected: pass.
- [ ] **Step 4:** Commit: "feat(ingress): optional [cluster] durable watermark (default off)".

### Task 2.6: Workspace-wide build/clippy gate

- [ ] **Step 1:** `cargo build --release -p kardamom-ingress -p kardamom-sequencer -p kardamom-executor -p kardamom-sealer -p kardamom-da-watcher -p kardamom-batcher --bins`. Expected: all build.
- [ ] **Step 2:** `cargo clippy --workspace --all-targets -- -D warnings` (matches CI lint). Fix any lints.
- [ ] **Step 3:** Commit: "chore: clippy/rustfmt for cluster wiring".

---

## Phase 3 — Deploy artifacts

### Task 3.1: Cluster contract in `group_vars/all.yml`

**Files:**
- Modify: `deploy/cluster/ansible/group_vars/all.yml`

- [ ] **Step 1:** Set `node_classes.sealer.count: 3` (members at `.51/.52/.53`).
- [ ] **Step 2:** Add `cluster_member_count: 3` and a `cluster_ports` block on the `aeron_channel_base` lane, e.g.:

```yaml
# Aeron Cluster member ports (uniform per role; unique by member node IP).
cluster_ports:
  ingress: 40200       # client-facing ingress (Rust cluster-client connects here)
  consensus: 40201     # Raft consensus
  log: 40202           # log replication
  catchup: 40203       # replay / catch-up (replicationChannel)
  archive_control: 40204
cluster_ingress_stream_id: 101
cluster_egress_stream_id: 102
# Per-client egress response port (sequencer/executor/ingress each bind their node IP here).
cluster_egress_port: 40210
```

- [ ] **Step 3:** Update the comment block in `node_classes` (the `sealer 1 PROTOCOL SINGLETON` line) to reflect the 3-member cluster.
- [ ] **Step 4:** Commit: "feat(cluster-deploy): 3-member cluster contract in group_vars".

### Task 3.2: `cluster.Dockerfile`

**Files:**
- Create: `deploy/cluster/docker/cluster.Dockerfile`

- [ ] **Step 1:** Thin JRE image wrapping the shadow jar:

```dockerfile
# kardamom cluster node: a single Aeron Cluster member (pure JVM; no native libs).
FROM eclipse-temurin:17-jre-jammy
WORKDIR /opt/kardamom
COPY kardamom-cluster-node.jar /opt/kardamom/cluster-node.jar
ENTRYPOINT ["java", "-cp", "/opt/kardamom/cluster-node.jar", "io.kardamom.sealer.cluster.ClusterNode"]
```

- [ ] **Step 2:** Commit: "feat(cluster-deploy): cluster-node Dockerfile".

### Task 3.3: `cluster.nomad.hcl`

**Files:**
- Create: `deploy/cluster/nomad/cluster.nomad.hcl`

- [ ] **Step 1:** Author the job: `count=3`, `constraint ${meta.role}==sealer`, `distinct_hosts`, host network, persistent `cluster/` + `archive/` volumes. Render `kardamom.cluster.members` (the full `id,ingress,consensus,log,catchup,archive|…` string) from the contract and `memberId` from `${NOMAD_ALLOC_INDEX}`. Pattern after `sealer.nomad.hcl`/`executor.nomad.hcl`:

```hcl
job "cluster" {
  datacenters = ["dc1"]
  type        = "service"
  constraint { attribute = "${meta.role}" value = "sealer" }

  group "cluster" {
    count = 3
    constraint { operator = "distinct_hosts" value = "true" }
    network { mode = "host" }

    task "cluster" {
      driver = "docker"
      config {
        image        = "192.168.56.10:5000/kardamom-cluster:dev"
        force_pull   = true
        network_mode = "host"
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
          "/opt/kardamom/cluster:/opt/kardamom/cluster",
          "/opt/kardamom/archive:/opt/kardamom/archive",
        ]
        args = [
          "-Dkardamom.cluster.memberId=${NOMAD_ALLOC_INDEX}",
          # Full member list rendered from the contract (see template below).
          "-Dkardamom.cluster.members=0,192.168.56.51:40200,192.168.56.51:40201,192.168.56.51:40202,192.168.56.51:40203,192.168.56.51:40204|1,192.168.56.52:40200,192.168.56.52:40201,192.168.56.52:40202,192.168.56.52:40203,192.168.56.52:40204|2,192.168.56.53:40200,192.168.56.53:40201,192.168.56.53:40202,192.168.56.53:40203,192.168.56.53:40204",
          "-Daeron.dir=/opt/kardamom/aeron-mount/dir",
          "-Dkardamom.cluster.dir=/opt/kardamom/cluster",
          "-Dkardamom.archive.dir=/opt/kardamom/archive",
          "-Dkardamom.cluster.ingressStreamId=101",
          "-Dkardamom.cluster.tickMs=2000",
        ]
      }
      resources { cpu = 1000  memory = 1024 }
    }
  }
}
```

> The hard-coded members string above matches the `.51/.52/.53` + `cluster_ports` contract; keep it in sync with `group_vars/all.yml` (or render via a `template` stanza if the deploy uses Consul-template — check how `deploy.sh` submits jobs).

- [ ] **Step 2:** `nomad job validate` runs in CI (`cluster-validate.yml` already globs `nomad/*.hcl`); locally just sanity-check HCL braces.
- [ ] **Step 3:** Commit: "feat(cluster-deploy): cluster.nomad.hcl (3-member Raft job)".

### Task 3.4: Wire `deploy.sh` + channels for cluster mode

**Files:**
- Modify: `deploy/cluster/scripts/deploy.sh`
- Modify: `deploy/cluster/config/channels.toml.tpl` and/or `sequencer.toml.tpl` (enable `[cluster]` for sequencer/executor/ingress; disable tx_ordering MDC in cluster mode)

- [ ] **Step 1:** In `deploy.sh`, deploy `cluster.nomad.hcl` instead of `sealer.nomad.hcl` when cluster mode is active (gate on an env flag e.g. `CLUSTER_SEALER=1`, default on for the new cluster-e2e). Read `deploy.sh` first to match its job-submission loop.
- [ ] **Step 2:** Render `[cluster]` config into the sequencer/executor/ingress allocs: `enabled=true`, `ingress_endpoints="0=192.168.56.51:40200,1=192.168.56.52:40200,2=192.168.56.53:40200"`, `initial_leader_member_id=0`, `egress_channel="aeron:udp?endpoint=${node_ip}:40210"`, stream ids 101/102. Disable tx_ordering MDC so the binaries take the cluster path (or rely on `cluster.enabled` taking precedence — make the binary prefer cluster when both set, and assert that in code).
- [ ] **Step 3:** Commit: "feat(cluster-deploy): deploy.sh + configs select the clustered sealer".

### Task 3.5: Contract drift check

**Files:**
- Modify: `deploy/cluster/scripts/check-contract.py`

- [ ] **Step 1:** Add assertions that the cluster stream ids + member ports in `group_vars/all.yml` match the values hard-coded in `cluster.nomad.hcl` and the rendered `[cluster]` configs (mirror the existing MDC-publisher contract check).
- [ ] **Step 2:** Run `./scripts/check-contract.py`. Expected: exit 0.
- [ ] **Step 3:** Commit: "test(cluster-deploy): contract drift check for cluster ports/streams".

---

## Phase 4 — Port chaos harness + CI wiring

### Task 4.1: Port `kardamom-load`

**Files:**
- Create (from `origin/claude/cluster-chaos-tests`): `crates/bench/src/load/{mod,engine,accounting,plan,scrape}.rs`, `crates/bench/src/bin/load.rs`
- Modify: `crates/bench/src/lib.rs` (add `pub mod load;`), `crates/bench/Cargo.toml` (add the deps that branch adds)

- [ ] **Step 1:** Copy each file from the branch: `cd /home/dev/kardamom && git show origin/claude/cluster-chaos-tests:crates/bench/src/bin/load.rs` (and the `load/` modules, `Cargo.toml`, `lib.rs` diff) → write into kardamom-2. Apply the exact `crates/bench/Cargo.toml` + `lib.rs` additions from `git diff origin/main...origin/claude/cluster-chaos-tests -- crates/bench`.
- [ ] **Step 2:** `cargo build --release -p kardamom-bench --bin kardamom-load`. Expected: builds. Fix any drift vs the current `bench` crate (the branch was cut from an older main; reconcile API changes).
- [ ] **Step 3:** Commit: "feat(bench): port kardamom-load sustained-load/chaos harness".

### Task 4.2: Port `chaos.sh` + add the 3 cluster cases

**Files:**
- Create (from branch): `deploy/cluster/scripts/chaos.sh`
- Modify: `deploy/cluster/scripts/chaos.sh` to add `cluster-leader-kill`, `cluster-follower-kill`, `cluster-quorum-loss-recover`

- [ ] **Step 1:** Copy `chaos.sh` from `origin/claude/cluster-chaos-tests`.
- [ ] **Step 2:** Add a `cluster_leader()` helper that finds the leader member node by grepping the cluster allocs' logs for `role=LEADER` (Task 1.2 marker), with a retry loop tolerating election windows:

```bash
# Echo the node-container name of the current Raft leader (kardamom-sealer-N), or empty.
cluster_leader() {
  local i node
  for i in 0 1 2; do
    node="kardamom-sealer-${i}"
    if docker exec "$node" sh -c 'docker ps -q --filter name=cluster | head -1 | xargs -r docker logs --tail 200 2>&1' \
        | grep -q 'role=LEADER'; then echo "$node"; return 0; fi
  done
  return 1
}
```
> Adjust the log-scrape to the actual inner container/log location; fallback to the Aeron cluster role counter if the marker is unreliable.

- [ ] **Step 3:** Add the three cases to the `run_case` `case` statement (steady `kardamom-load` already wraps each case):

```bash
cluster-leader-kill)
  leader="$(cluster_leader)"; [ -n "$leader" ] || fail "no leader found"
  log "leader-kill: hard-kill cluster member on ${leader}"
  inject_hard "${leader}" cluster
  # A new leader must emerge AND boundaries must keep advancing.
  sleep 5; assert_progress ;;
cluster-follower-kill)
  leader="$(cluster_leader)"
  for i in 0 1 2; do n="kardamom-sealer-${i}"; [ "$n" != "$leader" ] && { foll="$n"; break; }; done
  log "follower-kill: hard-kill cluster member on ${foll} (quorum 2/3 holds)"
  inject_hard "${foll}" cluster
  assert_progress ;;   # pipeline must be UNAFFECTED (no dip)
cluster-quorum-loss-recover)
  leader="$(cluster_leader)"
  # Kill two members (lose quorum).
  killed=0; for i in 0 1 2; do n="kardamom-sealer-${i}"; inject_hard "$n" cluster && killed=$((killed+1)); [ "$killed" -ge 2 ] && break; done
  b0="$(sealer_boundaries || echo 0)"; sleep 15; b1="$(sealer_boundaries || echo 0)"
  awk "BEGIN{exit !(${b1}==${b0})}" || fail "pipeline advanced without quorum (false progress)"
  log "quorum lost: boundaries flat at ${b0} (correct). Restoring one member."
  # Restart one member's node task so quorum (2/3) returns.
  on_control 'nomad job allocs cluster >/dev/null 2>&1 || true'   # touch
  docker exec kardamom-sealer-0 sh -c 'docker start $(docker ps -aq --filter name=cluster | head -1)' || true
  assert_progress ;;   # rejoin from snapshot/log → load drains gaplessly
```
> The `sealer_boundaries` scrape currently targets `kardamom-sealer-0:9003`. In cluster mode the boundary metric source changes — point `assert_progress`/`sealer_boundaries` at the executor's applied-block metric (or the cluster member that exposes boundary counters). Resolve which metric proves "blocks advancing" under the clustered sealer and update both helpers.

- [ ] **Step 4:** `bash -n deploy/cluster/scripts/chaos.sh`. Expected: no syntax errors.
- [ ] **Step 5:** Commit: "test(cluster): chaos cases leader-kill / follower-kill / quorum-loss-recover".

### Task 4.3: Wire `ci-cluster.sh` (port env knobs + cluster deploy)

**Files:**
- Modify: `deploy/cluster/scripts/ci-cluster.sh`

- [ ] **Step 1:** Apply the `cluster-chaos-tests` diff to `ci-cluster.sh` (`RUN_LOAD`/`RUN_CHAOS`/`CHAOS_CASES` wiring, the `load`/`chaos` stage selection): `git show origin/claude/cluster-chaos-tests:deploy/cluster/scripts/ci-cluster.sh` and reconcile with the current file (which has the subscriber-churn step + load smoke).
- [ ] **Step 2:** Ensure the SERVICES/deploy path uses the clustered sealer: build+push the `kardamom-cluster` image (from `cluster.Dockerfile` + the shadow jar staged into the build context), and let `deploy.sh` deploy `cluster.nomad.hcl`. Keep the single-tx `smoke.sh` gate.
- [ ] **Step 3:** `bash -n deploy/cluster/scripts/ci-cluster.sh`. Expected: clean.
- [ ] **Step 4:** Commit: "ci(cluster): ci-cluster.sh deploys clustered sealer + runs load/chaos shards".

### Task 4.4: `cluster-e2e.yml` — JDK, jar, image, shard

**Files:**
- Modify: `.github/workflows/cluster-e2e.yml`

- [ ] **Step 1:** Apply the `cluster-chaos-tests` sharding diff (matrix `load`, `chaos-executor`, `chaos-ingress`, `chaos-sequencer`, `chaos-sealer`) and ADD a `chaos-cluster` shard:

```yaml
shard: [load, chaos-executor, chaos-ingress, chaos-sequencer, chaos-sealer, chaos-cluster]
```
and in the "Configure shard" step:
```bash
chaos-cluster) { echo "RUN_LOAD=0"; echo "RUN_CHAOS=1"; echo "CHAOS_CASES=cluster-leader-kill cluster-follower-kill cluster-quorum-loss-recover"; } ;;
```

- [ ] **Step 2:** Add JDK setup + jar build before the image build:

```yaml
- uses: actions/setup-java@v4
  with: { distribution: temurin, java-version: '17' }
- name: Build cluster-node fat jar
  run: cd cluster/sealer-service && ./gradlew :service:shadowJar --no-daemon
```

- [ ] **Step 3:** Add `kardamom-load` to the cargo build step and `cluster-adapter`/`cluster-client` to the service build (they're deps of the binaries, so they build transitively — verify).
- [ ] **Step 4:** Commit: "ci(cluster): JDK + cluster image build + chaos-cluster shard".

---

## Phase 5 — Drive `cluster-e2e` to green

### Task 5.1: Local deterministic gate (must pass before pushing)

- [ ] **Step 1:** `cd cluster/sealer-service && ./gradlew test` — all JUnit incl. `SealerClusterFailoverTest` pass.
- [ ] **Step 2:** `cargo test -p kardamom-cluster-adapter -p kardamom-cluster-client` and `cargo build --release` of the 6 binaries + `kardamom-load`.
- [ ] **Step 3:** `cargo clippy --workspace --all-targets -- -D warnings`; `./deploy/cluster/scripts/check-contract.py`.

### Task 5.2: Push + trigger + watch + fix loop

- [ ] **Step 1:** Push the branch (mechanism from Task 0.1) to `origin`.
- [ ] **Step 2:** Trigger: `cd /home/dev/kardamom && gh workflow run cluster-e2e.yml --ref claude/cluster-e2e-clustered-sealer`.
- [ ] **Step 3:** Watch: `gh run list --workflow=cluster-e2e.yml -L 1` → `gh run watch <id>`. On failure: `gh run view <id> --log-failed`.
- [ ] **Step 4:** Triage per shard. Expected first-iteration failure modes + fixes:
  - **`chaos-cluster` no leader / client can't connect** → stream-id mismatch between `ClusterNode` and the Rust `[cluster]` config, or `clusterMembers` endpoint/port wrong; fix the contract.
  - **CPU starvation / election storms** → lower `CHAOS_TPS` for the cluster shard, raise SLOs, smaller JVM heaps (`-Xmx256m`), confirm 2s tick.
  - **UDP not flowing between members** → confirm the bridge multicast/iptables workarounds in `ci-cluster.sh` also permit unicast cluster UDP (they should); check `cluster_ports` aren't colliding with `aeron_channel_base` lanes.
  - **`load` / other shards regressed** → the cluster swap leaked into non-cluster shards; ensure `[cluster]` defaults off and only the cluster deploy enables it.
- [ ] **Step 5:** Use `superpowers:systematic-debugging` for each failure; fix, push, re-trigger. Repeat until **all shards green**.
- [ ] **Step 6:** Final: `gh run view <id>` shows success for every shard. Update the `ci-cluster.sh` header (drop the stale "EXPERIMENTAL / NOT YET RUN GREEN" once green) and remove issue #58's SPOF note where the cluster now closes it.

### Task 5.3: Finish the branch

- [ ] **Step 1:** Use `superpowers:requesting-code-review`, then `superpowers:finishing-a-development-branch` to open/stack the PR onto #62.

---

## Self-review checklist (done)

- **Spec coverage:** topology (3.1/3.3), Java launcher (1.2) + JUnit failover (1.3), Rust wiring seq/exec/ingress (2.3/2.4/2.5) + watermark factory (2.2), cluster image/nomad/deploy/contract (3.2–3.5), chaos port + 3 cases (4.1/4.2), CI JDK/jar/image/shard (4.3/4.4), drive-to-green (5.x), contract drift (3.5). All spec sections map to tasks.
- **Placeholders:** the two explicit `> NOTE`s (TestCluster API, boundary metric source) are *resolve-against-real-API* instructions, not unfinished plan content; assertions/behavior are specified.
- **Type consistency:** `ClusterConfig` field names + stream-id defaults (101/102) are identical across Tasks 2.3/2.4/2.5 and `ClusterNode` (1.2) and `cluster.nomad.hcl` (3.3) and `group_vars` (3.1). `cluster_watermark`/`cluster_ref_publisher`/`cluster_tx_ordering_subscription` factory shapes match `lib.rs`.
