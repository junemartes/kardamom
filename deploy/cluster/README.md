# kardamom multi-node cluster (Ansible → Nomad/Consul → Docker)

A reproducible multi-node kardamom cluster in two profiles that share one
Ansible tree and one set of Nomad jobs:

- **`local`, the container profile (CI and a developer host):**
  `terraform/containers` creates one privileged systemd + Docker-in-Docker
  container per node on a Docker bridge. The `container-*` Make targets
  drive the full bring-up + smoke + load + chaos suite
  (`.github/workflows/cluster-e2e.yml`).
- **`production`, the dedicated core and cloud profile:** infrastructure
  code, which is not part of this repository, owns the cloud
  network, the pools and the public entry points; the Nomad Autoscaler
  creates the elastic machines; `inventories/production` holds the dedicated
  core.

See [`DESIGN.md`](./DESIGN.md) for the original design rationale and
[`../../docs/failure-modes.md`](../../docs/failure-modes.md) for per-actor
failure/recovery behavior and the chaos cases that verify it.

> **Status.** The container profile runs green in CI (`cluster-e2e`, sharded
> across runners). The production profile shares the Ansible and Nomad
> surface with it and has not been run on real machines yet.

## Topology

Nodes are defined as **classes** in
[`ansible/group_vars/all.yml`](./ansible/group_vars/all.yml)
(`node_classes`: class → `{count, tier}` — the single source of truth).
Instance `<class>-<i>` is the Consul node `<class>-<i>.node.<datacenter>.consul`;
no file in the tree names its address:

| Class | Count | Runs |
|-------|-------|------|
| `control` | 1 | Nomad/Consul **server**, Docker registry, anvil (L1) |
| `sequencer` | 2 | 2 lanes × 2 racing replicas (one job group per lane, `seq-<lane>`, expanded by Nomad HCL; ports `9001 + 10 * lane`) |
| `ingress` | 2 | active/active JSON-RPC front door (:8545) |
| `executor` | 3 | state-machine replica appliers (libmdbx state) |
| `sealer` | 3 | **3-member Aeron Cluster (Raft)** — the Java `cluster` job: canonical ordering + archive-at-the-sealer durability folded into the Raft log |
| `aux` | 1 | validator, da_watcher, batcher, monitoring (off the chaos blast radius) |

### Names, not addresses

Every node runs its Consul agent as the node resolver (`roles/consul`: DNS
on the loopback port 53, the host's previous resolvers as recursors, and
`consul-resolver.service` restores `/etc/resolv.conf` at every boot, because
Docker regenerates it when a node container restarts). A
dedicated node is `<class>-<i>.node.<datacenter>.consul`; a service is
`<service>.service.consul` (`registry`, `anvil`, `ingress-jsonrpc`). The
Nomad jobs derive every peer list from a count and the datacenter, so a
job file, a config file or a script never names an address.
`ansible/contract.yml` rejects an address literal anywhere in
`nomad/`, `config/`, `ansible/`, the cluster and root justfiles
and the e2e workflow. The one exception is an environment file: the
Terraform variables of a production deployment. An image build captures the
recursors of the build server, so every elastic node forwards to them.

The test suite and the operator commands (`crates/chaos`) run on the
Docker host, outside the cluster resolver; they read every node address
from the node contract, and the justfile reads the control node address
from it too.

Every non-control node also runs the Aeron `ArchivingMediaDriver` (the `aeron`
Nomad system job). There is **no standalone sealer binary and no
recorder/quorum tier** anymore: ordering is the Java Aeron Cluster
(`cluster/sealer-service/`), and durability is the sealer archive (the old
Q-of-N recorder design is preserved, marked superseded, in
`docs/agents/log-config-and-recorder-spec.md`).

## Host prerequisites

**Quickest path:** from the repo root, `just cluster-bootstrap` installs the
host tools below for your platform, and `just cluster-doctor` verifies them.

- Ansible (`ansible-playbook`) + collections:
  `ansible-galaxy collection install ansible.posix community.docker community.general`.
- Docker (with the Buildx plugin) for the node containers and the image
  builds. The daemon must run privileged containers; on macOS or Windows
  that is Docker Desktop's Linux VM.
- Images are pushed from inside the control node (`REGISTRY_PUSH_NODE`,
  the justfile default), where the registry name `registry.service.consul`
  resolves. The host Docker daemon needs no insecure-registry entry.
- The **Nomad CLI** on the Ansible controller — used only to compile HCL
  and embed local config files. Ansible submits jobs through the Nomad API.
- For signed deployments, **cosign** on PATH, in the image builder's pinned
  cache, or configured with `workloads_cosign_binary`.
- **JDK 17 + Gradle wrapper** for the Java Aeron Cluster node jar:
  `(cd cluster/sealer-service && ./gradlew :service:shadowJar)` — `just
  images` / `just container-up` stage it into the `kardamom-cluster` image and
  fail loudly if it is missing.
- The **Rust service binaries** in `target/release` (`cargo build --release
  --bins` of the service crates, or the artifact of `scripts/ci/stage-cluster-dist.sh`):
  the image role wraps prebuilt binaries; nothing compiles inside an image.
- **OpenTofu** (1.12.6, the version the CI pins).
- Foundry's `cast` for the smoke tests (repo-level `just bootstrap`).

## Quick start

Requires `just` 1.49.0 or newer. From the repository root, use
`just --justfile deploy/cluster/justfile <recipe>`, or run in this directory:

```sh
cd deploy/cluster
just container-up      # tofu apply → node contract → ansible/cluster.yml
just container-test    # one shard's gates against that cluster (default: load)
just container-down    # tofu destroy: containers and their volumes
just container-reset   # destroy, then a fresh chain
just shard chaos-executor   # one shard end to end, the way CI runs it
```

The gates are the `kardamom-chaos` crate: one `#[ignore]` test per shard in
`crates/chaos/tests/shards.rs` (`load`, `semantics`, `chaos-executor`,
`chaos-ingress`, `chaos-sequencer`, `chaos-cluster`, `chaos-fleet`, `chaos-coordinated`,
`chaos-retention`,
`chaos-cache`). A
shard test brings the cluster up itself; `container-test` runs it with
`KARDAMOM_CHAOS_REUSE=1` against the cluster `container-up` made.
`KARDAMOM_CHAOS_CASES="graceful-executor"` narrows a chaos shard to some
cases; the other knobs (`CHAOS_TPS`, `LOAD_DURATION_S`, ...) are the
environment variables `crates/chaos/src/knobs.rs` reads. The operator
commands are `kardamom-cluster smoke | diagnostics | scale-sequencers`
(`cargo run -p kardamom-chaos --bin kardamom-cluster -- ...`).

`terraform/containers` is the Terraform root that owns the node containers.
It reads `ip_prefix` and `node_classes` from `ansible/group_vars/all.yml`,
so the node-class model has one source. It creates:

- the `kardamom-net` bridge network on `kardamom-br0` with the `/24` of
  `ip_prefix`;
- the `kardamom-node:ci` image from `docker/node.Dockerfile`;
- two named volumes per node for the inner Docker engine;
- one privileged systemd container per node, `kardamom-<class>-<i>`, at a
  stable address of the subnet (name order from host 10), with a health check
  on `systemctl is-system-running`.

`tofu apply` returns when systemd in every node is ready. `tofu output
node_contract` is the version 1 node contract. `just container-up` writes it
to `terraform/containers/node-contract.json`, and `ansible/containers.yml`
builds the in-memory inventory from it, prepares the host network
(`roles/host_prep`: socket buffers, bridge netfilter, multicast snooping)
and runs `bootstrap.yml` on the `container_nodes` group.

Terraform replaces a container when its image changes, which discards the
root filesystem of that node. A change to `node.Dockerfile` therefore gives
a fresh chain on the next `just container-up`. The Terraform state in
`terraform/containers/` is the record of the running cluster; a second
`just container-up` is a no-op apply followed by a convergence run.
Containers that an older harness left behind under the same names block the
first apply. Remove them once:

```sh
docker rm -f $(docker ps -aq --filter name=kardamom-)
docker volume rm $(docker volume ls -q --filter name=kardamom-)
docker network rm kardamom-net
```

Remove the old volumes too: Terraform adopts an existing volume by name, and
an old `kardamom-<node>-docker` volume would carry stale inner Docker state
into the new node.

CI builds once: a `build` job compiles the service binaries, the shard test
executable, the operator binary and the sealer jar, and stages them as one
artifact (`scripts/ci/stage-cluster-dist.sh`, the checkout's own layout). A
shard runner unpacks it and runs `just shard <name>` with `KARDAMOM_STAGED=1`,
with no Rust toolchain, JDK or Foundry, and `container-diagnostics` on
failure. `KARDAMOM_STAGED=1` makes the justfile run
`target/release/kardamom-chaos-shards` and `target/release/kardamom-cluster`
instead of `cargo`. The Aeron C library is compiled for x86-64-v3 through the
`scripts/ci/cc-x86-64-v3.sh` wrapper, so the artifact runs on any runner. A
failed shard leaves the cluster up; the runner is ephemeral, so nothing
destroys it after. Pass extra vars to `ansible/cluster.yml` with
`CLUSTER_VARS='{"images_tag": "x"}'` (one JSON object, no single quote).
`ansible/cluster.yml` is the convergence playbook; it expects the node contract.

## Ansible workload deployment

From the repository root, deploy to an already provisioned cluster with built
images:

```sh
ansible-playbook -i localhost, deploy/cluster/ansible/deploy.yml
```

The controller runs the playbook locally and connects to the Nomad HTTP API.
It verifies the image manifest before changing jobs, compiles the existing HCL
with `nomad job run -output`, plans changes, and registers only changed jobs.
Registration uses Nomad's job modify index to reject concurrent edits. Readiness
requires the current job version's running allocations for **every task group**,
including every Aeron node and both racing sequencer groups. Allocation readiness
is followed by the smoke, load and chaos gates of `crates/chaos` in CI.

Configuration lives in `ansible/roles/workloads/defaults/main.yml`. Existing
`NOMAD_ADDR`, `DIGEST_MANIFEST`, `KARDAMOM_REQUIRE_SIGNED`, settlement, light-client,
and chaos-shard environment settings remain supported. Ansible extra variables
can override the corresponding `workloads_*` settings directly. The topology and
current Consul configuration are unchanged in this first migration.

A settlement address or a newly built `kardamom-deploy` binary is required.
Without a supplied address, the playbook bootstraps the development Anvil factory,
queries `addresses --contract KardamomL2Settlement --json`, and deploys only if
that chain has no settlement registration. It fails instead of starting a batcher
with a placeholder. Supply an existing address to avoid development-chain bootstrap.
Owner keys are passed to the CLI through the environment and hidden from task output.

Signed mode requires a complete digest manifest and verifies the bundle and each
image with cosign before any workload mutation. It never falls back to mutable
tags. Local unsigned mode retains the explicit dev-tag fallback. Upstream Anvil
and the optional light-client image retain their jobspec image policies.

Plan an existing deployment without submitting jobs or changing contracts:

```sh
ansible-playbook -i localhost, deploy/cluster/ansible/deploy.yml --check \
  -e workloads_settlement_address=0xYOUR_EXISTING_SETTLEMENT_ADDRESS
```

Check mode requires an existing settlement address and a reachable Nomad API.
It compiles and plans all jobs, but does not wait for or create allocations.

`deploy.sh` and the image build/signing shell helpers have been removed.
Load and chaos tests remain independent of the workload deployment role.

The isolated deployment tests use the actual Ansible playbook and Nomad HCL
compiler against a local simulated Nomad API:

```sh
python3 -m unittest discover -s deploy/cluster/ansible/tests -v
```

They require `ansible-playbook` and `nomad` on PATH. The settlement repeatability
case additionally runs when Anvil and `target/debug/kardamom-deploy` are available;
it creates and tears down its own development chain.

## Ansible image builds

Both image paths now share `ansible/images.yml`. From the repository root:

```sh
# Wrap the prebuilt Linux binaries and Aeron libraries; the Java shadowJar must exist.
ansible-playbook -i localhost, deploy/cluster/ansible/images.yml
```

`just images` and the container CI runner both use prebuilt artifacts to build Aeron, the six Rust services, and the Java cluster image. The playbook
uses `community.docker.docker_image_build` and requires Docker Buildx and a builder
that loads its result into the local Docker engine. Builds re-evaluate source
changes while retaining BuildKit's layer cache.

The prebuilt path stages binaries, libraries, and the cluster jar in a temporary
build directory, leaving `target/release` untouched. Missing artifacts or conflicting
cached Aeron libraries fail before any image build. Temporary contexts are cleaned
on success or failure.

Settings are in `roles/images/defaults/main.yml`. `REGISTRY`, `TAG`, and
`DIGEST_MANIFEST` remain supported. A relative `DIGEST_MANIFEST` environment setting
is resolved against `deploy/cluster/` by both image and workload playbooks.
`REGISTRY_PUSH_NODE=control-0` (the default) keeps the host daemon out of it: Ansible exports an
image archive, copies it into `kardamom-control-0`, loads it into that node's Docker
engine, and pushes from there. The temporary node archive is removed even when
loading fails. This needs temporary disk space for one image archive on each side.

Pushes use Docker's CLI through Ansible `command.argv`, without a shell. This
preserves the digest of the exact push: the Docker push module returns a pre-push
inspection and does not expose that digest. Each manifest entry retains the
`repo:tag@sha256:...` format required by the existing Nomad jobs.

CI OIDC enables keyless signing of every pushed digest and the completed manifest.
The `cosign` role uses an installed executable or the pinned Linux amd64 download
and checksum in `group_vars/all.yml`. Other controller platforms need an installed
cosign when signing. Local builds skip signing. The playbook publishes the manifest
only after every image and required signature succeeds, preserving the previous
manifest on build/push/sign failure. Unsigned publication removes any old signature
bundle. The signed bundle is installed before the manifest; a concurrent verifier
may briefly reject a mismatched pair, but will never trust a partial manifest.

### Release images

`.github/workflows/release.yml` runs the same playbook against GHCR. A push to
`main` publishes `ghcr.io/<owner>/kardamom-<image>:main-<commit>`. A
`vMAJOR.MINOR.PATCH` tag on `main` publishes `:vMAJOR.MINOR.PATCH` and makes a
GitHub release. The release carries `images.digests`, `images.digests.sigbundle`
and `SHA256SUMS`. A `main` run keeps the same files as the workflow artifact
`images-main-<commit>`.

The job signs each image and the manifest as
`.github/workflows/release.yml@refs/heads/main` or `@refs/tags/<tag>`. A
production deployment sets `DIGEST_MANIFEST` to the downloaded manifest,
`KARDAMOM_REQUIRE_SIGNED=1`, and `KARDAMOM_CERT_IDENTITY_RE` to that identity.
`REGISTRY` accepts a host, an optional port, and an optional namespace path.

`--check` reports the planned image set without building, pushing, signing, or
changing files. Isolated image orchestration tests run the real Ansible roles and
Docker Buildx module with test Docker/cosign executables:

```sh
python3 -m unittest discover -s deploy/cluster/ansible/tests -p 'test_images.py' -v
```

These tests exercise staging, both push paths, exact digest capture, signing order,
failed transfers, failed signatures, and preservation of the previous manifest.
They do not replace a real Docker build and cluster smoke run.

## Layout

```
deploy/cluster/
  DESIGN.md                 design rationale (original; recorder tier since removed)
  justfile                  container-up / container-test / shard / container-down /
                            images / deploy / smoke / validate / check-contract
  ansible/
    ansible.cfg
    containers.yml         inventory from the node contract, host preparation
    cluster.yml            containers.yml + images.yml + deploy.yml
    group_vars/all.yml      ← canonical contract (classes, IPs, ports, versions,
                              deployment profile)
    bootstrap.yml          configure a host and join it to the substrate
    deploy.yml             workload deployment, signature and readiness gates
    images.yml             build, push, sign, and publish an image manifest
    roles/{profile,enroll,common,vswitch,netinfo,docker,firewall,consul,nomad,
           registry,workloads,images,cosign,elastic_image}/
    inventories/production/ example production inventory + profile values
  terraform/containers/     the local profile's nodes: bridge, image, volumes,
                            one container per node, the node contract
  docker/
    ci-service.Dockerfile   thin wrapper over prebuilt binaries
    cluster.Dockerfile      Java Aeron Cluster node (shadowJar + JRE 17)
    node.Dockerfile         systemd+DinD "node" container (local profile)
  nomad/
    aeron.system.nomad.hcl  ArchivingMediaDriver (driver+archive), all nodes
    cluster.nomad.hcl       3-member Aeron Cluster (Raft) sealer, .51/.52/.53
    anvil.nomad.hcl         in-cluster L1 for the smoke test + da-watcher
    ingress.nomad.hcl  sequencer.nomad.hcl  executor.nomad.hcl
    validator.nomad.hcl  da-watcher.nomad.hcl  batcher.nomad.hcl
  config/                   *.toml(.tpl) pulled into the job specs via file();
                            channels.toml.tpl is the shared LogConfig
  ansible/contract.yml      validate configuration mirrors and routing inputs
  ansible/shard-map.yml     render an initial identity map
```

The gates, the chaos cases and the operator commands are Rust:
`crates/chaos` (`tests/shards.rs`, `src/cases/`, `src/bin/kardamom-cluster.rs`).

The Nomad job specs pull their config payloads from `config/` with HCL2
`file()`, so manual CLI submissions must run **from `deploy/cluster/`**.
The Ansible deployment role sets this working directory itself.

## Deployment profiles

`bootstrap.yml` serves two profiles. `deployment_profile` in
`ansible/group_vars/all.yml` selects one; the roles read it.

| Profile | Hosts | Servers | Addresses | Security |
|---------|-------|---------|-----------|----------|
| `local` (default) | the node containers of `terraform/containers` | one control node runs the Consul and Nomad servers | explicit `node_ip` per inventory host | plain-HTTP registry, no host firewall |
| `production` | dedicated core, plus elastic cloud pools | three voting servers on the dedicated core | `roles/netinfo` resolves the address from the vSwitch VLAN or the private interface | gossip encryption, ACLs, agent TLS, `roles/firewall` |

In both profiles the Nomad agents find their servers through the local
Consul agent (`server_auto_join`, `client_auto_join`). There is no static
server list. The Consul agents join `consul_retry_join`: the control node
in the local profile, infrastructure DNS names in production. The Nomad
service names carry `cluster_id`, so two clusters in one Consul datacenter
cannot join each other.

`ansible/inventories/production/` holds an example production inventory and
the profile values. Copy it outside the repo, fill in the dedicated
inventory, the vSwitch inputs and an encrypted vault file with the secret
inputs in `group_vars/production/vault.yml`, then run `ansible-playbook -i <copy> -u root ansible/bootstrap.yml`.
`roles/profile` refuses a production run that lacks a security input.

Set `consul_dns_token` in the encrypted production vault or in the elastic
node's enrollment result. Consul 1.20 uses this as its default token for
DNS and tokenless local HTTP queries. Give it only the following policy;
the separate agent and Nomad tokens retain their registration permissions:

```hcl
service_prefix "" { policy = "read" }
node_prefix "" { policy = "read" }
```

The token is required on every production agent. The local profile keeps
ACLs disabled and requires no DNS token.

An elastic Cloud node has no inventory entry. At first boot its bootstrap
unit runs the same `bootstrap.yml` against the local host with the inputs
cloud-init wrote. `roles/enroll` runs the enrollment client the image
carries and loads the credentials it produced; without them the agents
stay disabled.

## Elastic pools

The elastic ingress and sequencer pools follow a pool contract: a
versioned JSON description of each pool. The infrastructure code that
writes the contract, the elastic node image build and the Nomad Autoscaler
installation are not part of this repository. This repository holds the
roles that an elastic node runs at first boot (`roles/elastic_image`,
`roles/enroll`, `bootstrap.yml`).

At first boot a VM from the image reads `/etc/kardamom/node.yml` that
cloud-init wrote from the pool's user data, starts
`kardamom-bootstrap.service`, and runs `bootstrap.yml` against itself. A
failed bootstrap leaves the agents disabled and the unit failed; the VM
still counts toward the pool bound, so a broken image cannot cause
unbounded creation.

## Aeron-in-Docker

- **Shared `aeron.dir`:** Ansible mounts a host tmpfs at
  `/opt/kardamom/aeron-mount`; the media-driver container and every co-located
  service container bind-mount the same path so they share the CnC file +
  mmap'd ring buffers.
- **Host networking:** all Aeron + service containers run
  `network_mode = "host"`, so Aeron UDP channel endpoints are just the node
  IP — no Docker port mapping.
- **Channels:** one shared `config/channels.toml.tpl` (dynamic Aeron MDC
  with Consul discovery; stream ids distinguish streams, control endpoints
  distinguish publishers) consumed by every service via `--log-config`. See
  `docs/aeron-discovery.md`.
- **Durability:** the sealer's Aeron Cluster members archive the canonical
  log (`archive-at-the-sealer`); executors persist state in libmdbx under
  `/opt/kardamom/state` and crash-recover by archive replay-merge.

## Performance pipeline (`kardamom-perf`)

`kardamom-perf` (`crates/bench`, `bin/perf.rs`) automates the saturation
campaign against this stack: fresh bring-up, ramp to the sustainable edge,
then a steady soak while async-profiler samples the sealer Raft leader's JVM
(itimer mode — works inside the nested containers, no perf_events needed).

```
cargo build --release -p kardamom-bench --bin kardamom-perf
kardamom-perf up                      # build + purge + fresh KEEP=1 deploy
kardamom-perf run --fresh             # up, then ramp -> profile -> report
kardamom-perf report --dir <outdir>   # re-render summary.md from artifacts
```

Each `run` writes a timestamped directory (default `target/perf/perf-<ts>/`):
`discovery-report.json` (the ramp), `load-report.json` (the profiled soak),
`flame.html` / `flame.svg` / `stacks.collapsed` (the profile), a mid-soak
`cpu-snapshot.txt` of every node container, and `summary.md` tying it
together (edge tx/s, delivery verdict, latency percentiles, CPU by node,
and the leader profile bucketed into service logic vs Aeron substrate).

Account budget: the discovery ramp signs from genesis accounts #1..#6 and the
profiled soak from #7..#15 (#0/#16 belong to the deploy's smoke gates), so a
`run` needs the fresh chain `up` deploys — nonces are managed locally
(ingress has no `eth_getTransactionCount`). `run --fresh` does both in one
go; profiling knobs (`--ceiling`, `--soak-fraction`, `--profile-secs`, ...)
are documented in `--help`.

## Monitoring

The `monitoring` job (`nomad/monitoring.nomad.hcl`) runs Prometheus and
Grafana on the aux node. Prometheus scrapes every service's metrics port by
its Consul node name, rendered from the node-class counts; Grafana
provisions the Prometheus datasource by the `prometheus` Consul service and
the dashboards from `deploy/grafana/provisioning/dashboards-json`, the one
source for every profile. From the host, read the node contract for the
aux node's address: Prometheus on port 9090, Grafana on port 3000
(anonymous viewer; admin `admin` with the `grafana_admin_password` job
variable, `kardamom` on the local profile). The autoscaler's Prometheus APM
reads the same service.

## Sustained-load + chaos suite

The `cluster-e2e` workflow runs the full suite on every trigger, **sharded
across runners** (each shard brings up its own container cluster from the
binaries one `build` job staged):

| Shard | Exercises |
|-------|-----------|
| `load` | 5-min sustained soak (`kardamom-load` ramp→soak; must-deliver + drop accounting + keep-pace) |
| `chaos-executor` | graceful + hard kill + **node-failure** (degrade to 2/3, node returns) + **node-replace** (the node comes back through the Terraform root on a new address with empty disks) |
| `chaos-ingress` | graceful + hard kill + **archive-driver-loss** (Aeron substrate kill under ingress-0) |
| `chaos-sequencer` | graceful + hard kill + **sequencer-replica-kill** (racing-twin failover, restarted replica must regain coverage) + **validator-lapse** |
| `chaos-cluster` | Raft sealer: **leader-kill** / **follower-kill** / **member-rejoin** / **node-replace-sealer** / **cpu-squeeze** |
| `chaos-fleet` | every replica of one role down at once: **cluster-quorum-loss-recover** (2 of 3 sealers) / **cluster-total-loss-recover** (all 3 sealers) / **executor-fleet-loss-recover** (all 3 executor nodes) / **executor-fleet-wipe-recover** (all 3 executor tasks, state DBs wiped, local checkpoints kept) / **executor-fleet-total-wipe-recover** (the executor job stopped, state DBs and checkpoints wiped, an executor image rebuilt from L1 installed on every node) / **redis-total-loss-recover** (the whole redis job stopped, then started empty; every mirror rebuilds from a checkpoint); every case ends with a recovery probe load that must land every transaction |
| `chaos-coordinated` | failures that cross a role's redundancy or the roles: **ingress-pair-loss-recover** (both ingress tasks) / **sequencer-lane-loss-recover** (both replicas of lane 0; no ref may sit below a floor) / **pipeline-blackout-recover** (every pipeline node killed at once; the consumers void an entry whose data the kill lost) |

`kardamom-load` is the harness (`crates/bench/src/load/`, run in process);
`crates/chaos` injects the failures under steady load and asserts Nomad
auto-recovery + pipeline progress + the load verdict. The old single-sealer
`sealer-hard` SPOF case ([#58]) is superseded by the Raft cluster cases.
Remaining untested surface is tracked in `docs/failure-modes.md` ("Known
gaps").

[#58]: https://github.com/junemartes/kardamom/issues/58

### Routing configuration

`ansible-playbook -i localhost, ansible/shard-map.yml` renders the initial
identity map from `partition_count`. It refuses to overwrite a map whose
version is above zero. Pass `-e shard_map_output=/path/to/map.toml` to render
an independent map.

Ansible parses `config/shard-map.toml` and passes its table to the static
sequencer job. The job expands one group per lane. During a resize, the Rust
controller passes both tables through `ansible/resize.yml`, which shares the
deployment image verification, variables, and job planner; Nomad retains losing
groups and starts gaining slots in shadow mode. After ingress cutover and
drain, the controller submits the target table alone. A dry run writes no files.

For manual sequencer submissions, pass the current table as the `shard_table`
Nomad variable. Omitting it selects the two-lane development identity map.
Use `just check-contract` for the Ansible contract assertions. Routing hashes,
rebalance behavior, and funded-account coverage are checked by Rust tests.
The Python files under `ansible/tests` remain isolated test fixtures and runners.
