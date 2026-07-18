# kardamom multi-node cluster (Ansible → Nomad/Consul → Docker)

A reproducible multi-node kardamom test/staging cluster. Two ways to
materialise the nodes, sharing the same Ansible playbook and Nomad jobs:

- **Containers (the path CI runs):** `scripts/ci-cluster.sh` boots one
  privileged systemd + Docker-in-Docker container per node on a
  `192.168.56.0/24` bridge and drives the full bring-up + smoke + load +
  chaos suite (`.github/workflows/cluster-e2e.yml`).
- **VMs (Vagrant):** `make up` boots one VM per node (libvirt primary,
  VirtualBox fallback) and provisions them with the same `site.yml`.

See [`DESIGN.md`](./DESIGN.md) for the original design rationale and
[`../../docs/failure-modes.md`](../../docs/failure-modes.md) for per-actor
failure/recovery behavior and the chaos cases that verify it.

> **Status.** The container path runs green in CI (`cluster-e2e`, sharded
> across runners). The Vagrant path shares all of its Ansible/Nomad surface
> with it but has not been exercised end-to-end on a real virtualization host.

## Topology

Nodes are defined as **classes** in
[`ansible/group_vars/all.yml`](./ansible/group_vars/all.yml)
(`node_classes`: class → `{count, ip_start, tier}` — the single source of
truth). Instance `<class>-<i>` gets IP `ip_prefix.<ip_start+i>`:

| Class | Count | IPs | Runs |
|-------|-------|-----|------|
| `control` | 1 | .10 | Nomad/Consul **server**, Docker registry, anvil (L1) |
| `sequencer` | 2 | .21–.22 | 2 shards × 2 racing replicas (job groups `seq-a`/`seq-b`, cross-placed via `meta.node_index`) |
| `ingress` | 2 | .31–.32 | active/active JSON-RPC front door (:8545) |
| `executor` | 3 | .41–.43 | state-machine replica appliers (libmdbx state) |
| `sealer` | 3 | .51–.53 | **3-member Aeron Cluster (Raft)** — the Java `cluster` job: canonical ordering + archive-at-the-sealer durability folded into the Raft log |
| `aux` | 1 | .61 | validator, da_watcher, batcher (off the chaos blast radius) |

Every non-control node also runs the Aeron `ArchivingMediaDriver` (the `aeron`
Nomad system job). There is **no standalone sealer binary and no
recorder/quorum tier** anymore: ordering is the Java Aeron Cluster
(`cluster/sealer-service/`), and durability is the sealer archive (the old
Q-of-N recorder design is preserved, marked superseded, in
`docs/agents/log-config-and-recorder-spec.md`).

## Host prerequisites

**Quickest path:** from the repo root, `just cluster-bootstrap` installs the
host tools below for your platform, and `just cluster-doctor` verifies them.

- For the **VM path**: [Vagrant](https://www.vagrantup.com/) + libvirt
  (primary) or VirtualBox (fallback).
- Ansible (`ansible-playbook`) + collections:
  `ansible-galaxy collection install ansible.posix community.docker`.
- Docker (with BuildKit) to build + push the service/Aeron images.
- The **host Docker daemon must allow the in-cluster registry as insecure**
  (plain HTTP): add `{ "insecure-registries": ["192.168.56.10:5000"] }` to
  `/etc/docker/daemon.json` (Linux) or Docker Desktop → Settings → Docker
  Engine, then restart Docker. Pushes fail without this.
- The **Nomad CLI** on the host — `scripts/deploy.sh` drives the cluster's
  Nomad HTTP API from the host.
- **JDK 17 + Gradle wrapper** for the Java Aeron Cluster node jar:
  `(cd cluster/sealer-service && ./gradlew :service:shadowJar)` — `make
  images` / `ci-cluster.sh` stage it into the `kardamom-cluster` image and
  fail loudly if it is missing.
- Foundry's `cast` for the smoke tests (repo-level `just bootstrap`).

## Quick start (VM path)

```sh
cd deploy/cluster
make up        # vagrant up → ansible → build+push images → nomad run
make smoke     # single-tx pipeline smoke against ingress
make status    # nomad/consul/job health
make down      # stop jobs + vagrant destroy
```

`make up` phases (each is an individual target too):

1. `make vms` — `vagrant up` boots one VM per `node_classes` instance.
2. `make provision` — `ansible-playbook site.yml` (inventory:
   `ansible/inventory.ini`, kept in sync with `node_classes`) installs
   Docker, Consul, Nomad, the registry, the tmpfs `aeron.dir`, and stamps
   each Nomad node's meta (`role`, `tier`, `node_ip`, `node_index`).
3. `make images` — build the service + Aeron + cluster images on the host and
   push them to the in-cluster registry.
4. `make deploy` — `scripts/deploy.sh` submits the jobs in dependency order:
   `aeron` (system) + `anvil`, then `cluster` (the Raft sealer), then
   `sequencer` / `ingress` / `executor` / `validator` / `da-watcher`, then
   the periodic `batcher`.

The container path is one command: `deploy/cluster/scripts/ci-cluster.sh`
(`KEEP=1` leaves the node containers up; `scripts/local-cluster.sh` wraps it
for Docker Desktop hosts).

## Layout

```
deploy/cluster/
  DESIGN.md                 design rationale (original; recorder tier since removed)
  Vagrantfile               one VM per node_classes instance (VM path)
  Makefile                  up / vms / provision / images / deploy / smoke /
                            validate / check-contract / down
  ansible/
    ansible.cfg, inventory.ini   (static VM inventory — mirrors node_classes;
                                  the container path generates its own)
    group_vars/all.yml      ← canonical contract (classes, IPs, ports, versions)
    site.yml
    roles/{common,docker,consul,nomad,registry}/
  docker/
    service.Dockerfile      multi-stage cargo build → slim runtime (VM path)
    ci-service.Dockerfile   thin wrapper over prebuilt binaries (CI path)
    cluster.Dockerfile      Java Aeron Cluster node (shadowJar + JRE 17)
    node.Dockerfile         systemd+DinD "node" container (CI path)
  nomad/
    aeron.system.nomad.hcl  ArchivingMediaDriver (driver+archive), all nodes
    cluster.nomad.hcl       3-member Aeron Cluster (Raft) sealer, .51/.52/.53
    anvil.nomad.hcl         in-cluster L1 for the smoke test + da-watcher
    ingress.nomad.hcl  sequencer.nomad.hcl  executor.nomad.hcl
    validator.nomad.hcl  da-watcher.nomad.hcl  batcher.nomad.hcl
  config/                   *.toml(.tpl) pulled into the job specs via file();
                            channels.toml.tpl is the shared LogConfig
  scripts/
    lib.sh                  shared control-node helpers (nomad via docker exec)
    deploy.sh               submit jobs in dependency order, wait for allocs
    smoke.sh                single-tx smoke test against ingress
    smoke-load.sh           bash sustained-load smoke (legacy fallback)
    ci-cluster.sh           container-node bring-up + full CI suite
    local-cluster.sh        ci-cluster.sh wrapper for Docker Desktop
    chaos.sh                chaos suite (kill components under load)
    check-contract.py       fail if any mirror of group_vars/all.yml drifts
```

The Nomad job specs pull their config payloads from `config/` with HCL2
`file()`, so submit them **from `deploy/cluster/`** (`scripts/deploy.sh` and
`make validate` already do).

## Aeron-in-Docker

- **Shared `aeron.dir`:** Ansible mounts a host tmpfs at
  `/opt/kardamom/aeron-mount`; the media-driver container and every co-located
  service container bind-mount the same path so they share the CnC file +
  mmap'd ring buffers.
- **Host networking:** all Aeron + service containers run
  `network_mode = "host"`, so Aeron UDP channel endpoints are just the node
  IP — no Docker port mapping.
- **Channels:** one shared `config/channels.toml.tpl` (UDP multicast; stream
  ids distinguish publishers) consumed by every service via `--log-config`.
- **Durability:** the sealer's Aeron Cluster members archive the canonical
  log (`archive-at-the-sealer`); executors persist state in libmdbx under
  `/opt/kardamom/state` and crash-recover by archive replay-merge.

## Sustained-load + chaos suite

The `cluster-e2e` workflow runs the full suite on every trigger, **sharded
across runners** (each shard brings up its own container cluster):

| Shard | Exercises |
|-------|-----------|
| `load` | 5-min sustained soak (`kardamom-load` ramp→soak; must-deliver + drop accounting + keep-pace) |
| `chaos-executor` | graceful + hard kill + **node-failure** (degrade to 2/3, node returns) |
| `chaos-ingress` | graceful + hard kill + **archive-driver-loss** (Aeron substrate kill under ingress-0) |
| `chaos-sequencer` | graceful + hard kill + **sequencer-replica-kill** (racing-twin failover, restarted replica must regain coverage) + **validator-lapse** |
| `chaos-cluster` | Raft sealer: **leader-kill** / **follower-kill** / **quorum-loss-recover** |

`kardamom-load` is the harness (`crates/bench/src/load/`); `chaos.sh` injects
the failures under steady load and asserts Nomad auto-recovery + pipeline
progress + the load verdict. The old single-sealer `sealer-hard` SPOF case
([#58]) is superseded by the Raft cluster cases (a `sealer-hard` arm is kept
in `chaos.sh` only for legacy single-sealer deploys). Remaining untested
surface is tracked in `docs/failure-modes.md` ("Known gaps").

[#58]: https://github.com/junemartes/kardamom/issues/58
