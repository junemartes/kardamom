# kardamom multi-node cluster (Vagrant → Ansible → Nomad/Docker)

A reproducible **5-node** kardamom test/staging cluster on a single host. It is
the multi-host successor to `crates/e2e/tests/multiprocess_e2e.rs`: Vagrant boots
the VMs, Ansible installs the Nomad/Consul/Docker substrate, and Nomad runs the
Aeron media-driver/archive and the kardamom service pipeline as containers.

See [`DESIGN.md`](./DESIGN.md) for the full design rationale.

> **Status: scaffold.** All cluster definition lives here and is reviewable, but
> the full `make up` flow has **not** been executed end-to-end (this was authored
> in an environment without libvirt/Vagrant/Nomad). It also depends on the
> service-side changes listed under **[Required service changes](#required-service-changes)**
> before the pipeline will actually run multi-host over Aeron UDP. Treat job
> specs / playbooks as needing a real run-through on a virtualization host.

## Topology (default)

| Node | IP | Role | Workloads |
|------|----|------|-----------|
| `r1` | 192.168.56.11 | recorder + control | Nomad/Consul **server**, Docker registry, anvil (L1), media driver, archive, recorder (id 0) |
| `r2` | 192.168.56.12 | recorder | media driver, archive, recorder (id 1) |
| `r3` | 192.168.56.13 | recorder | media driver, archive, recorder (id 2) |
| `w1` | 192.168.56.21 | worker | media driver, sequencer #0, executor (+state), ingress (JSON-RPC :8545) |
| `w2` | 192.168.56.22 | worker | media driver, sequencer #1, sealer, da_watcher, batcher |

The 3 recorders form the Aeron quorum (tolerates 1 failure). All values are
defined once in [`ansible/group_vars/all.yml`](./ansible/group_vars/all.yml).

## Host prerequisites

**Quickest path:** from the repo root, `just cluster-bootstrap` installs all of
the host tools below for your platform, and `just cluster-doctor` verifies them.

Install on the **host** machine (the in-VM Nomad/Consul agents are installed
by Ansible):

- [Vagrant](https://www.vagrantup.com/) + a provider: **libvirt** (primary) or
  VirtualBox (fallback).
- Ansible (`ansible-playbook`) + collections:
  `ansible-galaxy collection install ansible.posix community.docker`.
- Docker with **BuildKit** (`DOCKER_BUILDKIT=1`) to build + push the
  service/Aeron images. Note: each service image compiles the workspace
  (including the bundled Aeron C/Java sources via rusteron) — the first build is
  slow and needs the full native toolchain (baked into the builder stage).
- The **host Docker daemon must allow the in-cluster registry as insecure**
  (it is plain HTTP): add `{ "insecure-registries": ["192.168.56.11:5000"] }`
  to `/etc/docker/daemon.json` (Linux) or Docker Desktop → Settings → Docker
  Engine, then restart Docker. `make images` pushes fail without this.
- The **Nomad CLI** on the host — `scripts/deploy.sh` drives the cluster's
  Nomad HTTP API from the host (`just cluster-bootstrap` installs a pinned
  version).
- Foundry's `cast` for the smoke test (the repo-level `just bootstrap`
  installs Foundry).
- ~18 GB RAM free (3×3 GB recorder VMs + 2×4 GB worker VMs; recorders run a
  JVM archive + the executor runs revm).

## Quick start

```sh
cd deploy/cluster
make up        # vagrant up → ansible → build+push images → nomad run
make smoke     # pipeline smoke test against ingress (see note below)
make status    # nomad/consul/job health
make down      # stop jobs + vagrant destroy
```

`make up` runs these phases (each is an individual target too):

1. `make vms` — `vagrant up` boots the 5 VMs with static IPs + role tags.
2. `make provision` — `ansible-playbook site.yml` installs Docker, Consul, Nomad,
   the local registry, the tmpfs `aeron.dir`, and tags Nomad nodes with their role.
3. `make images` — build the per-service + Aeron images on the host and push them
   to the registry on `r1`.
4. `make deploy` — `nomad run` the Aeron **system** job (ArchivingMediaDriver on
   every node), the anvil L1, then the service jobs.

`make smoke` submits `eth_sendRawTransaction` through ingress and asserts
receipts (see `scripts/smoke.sh`). It is intentionally **not** chained into
`make up`: until the channels-config plumbing lands (issue #36, item 1 under
[Required service changes](#required-service-changes)) the services use
single-host IPC channel defaults, so the cross-host pipeline — and therefore
the smoke test — is **expected to fail**.

## Layout

```
deploy/cluster/
  DESIGN.md                 design rationale
  Vagrantfile               5 libvirt/VirtualBox VMs, static IPs, role tags
  Makefile                  up / vms / provision / images / deploy / smoke /
                            validate / check-contract / down
  .yamllint                 lint config (matches ansible-lint's yaml rule)
  ansible/
    ansible.cfg, inventory.ini
    group_vars/all.yml      ← canonical contract (IPs, ports, versions, paths)
    site.yml
    roles/{common,docker,consul,nomad,registry}/
  docker/
    service.Dockerfile      multi-stage cargo build → slim runtime (BIN arg)
                            (the Aeron image builds straight from the canonical
                            crates/log/docker/aeron/Dockerfile — no copy here)
  nomad/
    aeron.system.nomad.hcl  ArchivingMediaDriver (driver+archive), system job,
                            all nodes
    anvil.nomad.hcl         in-cluster L1 for the smoke test
    ingress.nomad.hcl  sequencer.nomad.hcl  executor.nomad.hcl
    sealer.nomad.hcl   da-watcher.nomad.hcl  batcher.nomad.hcl
  config/                   *.toml(.tpl) pulled into the job specs via file()
  scripts/
    deploy.sh               submit jobs in dependency order, wait for allocs
    smoke.sh                transfers smoke test against the ingress endpoint
    check-contract.py       fail if any mirror of group_vars/all.yml drifts
```

The Nomad job specs pull their config payloads from `config/` with HCL2
`file()`, so submit them **from `deploy/cluster/`** (`scripts/deploy.sh` and
`make validate` already do).

## Aeron-in-Docker

- **Shared `aeron.dir`:** Ansible mounts a host tmpfs at `/opt/kardamom/aeron-mount`;
  the media-driver container and every co-located service container bind-mount the
  same path so they share the CnC file + mmap'd ring buffers.
- **Host networking:** all Aeron + service containers run `network_mode = "host"`,
  so Aeron UDP channel endpoints are just the VM IP — no Docker port mapping.
- **Channels:** `aeron:ipc?…` (single host) → `aeron:udp?endpoint=<node-ip>:<port>`.
  Endpoints are rendered from the contract in `config/channels.toml.tpl`.
- **Archive** runs only on recorders, sharing `aeron.dir` + a persistent
  `archive_dir` volume.

## Required service changes

The multi-host UDP topology depends on service-side work that is **not yet in the
codebase** (these are deployment prerequisites, tracked separately from this PR):

1. **Channels config plumbing.** *(tracked in #36)* Several binaries currently derive channels from
   `LogConfig::default().channels` (IPC URIs hardcoded) — e.g. `kardamom-da-watcher`
   uses `LogConfig::default().channels`, and `ingress`/`executor` build channels
   from defaults. To run over UDP across hosts, each service must accept a
   `--log-config <toml>` (or `KARDAMOM_LOG_CONFIG`) that supplies the
   `[channels]`/`[aeron]` config (UDP endpoints). Only `kardamom-sealer` currently
   takes a channel URI in its own config. The Nomad jobs here already render and
   mount a channels config (`config/channels.toml.tpl`) in anticipation of this
   flag; until the flag exists, the services ignore it and use IPC defaults
   (single-host only).
2. **Batcher is offline.** *(tracked in #39)* `kardamom-batcher` today reads Aeron Archive segment
   files in `--dry-run` (default) rather than running as a live service with L1
   broadcast. Its Nomad job is therefore modeled as a **periodic/batch** job
   pointed at the recorders' archive segments, not an always-on service. Wiring
   the live L1 broadcast path is a follow-up.

3. **No standalone recorder/quorum process.** *(tracked in #38)* The durability story
   (`recorder_id`, `QuorumConfig`, fsync-watermark + quorum-watermark channels,
   and ingress `--ack-policy on-quorum`) has no dedicated deployable binary in the
   workspace today — the recorder/quorum logic lives in `kardamom-log` as library
   code, and recording is done by the Aeron Archive. Until the recorder/quorum
   role has a process home (or is confirmed embedded in a specific service), the
   "3-recorder quorum" is topology intent: the Archive runs on all nodes, but the
   `on-quorum` ack path won't be satisfied. The ingress job therefore defaults
   `--ack-policy` to `on-offer` (as `multiprocess_e2e` does); once #38 lands,
   submit with `nomad job run -var ack_policy=on-quorum ingress.nomad.hcl`.

4. **Per-node channel rendering.** *(tracked in #37)* With two sequencers on different VMs (w1/w2),
   the per-sequencer (`{sid}`) and per-recorder (`{rid}`) channel URI *templates*
   can't encode a distinct per-host IP from a single file. The current
   `channels.toml.tpl` uses Aeron MDC (`control-mode=dynamic`) for those and fixed
   unicast endpoints for the singleton streams; the eventual `--log-config` flow
   will likely need the channels config rendered **per node** (one file per
   worker) rather than one shared file.

> Note: there is **no `aeron-live` feature** to toggle — `rusteron` is an
> unconditional dependency of `kardamom-log`, so a plain `cargo build` already
> produces real-Aeron binaries. The service Dockerfile builds with no extra
> feature flag (an `AERON_FEATURE` build-arg placeholder is provided in case
> Aeron is later made optional).

## Verification status

| Check | Status |
|-------|--------|
| Design reviewed & approved | ✅ (`DESIGN.md`) |
| Artifacts authored | ✅ |
| `nomad job validate` (all 8 specs, Nomad 1.9.5) | ✅ pass; also in CI (`cluster-validate`) |
| `yamllint` / `ansible-lint` (production profile) | ✅ pass; also in CI (`cluster-validate`) |
| Contract drift (`scripts/check-contract.py`) | ✅ pass; also in CI (`cluster-validate`) |
| `make up` on a real virtualization host | ⛔ not run (no libvirt in authoring env) |
| Multi-host Aeron UDP pipeline | ⛔ blocked on [required service changes](#required-service-changes) |

Run `make up` on a host with the toolchain before relying on this. The Aeron
UDP-over-Docker-host-networking path (MTU, SO_RCVBUF, archive fsync on VM disk)
is the highest-risk area to exercise first.
