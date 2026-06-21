# kardamom multi-node cluster (Vagrant → Ansible → Nomad/Docker)

A reproducible **5-node** kardamom test/staging cluster on a single host. It is
the multi-host home of the end-to-end pipeline test — the `cluster-e2e` client
(`crates/e2e/src/bin/cluster_e2e.rs`) drives it over ingress JSON-RPC + the
in-cluster anvil L1 — replacing the single-host `multiprocess_e2e.rs`. Vagrant
boots the VMs, Ansible installs the Nomad/Consul/Docker substrate, and Nomad runs
the Aeron media-driver/archive and the kardamom service pipeline as containers.

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
| `r1` | 192.168.56.11 | recorder + control | Nomad/Consul **server**, Docker registry, anvil (L1), media driver, archive, `kardamom-recorder` (id 0), quorum aggregator |
| `r2` | 192.168.56.12 | recorder | media driver, archive, `kardamom-recorder` (id 1) |
| `r3` | 192.168.56.13 | recorder | media driver, archive, `kardamom-recorder` (id 2) |
| `w1` | 192.168.56.21 | worker | media driver, sequencer #0, executor (+state), ingress (JSON-RPC :8545) |
| `w2` | 192.168.56.22 | worker | media driver, sequencer #1, sealer, da_watcher, batcher |

The 3 `kardamom-recorder` processes record `tx_ordering` and publish their
fsync watermarks; the quorum aggregator (a single `kardamom-recorder
--aggregate` on r1) combines them into the quorum watermark that ingress
gates on with `--ack-policy on-quorum` (tolerates 1 recorder failure). All
values are defined once in
[`ansible/group_vars/all.yml`](./ansible/group_vars/all.yml).

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
    recorder.system.nomad.hcl  kardamom-recorder, records tx_ordering +
                            publishes fsync watermark (role=recorder)
    quorum.nomad.hcl        kardamom-recorder --aggregate (quorum watermark,
                            count=1)
    anvil.nomad.hcl         in-cluster L1 for the smoke test
    ingress.nomad.hcl  sequencer.nomad.hcl  executor.nomad.hcl
    sealer.nomad.hcl   da-watcher.nomad.hcl  batcher.nomad.hcl
  config/                   *.toml(.tpl) pulled into the job specs via file();
                            channels.toml.tpl is the shared LogConfig
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
- **Channels:** `aeron:ipc?…` (single-host default) → `aeron:udp?endpoint=<mcast-group>:<port>|interface=192.168.56.0/24`.
  The whole `LogConfig` (channels + quorum + archive control) is rendered once
  in `config/channels.toml.tpl` and consumed by every service + the recorder
  via `--log-config` (issue #36). One shared file works on every node because
  channels are **UDP multicast**: per-stream identity is the stream id, so the
  `{sid}`/`{rid}` template substitutions only label the `alias` — no per-node
  rendering needed (this is what closes #37).
- **Archive** runs on all nodes; `kardamom-recorder` records `tx_ordering` only
  on the recorders, sharing `aeron.dir` + a persistent `archive_dir` volume.

## Required service changes

The deployment originally depended on four service-side changes. **#36 and #38
are now implemented in this PR** (so the multi-host UDP pipeline and the
on-quorum durability path are wired end-to-end); #37 is dissolved by the
multicast channel layout; only the batcher (#39) remains out of scope.

1. ✅ **Channels config plumbing (#36).** Every pipeline binary
   (`ingress`/`sequencer`/`executor`/`sealer`/`da-watcher`) and the new
   `kardamom-recorder` now accept `--log-config <toml>` (env
   `KARDAMOM_LOG_CONFIG`) and load a `LogConfig` from it, falling back to the
   built-in single-host IPC defaults when unset (so single-host local runs like
   `full_pipeline_e2e` are unchanged). The Nomad jobs render `config/channels.toml.tpl` and pass
   `--log-config /local/channels.toml`.
2. ⛔ **Batcher is offline (#39).** `kardamom-batcher` still reads Aeron Archive
   segment files in `--dry-run`; its Nomad job remains a periodic/batch job, not
   an always-on service. Wiring the live L1 broadcast path is a follow-up.
3. ✅ **Deployable recorder/quorum process (#38).** `kardamom-recorder` records
   `tx_ordering` on each recorder (`recorder.system.nomad.hcl`, one per
   `role=recorder` node, reading `${meta.recorder_id}`) and publishes its fsync
   watermark; a single `--aggregate --no-record` instance (`quorum.nomad.hcl`)
   publishes the Q-of-N quorum watermark. The ingress job therefore defaults
   `--ack-policy` to **`on-quorum`** (override to `on-offer` for an
   Aeron-substrate-only bring-up).
4. ✅ **Per-node channel rendering (#37) — not needed.** The multicast layout
   uses one shared `channels.toml` for all nodes (stream-id, not per-host IP,
   distinguishes publishers), so there is nothing to render per node.

> Note: there is **no `aeron-live` feature** to toggle — `rusteron` is an
> unconditional dependency of `kardamom-log`, so a plain `cargo build` already
> produces real-Aeron binaries. The service Dockerfile builds with no extra
> feature flag (an `AERON_FEATURE` build-arg placeholder is provided in case
> Aeron is later made optional).

## Verification status

| Check | Status |
|-------|--------|
| Design reviewed & approved | ✅ (`DESIGN.md`, `docs/agents/log-config-and-recorder-spec.md`) |
| `--log-config` (#36) + `kardamom-recorder` (#38) | ✅ implemented; unit + config tests pass |
| `nomad job validate` (all 10 specs, Nomad 1.9.5) | ✅ pass; also in CI (`cluster-validate`) |
| `yamllint` / `ansible-lint` (production profile) | ✅ pass; also in CI (`cluster-validate`) |
| Contract drift (`scripts/check-contract.py`) | ✅ pass; also in CI (`cluster-validate`) |
| Pipeline + on-quorum + redundancy in containers | ⚙️ `cluster-e2e` workflow (gated; see below) |
| `make up` on a real virtualization host | ⛔ not run (no libvirt in authoring env) |

The **single biggest thing to validate** is whether Aeron preserves the
canonical `tx_ordering` order across multiple cross-host publishers (2
sequencers + the sealer) over UDP multicast — this is the property the
container `cluster-e2e` job exercises. The single-host IPC defaults (no
`--log-config`) remain the known-good path. Also exercise the
UDP-over-host-networking tuning (MTU, `SO_RCVBUF`, archive fsync on VM disk).

## Sustained-load + chaos suite

The `cluster-e2e` workflow runs the full suite on every trigger, **sharded
across runners** (each shard brings up its own cluster):

| Shard | Exercises |
|-------|-----------|
| `load` | 5-min sustained soak (`kardamom-load` ramp→soak; must-deliver + drop accounting + keep-pace) |
| `chaos-executor` | graceful + hard kill + **node-failure** (Nomad reschedule) |
| `chaos-ingress` | graceful + hard kill (singleton restart) |
| `chaos-sequencer` | graceful + hard kill (partition restart) |
| `chaos-sealer` | graceful restart (SPOF recovery) |

`kardamom-load` is the harness (`crates/bench`, `bin/load.rs`); `chaos.sh`
injects the failures under steady load and asserts Nomad auto-recovery + that
the pipeline keeps producing blocks.

### Known resilience gaps

- **Hard sealer crash → executors freeze** ([#58]). After a `docker kill`
  (SIGKILL) of the singleton sealer under load, the sealer process restarts and
  resumes sealing, but the executors do **not** re-attach to its restarted
  canonical `tx_ordering` MDC publication — they freeze and the pipeline stops
  processing. A **graceful** sealer restart (SIGTERM) recovers cleanly. The
  sealer is a singleton SPOF (HA is future work), so the `sealer-hard` chaos
  case is **excluded from the always-on suite** and tracked in [#58]; reproduce
  it on demand with `CHAOS_CASES=sealer-hard deploy/cluster/scripts/chaos.sh`.

[#58]: https://github.com/junemartes/kardamom/issues/58
