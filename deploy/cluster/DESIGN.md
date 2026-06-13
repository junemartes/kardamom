# Nomad + Ansible Deployment Backend — Design

- **Date:** 2026-05-29
- **Status:** Approved (brainstorming) — pending implementation plan
- **Branch:** `claude/nomad-ansible-deploy` (off `claude/kardamom`)
- **Topic:** A reproducible multi-node test/staging cluster that brings up the
  full kardamom Aeron service pipeline via Vagrant → Ansible → Nomad (Docker
  driver), as the real-cluster successor to `crates/e2e/tests/multiprocess_e2e.rs`.

## 1. Context

The kardamom rollup runs as a pipeline of independent service processes
communicating over Aeron:

- **Deployable services** (each a `kardamom-<svc>` binary, configured uniformly
  with `--config <TOML>` + `--aeron_dir` + a few flags):
  `ingress`, `sequencer`, `executor` (embeds the `StateWriter`), `sealer`,
  `da_watcher`, `batcher`. `state` is a library, not a standalone process.
- **Aeron substrate:** every node runs an Aeron **Media Driver** (mmap'd ring
  buffers in a tmpfs `aeron.dir`); a quorum of **recorders** runs the Aeron
  **Archive** to durably record the canonical `tx_ordering` stream. Channels are
  config-driven URI strings (`ChannelsConfig` in `kardamom-log`).
- **L1 coupling:** `da_watcher` reads L1 deposits over an RPC URL; `batcher`
  posts batches to L1.

Today the only multi-process bring-up is `multiprocess_e2e.rs`: it runs Aeron in
one Docker container and spawns the services as host subprocesses pointed at the
container's bind-mounted `aeron.dir`, over `aeron:ipc?…` channels (single host).
Its own comment notes "ansible / nomad would launch each `kardamom-*` binary the
same way this test does." This spec turns that aspiration into a real,
reproducible multi-node cluster.

There are no existing `ansible/`, `nomad/`, or `terraform/` artifacts; this work
is greenfield and **additive** (new files under `deploy/cluster/`).

## 2. Goals / Non-goals

**Goals**
- One-command, reproducible bring-up of a 5-node kardamom cluster on a single
  developer machine: `make up` ≈ `vagrant up` → build/push images → Ansible →
  `nomad run` → smoke test.
- Real multi-host topology: services on different VMs communicate over Aeron
  **UDP**; a 3-node recorder quorum durably records `tx_ordering`.
- Pure-Nomad orchestration of all workloads via the **Docker driver** (Aeron and
  every service is a container).
- A smoke test that drives `eth_sendRawTransaction` through the ingress proxy and
  asserts receipts — the reproducible successor to `multiprocess_e2e`.

**Non-goals (explicitly deferred / separate specs)**
- Production HA, cloud provisioning, autoscaling, secrets management hardening.
- Per-service metrics instrumentation + Grafana dashboards (separate workstream;
  services do not yet expose Prometheus endpoints).
- The in-process bench *orchestrator* (separate "Phase 1" idea).
- Changes to service business logic. If a service needs a new flag/config knob to
  be deployable (e.g. to set a UDP channel endpoint), that is in scope; reworking
  service internals is not.

## 3. Target environment

- **Provisioner:** Vagrant with **libvirt** (primary) and **VirtualBox**
  (fallback) providers. A `Vagrantfile` declares N Linux VMs (default Ubuntu LTS)
  with **static private IPs** on a host-only/NAT network.
- **Node count:** parameterized; **default 5**.

## 4. Topology (default 5 nodes)

| Node | Role | Runs |
|------|------|------|
| `r1` | recorder + control plane | media driver, archive, recorder/quorum member; **Nomad server**, **Consul server**, **local Docker registry**; Nomad client |
| `r2` | recorder | media driver, archive, recorder; Nomad+Consul client |
| `r3` | recorder | media driver, archive, recorder; Nomad+Consul client |
| `w1` | worker | media driver; sequencer #0, executor, ingress; Nomad+Consul client |
| `w2` | worker | media driver; sequencer #1, sealer, da_watcher, batcher; Nomad+Consul client |

- **Aeron quorum:** 3 recorders (`r1..r3`); tolerates 1 recorder failure. Each
  recorder gets a distinct `recorder_id`.
- **Sequencers:** M=2 (`w1`, `w2`), partitioned by `keccak(sender) % M`.
- Node→workload placement is enforced via Nomad **node metadata/class**
  (`role=recorder|worker`) set by Ansible, and Nomad job `constraint`s.

## 5. Aeron-in-Docker pattern (the crux)

Two facts drive the container design:
1. Aeron clients reach the media driver via **mmap'd ring buffers** in
   `aeron.dir` — shared memory, not a socket.
2. Cross-node communication is **UDP**.

Therefore:

- **Shared tmpfs `aeron.dir` per node.** Ansible mounts a host tmpfs (e.g.
  `/dev/shm/aeron` or a dedicated `tmpfs` mount). The media-driver container and
  every co-located service container **bind-mount the same host path**, so they
  share the CnC file and ring buffers (same inode-backed mmap).
- **Host networking.** All Aeron + kardamom containers run with
  `network_mode = "host"`. Aeron UDP channel endpoints are then simply the VM's
  static IP — no Docker port-mapping. (Trade-off: no per-container network
  isolation; acceptable for a test cluster, and the simplest correct path for
  Aeron UDP.)
- **Archive on recorders.** Recorder nodes additionally run an archive container
  sharing `aeron.dir` plus a persistent `archive_dir` volume on the VM disk
  (segment files + catalog).

**Channel configuration.** `ChannelsConfig` URIs switch from `aeron:ipc?alias=…`
to `aeron:udp?endpoint=<node-ip>:<port>` (control/data as Aeron requires). The
concrete endpoints are rendered per-node (see §7). Stream-id schemes
(`tx_data_stream_id_base + sequencer_id`, etc.) are unchanged.

## 6. Components & repo layout (all new under `deploy/cluster/`)

```
deploy/cluster/
  Vagrantfile                  # N libvirt/virtualbox VMs, static IPs, role tags
  Makefile (or justfile)       # `make up` / `make down` / `make smoke` one-shot
  ansible/
    inventory.yml              # generated from Vagrant (static IPs + roles)
    site.yml
    roles/
      common/                  # base packages, sysctl, tmpfs mount for aeron.dir
      docker/                  # install Docker engine
      consul/                  # Consul server (r1) + agents
      nomad/                   # Nomad server (r1) + clients; node meta role=…
      registry/                # local Docker registry container on r1
      aeron/                   # archive_dir, tmpfs, JDK baked into image (role prep)
      kardamom/                # node config rendering, job submission helpers
  docker/
    aeron.Dockerfile           # media driver + archive (reuse crates/log/docker/aeron)
    service.Dockerfile         # multi-stage cargo build → slim runtime per service
  nomad/
    aeron-driver.system.nomad  # media driver, system job (all nodes)
    aeron-archive.system.nomad # archive, system job (constraint role=recorder)
    ingress.nomad
    sequencer.nomad            # parameterized by partition / node
    executor.nomad
    sealer.nomad
    da-watcher.nomad
    batcher.nomad
  config/
    channels.tpl               # Aeron UDP channel template (Consul-templated)
    <svc>.toml.tpl             # per-service config templates
    genesis/dev.toml           # chain genesis (funded accounts for smoke test)
```

## 7. Service discovery & config rendering (Consul)

- **Consul** (server on `r1`, agents everywhere) provides service registration +
  DNS/template lookups.
- Nomad job **`template` stanzas** render each service's TOML config and the Aeron
  channel endpoints from Consul (node addresses, the L1 RPC endpoint, ingress
  address). This avoids hand-maintaining IPs across configs.
- Genesis (`config/genesis/dev.toml`) is shipped to executor allocs via a Nomad
  `artifact`/`template` and passed with `--chain`. It prefunds the smoke-test
  signer accounts (Anvil-derived).

## 8. Image build & delivery (local registry)

- **Per-service image:** one multi-stage `service.Dockerfile` (cargo build →
  slim Debian runtime), parameterized by `--build-arg BIN=kardamom-<svc>`,
  producing `localhost:5000/kardamom-<svc>:<tag>`.
- **Aeron image:** built from the existing `crates/log/docker/aeron/Dockerfile`
  (media driver + archive + JDK).
- **Delivery:** host runs `docker build` + `docker push` to the **local registry**
  on `r1`; Ansible configures each VM's Docker to treat the registry as insecure
  (test cluster) and pull from it. Nomad docker driver references the registry
  images.

## 9. Bring-up flow

`make up`:
1. `vagrant up` — create/boot the 5 VMs with static IPs + role tags.
2. Ansible `site.yml` — install Docker/Consul/Nomad/registry; mount tmpfs; set
   Nomad node metadata (`role`); start the cluster; wait for Nomad+Consul healthy.
3. Host builds + pushes the 7 images to the registry on `r1`.
4. `nomad run` the **system** jobs (Aeron driver everywhere, archive on recorders);
   wait for the quorum to form (recording position advances).
5. `nomad run` the **service** jobs (ingress, 2× sequencer, executor, sealer,
   da_watcher, batcher); wait for ingress JSON-RPC to bind.
6. **Smoke test** (§10).

`make down` tears down jobs + `vagrant destroy`.

## 10. Verification / testing

- **Cluster health:** Ansible/Make asserts `nomad node status` shows 5 ready
  clients and all jobs `running`.
- **Pipeline smoke test:** drive `eth_sendRawTransaction` at the ingress endpoint
  and assert receipts come back with `status=true` — reusing the
  `multiprocess_e2e` submission logic (or `kardamom-bench transfers` once the
  bench is available on this branch; here we use a minimal curl/jsonrpsee client
  to avoid coupling to the bench rework). Optionally extend to the deposit path
  (da_watcher → sequencer → executor) like `multiprocess_e2e`'s deposit test.
- **CI note:** the full VM cluster is **not** run in GitHub CI (needs nested
  virtualization). CI validates only what is cheap: `ansible-lint`/`yamllint`,
  `nomad job validate` on the rendered specs, and `docker build` of the images.
  The full `make up` smoke test is a documented, locally-run / self-hosted-runner
  target.

## 11. Milestones (one spec, built in layers)

1. **Base cluster:** Vagrantfile + Ansible (Docker, Consul, Nomad, registry,
   tmpfs). Exit: `vagrant up` yields a healthy 5-node Nomad/Consul cluster.
2. **Aeron substrate:** service+aeron images, registry push, Aeron system jobs.
   Exit: media drivers up on all nodes, archive + recorder quorum forms (recording
   position advances).
3. **Service pipeline:** the 6 service jobs + Consul-templated configs + UDP
   channels + genesis. Exit: full pipeline `running`, ingress JSON-RPC reachable.
4. **Smoke test + one-shot + docs:** `make up`/`make smoke`/`make down`, README,
   CI validate-only checks.

## 12. Risks & open questions

- **Aeron UDP over Docker host networking:** MTU / flow-control / SO_RCVBUF tuning
  may be needed; recorders' archive fsync under VM disk may be slow. Mitigation:
  keep `file_sync_level` tunable; document sysctls (`net.core.rmem_max`, etc.) in
  the `common` role.
- **Resource footprint:** 5 VMs (each needs a JVM for the archive on recorders +
  revm in the executor) is heavy on one dev machine. Mitigation: parameterize node
  count/RAM; document minimum host specs; allow a reduced 3-node profile.
- **tmpfs sizing** for `aeron.dir` (term buffers can be large). Set an explicit
  size in the `common` role.
- **Provider variance:** libvirt vs VirtualBox networking differs; libvirt is
  primary, VirtualBox best-effort.
- **State writer persistence:** the executor's `StateWriter` (libmdbx) needs a
  persistent volume on its node; define its `data_dir` volume.
- **Recorder identity:** `recorder_id` must be unique and stable per recorder;
  derive from node metadata.
- **L1 endpoint for smoke test:** da_watcher/batcher need an L1 RPC. For the smoke
  test we run an `anvil` (or a stub) — decide whether anvil runs as a 6th
  container/job on `r1` or is assumed external. (Leaning: an `anvil` Nomad job on
  `r1` for self-containment.)

## 13. Success criteria

- `make up` on a clean machine yields a 5-node cluster running the full pipeline,
  and `make smoke` submits transfers through ingress and gets successful receipts,
  reproducibly.
- All cluster definition lives in-repo under `deploy/cluster/` and is
  version-controlled; no manual steps beyond `make up`.
