# Integrity sweeps (attested-identity plan, P0.2)

Read-only checks that the running cluster still matches what the deploy
declared. Companion to digest pinning (P0.1: `images.digests` +
`image_ref`) and `readonly_rootfs` (P0.3). All three scripts follow the
chaos suite's access pattern — `docker exec kardamom-<node> docker ...`
against the DinD container cluster — enumerate nodes from
`group_vars/all.yml` via `lib-topology.sh`, exit nonzero on findings, and
take `--help`.

## What each script proves

- **`image-drift.sh`** — for every kardamom task container on every node:
  the image ref the task was started from AND the RepoDigests of the image
  actually backing it both match the deploy's digest manifest
  (`deploy/cluster/images.digests`, written at push time). Catches: a task
  running the mutable-tag fallback, a node-cached stale image, a re-pushed
  image behind a stale ref, a container started by hand.
- **`fs-drift.sh`** — `docker diff` per kardamom container is empty except
  for the per-service allowlist (`fs-allowlist.txt`; JVM `/tmp` etc.).
  Catches: anything written into the image filesystem the deploy did not
  declare — dropped tools, modified binaries, unpacked payloads. Bind
  mounts (state/aeron/archive dirs) are outside its view by design.
- **`egress-inventory.sh`** — normalized per-node snapshot of TCP peers,
  UDP binds and multicast memberships, attributed to owning processes
  (all kardamom tasks are host-network, so node netns == task netns).
  `--generate` writes a baseline; `--expected FILE` diffs against one
  (seed: `expected-peers.tpl`, derived from `channels.toml.tpl` + the job
  specs). Catches: a service talking to an endpoint nobody declared —
  exfiltration, C2, or an undocumented dependency. Inventory only;
  enforcement comes after the inventory is quiet (plan P0.2).

## What this cannot prove

These sweeps are point-in-time observations made THROUGH the same host
docker/dockerd stack they are auditing: a compromised node kernel, dockerd,
or Nomad agent can lie to every probe here, and an in-memory-only implant
(injected code in a running process, no rootfs write, reusing an
already-allowed socket) is invisible to all three — image-drift proves what
was *loaded*, not that it has not since been exploited through its own
legitimate interfaces; fs-drift sees only rootfs layers, not bind-mounted
data dirs or memory; egress-inventory sees sockets that exist *during the
snapshot* and misses short-lived connections between runs. The honest claim
is: "at sweep time, the observable container state still matched the
deploy record, per an observer inside the same trust domain." Moving the
observer out of the workload's trust domain is exactly the P1 (SPIRE) and
P2 (TPM/Keylime, continuous quotes) work in
`docs/specs/attested-identity-plan.md`.

## Cadence

Cheap and read-only — run them often:

- `image-drift.sh` + `fs-drift.sh`: every 5 minutes.
- `egress-inventory.sh --expected`: every 15 minutes (socket snapshots are
  noisier; short-lived findings need a human eye during rollout anyway).
- All three once immediately after every deploy (a natural deploy.sh
  follow-up alongside `smoke.sh`).

Example systemd timer on the operator host (or the orchestrator container):

```ini
# /etc/systemd/system/kardamom-integrity.service
[Service]
Type=oneshot
ExecStart=/bin/bash -c '<repo>/deploy/cluster/scripts/integrity/image-drift.sh && \
  <repo>/deploy/cluster/scripts/integrity/fs-drift.sh && \
  <repo>/deploy/cluster/scripts/integrity/egress-inventory.sh --expected <repo>/deploy/cluster/scripts/integrity/expected-peers.txt'

# /etc/systemd/system/kardamom-integrity.timer
[Timer]
OnCalendar=*:0/5
```

(cron equivalent: `*/5 * * * *` for the drift pair, `*/15 * * * *` for the
egress compare.) Wire the nonzero exit to paging, not to a log file nobody
reads: a drift finding is either an incident or a missing declaration, and
both want a human now.
