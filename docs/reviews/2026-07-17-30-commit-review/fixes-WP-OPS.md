# Fixes — WP-OPS (deploy scripts, CI, docs)

Findings: F03.1, F01.1, F01.3, F04.1, F02.5, F15.3, F15.6, F15.7b, F07.6,
F09.6b/F10.9b, F16.1, F16.2, F16.3, F16.4, F16.6, F17.1, F18.1, F12.11,
F11.1b, F22.2, F04.2 — plus the WP-SEQ and WP-VAL doc/script hand-offs.

Ownership note: edits stay inside the parent-granted `deploy/**`,
`.github/workflows/**`, `justfile`, `docs/**` surface. That includes a few
files outside SUMMARY's narrower "Owns" line but explicitly assigned here by
findings/hand-offs: `deploy/cluster/nomad/validator.nomad.hcl` (F10.9b
comment parts), `deploy/cluster/{Vagrantfile,Makefile}`,
`deploy/cluster/ansible/group_vars/all.yml` (F16.1/F16.6),
`deploy/cluster/docker/node.Dockerfile` (comment only). No Cargo.toml, no git
mutations (the one file removal is a plain working-tree `rm`).

New file: `deploy/cluster/scripts/lib.sh` (shared control-node helpers).

## Per-finding status

### F03.1 [M] — chaos.sh bridge probe never implemented; EXECUTOR_IPS dead — FIXED
`deploy/cluster/scripts/chaos.sh`. New `exec_metrics()` helper: probes
`http://<executor-ip>:9004/metrics` DIRECTLY over the cluster bridge first
(the exporter binds 0.0.0.0:9004 — the bind that was previously pointless),
falling back to `docker exec` for loopback-only deploys. Both progress probes
(`sealer_boundaries`, `executor_progress`) now go through it, so the
dockerd-stall flake mechanism (privileged sibling kill freezing every
`docker exec` probe, issue #76) is out of the primary observation path.
`EXECUTOR_IPS` is now load-bearing and its comment says so.

### F01.1 [M] — chaos.sh comments describe the discarded side-stream refetch — FIXED
`chaos.sh`: the `run_validator_lapse` header and the `run_case` dispatch
comment now describe the real mechanism — live term-buffer drain on resume +
the #78 catch-up skip bounding aged-out blocks — and the phantom
`bal_refetched` assertion claim is gone. (The function's actual assertions
were already correct; only the narrative lied.)

### F01.3 [L] — warm-up loop proceeds after 150s budget expires unmet — FIXED
`chaos.sh run_validator_lapse`: a `warmed` flag is set only when the
condition is met; budget expiry now fails with
"never warmed up ... not verifying live BEFORE the pause" instead of logging
"warmed up" and producing a misleading downstream failure.

### F04.1 [L] — archive-driver-loss trivially passes on 0/empty baseline — FIXED
`chaos.sh`: fail fast before injecting when `count_running aeron` reads
empty/0. The kill-that-misses half is also covered now: `inject_hard` resolves
the container id first and fails if nothing matches (see F15.3).

### F02.5 [L] — sequencer-replica-kill load not pinned to killed shard — FIXED
`chaos.sh`:
- Precomputed `ACCT_SHARD` map for the 16 funded Anvil accounts
  (`keccak256(address)[..8] as BE u64 % 2`, matching
  `crates/ingress/src/routing.rs::partition_for`; derivation documented,
  values verified with `cast keccak`). The case now advances `CHAOS_ACCT`
  past shard-1 accounts (burned, never reused) so its single sender provably
  lands on shard 0 — the shard whose replica A (seq-a on node-0) is killed.
- `hard-sequencer` now kills the explicit `sequencer-a` task (the old
  `name=sequencer` filter matched both replica groups and killed an
  arbitrary one).
- **WP-SEQ hand-off implemented**: new `assert_replica_republishes` polls the
  restarted replica's own exporter (node-0 :9001; bridge-direct with docker
  exec fallback) until `kardamom_sequencer_tx_published_to_b_total` > 0 —
  the F02.1 zombie (floors stuck below the live join point, alloc healthy,
  publishing nothing) now fails the case explicitly. The case's load window
  is widened to `INJECT_DELAY + restart SLO + 60s` so traffic is still
  flowing when the assertion runs.

### F15.3 [M] — assert_count recovery SLO passes vacuously — FIXED
`chaos.sh`: injectors record what they killed; `assert_count` now requires
observed recovery, not just a count:
- graceful (`nomad alloc stop`): >= N running allocs **excluding** the
  stopped alloc id (Nomad replaces the alloc);
- hard (inner `docker kill`): the killed task running again under a **new
  container id** on the same node (Nomad restarts in-place, so the alloc id
  survives — the container id is the observable that must change), plus the
  count.
Markers are consumed by the first `assert_count` after an injection;
whole-node kills keep the plain count semantics (the count genuinely dips).

### F15.6 [L] — fixed INJECT_DELAY sleep with no load-flowing check — FIXED
`chaos.sh run_case`: `INJECT_DELAY` is now a minimum. After it elapses the
case polls `kardamom_ingress_tx_received_total` (summed across both ingress
nodes, new `ingress_received` helper) against a pre-load baseline and only
injects once it moves; bounded by new `LOAD_FLOW_TIMEOUT_S` (default 60,
documented in the header), failing rather than killing into an idle
pipeline. Also fails immediately if kardamom-load already exited.

### F15.7b [N] — shell helpers duplicated; failed scrapes missing from service_up — PARTIAL
- Duplication FIXED: new `deploy/cluster/scripts/lib.sh` holds
  `on_control` / `running_alloc` / `running_allocs` / `all_allocs` /
  `count_running`; chaos.sh sources it (private copies deleted) and
  ci-cluster.sh sources it too (its executor-churn snippet and the F07.6
  divergence loop now use the helpers).
- `service_up` sub-part DEFERRED: that code lives in
  `crates/bench/src/load/scrape.rs` (WP-BENCH's Owns; their pass fixed only
  the plan.rs part of F15.7). Needs a one-line follow-up there: treat a
  failed scrape as down in non-chaos mode instead of omitting the key.

### F07.6 [N] — divergence grep reads only first validator alloc — FIXED
`ci-cluster.sh`: the "halted on divergence" log check now loops over EVERY
alloc of the validator job (`all_allocs` from lib.sh), so a pre-reschedule
divergence in an old alloc's log is no longer missed.

### F09.6b [N] — ci-cluster.sh shadow-check comment says cadence 1 — FIXED
Comment now matches `validator.nomad.hcl` (`--trie-shadow-check 8`, every 8th
block, with the CI-core rationale pointer).

### F10.9b [N] — stale placement/co-resident comments (script + hcl parts) — FIXED
- `ci-cluster.sh` 7c header: "one alloc on an executor-class node" → "one
  alloc on the aux node — kept out of the executor-chaos blast radius".
- `validator.nomad.hcl`: the port-40230 arg comment no longer justifies
  itself via a nonexistent "co-resident executor"; also refreshed the
  `--replay-destination-endpoint` comment which still claimed "used only when
  the state DB is non-empty" — stale after WP-VAL's F13.3 fix (replay-merge
  is now always used when the flag is set, fresh starts included).

### F16.1 [M] — static-inventory/Vagrant path broken — FIXED (regenerated, not deleted)
Per the caution: the Vagrant path is kept and made consistent with the
node-class contract instead of deleted.
- `ansible/inventory.ini`: rewritten to the node-class model (12 hosts,
  `<class>-<i>` at `ip_prefix.<ip_start+i>`), every host defining the vars
  the templates require — `node_ip`, `role` (= class, matching the Nomad
  jobs' `${meta.role}` constraints), `tier`, `node_index` — with a keep-in-
  sync header pointing at `node_classes`.
- `Vagrantfile`: NODES list rewritten to the same 12-node class topology
  (control-0@.10, sequencer-0/1@.21-.22, ingress-0/1@.31-.32,
  executor-0..2@.41-.43, sealer-0..2@.51-.53, aux-0@.61).
- `ansible/inventory.containers.ini`: DELETED (dead — ci-cluster.sh generates
  its own container inventory; also part of F16.6). Safe: check-contract.py
  references it only in its dead legacy `cluster_nodes` branch (group_vars
  uses `node_classes`; verified check-contract still passes), and the
  node.Dockerfile comment that pointed at it was updated.
- `Makefile`: SERVICES `sealer` → `validator` (there is no kardamom-sealer
  binary to build; the validator job was missing), `images` now builds/pushes
  the Java `kardamom-cluster` image too (failing loudly with the gradle
  command if the shadowJar is missing — previously the Vagrant path could
  never deploy `cluster.nomad.hcl`), `status`/`down` use
  `vagrant ssh control-0` (was the removed `r1`), stale "smoke expected to
  FAIL until #36" note removed, `clean` covers the new image + staged jar.
- Verified: `ansible-inventory --list` parses; `nomad.hcl.j2` and
  `consul.hcl.j2` render correctly against the new inventory (checked
  executor-1: bind_addr .42, role=executor, tier=worker, node_index=1;
  control-0: server=true; sequencer-1: node_index=1) — the exact renders
  that previously failed on undefined `node_ip`/`tier`.
- NOT done (out of scope, noted): template `| default(...)` fallbacks — the
  inventory now defines the vars, and an omission still fails at render time
  with Ansible's undefined-variable error naming the var.

### F16.2 [L] — cluster-doctor greps old registry IP .11 — FIXED
`justfile:402`: pattern now `192\.168\.56\.10:5000`, matching the message
text and `group_vars` (`registry_host`).

### F16.3 [L] — cluster-e2e.yml diagnostics use removed container names — ALREADY FIXED at HEAD
The workflow's failure step already iterates
`docker ps --format '{{.Names}}' | grep '^kardamom-'`; no `cp1/r1/...` list
exists anywhere in `.github/workflows/` (verified by grep). No change needed.

### F16.4 [L] — smoke-load.sh NODE_IP map uses removed r1..w2 topology — FIXED
`smoke-load.sh`: the map is now generated from `node_classes` in
`group_vars/all.yml` (same no-PyYAML regex parse as ci-cluster.sh /
check-contract.py; generation verified to emit all 12 current nodes). Only
built when `METRICS_VIA_DOCKER=0`; on that path a missing group_vars file or
an unmappable node is a hard error instead of a silent 127.0.0.1 fallback
that scraped the wrong host.

### F16.6 [N] — vestigial recorder/quorum config; i32→u32 port wrap — PARTIAL
- FIXED (owned): `group_vars/all.yml` — `recorder_count`, the "3 RECORDERS
  COLLOCATED" paragraph, the `recorder: 3 quorum N=3` replication line, and
  the "each with a recorder" comments removed (aux comment updated to include
  the validator). No references to `recorder_count` remain anywhere. The dead
  `inventory.containers.ini` deleted (see F16.1). check-contract.py still
  passes.
- LEFT AS-IS (deliberate): `channels.toml.tpl`'s `[quorum] n=1 q=1` block —
  it is already explicitly marked VESTIGIAL in place, and removing it safely
  requires first removing/defaulting the `quorum` field in the Rust
  `LogConfig` (crates/log — WP-LOG/WP-CFG territory).
- DEFERRED (cross-WP): `tx_receipts_endpoint_base_port: i32` silently
  wrapping via `as u32` — `crates/log/src/config.rs:146,241,267`, WP-LOG's
  Owns; recommend validating `> 0` when MDS is enabled.

### F17.1 [N] — SMOKE_SENDER_OFFSET silently clamps — FIXED
`smoke-load.sh`: a NEGATIVE offset is now a hard `fail` (clamping to 0 would
land on exactly the reserved account the knob exists to avoid); the >15 clamp
and the senders-window clamp emit `warn` lines. The `log`/`warn`/`fail`
helpers moved above the config section so they exist at validation time.

### F18.1 [L] — deploy/cluster/README.md documents removed recorder topology — FIXED
Rewritten to the current architecture: node-class topology table (control /
sequencer×2-racing-replicas / ingress×2 / executor×3 / sealer = 3-member Aeron
Cluster / aux with validator), archive-at-the-sealer durability (with a
pointer to the superseded recorder spec), both bring-up paths (container CI
path + Vagrant path), current nomad-spec and scripts listing (incl.
`validator.nomad.hcl`, `cluster.nomad.hcl`, `lib.sh`), corrected registry IP
(.10), JDK-17/shadowJar prerequisite, current CI shard table (load /
chaos-executor / chaos-ingress / chaos-sequencer / chaos-cluster), and the
#58 single-sealer SPOF gap marked superseded by the Raft cases.

### F12.11 [N] — cluster-e2e.yml contradictory `cluster` feature comments — FIXED
Both stale sentences (header + build-step preamble claiming the binaries
"MUST be built WITH their `cluster` feature") deleted; the surviving text
states the cluster transport is unconditional (no cargo feature). YAML
re-validated.

### F11.1b [M] — observability.md port table omits validator — FIXED
`docs/observability.md`: `kardamom-validator | 127.0.0.1:9007` row added
(no dashboard yet), with a note explaining 9007 exists because 9006 is the
ingress default (the historical local collision), and that the CLUSTER
deploy pins the validator to `0.0.0.0:9006` on the aux node
(validator.nomad.hcl — no ingress there), which is what ci-cluster.sh's
verdict scrapes. Matches WP-VAL's bin change (default 9006 → 9007).

### F22.2 [N] — historical spec lacks superseded note — FIXED
`docs/agents/log-config-and-recorder-spec.md`: Status changed from "Draft —
pending plan approval" to "Historical — superseded by archive-at-the-sealer
durability", naming the removed machinery, noting `--log-config` lives on,
and marking it a point-in-time record not to implement from.

### F04.2 [N] — ~1MB raster diagrams, no editable source — ACCEPTED (mitigated)
Per instruction, images NOT deleted. No editable sources exist in the repo
(the diagrams are generated by matplotlib scripts — `arch_diagram.py`,
`state_diagrams.py` — kept outside the repo per the established
docs+diagram workflow). Added `docs/img/README.md` recording that
provenance, the regenerate-don't-hand-edit rule, and a recommendation to
commit the generating scripts alongside any future re-render. Follow-up for
the maintainer: check the scripts in.

## Hand-offs consumed (from other WPs)

- **WP-SEQ → docs**: `docs/agents/replicated-sequencer-shards-spec.md`
  "Restart / rejoin semantics" rewritten around the stream-adaptive
  nonce-floor fast-forward (`nonce_floor_lag_ms`, hydration as a lower bound,
  the new regression tests, the chaos assertion); "Observability" rewritten
  to the actual state (both replica groups on 0.0.0.0 :9001/:9011, host_id
  replica stamping, `max by (partition)` aggregation rule, fast-forward
  panel/alert signal) replacing the false ":9011 twins are additional, not
  double-counted" claim. `docs/failure-modes.md` replica-restart bullet
  updated the same way (committed-state hydration claim removed).
- **WP-SEQ → chaos.sh**: restarted-replica coverage assertion implemented
  (see F02.5).
- **WP-VAL → docs/hcl/scripts**: observability port table (F11.1b),
  validator.nomad.hcl + ci-cluster.sh stale comments (F10.9b), including the
  replay-flag comment made stale by the F13.3 behavior change.

## Verification

- `bash -n` on every touched/added script (`chaos.sh`, `ci-cluster.sh`,
  `smoke-load.sh`, `lib.sh` — plus the untouched rest of
  `deploy/cluster/scripts/*.sh`) — all pass. shellcheck not available on this
  host.
- `python3 deploy/cluster/scripts/check-contract.py` — pass (after the
  group_vars + inventory changes).
- YAML: `cluster-e2e.yml` and `group_vars/all.yml` parse (PyYAML; yamllint
  not installed).
- `nomad job validate nomad/validator.nomad.hcl` — pass (comment-only edits;
  no agent connection available, spec-level validation only).
- `ansible-inventory -i ansible/inventory.ini --list` — parses; template
  render checks against the new inventory: `nomad.hcl.j2` (executor-1 →
  bind_addr .42 / role executor / tier worker / node_index 1; sequencer-1 →
  node_index 1) and `consul.hcl.j2` (control-0 → server=true) all render with
  no undefined variables — the F16.1 failure mode.
- `make -n` parses the edited Makefile.
- smoke-load.sh node-map generator executed standalone: emits all 12
  `<class>-<i> <ip>` pairs matching node_classes.
- ACCT_SHARD values cross-checked with `cast keccak` per address (u64 parity
  of the 16th hex digit; the earlier full-u64 arithmetic overflow in zsh was
  caught and redone).
- Not run: a live cluster/chaos run (no DinD cluster on this host — the
  dev-host inotify limit blocks systemd containers per the environment
  notes); Vagrant path (no libvirt/ruby here).
