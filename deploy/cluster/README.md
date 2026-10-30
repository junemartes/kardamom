# Kardamom multi-node cluster (Ansible, Nomad, Consul, Docker)

This directory builds a reproducible multi-node Kardamom cluster. It has two profiles that share one Ansible tree and one set of Nomad jobs.

| Profile | Use | Nodes |
|---------|-----|-------|
| `local` | CI and a developer host | `terraform/containers` creates one privileged systemd and Docker-in-Docker container per node on a Docker bridge. The `container-*` recipes run the full bring-up, smoke, load and chaos suite (`.github/workflows/cluster-e2e.yml`). |
| `production` | A dedicated core and elastic cloud pools | Infrastructure code outside this repository owns the cloud network, the pools and the public entry points. The Nomad Autoscaler creates the elastic machines. `ansible/inventories/production` holds the dedicated core. |

- The production profile is described in [`PRODUCTION.md`](./PRODUCTION.md).
- The design record is [`DESIGN.md`](./DESIGN.md).
- The failure model, and the chaos case that proves each behavior, is in [`../../docs/failure-modes.md`](../../docs/failure-modes.md).

> **Status.** CI runs the container profile (`cluster-e2e`, sharded across runners). The production profile shares the Ansible and Nomad surface with it. CI does not run the production profile.

## Topology

The node classes are the single source of truth. They are in [`ansible/group_vars/all.yml`](./ansible/group_vars/all.yml) under `node_classes`.

- A class has these fields:
  - `count`: the number of nodes.
  - `tier`: a coarse group (`control`, `sequencer` or `worker`).
  - `roles`: the service classes that every node of the class hosts. Optional.
  - `instance_roles`: the service classes that one node hosts, by index. Optional.
- Instance `<class>-<i>` is the Consul node `<class>-<i>.node.<datacenter>.consul`.
- No file in the tree names a node address.

The local profile defines these classes:

| Class | Count | Runs |
|-------|-------|------|
| `control` | 1 | Nomad and Consul server, Docker registry, anvil (the in-cluster L1) |
| `sequencer` | 2 | 2 lanes with 2 racing replicas each. One job group per lane (`seq-<lane>`, expanded by the Nomad HCL). The metrics port is `9001 + 10 * lane`. |
| `ingress` | 2 | The active/active JSON-RPC front door (`:8545`) and the notifier status feed (`:8547`). Role `redis`. Instance 1 also has role `redis-replica`. With `KARDAMOM_NOTIFIER_NODES=on`, the notifier moves to the `notifier` nodes. See [The notifier nodes](#the-notifier-nodes). |
| `executor` | 3 | State-machine replica appliers (libmdbx state). The state mirror runs here too. With `KARDAMOM_EGRESS=1`, the egress runs on two of them (`:8548`, metrics `:9013`), unless `KARDAMOM_EGRESS_NODES=on` moves it to its own nodes. See [The egress](#the-egress). |
| `sealer` | 3 | A 3-member Aeron Cluster (Raft). The Java `cluster` job orders the transactions. The Raft log and its archive hold the canonical log. |
| `aux` | 1 | The service roles `redis`, `redis-primary`, `batcher`, `monitoring`, `validator`, `da-watcher` and `indexer`. |

- A service job places itself on a role, not on a node name. The roles `batcher`, `monitoring`, `validator`, `da-watcher` and `indexer` all sit on `aux-0` in the local profile.
- The DA service (`da-proxy` or `da-store`), the light client and the fault proxy follow the same rule. The DA proxy and the DA stand-in run on the `batcher` role. The light client runs on the `validator` role.
- The production profile gives each service class its own nodes. See [Placement by role set](./PRODUCTION.md#placement-by-role-set).

### Names, not addresses

Every node runs its Consul agent as the node resolver (`roles/consul`).

- The agent answers DNS on the loopback port 53. The previous resolvers of the host are its recursors.
- `consul-resolver.service` restores `/etc/resolv.conf` at every boot. Docker regenerates this file when a node container restarts.
- A node is `<class>-<i>.node.<datacenter>.consul`. A service is `<service>.service.consul` (for example `registry`, `anvil`, `ingress-jsonrpc`).
- The Nomad jobs derive every peer list from a count and the datacenter. A job file, a config file or a script never names an address.
- `ansible/contract.yml` rejects an address literal in `nomad/`, `config/`, `ansible/`, the cluster and root justfiles, and the e2e workflow. The one exception is an environment file: the Terraform variables of a production deployment.
- An image build captures the recursors of the build server. Every elastic node forwards to them.

The test suite and the operator commands (`crates/chaos`) run on the Docker host, outside the cluster resolver.

- They read every node address from the node contract.
- The justfile reads the control node address from the contract too.

The Aeron `ArchivingMediaDriver` (the `aeron` Nomad system job) runs on every node except the control and the sealer nodes. Each sealer member embeds its own driver in the Java Aeron Cluster (`cluster/sealer-service/`).

- There is no standalone sealer binary and no recorder tier.
- The Java Aeron Cluster orders the transactions.
- The sealer archive provides durability.

## Host prerequisites

The quickest path starts at the repository root.

1. Run `mise trust`.
2. Run `mise run setup`. It installs the pinned CLI tools and the Ansible collections.
3. Install and start Docker.
4. Run `mise exec -- just cluster-doctor` to check the host.

See the root [quick start](../../README.md#quick-start) for shell activation and native prerequisites. `just cluster-bootstrap` installs OS-level packages.

| Need | Detail |
|------|--------|
| Ansible | `ansible-playbook` and the collections: `ansible-galaxy collection install -r deploy/cluster/ansible/requirements.yml` from the repository root. |
| Docker | With the Buildx plugin, for the node containers and the image builds. The daemon must run privileged containers. On macOS or Windows, that is the Linux VM of Docker Desktop. |
| Registry push | Images are pushed from inside the control node (`REGISTRY_PUSH_NODE`, the justfile default). The registry name `registry.service.consul` resolves there. The host Docker daemon needs no insecure-registry entry. |
| Nomad CLI | On the Ansible controller. It compiles HCL and embeds local config files. Ansible submits the jobs through the Nomad API. |
| cosign | For signed deployments. It must be on `PATH`, in the pinned cache of the image builder, or set with `workloads_cosign_binary`. |
| JDK 17 and the Gradle wrapper | For the sealer jar: `(cd cluster/sealer-service && ./gradlew :service:shadowJar)`. `just images` and `just container-up` stage it into the `kardamom-cluster` image. They fail if it is missing. |
| Rust service binaries | In `target/release`: `cargo build --release --bins` of the service crates, or the artifact of `just stage-dist`. The image role wraps prebuilt binaries. Nothing compiles inside an image. |
| OpenTofu | Version 1.12.6, the version that CI pins. |
| Foundry `cast` | For the smoke tests (repository-level `just bootstrap`). |

## Quick start

`just` 1.49.0 or newer is required. Run these recipes from the repository root or from this directory.

```sh
just container-up      # tofu apply, node contract, ansible/cluster.yml
just container-test    # one shard's gates against that cluster (default: load)
just container-down    # tofu destroy: containers and their volumes
just container-reset   # destroy, then a fresh chain
just shard chaos-executor   # one shard end to end, the way CI runs it
```

- Every cluster recipe also exists at the root. This includes `just images`, `just deploy`, `just rollback`, `just smoke`, `just validate`, `just check-contract`, `just container-diagnostics` and `just clean`.
- The root recipes run in `deploy/cluster`. Relative paths and environment overrides behave the same in both places.
- Pass extra variables to `ansible/cluster.yml` with `CLUSTER_VARS='{"images_tag": "x"}'`. It is one JSON object with no single quote.
- `ansible/cluster.yml` is the convergence playbook. It expects the node contract.

### The local profile

`terraform/containers` is the Terraform root that owns the node containers. See its [README](./terraform/containers/README.md). It reads `node_classes` from `ansible/group_vars/all.yml`. The node-class model has one source.

It creates:

- The `kardamom-net` bridge network on `kardamom-br0`, with the range of the `subnet` variable (default `192.168.56.0/24`).
- The `kardamom-node:ci` image from `docker/node.Dockerfile`.
- Two named volumes for each node, for the inner Docker engine.
- One privileged systemd container for each node, `kardamom-<class>-<i>`. It has a stable address of the subnet (name order from host 10) and a health check on `systemctl is-system-running`.

The bring-up sequence:

1. `tofu apply` returns when systemd is ready in every node.
2. `tofu output node_contract` is the version 1 node contract. `just container-up` writes it to `terraform/containers/node-contract.json`.
3. `ansible/containers.yml` builds the in-memory inventory from the contract.
4. It prepares the host network (`roles/host_prep`: socket buffers, bridge netfilter, multicast snooping).
5. It runs `bootstrap.yml` on the `container_nodes` group.

State and replacement:

- Terraform replaces a container when its image changes. This discards the root filesystem of that node.
- A change to `node.Dockerfile` therefore gives a fresh chain on the next `just container-up`.
- The Terraform state in `terraform/containers/` is the record of the running cluster.
- A second `just container-up` is a no-op apply followed by a convergence run.
- `just container-prepare` runs the bring-up without `deploy.yml`. `just container-save <dir>` then stops the nodes and keeps each one as the image `kardamom-node-<node>:prepared`, with its volumes. With `node_images` set, a node of generation 0 starts from its saved image. `ansible/resume.yml` resumes it. [The prepared cluster in CI](../../docs/chaos-suite.md#the-prepared-cluster-in-ci) describes the flow.

Containers that another harness left behind under the same names block the first apply. Remove them once. Remove the volumes too: Terraform adopts an existing volume by name, and an old `kardamom-<node>-docker` volume carries stale inner Docker state into the new node.

```sh
docker rm -f $(docker ps -aq --filter name=kardamom-)
docker volume rm $(docker volume ls -q --filter name=kardamom-)
docker network rm kardamom-net
```

### How CI builds

CI builds once.

- A `build` job compiles the service binaries, the shard test executable, the operator binary and the sealer jar. It stages them as one artifact (`just stage-dist`, the layout of the checkout).
- A shard runner unpacks the artifact and runs `just shard <name>` with `KARDAMOM_STAGED=1`. It needs no Rust toolchain, JDK or Foundry.
- `KARDAMOM_STAGED=1` makes the justfile run `target/release/kardamom-chaos-shards` and `target/release/kardamom-cluster` instead of `cargo`.
- The Aeron C library is compiled for x86-64-v3 through the `scripts/ci/cc-x86-64-v3.sh` wrapper. The artifact runs on any runner.
- A failed shard runs `container-diagnostics` and leaves the cluster up. The runner is ephemeral, so nothing destroys the cluster afterward.

## Ansible workload deployment

From the repository root, deploy to an already provisioned cluster with built images:

```sh
ansible-playbook -i localhost, deploy/cluster/ansible/deploy.yml
```

The controller runs the playbook locally and connects to the Nomad HTTP API.

1. It verifies the image manifest before it changes a job.
2. It compiles the HCL with `nomad job run -output`.
3. It plans the changes.
4. It registers only the jobs that changed. The Nomad job modify index rejects a concurrent edit.

Plan an existing deployment without a job submission or a contract change:

```sh
ansible-playbook -i localhost, deploy/cluster/ansible/deploy.yml --check \
  -e workloads_settlement_address=0xYOUR_EXISTING_SETTLEMENT_ADDRESS
```

Check mode needs an existing settlement address and a reachable Nomad API. It compiles and plans all jobs. It does not wait for allocations and does not create them.

### Rolling deploys

A deploy rolls each service job one instance at a time, under a health check.

- A Consul check means "this instance does its job":
  - The Rust services answer `/ready`, beside `/metrics`.
  - The ingress RPC port answers `/health`.
  - The sealer admin port (`cluster_ports.admin`, 40205) answers `/ready`.
  - The rules of each service are in [Readiness](../../docs/observability.md#readiness).
- The role waits for the Nomad deployment of each job and requires `successful`. A deployment that Nomad marks `failed` fails the play at that job. The next job is not touched.
- A system job (the Aeron drivers) has no deployment. The role waits for its allocations instead.
- The first job of each deploy is `data-dirs`, a sysbatch job on every node of every pool. It makes each data directory of `node_data_dirs` in `ansible/group_vars/all.yml` and sets its mode (`0777`). The role waits until each allocation is complete, and registers no other job before.
  - The job runs the Aeron image of the release manifest, as root, with a shell as its entrypoint. It needs no image outside the manifest.
  - A release changes the Aeron image, so each release registers the job again, and it runs again on every node. A rollback reverts it with the other jobs.
  - Nomad stops the allocation of a node that goes down, and the wait no longer counts it. An allocation that does not complete fails the deploy when the poll budget runs out (`workloads_poll_retries` polls, `workloads_poll_delay` seconds apart).
  - The common role makes the same directories when it provisions a node. The job gives a node that existed before a service the directory of that service.
  - Without the directory, Docker makes the bind-mount source as `root` with mode `0755`, and a service that runs as uid 10001 cannot write to it.
  - A new data directory goes into `node_data_dirs` only.
- The role waits for every job except `cluster-backup`. The transaction canary is the last job: an unhealthy canary fails the deploy.
  - The canary is healthy when its task runs and `/ready` answers for 10 s (`min_healthy_time`), within 2 minutes (`healthy_deadline`). Its `/ready` reads only the liveness of its exporter. A failed or slow probe does not change it, so only a canary that does not start, or that does not stay running, fails the deploy.

The `update` stanza of each job:

| Job | `max_parallel` | `health_check` | `min_healthy_time` | `auto_revert` | Other |
|-----|----------------|----------------|--------------------|---------------|-------|
| `ingress`, `sequencer` | 1 | `checks` | 15s | `true` | `canary = var.canary`, `auto_promote = false` |
| `cluster` (sealer) | 1 | `checks` | 60s | `false` | One task group for each member |
| `cluster-backup` | 1 | `task_states` | 30s | `false` | Only with a node in the group `backup`. The deploy does not wait for it. |
| `executor`, `validator`, `batcher`, `state-mirror` | 1 | `checks` | 15s | `false` | |
| `notifier`, `canary`, `egress` | 1 | `checks` | 10s | `false` | |
| `da-watcher`, `l1-indexer`, `l1-light-client`, `da-proxy`, `da-store` | 1 | `task_states` | 10s | `false` | |
| `aeron` (system job) | 1 | `task_states` | 15s | not set | `stagger = "30s"` |

The sealer roll:

- The sealer has one task group for each Raft member, and the role makes one registration for each member.
- The role rolls the followers first and the leader last. It finds the leader through the admin port (`GET /status`).
- Each step replaces one member. The `update` stanza holds the next step until that member rejoins and catches up.
- A fresh job, or a job that still has one shared group, registers in one step.
- The executor job on the sealer nodes rolls its pinned groups the same way, one group at a time. See [The carry window](#the-carry-window).

The canary:

- The ingress and the sequencer can deploy a canary. Set `KARDAMOM_CANARY=1`. The canary is one allocation more than the count, so it needs one spare node of the class. The container profile has none.
- The role smokes the ingress canary by its node address, and promotes it. Then it replaces the old instances.
- `kardamom-deploy smoke` sends the smoke transfer, from genesis account #0. Every release ships this binary, signed.
- For the sequencer, the canary is healthy by its own check. The smoke then goes through a running ingress and proves that the chain still accepts a transaction.
- A failed smoke fails the play. The deployment stays unpromoted, and the old instances keep serving. `nomad deployment fail` removes the canary.
- `auto_revert` is on for these two stateless classes only.

The deploy record and the rollback:

- Before the first registration, the role writes the attempt record: the target, the version of every job, and the rollback floor. Each registration adds the job to the record. A deploy that succeeds makes the attempt the accepted release. See [The deploy record](#the-deploy-record).
- `just rollback <env>` reverts every job of the last release to its version before that release, in reverse deploy order, under the waits of the deploy (`/v1/job/<id>/revert` with `EnforcePriorVersion`). The sealer rolls back member by member. The runbook is [`docs/runbooks/deploy-rollback.md`](../../docs/runbooks/deploy-rollback.md).
  - After a failed attempt, the rollback goes to the accepted release. After a successful deploy, it goes to the release before it.
  - A job that Nomad already reverted (`auto_revert`), or that an earlier run reverted, is done. A rollback that stops resumes after the jobs in `rolled_back`.
  - It refuses a rollback floor, and a second rollback.
  - When Nomad dropped the versions to revert to, it writes `deployed/<env>/rollback.digests` and names `just rollback-rerender <env>`: a normal rolling deploy of the older images with the job files of the checkout.
- A successful deploy also records its manifest under `deployed/<env>/`. `KARDAMOM_ENV` selects `<env>` (default `local`). `images.digests` is what the last deploy registered. `images.digests.previous` is what it replaced.
- The validator keeps a divergence verdict in a file beside its state. The verdict survives a restart and a deploy, and `/ready` keeps failing. An operator clears it with `kardamom-validator --state-dir <dir> --clear-verdict` inside the allocation.
- The smoke, load and chaos gates of `crates/chaos` follow in CI.

### The deploy record

The deploy record of an environment lives in the Nomad variable `kardamom/deploys/<env>`. The role mirrors it to `deployed/<env>/attempt.json`, `accepted.json` and `accepted.formats.toml`. The variable is the record that every operator and the rollout tooling read. The mirror is for an operator without the Nomad token. The variable has two items, `attempt` and `accepted`, each one JSON document of this shape:

| Field | Meaning |
|---|---|
| `env` | The environment name. |
| `status` | `started` while the attempt runs or after it failed. `accepted` after the deploy succeeded. `rolled_back` after `just rollback`. |
| `started_at`, `accepted_at`, `rolled_back_at` | UTC times. |
| `operator` | `KARDAMOM_OPERATOR`, else `GITHUB_ACTOR`, else `USER`. |
| `target.revision` | The revision of the deployed tree: `KARDAMOM_REVISION`, else `git rev-parse HEAD` of the checkout, else `GITHUB_SHA`, else jj. In a git checkout the gate refuses a revision that is not a commit of the checkout. |
| `target.manifest` | The image manifest, by service: `{"ingress": "<registry>/kardamom-ingress@sha256:..."}`. |
| `target.formats` | Optional: the text of `formats.toml`. A deploy does not write it, because Nomad limits the variable to 64 KiB. When the accepted release holds it, the gate compares the formats of the target with it; without it there is no base. |
| `before.revision`, `before.manifest` | The same fields of the accepted release at the start of the attempt. `before.formats` is there only when the accepted release holds the formats text; a rollback carries it back. |
| `before.jobs` | The Nomad job version of every job the namespace knows at the start: `{"ingress": {"before": 3}}`. A registration adds `after`, the version the attempt registered. A job that the attempt adds has `before: null`. |
| `changed` | The jobs the attempt registered, in registration order. A job joins before its registration. The rollback reverts them in reverse order. |
| `rolled_back` | The jobs a rollback reverted so far, in that order. A rollback that stops resumes after them. |
| `floor` | `{}`, or `{"formats": {"<id>": <version>}, "revision": "..."}` when `KARDAMOM_ALLOW_ONE_WAY` accepted a one-way format change. `just rollback` does not cross a floor. |
| `retired` | The retired format ids that `KARDAMOM_ALLOW_RETIRED` accepted, or `[]`. |
| `restored_from` | On an `accepted` record that a rollback restored: the `started_at` of the rolled-back release. |

- `attempt` is always the last attempt. `accepted` is the last release that a deploy completed, or that a rollback restored. A failed attempt never becomes `accepted`.
- The gate refuses a deploy while the last attempt is `started`: it runs, or it died. `KARDAMOM_REPLACE_ATTEMPT=1` deploys over a dead one. The record then holds the mixed versions that run, so a rollback of it goes to that mixed state. Roll a failed attempt back before you deploy the fix.
- Every write of the record is a check-and-set on the modify index of the variable. Two controllers cannot write over each other: the second one stops.
- The deploy token needs `write`, `read` and `list` on `kardamom/deploys/*` in its namespace. The items stay under the 64 KiB limit of a Nomad variable: the two registries are the largest part.
- An environment name has letters, digits, `-` and `_` only. The Nomad variable path admits no other character.

### Sealer bootstrap

A new sealer cluster needs one bootstrap. The bootstrap decides which blank members start at log position 0.

- A blank sealer member has no Raft recording log.
  - During the bootstrap, a blank member starts at log position 0.
  - At any other time, a blank member copies the latest snapshot from a peer before it starts. It waits while no peer answers.
- Set `KARDAMOM_CLUSTER_BOOTSTRAP=1` for the first deploy of a new cluster.
  - The role writes the Nomad variable `nomad/jobs/cluster` while it registers the new cluster job.
  - Each member reads the variable through the file `bootstrap` in its task directory.
  - The role deletes the variable when the deployment ends, also when it fails. A later start of a blank member then reads no flag.
- The role ignores the flag when Nomad knows the job. A re-deploy with the flag set is safe.
- `just container-up` and the chaos bring-up set the flag.
- Do not set the flag when you register a purged job again for a cluster that holds state.
  Nomad does not know a purged job, so the role would open the bootstrap.
- A cluster that lost all its state can start from a seed that `kardamom-reconstruct --sealer-seed` writes. The deploy has no switch for the seed. See the [sealer README](../../cluster/sealer-service/README.md#seeded-start).
- A bootstrap with `KARDAMOM_GENESIS_CARRY=on` starts the new chain with the carry window. See [The carry from genesis](#the-carry-from-genesis).

### Sealer log purge

Each sealer member purges its own Raft log behind its snapshots.

- The log of the 3 newest snapshots stays. Set `KARDAMOM_CLUSTER_LOG_PURGE_KEEP` to change the count. The value 0 turns the purge off.
- The log of every block that the batcher has not posted also stays.
- A member that stops for longer than this margin seeds from a peer when it comes back.
- See "Raft log purge" and "Follower below the purge point" in [`../../docs/failure-modes.md`](../../docs/failure-modes.md#sealer-the-aeron-cluster-raft).

### The upgrade check

Run `just sealer-upgrade-check <env> <feature> <activation_ms>` before the Safe signs an upgrade transaction, with the feature id and the activation time of the transaction. A new network sends `initiateUpgrade(2, 0)` (`FEATURE_SEALER_V3` on at once) once after its first deploy: run `just sealer-upgrade-check <env> 2 0` before it. The first `setFeature` system deposit installs the chain-state code, whatever feature it schedules (see below). The procedure is "Decision version" in the [sealer README](../../cluster/sealer-service/README.md). The check refuses:

- feature 2 (`FEATURE_SEALER_V3`) with an activation time other than 0: off is no longer supported;
- a configured sealer member that is down, or that reads a snapshot version below 14;
- feature 4 (`FEATURE_SEALER_V5`, the carry flag) when a sealer member reads a snapshot version below 16;
- feature 3 or 4 with an activation time other than 0 while a member reports `kardamom_sealer_feature_state{feature="5"}` at 1 (held) or 2 (on). Without decision version 5 the rules of feature 5 stop, and a carry executor stops on the first reference. Turn feature 5 off first, and empty `KARDAMOM_CARRY_EXECUTORS`;
- feature 5 (`FEATURE_CARRY_WINDOW`, the carry window), at any activation time, unless:
  - every member reports `snapshotReadsMax` 18 or more, `decisionVersion` 5 and `retainedStore` `heap`;
  - the sequencer job has the metas `sealer_version_kept` and `v2_loop` (the v2 loop, `KARDAMOM_SEQUENCER_ADMISSION=on`), the batcher and the validator jobs have `reads_egress`, and the egress job has `ref_mark`, each with every live allocation on that job version (`workloads_upgrade_window_jobs`);
  - every egress instance reports its reference mark, and every `kardamom_batcher_egress_cursor_index` and `kardamom_validator_egress_cursor_index` is above the highest `kardamom_egress_ref_frame_max_index`. An instance that does not answer is a refusal, and so is a chain with no batcher or no validator;
- a job without its metas (`workloads_upgrade_jobs` in `ansible/roles/workloads/defaults/main.yml`), or a live allocation of an older job version:
  - the sequencer: `nonce_burn`;
  - the executor and the validator: `nonce_burn` and `chain_state_install`;
  - the ingress: `nonce_burn`.

The meta `chain_state_install` marks a release that installs the `KardamomChainState` code with the first `setFeature` system deposit of any feature on a genesis without it. That release also reads the flags once per block, frozen at the start of the block. The genesis of this deploy (`config/genesis/dev.toml`) has no such code. An older executor or validator runs the deposit as a call to an empty account and forks the state at that deposit. See "A genesis without the chain-state code" in [`../../docs/failure-modes.md`](../../docs/failure-modes.md#l1-governed-upgrades-feature-flags).

### Settlement

A deployment needs a settlement address or a built `kardamom-deploy` binary (`DEPLOY_BIN`).

- Without an address, the playbook bootstraps the development Anvil factory. It queries `addresses --contract KardamomL2Settlement --json`. It deploys only if that chain has no settlement registration.
- The playbook fails. It does not start a batcher with a placeholder address.
- To avoid the development-chain bootstrap, supply an existing address.
- Owner keys reach the CLI through the environment. They are hidden from the task output.

### Signed deployments

Signed mode needs a complete digest manifest.

- It verifies the bundle and each image with cosign before it changes a workload.
- It never falls back to a mutable tag.
- Local unsigned mode keeps an explicit dev-tag fallback.
- The upstream images (anvil, the node exporter, Prometheus, Alertmanager, Grafana) are pinned by tag and digest in `upstream_images` (`ansible/group_vars/all.yml`). They are not signed. The light-client image is built by the image playbook, pinned by digest and signed like the service images.

### The egress

The egress (`kardamom-egress`, `nomad/egress.nomad.hcl`) is the stage after the executors. It merges the `exec_frames` copies of every executor into one canonical stream and keeps the stream and the receipts on disk. It serves the readers after the executors:

- `egress_order` (Aeron stream 1009): the merged frames, live.
- `egress_receipts` (Aeron stream 1011): the receipts of every executor, each once, with their account rows.
- HTTP on `:8548` (`ports.egress_http`), as the Consul service `kardamom-egress`:
  - `GET /v1/frames?from_index=<i>&from_block=<b>`: the range read of a reader that resumes.
  - `GET /health`: the Consul check.
  - `POST /`: `eth_getTransactionReceipt`, as the state query of an executor.
- Metrics on `:9013` (`ports.egress_metrics`), as the Consul service `kardamom-egress-metrics`.

Placement:

- Two instances, on two different executor nodes (`distinct_hosts`). The job variable `egress_count` sets the count, at least 2.
- The egress reads every executor, and the executor nodes keep state on their own disks.
- The readers of the canonical stream (the validator and the batcher) stay on the aux node. So the loss of one node never stops the egress and those readers together.
- Each instance keeps its store in `/opt/kardamom/egress` (`paths.egress_dir`) on its node.

The floor of the store:

- Each instance keeps every block above the posted head. It reads the posted head from the `l1_blocks` stream of the L1 follower, so the follower must run.
- With `WITHDRAWAL_ORACLE_ADDRESS`, each instance keeps every block above the lower of the posted head and the final head. It reads the final head from the oracle on L1: through the light client when one runs, else through the in-cluster anvil. With a real L1 and no light client, the deploy gives the egress no L1 endpoint, and the egress refuses to start with the oracle alone.
- Each instance also keeps `keep_blocks` blocks below its head (job variable, default 43,200: 1 day at 2 s a block), for the readers that resume. A reader further behind than this window and the floor gets `410` with the floor of the store. The executor and the validator then recover from a peer checkpoint.
- Size the disk of the executor nodes for the larger of the window and the stretch above the floor: the DA-lag budget, and the challenge window with an oracle. `KardamomEgressDiskFillsSoon` fires before the disk is full.

Every switch is off by default. With every switch off, the deploy renders every job as before. Switch them on in this order, one deploy for each step:

1. `KARDAMOM_EXEC_FRAMES=on`: the executors publish their frames. Nothing reads them yet.
2. `KARDAMOM_EGRESS=1`: the deploy adds the egress after the executors and before the notifier, the validator and the batcher. Prometheus scrapes it.
3. The reader switches, one reader at a time: `KARDAMOM_NOTIFIER_RECEIPTS_FROM_EGRESS`, `KARDAMOM_INGRESS_RECEIPTS_FROM_EGRESS`, `KARDAMOM_SEQUENCER_RECEIPTS_FROM_EGRESS`. `KARDAMOM_VALIDATOR_EGRESS` and `KARDAMOM_BATCHER_EGRESS` only after the carry flag is active on the chain, and only when the reader is past the last stored reference (see [The carry window](#the-carry-window)).

The preflight refuses a reader switch without the egress, the egress without the frames, and a value other than `on`, `off` or empty. To go back, switch off in the reverse order.

The merge chains the copy of an executor only behind a frame that another source delivers. After a start, an instance reads the frames between its head and the copies from the range read of a peer (`--peers`, `crates/egress/src/catchup.rs`, #743). So an instance with an empty store fills it from a running peer. With no peer that holds the frames, it starts again after the next boundary of a copy, and its store then begins there. A new node of a rescheduled instance has an empty store too.

#### The egress nodes

The switch `KARDAMOM_EGRESS_NODES=on` (job variable `egress_nodes`) moves the egress off the executor nodes to its own nodes (`docs/specs/2026-10-25-client-events.md`, 4.7). Off, the deploy renders every job as before.

- An egress node has `egress` in `meta.roles`. Its name is `egress-<i>`. The kardamom-infra staging inventory names `egress-0` and `egress-1`.
- On, the job constrains on `meta.roles` with `set_contains "egress"`, with `distinct_hosts`. Only the egress job changes.
- On, the peers of each instance name the egress nodes and every executor node. So during the move, a moved instance fills its empty store from the instance that is still on an executor node.
- The preflight counts the Nomad nodes that are `ready` and `eligible` and have `egress` in `meta.roles`. With the egress deployed (`KARDAMOM_EGRESS=1`), the deploy stops before it changes a job when the count is below `egress_count` (2).
- The readers reach the egress through the Consul service `kardamom-egress`, so no reader changes.

A change of the constraint does not move a running allocation. Move the instances one at a time:

1. Create the egress nodes and bootstrap them (kardamom-infra).
2. Deploy with `KARDAMOM_EGRESS_NODES=on`. The job registers with the new constraint. The two instances keep running on the executor nodes.
3. Stop one instance: `nomad alloc stop <alloc>`. Nomad places it again on an egress node. Its store is empty, and it fills from the other instance.
4. Check the moved instance before the next step:
   - `kardamom_egress_head_distance_frames` is near 0;
   - its `GET /health` answers;
   - its `kardamom_egress_ref_frame_max_index` equals the one of the other instance.
5. Stop the other instance. It moves and fills from the moved one.

To go back, deploy with `KARDAMOM_EGRESS_NODES=off`, then move the instances back the same way, one at a time.

### The carry window

The carry window moves the executors to the bytes that the sealer carries, with no stop of the chain. Three switches and one deploy value do it. Each is off by default. With each one off, the deploy renders every job as before. The runbook of the rollout on a live chain is [`docs/runbooks/carry-window-rollout.md`](../../docs/runbooks/carry-window-rollout.md).

| Switch | What it sets | When |
|---|---|---|
| `KARDAMOM_MEMBER_FEED=on` | On each sealer member: `-Dkardamom.cluster.feedAeronDir=/opt/kardamom/aeron-mount/dir` and `-Dkardamom.cluster.feedRingBytes` (job variable `cluster_feed_ring_bytes`, 16 MiB). The shared media driver (`aeron` job) also runs on the sealer nodes. | Any time after the release. The sealer rolls member by member. It needs no allowance: the feed is not a setting that every member must match. |
| `KARDAMOM_EXEC_ON_SEALERS=on` | The executor groups `executor-<i>`, each pinned to `sealer-<i>`, in reference mode. The state mirrors and every job that reaches an executor by its node follow them. | After the feed. |
| `KARDAMOM_CARRY_EXECUTORS=<list>` | `--carry-bytes` and `--egress` on each listed group: `0`, then `0,1`, then `0,1,2`. | After feature 5, one executor at a time. |
| `KARDAMOM_SEALER_VERSION_FLOOR=<n>` | `--sealer-version-floor <n>` on each sequencer. Empty or `0` sends no flag. | After every reader runs this release. Set the lowest `decisionVersion` of the members: `5` on staging. |

The member feed:

- Each member sends its retained frames to the executor on its own node over `aeron:ipc`. It publishes into the shared media driver of the node, not into its own embedded driver. The node is the pair: no setting names the member.
- The member walks its frames on the heap. The deploy sets no `carryBytes` and no token, and writes no Nomad variable for a member group.
- The archive of the shared driver records the `exec_txs` stream of an executor on the node in `/opt/kardamom/archive/dir`, beside the archive of the member in `/opt/kardamom/archive`.
- See [Member feed](../../cluster/sealer-service/README.md#member-feed).

The executors on the sealer nodes:

- Group `executor-<i>` runs on `sealer-<i>`. Its recorder id, void voter id and host id are `i`. Each group has its own role constraint, and the job has none.
- The executor reads the cluster egress, as on the executor nodes. Only `KARDAMOM_CARRY_EXECUTORS` turns on `--carry-bytes`.
- The jobs that reach an executor by its node use `sealer-<i>`: the nonce queries of the sequencers and the ingresses, the block references of the batcher, the checkpoint peers of the executors and the validator, and the Prometheus targets.
- The egress stays on the executor nodes.

The staged roll of the executors:

- Nomad orders no groups, so one registration of the pinned groups would replace every executor at once. So the role rolls them one group at a time (`tasks/executor.yml`, `tasks/roll_step.yml`), the same way as the sealer members. It waits for the health check of each group before the next step.
- The move from the executor nodes goes one executor at a time too, from the highest id down. Each step has two registrations. The first lowers the count of the group on the executor nodes by one, and the role waits until that allocation stops. Nomad stops the allocation with the highest index first, and that index is the id of the executor. The second starts the pinned group of that id. So two executors never run with one id. The last step removes the group on the executor nodes.
- The group on the executor nodes keeps the job constraint of the registered job during the move. That is an in-place update for Nomad: it restarts no executor.
- After each registration, the role waits for the deployment that the evaluation of that registration names, not for the latest deployment of the job.
- During the move, the jobs that follow the executors already ask `sealer-<i>`. An executor that did not move yet does not answer there, so they ask the others.
- A new job, or a move back to the executor nodes, registers in one step.
- `just rollback` rolls the pinned groups back one group at a time when both versions have them.

The carry executors:

- A listed executor reads the member feed of its node, then the egress (`--egress`, the Consul service `kardamom-egress`), then a peer checkpoint.
- A carry executor sends no recorded cursor, so the list refuses `KARDAMOM_EXEC_CURSOR=on`.
- To move an executor back to reference mode, leave it out of the list.

The gates of the carry window refuse a move before any job changes. Each gate runs only when the deploy makes its move, so a later deploy with the same values reads nothing:

- The preflight:
  - `KARDAMOM_EXEC_ON_SEALERS=on` without `KARDAMOM_MEMBER_FEED=on`, or with an executor count other than the sealer count.
  - `KARDAMOM_CARRY_EXECUTORS` with an index that repeats or has no member, or without `KARDAMOM_EXEC_ON_SEALERS=on`, `KARDAMOM_EGRESS=1` and the egress URL of the executors, or with `KARDAMOM_EXEC_CURSOR=on`.
  - `KARDAMOM_SEALER_VERSION_FLOOR` other than empty or 0 to 5.
  - `KARDAMOM_CLUSTER_RETENTION` above 131,072 frames.
- The version floor, when it changes to a value above 0:
  - a sealer job that Nomad does not know: a new chain starts on decision version 3. A genesis bootstrap passes (see [The carry from genesis](#the-carry-from-genesis));
  - a member that does not run, or a floor above the lowest `decisionVersion` of the members;
  - an executor, validator, batcher or egress job without the meta `relayed_full_ref`, or a live allocation of an older job version. Without the egress job, the egress passes.
  - the v1 loop of the sequencers: a target without `KARDAMOM_SEQUENCER_ADMISSION=on`, or a registered sequencer job without the meta `v2_loop`. The v1 loop offers record type 0 at every floor.
- A batcher or validator job that Nomad does not know, switched to the egress, while an egress instance reports a stored reference (`kardamom_egress_ref_frame_max_index` above 0). It has no block to read against it. Under decision versions 3 and 4 both loops of the sequencer offer record type 0, so a new chain stores references too. Deploy the reader on the cluster egress first.
- The move of the batcher or the validator to the egress (`KARDAMOM_BATCHER_EGRESS`, `KARDAMOM_VALIDATOR_EGRESS`), when the registered job does not carry the meta `reads_egress`:
  - an egress job without the meta `ref_mark`, or a live egress allocation of an older job version;
  - an egress instance that does not report `kardamom_egress_ref_frame_max_index` and `kardamom_egress_ref_frame_max_block`;
  - a batcher whose `kardamom_batcher_last_posted_block`, or a validator whose `validator_committed_block`, is not above the highest `kardamom_egress_ref_frame_max_block`.
- The move of an executor to carry mode, for each listed executor that does not carry yet:
  - a sealer job that Nomad does not know, except on a genesis bootstrap;
  - a group `executor-<i>` that does not run on the sealer nodes;
  - an executor job without the meta `carried_ref_fault`;
  - a member that does not run, or that reports `carryWindow` false;
  - the member `cluster-<i>` that reports the feed `off`;
  - the egress checks above;
  - `kardamom_executor_block_number` of the executor minus 32 not above the highest `kardamom_egress_ref_frame_max_block`. At its first carry start, the executor compares the stream with its committed blocks of that window. An egress that stored no reference (`kardamom_egress_ref_frame_max_index` 0) holds none to join, so the check passes.
- The move back to the v1 loop: a target without `KARDAMOM_SEQUENCER_ADMISSION=on` while the registered sequencer job carries `v2_loop`, and a member reports `carryWindow` true, holds feature 5 (`kardamom_sealer_feature_state{feature="5"}` at 1 or 2), or does not run. `KARDAMOM_ALLOW_MUST_MATCH=admission` allows it.
- The sealer retention, when the sealer job changes:
  - a lower value than the registered sealer, unless `KARDAMOM_ALLOW_MUST_MATCH` names `retention`;
  - a value whose frames, at the mean frame size of the members, pass 48 MiB, well below the 64 MiB backlog limit of a cluster session. At a mean of 300 bytes, 131,072 frames (about 39.3 MB) fit. The mean is the sum of `retainedBytes` over the sum of `retainedFrames` of the member status (the gauges `kardamom_sealer_retained_bytes` and `kardamom_sealer_retained_frames`).

The gates read the metrics of each instance by the node address that the Nomad API advertises. A gauge matches a sample with the labels of its exporter (`service` and `host_id`, or `service` and `member_id` on a sealer), and a gauge with labels, such as `kardamom_sealer_feature_state{feature="5"}`, matches the sample that carries each of them. The ports: the egress on `ports.egress_metrics`, the batcher on 9002, the validator on 9006, the executor on 9004, and the sealer on its admin port (`workloads_metrics_ports`). An instance that does not answer counts as `-1`, so the gate refuses.

The order on a live chain:

1. Deploy the release with every switch off and the floor 0.
   The sequencers run the v2 loop (`KARDAMOM_SEQUENCER_ADMISSION=on`). The v1 loop offers each transaction without its bytes, and the sealer refuses such an offer after the switch.
2. Set `KARDAMOM_SEALER_VERSION_FLOOR` to the lowest `decisionVersion` of the members.
3. Move the batcher and the validator to the egress.
4. Deploy with `KARDAMOM_MEMBER_FEED=on`.
5. Deploy with `KARDAMOM_EXEC_ON_SEALERS=on`.
6. Run the upgrade check of feature 5, then send the upgrade transaction (see [The upgrade check](#the-upgrade-check)).
7. After the switch, list the executors in `KARDAMOM_CARRY_EXECUTORS`, one at a time.

To go back, reverse the order: empty the list first, then the executors, then the feed. A seeded member runs the decision rules that the seed carries. Only before a fleet rebuild from a seed of version 1, which holds no rules, set the floor to 0: such a member runs decision version 3.

#### What a node needs for the carry window

The infrastructure of an environment sets these. The deploy does not change them.

- Each sealer node has `/opt/kardamom/state`, `/opt/kardamom/checkpoints` and `/opt/kardamom/mirror`, writable by the executor and the state mirror. They hold the state database, the checkpoints and the mirror store.
- Each sealer node has the Aeron tmpfs `/opt/kardamom/aeron-mount` with room for the shared driver as well as the member: the term buffers of the executor streams and the 24 MiB log of the feed.
- Each sealer node has the memory of the member (heap plus 640 MB), the shared driver (768 MB), the executor (1536 MB) and the state mirror (512 MB), plus the page cache of the state database.
- Each sealer node has the disk of the Raft log, the state database, the checkpoints and the `exec_txs` recording. The heap design needs no disk for a retained file and no secret for a token.
- The node meta of a sealer node keeps `role = sealer` and `node_index = <i>`. To advertise its `exec_txs` recording, the role set of the node also names `executor`, so the Nomad agent stamps `archive_topics = exec_txs`. No consumer reads that stream yet.
- The fixed ports of the executor (9004, 9014, 9024) and of the state mirror (9007) are free on a sealer node and open between the nodes.
- The executor nodes stay: they run the egress.

### The notifier nodes

The notifier (`nomad/notifier.nomad.hcl`) runs on the ingress nodes by default. The switch `KARDAMOM_NOTIFIER_NODES=on` moves it to its own nodes:

- A notifier node has the class `notifier` (`role=notifier`, `tier=worker`). Its name is `notifier-<i>`.
- The notifier job constrains on `meta.roles` with `set_contains "notifier"`, with `distinct_hosts`.
- `KARDAMOM_NOTIFIER_COUNT` sets the instance count. The default is 1. Set it to the count of the notifier nodes.
- The notifier keeps no state on disk, and an instance needs no peers. The webhooks are removed (2026-10-25); offline delivery comes later through a task queue. So a change of the count moves nothing: it only starts or stops instances.
- The memory reservation is 512 MB, not 1024 MB. On staging the notifier uses at most about 300 MiB.
- The ring keeps at most 250,000 events, not `ring_max_events` (1,000,000 by default). 250,000 events use about 75 MB. At the staging load-test peak of 5,100 events a second (1,700 tx/s, three stages), the ring holds about 50 seconds. So a client that reconnects replays at most 250,000 events, and on a load peak only the last 50 seconds. We measure the memory in the first load test on the notifier nodes and adjust the cap.
- The media driver of a notifier node idles at 1000 µs. The aeron job runs a second group, `aeron-notifier`, on the nodes of class `notifier`. The group `aeron` keeps `KARDAMOM_IDLE_CAP_US` on every other node. The notifier is not on the transaction hot path.

Off, the deploy renders every job as before. On, only the notifier and aeron jobs change. The canary and Prometheus find the notifier through its Consul services `kardamom-notifier` and `kardamom-notifier-metrics`.

The edge load balancer needs a change only in an environment that exposes the feed on `:8547`. There, add the notifier nodes to its targets before the switch goes on. The staging load balancer (kardamom-infra) serves only the JSON-RPC, so staging needs no change.

Do the move in this order:

1. Create `notifier-0`. Bootstrap it like the other nodes, with `role=notifier`.
2. Deploy with `KARDAMOM_NOTIFIER_NODES=on` and `KARDAMOM_NOTIFIER_COUNT=1`. The `aeron-notifier` driver on `notifier-0` fails its first start: the old `aeron` driver stops in the same evaluation and leaves a fresh `cnc.dat`. The new driver starts after the restart delay of 15 s.
3. Measure the memory and the CPU of the notifier and of its media driver on `notifier-0`.
4. Create `notifier-1`. Deploy with `KARDAMOM_NOTIFIER_NODES=on` and `KARDAMOM_NOTIFIER_COUNT=2`.

`KARDAMOM_NOTIFIER_COUNT` must not be above the count of the notifier nodes. With `distinct_hosts`, an instance without a node is never placed, and the deployment never turns healthy. So with the switch on, the preflight of the deploy counts the Nomad nodes that are `ready` and `eligible` and have `notifier` in `meta.roles`. The deploy stops before it changes a job when the count is above that number.

The WebSocket clients reconnect when the notifier moves: Nomad stops the instances on the ingress nodes and starts new ones on the notifier nodes. The ring of a new instance starts empty, so a client that reconnects cannot replay the events from before the move.

To go back, deploy with `KARDAMOM_NOTIFIER_NODES=off`.

### The carry from genesis

`KARDAMOM_GENESIS_CARRY=on` starts a new chain with features 3, 4 and 5 active at genesis: decision version 5 and the carry window from the first block. The chain needs no upgrade transaction and no staged move of the executors. A live chain keeps the rollout above and the runbook [`docs/runbooks/carry-window-rollout.md`](../../docs/runbooks/carry-window-rollout.md). Staging and production leave the variable unset, and then every job renders as before.

The variable sets:

- `-Dkardamom.cluster.genesisFeatures=3,4,5` on every sealer member (job variable `genesis_carry` of the sealer). Every member must match it. A member takes it only when it starts at genesis.
- `genesis_features = [3, 4, 5]` in the genesis of the executors and the validator (job variable `genesis_carry`, the fragment `config/genesis/carry.toml` in front of `config/genesis/dev.toml`).
- The default of every carry switch (`workloads_genesis_carry_switches`):
  - `KARDAMOM_MEMBER_FEED=on` and `KARDAMOM_EXEC_ON_SEALERS=on`;
  - `KARDAMOM_EXEC_FRAMES=on` and `KARDAMOM_EGRESS=1`;
  - `KARDAMOM_BATCHER_EGRESS=on` and `KARDAMOM_VALIDATOR_EGRESS=on`;
  - `KARDAMOM_SEQUENCER_ADMISSION=on`: only the v2 loop offers the bytes of a transaction;
  - `KARDAMOM_SEALER_VERSION_FLOOR=5`;
  - `KARDAMOM_CARRY_EXECUTORS` with every member index, such as `0,1,2`.

The preflight refuses:

- a value other than `on`, `off` or empty;
- a carry switch with another value than the one above, such as a floor below 5 or a partial list of carry executors;
- `KARDAMOM_EXEC_CURSOR=on`.

The release gate accepts the variable only in two cases:

- A genesis bootstrap: `KARDAMOM_CLUSTER_BOOTSTRAP=1`, a sealer job that Nomad does not know (the plan says `Added`), and no job of `workloads_genesis_pipeline_jobs` (the sequencer, the executor, the batcher, the validator and the egress) in Nomad.
- A registered sealer job whose every member carries `genesisFeatures=3,4,5`. So the chaos lifecycle and `just container-test` deploy again without the bootstrap.

The gate also refuses:

- any other case, with a pointer to the runbook of the rollout;
- a target or registered sealer job that names a seed (`seedSnapshot`) or the retained file (`carryBytes=true`): a seeded member does not start at genesis;
- a change of `genesisFeatures` on a registered sealer job, also with `KARDAMOM_ALLOW_MUST_MATCH`. A member with no snapshot replays its log from genesis, and with other genesis features it forks. So a chain keeps the variable for its life.

On a genesis bootstrap no member and no other job of the pipeline runs yet. The gates of the carry window read the genesis facts:

- The rules of each member come from the rendered sealer job: decision version 5, the carry window, and the feed (`tasks/sealer_rules.yml`). The floor 5 and the carry executors pass on these rules.
- The reference mark of the egress is index 0 at block 0: the carry window refuses every reference from the first block (`tasks/egress_mark.yml`). So the new readers start on the egress, and the executors start in carry mode.
- A job that Nomad does not know runs no instance of an older release, so the meta checks of the registered jobs pass it. The deploy registers it from this release.
- After the new members start, and before any other job registers, every member must report `decisionVersion` 5, `carryWindow` true and `genesisFeatures` `"3,4,5"` on its status. Else the deploy stops: the sealer image does not take the setting.

On a registered chain the gates read the live state as above. The upgrade check reads the live members too: every member holds feature 5 from genesis, so the check refuses feature 3 or 4 off.

`KARDAMOM_GENESIS_CARRY=on just container-up` starts a container cluster with the carry from genesis. The chaos lifecycle sets the variable for each shard (`DeployVars`).

### Configuration switches

The workloads role reads these environment variables. The defaults are in `ansible/roles/workloads/defaults/main.yml`. An Ansible extra variable can set the matching `workloads_*` variable directly. An empty value means "use the default of the job".

For the behavior of the L1 switches, see [`../../docs/l1-data-path.md`](../../docs/l1-data-path.md).

| Area | Variable | Default | Meaning |
|------|----------|---------|---------|
| Controller | `NOMAD_ADDR` | `http://127.0.0.1:4646` | The Nomad API. The justfile derives it from the node contract. |
| Controller | `NOMAD_TOKEN` | empty | The Nomad ACL token. |
| Controller | `NOMAD_NAMESPACE` | `default` | The Nomad namespace. |
| Controller | `KARDAMOM_ENV` | `local` | The environment name. It selects `deployed/<env>/`. |
| Controller | `KARDAMOM_OPERATOR`, `KARDAMOM_REVISION` | `GITHUB_ACTOR` or `USER`; `GITHUB_SHA` or the git or jj revision | The operator and the revision of the deploy record. The snapshot check also takes `KARDAMOM_REVISION` as the revision of a sealer image that no record, registration or tag names. |
| Controller | `KARDAMOM_REGISTRY_URL` | the Nomad API host on the registry port | The registry API that holds the images of the manifest, as the controller reaches it. `off` skips the image check, in the local profile only. A registry that answers 401, such as `https://ghcr.io`, gets an anonymous pull token from the realm of its challenge. |
| Controller | `KARDAMOM_ALLOW_ONE_WAY` | empty | The format ids whose one-way change the operator accepts, comma-separated. The record then carries a rollback floor. |
| Controller | `KARDAMOM_ALLOW_RETIRED` | empty | The retired format ids that the operator accepts in the rolling path, comma-separated. Each must be neither shared nor permanent, and the release must remove the service that owned it. The record keeps the list in `retired`. No floor follows. |
| Controller | `KARDAMOM_ALLOW_MUST_MATCH` | empty | The sealer settings of a documented procedure that change in the rolling path, comma-separated. |
| Controller | `KARDAMOM_ALLOW_SEALER_DOWN` | empty | The sealer groups that do not run and that a roll of the sealer accepts, comma-separated: a member that crash-loops on its release. The snapshot check does not read their snapshots, and the staged roll rolls them first. A named group that runs is refused. |
| Controller | `KARDAMOM_REPLACE_ATTEMPT` | `0` | `1` deploys over an attempt that is still `started`, when it died. |
| Controller | `KARDAMOM_ROLLBACK_BELOW_FLOOR` | empty | The format ids of a floor that `just rollback` crosses on purpose, after a check that no writer wrote the new version. |
| Signing | `DIGEST_MANIFEST` | `images.digests` | The image manifest. A relative path starts in `deploy/cluster/`. |
| Signing | `KARDAMOM_REQUIRE_SIGNED` | `0` | `1` requires the signature of the manifest and of each image. |
| Signing | `KARDAMOM_CERT_IDENTITY_RE` | the workflows of the project repository | A regex for the signer identity. |
| Signing | `KARDAMOM_CERT_OIDC_ISSUER` | `https://token.actions.githubusercontent.com` | The OIDC issuer of the signer. |
| Signing | `KARDAMOM_COSIGN_CACHE`, `KARDAMOM_COSIGN_VERSION` | the user cache, the pinned version | Where the controller finds a cosign that is not on `PATH`. |
| Settlement | `SETTLEMENT_ADDRESS` | empty | An existing settlement address. Empty: deploy one with `kardamom-deploy`. |
| Settlement | `DEPLOY_BIN` | `target/release/kardamom-deploy` | The settlement deployer. The release gate also runs its `formats` command, and the canary gate its `smoke` command. |
| Settlement | `L1_OWNER`, `L1_OWNER_KEY` | the Anvil dev account 0 | The owner address and key that the deployer uses. |
| Settlement | `BATCHER_EOA` | the Anvil dev account 2 | The batcher address that the deployer registers. |
| Settlement | `L2_CHAIN_ID` | `chain_id` of `all.yml` (412346) | The L2 chain id. |
| Real L1 | `L1_RPC` | empty | The L1 endpoint. Empty: the in-cluster anvil, found through the Nomad API. Set: the batcher uses it. |
| Real L1 | `BATCHER_KEY` | the Anvil dev key | The L1 key of the batcher. The DA proxy signs with it. |
| Real L1 | `LOCKBOX_ADDRESS` | empty | The lockbox contract. Empty: a placeholder address, and the deposit path is idle. The light client needs it. |
| Batcher cadence | `BATCHER_BLOCKS_PER_BATCH` | empty (`5` in the job) | The L2 blocks of one post. `5` suits anvil. A real L1 takes a larger group. |
| Batcher cadence | `BATCHER_FLUSH_MS` | empty (`3000` in the job) | The wait before the batcher posts a group that holds a transaction. |
| Batcher cadence | `BATCHER_IDLE_FLUSH_MS` | empty (the same as `BATCHER_FLUSH_MS`) | The wait before the batcher posts a group of empty blocks. |
| EigenDA | `EIGENDA_NETWORK` | empty | `sepolia_testnet` or `mainnet` deploys the `da-proxy` job. Empty deploys the file-backed `da-store` stand-in. |
| EigenDA | `EIGENDA_CERT_VERIFIER` | empty (the known address of `sepolia_testnet`) | The `EigenDACertVerifierRouter` of the network. |
| EigenDA | `eigenda_ledger_mode` (job variable) | `on-demand-only` | How the signer pays: `on-demand-only`, `reservation-only` or `reservation-and-on-demand`. The role does not pass it. Set it as a variable of `nomad/da-proxy.nomad.hcl`. |
| Light client | `L1_LIGHT_CLIENT_EXECUTION_RPC` | empty | The untrusted execution RPC. Set: the role deploys the light client. |
| Light client | `L1_LIGHT_CLIENT_CONSENSUS_RPC` | empty | The beacon API. It is required with the execution RPC. |
| Light client | `L1_LIGHT_CLIENT_CHECKPOINT` | empty | The weak-subjectivity checkpoint. It is required with the execution RPC. |
| Light client | `L1_LIGHT_CLIENT_NETWORK` | `mainnet` | The network that the light client follows. |
| Light client | `L1_LIGHT_CLIENT_HOST`, `L1_LIGHT_CLIENT_PORT` | `kardamom-l1-light-client.service.<datacenter>.consul`, `8548` | Where the validator and the followers reach the light client. |
| Monitoring | `ALERTMANAGER_CONFIG_FILE` | empty | A file with the Alertmanager routes and receivers of the environment. Set: the role writes it, with the inhibit rules of `deploy/alertmanager-inhibit.yml` added, to the item `alertmanager` of `nomad/jobs/monitoring`. The file must not hold `inhibit_rules`. Empty: the role leaves the variable as it is. |
| Monitoring | `PROMETHEUS_RULES_FILE` | empty (`groups: []`) | The extra Prometheus rule file of the environment, the item `rules`. The role writes it only with `ALERTMANAGER_CONFIG_FILE`. Prometheus loads `deploy/alerts.yml` in all cases. |
| Batcher L1 | `BATCHER_L1_RPC` | `L1_RPC` | Comma-separated endpoints, best first. The batcher falls back on errors and ranks failing endpoints last. The DA proxy and settlement bootstrap keep the single `L1_RPC` endpoint. |
| Followers | `L1_FOLLOWERS_RPC` | the fault proxy if deployed, else the light client if deployed, else `L1_RPC` | The L1 that the indexer (the L1 follower) reads. A comma-separated list. With two or more entries, a block counts only when two agree. The da-watcher reads the follower's stream, not L1. |
| da-watcher, batcher | `L1_SILENCE_S` | empty (`1152` in the binaries) | Seconds with no `l1_blocks` record before the da-watcher and the batcher pause with the follower as their root. |
| da-watcher | `EPOCH_PACE_MS` | empty (`2000` in the binary) | The pause between two epochs of one L1 finality step, in ms. The container recipes and CI use `100`: the tests mine bursts of L1 blocks. Keep it below the 4 s gather window of the validator. |
| Indexer | `L1_INDEXER_START_BLOCK` | empty (`1` with the fault proxy) | The first L1 block to index on an empty archive. Empty: the finalized block at the first start. |
| Indexer | `L1_INDEXER_POLL_S` | empty (`12` in the binary) | One slot, in seconds: the read cadence while the finalized tip does not move, and the whole cadence without a beacon API. |
| Indexer | `L1_BEACON_API` | `L1_LIGHT_CLIENT_CONSENSUS_RPC` | The beacon API of the finality schedule. Empty: the follower reads every slot. |
| Indexer | `L1_MAX_LOG_RANGE` | empty (`10` in the binary) | The most blocks one log query spans: the provider's cap. |
| Indexer | `L1_INDEXER_COUNT` | empty (`2` in the job) | Follower instances, each on its own node with the `indexer` role. |
| Indexer | `L1_INDEXER_HOST`, `L1_INDEXER_PORT` | `kardamom-l1-indexer.service.<datacenter>.consul`, `8549` | Where the batcher reaches the indexer. |
| Fault proxy | `KARDAMOM_L1_FAULT_PROXY` | `0` | `1` deploys the lying L1 of the `chaos-l1` shard in front of the in-cluster anvil. All followers read L1 through it. |
| Sealer | `KARDAMOM_CLUSTER_RETENTION` | empty (`65536` frames in the job) | The egress replay retention, in frames, at most 131,072. It is a minimum. The sealer keeps a frame above the posted head even past this window, unless the carry window applies. On a live chain the release gate refuses a lower value unless `KARDAMOM_ALLOW_MUST_MATCH` names `retention`, and a value whose frames pass 48 MiB. See [The carry window](#the-carry-window) and the [sealer README](../../cluster/sealer-service/README.md). |
| Sealer, batcher | `KARDAMOM_DA_LAG_BUDGET_BLOCKS` (job variables `cluster_da_lag_budget_blocks`, `da_lag_budget_blocks`) | empty (`10000` in the jobs) | The DA-lag budget, in blocks. The sealer refuses user transactions when the sealed head is more than this far past the posted head. `0` turns the guard off. Every member must use the same value, including during snapshot-tail replay. The budget is process configuration and is absent from the snapshot; changing it on a live chain can change canonical decisions. The batcher posts at half of it. The deploy refuses `BATCHER_IDLE_FLUSH_MS / 1000` at or above it. |
| Sealer, executor, validator | `KARDAMOM_GENESIS_CARRY` (job variable `genesis_carry`) | empty | `on`, `off` or empty. On, a new chain starts with features 3, 4 and 5 active at genesis, and every carry switch is on. Only a new cluster, or a chain that started with it, takes it. See [The carry from genesis](#the-carry-from-genesis). |
| Sealer | `KARDAMOM_CLUSTER_BOOTSTRAP` | `0` (`1` in `just container-up`) | `1` opens the sealer bootstrap while the role registers a cluster job that Nomad does not know. See [Sealer bootstrap](#sealer-bootstrap). |
| Sealer | `KARDAMOM_CLUSTER_SNAPSHOT_S` | empty (`300` in the job) | The interval of the Raft snapshot, in seconds. `0` disables it. |
| Sealer | `KARDAMOM_CLUSTER_LOG_PURGE_KEEP` | empty (`3` in the job) | How many of the newest Raft snapshots keep their log. `0` turns the log purge off. See [Sealer log purge](#sealer-log-purge). |
| Sealer | `KARDAMOM_CLUSTER_DRIVER_THREADING_MODE` | empty (`SHARED` in the job) | The threading mode of the media driver inside each sealer member: `SHARED`, `SHARED_NETWORK` or `DEDICATED`. The Raft log passes through this driver. `SHARED` runs all its work on one thread, which saturates first under load. Each member reads its own value. |
| Sealer | `KARDAMOM_CLUSTER_FILE_SYNC_LEVEL` | empty (`0` in the job) | The sync level of the Raft log and the archive. `0` leaves a write in the page cache, and a commit waits for no disk sync. The kernel writes the log back on its own schedule. `1` syncs the data of each write batch. `2` syncs data and metadata. At `0`, a power loss or a kernel crash on a majority of members before the write-back can drop an entry that a quorum acknowledged. So each member runs on its own machine. See [the durability of the Raft log](../../docs/failure-modes.md#durability-of-the-raft-log). |
| Sealer | `KARDAMOM_CLUSTER_HEAP_MB` | empty (`384` in the job) | The heap of each sealer member in MB (job variable `cluster_heap_mb`). The job memory is the heap plus 640 MB. The retained frames are on the heap until the DA layer posts them, so the heap bounds the DA lag that a member survives at a given rate. The member exits on an OutOfMemoryError and restarts from its snapshot. |
| Backup sealer | `KARDAMOM_BACKUP_FILE_SYNC_LEVEL` | empty (`1` in the job) | The sync level of the archive of the backup sealer (job `cluster-backup`, variable `backup_file_sync_level`). The deploy registers the job only when the inventory has a node in the group `backup`. The backup is outside the quorum, so its syncs never delay a commit. Its copy is the recovery source after a power loss on a majority of the members. |
| Monitoring | `KARDAMOM_PROMETHEUS_RETENTION_TIME`, `KARDAMOM_PROMETHEUS_RETENTION_SIZE` | empty (`24h` and `0B` in the job) | How long and how much Prometheus keeps (job variables `prometheus_retention_time`, `prometheus_retention_size`). `0B` sets no size limit. At a 1 s scrape, staging writes about 3 GB a day. |
| Sealer | `KARDAMOM_REMOTE_ORIGINS` | empty (`412347,412399` in the job) | The peer chain ids whose cross-chain records the sealer seals. All members use the same list. |
| Ingress | `KARDAMOM_INGRESS_ACK` (job variable `ack`) | empty (`receipt`) | When `eth_sendRawTransaction` answers. `receipt` waits for the receipt. `validated` answers when the ingress validated the transaction and published it on `tx_data`. See [`../../docs/json-rpc.md`](../../docs/json-rpc.md). |
| Sealer | `KARDAMOM_MEMBER_FEED` (job variable `member_feed` of the sealer and the `aeron` job) | empty (`off` in the jobs) | `on` or `off`. On, each member feeds its retained frames to the executor on its own node, and the shared media driver runs on the sealer nodes. See [The carry window](#the-carry-window). |
| Executor | `KARDAMOM_EXEC_ON_SEALERS` (job variable `exec_on_sealers` of every job that it changes) | empty (`off` in the jobs) | `on` or `off`. On, executor `<i>` runs on `sealer-<i>`, and the executor job rolls one group at a time. It needs `KARDAMOM_MEMBER_FEED=on`. |
| Executor | `KARDAMOM_CARRY_EXECUTORS` (job variable `carry_executors`) | empty | The member indexes of the carry executors, such as `0,1`. Each one runs with `--carry-bytes` and `--egress`. It needs `KARDAMOM_EXEC_ON_SEALERS=on` and `KARDAMOM_EGRESS=1`. |
| Sequencer | `KARDAMOM_SEALER_VERSION_FLOOR` (job variable `sealer_version_floor`) | empty (`0` in the job) | The decision version that a sequencer assumes before the first report of a sealer session, from 0 to 5. The release gate refuses a value above the lowest `decisionVersion` of the members, and a value above 0 before every reader runs this release. |
| Sequencer | `KARDAMOM_SEQUENCER_ADMISSION` (job variable `admission`) | empty (`off` in the job) | `on` or `off`. On, the sequencers run the v2 loop (the `[admission]` section of the config), with 512 forwarded and 512 parked transactions for each sender. The carry window needs it: only the v2 loop takes the bytes of a transaction under decision version 5, and the sealer refuses a reference without bytes while the window is on. |
| Executor | `KARDAMOM_EXEC_CURSOR` (job variable `exec_cursor`) | empty (`off` in the job) | `on` or `off`. On, each executor sends its recorded cursor to the sealer, the floor of the record-lag guard. Switch it on only after every sealer member reads ingress kind 9. A sealer member that does not read it drops the frame. |
| Egress | `KARDAMOM_EGRESS_NODES` (job variable `egress_nodes`) | empty (`off` in the job) | `on` or `off`. On, the egress runs on the nodes with `egress` in `meta.roles`, and its peers name those nodes too. Off, it runs on the executor nodes. See [The egress nodes](#the-egress-nodes). |
| Egress | `KARDAMOM_EGRESS` | `0` | `1` deploys the egress job (`nomad/egress.nomad.hcl`) and its scrape (job variable `egress` of the monitoring job). It needs `KARDAMOM_EXEC_FRAMES=on`. See [The egress](#the-egress). |
| Egress | `WITHDRAWAL_ORACLE_ADDRESS` (job variables `withdrawal_oracle` and `l1_rpc_url` of the egress) | empty (no oracle) | The L1 address of `WithdrawalOutputOracle`. Set, the egress keeps every block above the lower of the posted head and the final head (`--withdrawal-oracle`, `--l1-rpc-url`). Empty, it keeps every block above the posted head. The deploy does not create the oracle. See [The egress](#the-egress). |
| Egress | `KARDAMOM_EXEC_FRAMES` (job variable `exec_frames` of the executor) | empty (`off` in the job) | `on` or `off`. On, each executor publishes the frames that it executes on `exec_frames` (`--exec-frames`). |
| Egress | `KARDAMOM_VALIDATOR_EGRESS`, `KARDAMOM_BATCHER_EGRESS` (job variable `egress_url`) | empty (the job reads the cluster egress) | `on` or `off`. On, the validator or the batcher reads the canonical stream from the egress (`--egress`), and its job carries the meta `reads_egress`. Switch it on only on a chain where the carry flag is active. It needs `KARDAMOM_EGRESS=1`. The release gate refuses the move while the reader is not past the last stored reference. See [The carry window](#the-carry-window). |
| Egress | `KARDAMOM_SEQUENCER_RECEIPTS_FROM_EGRESS` (job variable `receipts_from_egress` of the sequencer) | empty (`off` in the job) | `on` or `off`. On, the sequencers read the receipts and their account rows from `egress_receipts` (`receipts_from_egress = true` in the config). It needs `KARDAMOM_EGRESS=1`. |
| Egress | `KARDAMOM_INGRESS_RECEIPTS_FROM_EGRESS` (job variable `receipt_query_endpoints` of the ingress) | empty (the executors) | `on` or `off`. On, `eth_getTransactionReceipt` asks the egress on a cache miss (`--receipt-query-endpoints`), then the executors when the egress has no receipt. It needs `KARDAMOM_EGRESS=1`. |
| Client events | `KARDAMOM_TX_EVENTS` (job variable `tx_events` of the ingress and the sequencer) | empty (`off` in the jobs) | `on` or `off`. On, the ingress publishes an `accepted` record and each sequencer a `sent` record on `tx_events` (`docs/specs/2026-10-25-client-events.md`). Each sequencer twin publishes under its own origin: its allocation index (`--tx-events-origin`) and its lane. With `off`, the jobs render as before the switch. Best effort: a refused offer drops the frame, and no service waits for it. |
| Egress | `KARDAMOM_NOTIFIER_RECEIPTS_FROM_EGRESS` (job variable `receipts_from_egress` of the notifier) | empty (`off` in the job) | `on` or `off`. On, the notifier reads the receipts from `egress_receipts` (`--receipts-from-egress`). It needs `KARDAMOM_EGRESS=1`. |
| Notifier | `KARDAMOM_NOTIFIER_EVENTS` (job variable `events` of the notifier) | empty (`off` in the job) | `on` or `off`. On, the notifier takes the stages from `tx_events` and the merged receipts of `egress_receipts` through its stage table, and sends only the latest state of each hash (`docs/specs/2026-10-25-client-events.md`). It keeps only the rejections of `tx_status` and `tx_errors`, which go through the stage table too. It needs `KARDAMOM_EGRESS=1`. What changes for a subscriber: `offered` becomes `sent`; `sealed` goes; `executed` comes only with `KARDAMOM_NOTIFIER_EXECUTED=on`; `receipt` and `rolled_back` are new; and a stage that comes after a later one is not sent. Turn it on only after P6 and P7 of the spec update the canary (its feed reader `feed.rs` and the `missing_sealed` outcome of its probes) and the e2e scenario `tx_status`. |
| Notifier | `KARDAMOM_NOTIFIER_EXECUTED` (job variable `executed` of the notifier) | empty (`off` in the job) | `on` or `off`. With `KARDAMOM_NOTIFIER_EVENTS=on`, the notifier also reads the `tx_receipts` of every executor for the `executed` stage and the latencies of each executor. For the evaluation of the stages. |
| Notifier | `KARDAMOM_NOTIFIER_NODES` (job variable `notifier_nodes` of the notifier and the aeron jobs) | empty (`off` in the jobs) | `on` or `off`. On, the notifier runs on the nodes of role `notifier`, and their media drivers idle at 1000 µs. Off, it runs on the ingress nodes. See [The notifier nodes](#the-notifier-nodes). |
| Notifier | `KARDAMOM_NOTIFIER_COUNT` (job variable `notifier_count` of the notifier) | empty (`1` in the job) | The notifier instance count with `KARDAMOM_NOTIFIER_NODES=on`: the count of the notifier nodes. The preflight refuses a count above the count of the ready notifier nodes. |
| Executor | `KARDAMOM_CHECKPOINT_LOG` (job variable `checkpoint_log`) | empty (`off` in the job) | `on` or `off`. On, each executor keeps the block log of its checkpoints and copies the state only when the base rule asks for a new base, at 16 MiB/s. Off, it copies the whole state each 20 s. Each release reads the bases of both modes. See [`../../docs/specs/2026-10-22-incremental-checkpoints.md`](../../docs/specs/2026-10-22-incremental-checkpoints.md). |
| Fees | `PRIORITY_FEES` | empty (`off`) | `on` or `off`. One value sets the sequencer tip, the sealer ordering window and the executor and validator fee schedule. Turning it on for an existing chain is a chain upgrade. See [`../../docs/priority-fees.md`](../../docs/priority-fees.md). |
| Aeron | `KARDAMOM_AERON_THREADING_MODE` | empty (`SHARED` in the job) | The threading mode of the media driver of each node: `SHARED` (one thread), `SHARED_NETWORK` (the sender and the receiver on one thread, the conductor on its own) or `DEDICATED` (three threads). `SHARED` costs the least CPU, but at a high message rate its one thread saturates and every stream of the node waits. |
| Aeron | `KARDAMOM_ARCHIVE_FILE_SYNC_LEVEL` | empty (`1` in the job) | The sync level of the `tx_data`, `tx_deposits` and `exec_txs` recordings and of the archive catalog (job variable `archive_file_sync_level`). `0` leaves a write in the page cache. `1` syncs the data of each write batch. `2` also syncs the metadata. Level 1 costs throughput on the recording path. |
| Aeron | `AERON_STALL_TOLERANCE_MS` | `10000` | How long every Aeron party waits through a stalled peer, in ms. It also sets how long a media driver keeps the image of a silent publisher. The container recipes and CI use `30000`. See "Aeron stall tolerance" in [`../../docs/failure-modes.md`](../../docs/failure-modes.md#substrate-the-shared-failure-domain). |
| Aeron | `KARDAMOM_IDLE_CAP_US` | `50` | The longest idle wait of every hot-path thread, in µs from 1 to 1000000 (job variable `idle_cap_us`). Only the `aeron`, `cluster`, `ingress`, `sequencer` and `executor` jobs take it. A lower value gives a lower latency after a quiet spell and costs more idle CPU. The container recipes and CI use `1000`. See "Hot-path idle cap" in [`../../docs/failure-modes.md`](../../docs/failure-modes.md#substrate-the-shared-failure-domain). |
| Deploy | `KARDAMOM_CANARY` | `0` | `1` deploys one canary of the ingress and of the sequencer. |
| Chaos | `KARDAMOM_CHAOS_CASES`, `CHAOS_TPS`, `LOAD_DURATION_S` and others | | The knobs of the chaos suite. See [`../../docs/chaos-suite.md`](../../docs/chaos-suite.md). |

Preflight checks fail the deploy before it changes a job:

- The light client needs the execution RPC, the consensus RPC, the checkpoint and the lockbox address together.
- A supplied settlement address must be a non-zero 20-byte hex address.
- Without a settlement address, `kardamom-deploy` must exist and support `addresses --contract --json`.

The release gate (`roles/workloads/tasks/gate.yml`) refuses a release before it changes a job. Each refusal names its cause:

| Refusal | Check | What to do |
|---|---|---|
| The last attempt is still started | The record's `attempt.status`. | Let it finish. If it died, `just rollback <env>`, or `KARDAMOM_REPLACE_ATTEMPT=1`. |
| The chain stands on a halt or a pause | `kardamom_chainStatus` on a running ingress: `roots` is empty, and no `pause` stands on the sealer, the ingress or a service. A cluster without the ingress job has no chain to check; an ingress job without a running allocation is refused. When Nomad does not know the sealer job, the gate excuses `sealer_no_quorum` and the upstream pauses on it: the coordinated restart of the sealer purges the job, and the deploy brings it back. | Clear the halt (`docs/runbooks/`). A deploy cannot tell a failed release from a halted environment. |
| An image is not in the registry | `HEAD /v2/<name>/manifests/<digest>` for every manifest record, at `KARDAMOM_REGISTRY_URL`. `off` skips it in the local profile only. | Push the images, or fix the manifest. |
| A coordinated format change | `kardamom-deploy formats` compares the `formats.toml` of the accepted release with the target's, with the rules of [`docs/formats.md`](../../docs/formats.md). A `rolling`, `mixed_fleet` or `retired` finding is coordinated. A `retired` finding that `KARDAMOM_ALLOW_RETIRED` names rolls, when the format is neither shared nor permanent. The comparison needs the formats text in the accepted release. A deploy does not write the text into the record, so after the first deploy of this release there is no base and no comparison. Do not rely on this check after that deploy. | Stop the writers, or follow the runbook of the format. When the release removes the service that owned a retired format, set `KARDAMOM_ALLOW_RETIRED=<ids>`. |
| A stale retired allowance | `KARDAMOM_ALLOW_RETIRED` names a format that the release does not retire, or one that is shared or permanent. | Unset it, or remove the id. |
| A one-way format change | A `rollback` finding that `KARDAMOM_ALLOW_ONE_WAY` does not name. An allowance that covers no finding is refused too. | Set `KARDAMOM_ALLOW_ONE_WAY=<ids>`. The record then carries a rollback floor. |
| A sealer setting that every member must match | The settings of `workloads_sealer_must_match` in the compiled sealer job against the registered cluster. | Change it through a coordinated restart of the sealer. A documented procedure names the settings in `KARDAMOM_ALLOW_MUST_MATCH`. |
| A shard map change | `config/shard-map.toml` against the map of the registered ingress. | Use the resize controller (`kardamom-cluster scale-sequencers`). |
| A sealer member holds a snapshot that the target cannot read | `snapshotLatest` of `GET /status` on the admin port of every configured member, against `reads_max` of `sealer-snapshot` in `formats.toml` at the revision of the target sealer. That revision comes from the first of: a deploy record that names the target sealer image; the target revision, when every registered member runs the image or the manifest pins no sealer image; the image tag (`main-<12 hex>` or `vX.Y.Z`); `KARDAMOM_REVISION`. A sealer image that none of them names is refused. The check runs only when the plan of the sealer job shows a change, such as a new image digest: a running member reads no snapshot. A sealer job that Nomad does not know skips the check. When the sealer job changes, a configured member that does not run is refused: the gate cannot read its version. `KARDAMOM_ALLOW_SEALER_DOWN` names the down members that the operator accepts. `just rollback` runs the same check against the release before. | Deploy a release that reads that snapshot version. Start a member that does not run, and let it catch up. For an image without a revision, set `KARDAMOM_REVISION` to its revision. |

The gate reads only, so it also runs in check mode. A cluster without an accepted release has no formats to compare with.

### Secrets

A secret is a keyed L1 URL, a private key, or a receiver token. No job definition holds one.

- `L1_RPC`, `BATCHER_L1_RPC`, `L1_FOLLOWERS_RPC`, `L1_LIGHT_CLIENT_EXECUTION_RPC`, `L1_LIGHT_CLIENT_CONSENSUS_RPC`, `BATCHER_KEY` and `ALERTMANAGER_CONFIG_FILE` reach the role as environment variables.
  - In CI, they come from GitHub environment secrets. They are never plain GitHub variables.
- The role writes the secrets of a job to the Nomad Variable `nomad/jobs/<job>` (`roles/workloads/tasks/secrets.yml`).
  - It writes the variable before it registers the job, and only on a change.
  - These tasks run with `no_log`, so the play output does not show a secret.
- The job renders its variable with a `template` block (`env = true`) into `secrets/`. The task gets the items as environment variables. Values use `.Value | toJSON` so quotes, newlines and `#` survive [Nomad environment parsing](https://developer.hashicorp.com/nomad/docs/job-specification/template#environment-variables).
  - A task reads `nomad/jobs/<job>` with its workload identity. That needs no ACL policy.
  - The `secrets/` directory does not show in `nomad alloc fs`.
  - A `nomad job inspect` shows the template, not the values.
- The deploy token needs `write`, `read` and `list` on `nomad/jobs/*` and on `kardamom/deploys/*` in its namespace.
- A job that renders its variable waits for it. Without the role, write it first: `nomad var put nomad/jobs/<job> KEY=value`.

| Job | Variable item | The task reads it as |
|-----|---------------|----------------------|
| `batcher` | `KARDAMOM_L1_RPC` | The L1 endpoints: the fault proxy, else `BATCHER_L1_RPC` (default `L1_RPC`), else the in-cluster anvil. |
| `batcher` | `KARDAMOM_L1_KEY` | The L1 key: `BATCHER_KEY`, else the anvil dev key. |
| `da-watcher`, `l1-indexer` | `KARDAMOM_L1_RPC` | The followers' L1 (`L1_FOLLOWERS_RPC` and its defaults). |
| `da-proxy` | `EIGENDA_PROXY_EIGENDA_V2_ETH_RPC` | `L1_RPC`. |
| `da-proxy` | `EIGENDA_PROXY_EIGENDA_V2_SIGNER_PRIVATE_KEY_HEX` | `BATCHER_KEY`. Without it the proxy is read-only. |
| `l1-indexer` | `KARDAMOM_BEACON_API` | The beacon API for its finality schedule. |
| `l1-light-client` | `HELIOS_EXECUTION_RPC`, `HELIOS_CONSENSUS_RPC` | The upstream execution and beacon URLs. |
| `monitoring` | `alertmanager`, `rules` | Files: `secrets/alertmanager.yml` and `local/operator-rules.yml`. |

The followers name an L1 source in their logs by its scheme and host only. A provider key is in the path or the query of the URL, so the logs do not show it.

The L1 deployment rules:

- The indexer deploys with a real L1 (`L1_RPC`), with a light client, or with the fault proxy.
- The batcher reads the indexer when the indexer is deployed.
- The role waits for the `/health` of the DA proxy before it deploys the batcher.

The inclusion horizon is not an environment switch.

- The ingress job variable `inclusion_horizon_blocks` and the sealer job variable `cluster_inclusion_horizon_blocks` both default to `64`.
- The two must be equal. The proxy stamps a deadline with the horizon, and the sealer holds an id until that deadline.
- The check in `roles/contract` fails when they differ. `just check-contract` and `just validate` run it, and CI runs it too.
- Change both defaults together.

### Tests of the deploy role

The isolated deployment tests use the real Ansible playbook and the Nomad HCL compiler against a local simulated Nomad API.

```sh
python3 -m unittest discover -s deploy/cluster/ansible/tests -v
```

- They need `ansible-playbook` and `nomad` on `PATH`.
- The settlement repeatability case also runs when Anvil and `target/debug/kardamom-deploy` are available. It creates and removes its own development chain.
- The Python files under `ansible/tests` are test fixtures and runners.

## Ansible image builds

Both image paths share `ansible/images.yml`. From the repository root:

```sh
# Wrap the prebuilt Linux binaries and Aeron libraries; the Java shadowJar must exist.
ansible-playbook -i localhost, deploy/cluster/ansible/images.yml
```

`just images` and the container CI runner both use prebuilt artifacts. The playbook builds these images:

- `aeron`, `redis` and `cluster` (the Java sealer).
- `l1-light-client`, built from the pinned helios release binary (`docker/helios/Dockerfile`). The build checks the SHA-256 of the release asset.
- The 13 Rust service images of `roles/images/defaults/main.yml`: `ingress`, `sequencer`, `executor`, `validator`, `da-watcher`, `batcher`, `state-mirror`, `l1-indexer`, `da-store`, `l1-fault-proxy`, `notifier`, `canary` and `egress`.

The build:

- It uses `community.docker.docker_image_build`. It needs Docker Buildx and a builder that loads its result into the local Docker engine.
- It re-evaluates source changes and keeps the BuildKit layer cache.
- It stages the binaries, the libraries and the cluster jar in a temporary build directory. `target/release` stays untouched.
- A missing artifact, or conflicting cached Aeron libraries, fails before any image build.
- It removes the temporary contexts on success and on failure.

Settings are in `roles/images/defaults/main.yml`.

- `REGISTRY`, `TAG` and `DIGEST_MANIFEST` are environment variables. A relative `DIGEST_MANIFEST` resolves against `deploy/cluster/` in both the image and the workload playbooks.
- `REGISTRY_PUSH_NODE=control-0` (the default) keeps the host daemon out of the push. Ansible exports an image archive, copies it into `kardamom-control-0`, loads it into the Docker engine of that node, and pushes from there. It removes the temporary archive even when the load fails. It needs temporary disk space for one image archive on each side.
- Pushes use the Docker CLI through Ansible `command.argv`, without a shell. This keeps the digest of the exact push: the Docker push module returns a pre-push inspection and does not expose that digest.
- Each manifest entry has the form `repo:tag@sha256:...` that the Nomad jobs need.
- Upstream copies: with `REGISTRY_PUSH_NODE` set (the container path), the playbook also copies each image of `upstream_images` into the in-cluster registry.
  - The copy is a build `FROM` the pinned reference (`docker/upstream/Dockerfile`). The host pulls each image one time, with the retries of the build, and the copy takes the transfer and the push of the kardamom images.
  - The manifest records each copy as `upstream-<variable>`, for example `upstream-anvil-image`. The deploy passes the copy to the job variable (`anvil_image`, `node_exporter_image`, `prometheus_image`, `alertmanager_image`, `grafana_image`). A node then pulls nothing from an external registry, and one stuck pull cannot fail the bring-up.
  - A release for a real registry (no `REGISTRY_PUSH_NODE`) copies nothing. Its jobs pull the upstream references.
  - A job file repeats each reference as its variable default. `just check-contract` checks the copy.

Signing:

- CI OIDC enables keyless signing of each pushed digest and of the completed manifest.
- The `cosign` role uses an installed executable, or the pinned Linux amd64 download and its checksum in `group_vars/all.yml`. Other controller platforms need an installed cosign for signing.
- Local builds skip signing.
- The playbook publishes the manifest only after every image and every required signature succeed. It keeps the previous manifest when a build, a push or a signature fails.
- An unsigned publication removes any old signature bundle.
- The signed bundle is installed before the manifest. A concurrent verifier can briefly reject a mismatched pair. It never trusts a partial manifest.

`--check` reports the planned image set. It does not build, push, sign or change a file.

The isolated image tests run the real Ansible roles and the Docker Buildx module with test Docker and cosign executables:

```sh
python3 -m unittest discover -s deploy/cluster/ansible/tests -p 'test_images.py' -v
```

They test staging, both push paths, exact digest capture, signing order, failed transfers, failed signatures, and the preservation of the previous manifest. They do not replace a real Docker build and a cluster smoke run.

### Release images

`.github/workflows/release.yml` runs the same playbook against GHCR.

- A push to `main` publishes `ghcr.io/<owner>/kardamom-<image>:main-<commit>`.
- The image set is the one in [Ansible image builds](#ansible-image-builds). It includes `l1-fault-proxy`. The release build compiles each service binary of the set, `kardamom-l1-fault-proxy` among them. A missing binary stops the image step.
- A `vMAJOR.MINOR.PATCH` tag on `main` publishes `:vMAJOR.MINOR.PATCH` and makes a GitHub release.
- The release carries `images.digests`, `images.digests.sigbundle`, the signed settlement deployer (`kardamom-deploy`, `kardamom-deploy.sigbundle`) and `SHA256SUMS`. A deployment passes the deployer as `DEPLOY_BIN`.
- Each run also pushes the release bundle `ghcr.io/<owner>/kardamom-release:<tag>` and signs it with cosign. The bundle is one tar file. It holds `images.digests`, `kardamom-deploy` and their signature bundles. A deploy host reads it from the registry with the image tag.
- A `main` run keeps the same files as the workflow artifact `images-main-<commit>`.
- The job signs each image and the manifest as `.github/workflows/release.yml@refs/heads/main` or `@refs/tags/<tag>`.
- A production deployment sets `DIGEST_MANIFEST` to the downloaded manifest, `KARDAMOM_REQUIRE_SIGNED=1`, and `KARDAMOM_CERT_IDENTITY_RE` to that identity.
- `REGISTRY` accepts a host, an optional port and an optional namespace path.

Operator steps for a new repository:

1. Make each `ghcr.io/<owner>/kardamom-*` package public. Or give the Nomad clients pull credentials.
2. Add a tag ruleset on `refs/tags/v*`. It limits who can publish a release.
3. Know that a tag run builds the images again. The digests of a tag build differ from the digests of `main-<commit>`.

## Layout

```
deploy/cluster/
  DESIGN.md                 design record
  PRODUCTION.md             the production profile
  justfile                  container-up / container-test / shard / container-down /
                            container-prepare / container-save / container-load / container-restore /
                            images / deploy / rollback / rollback-rerender / smoke / validate / check-contract
  ansible/
    ansible.cfg
    containers.yml          container-nodes.yml + bootstrap.yml + container-host.yml
    container-nodes.yml     inventory from the node contract, host preparation
    container-host.yml      host network settings after the inner Docker engines start
    cluster.yml             containers.yml + images.yml + deploy.yml
    resume.yml              a saved cluster: container-nodes.yml, substrate waits, deploy.yml
    rollback.yml            revert the last release by the deploy record
    group_vars/all.yml      the contract (classes, ports, versions, profile)
    bootstrap.yml           configure a host and join it to the substrate
    deploy.yml              workload deployment, signature and readiness gates
    images.yml              build, push, sign and publish an image manifest
    roles/{profile,enroll,common,vswitch,netinfo,docker,firewall,consul,nomad,
           registry,workloads,images,cosign,elastic_image,host_prep,contract}/
    inventories/production/ example production inventory and profile values
  terraform/containers/     the local profile's nodes: bridge, image, volumes,
                            one container per node, the node contract
  docker/
    ci-service.Dockerfile   thin wrapper over prebuilt binaries
    cluster.Dockerfile      Java Aeron Cluster node (shadowJar + JRE 17)
    node.Dockerfile         systemd + Docker-in-Docker node container (local profile)
    helios/Dockerfile       the L1 light client, from the pinned helios release
  nomad/
    data-dirs.sysbatch.nomad.hcl  the data directories of the services, on every node
    aeron.system.nomad.hcl  ArchivingMediaDriver (driver + archive)
    cluster.nomad.hcl       3-member Aeron Cluster (Raft) sealer
    anvil.nomad.hcl         in-cluster L1 for the smoke test, the follower and the batcher
    ingress.nomad.hcl  sequencer.nomad.hcl  executor.nomad.hcl
    validator.nomad.hcl  da-watcher.nomad.hcl  batcher.nomad.hcl
    state-mirror.nomad.hcl  redis.nomad.hcl  monitoring.nomad.hcl
    notifier.nomad.hcl      the transaction status feed, on the ingress nodes or the notifier nodes
    canary.nomad.hcl        the transaction canary, on the monitoring node
    da-proxy.nomad.hcl      the EigenDA proxy, on an EigenDA network
    da-store.nomad.hcl      the file-backed stand-in for it, without one
    l1-light-client.nomad.hcl  l1-indexer.nomad.hcl  (real L1, or the chaos-l1 shard)
    l1-fault-proxy.nomad.hcl   the lying L1 of the chaos-l1 shard, in front of anvil
  config/                   *.toml(.tpl) pulled into the job specs with file();
                            channels.toml.tpl is the shared LogConfig
```

- `ansible/contract.yml` validates the configuration mirrors and the routing inputs.
- `ansible/shard-map.yml` renders an initial identity map.
- The gates, the chaos cases and the operator commands are Rust: `crates/chaos` (`tests/shards.rs`, `src/cases/`, `src/bin/kardamom-cluster.rs`).
- The Nomad job specs pull their config payloads from `config/` with HCL2 `file()`. A manual CLI submission must run from `deploy/cluster/`. The Ansible deployment role sets this working directory itself.

## Deployment profiles

`bootstrap.yml` serves two profiles. `deployment_profile` in `ansible/group_vars/all.yml` selects one. The roles read it.

| Profile | Hosts | Servers | Addresses | Security |
|---------|-------|---------|-----------|----------|
| `local` (default) | The node containers of `terraform/containers` | One control node runs the Consul and Nomad servers | Explicit `node_ip` for each inventory host | Plain-HTTP registry, no host firewall |
| `production` | A dedicated core and elastic cloud pools | One or three voting servers on the dedicated core | `roles/netinfo` resolves the address from the vSwitch VLAN or the private interface | Gossip encryption, ACLs, agent TLS, `roles/firewall` |

- In both profiles the Nomad agents find their servers through the local Consul agent (`server_auto_join`, `client_auto_join`). There is no static server list.
- `nomad_heartbeat_grace` is the time after a missed client heartbeat before a Nomad server marks the node down and replaces its allocations. The local profile uses `60s`: its node containers share one host and stall under CPU load. Production keeps the Nomad default of `10s`.
- `consul_http_max_conns_per_client` (`1024`, Consul's default is 200) limits the HTTP connections of one client address on a Consul agent. All local processes share the loopback address, so the limit applies to the whole node. Each process holds a blocking query for each watched service.
- The Consul agents join `consul_retry_join`: the control node in the local profile, infrastructure DNS names in production.
- The Nomad service names carry `cluster_id`. Two clusters in one Consul datacenter cannot join each other.
- `roles/profile` refuses a production run that lacks a security input.

The inventory, the bootstrap steps, the elastic first boot and the host settings of the production profile are in [`PRODUCTION.md`](./PRODUCTION.md).

## Aeron-in-Docker

- **Shared `aeron.dir`:** Ansible mounts a host tmpfs at `/opt/kardamom/aeron-mount`. The media-driver container and every co-located service container bind-mount the same path. They share the CnC file and the memory-mapped ring buffers.
- **Host networking:** all Aeron and service containers run with `network_mode = "host"`. An Aeron UDP channel endpoint is the node address. There is no Docker port mapping.
- **Channels:** one shared `config/channels.toml.tpl` is the LogConfig. It uses dynamic Aeron MDC with Consul discovery. Stream ids distinguish streams. Control endpoints distinguish publishers. Every service reads it with `--log-config`. See [`../../docs/aeron-discovery.md`](../../docs/aeron-discovery.md).
- **Durability:** the Aeron Cluster members of the sealer archive the canonical log. Executors keep state in libmdbx under `/opt/kardamom/state` and recover from a crash by archive replay-merge.
- **MTU 1344:** the media driver and the cluster JVM set `-Daeron.mtu.length=1344`. The value fits a path MTU of 1400 bytes.
- **Reserved port block:** `fixed_port_block` (`40000-40299`) takes the fixed Aeron ports out of the Linux ephemeral range.
- **Stall tolerance:** `AERON_STALL_TOLERANCE_MS` sets the driver timeout, the client liveness timeout, the image liveness timeout and the publication unblock timeout of each media driver: the `aeron` job and the embedded driver of each sealer member.
- The derivation of the MTU and the port rules are in [Host settings](./PRODUCTION.md#host-settings).

## Performance pipeline (`kardamom-perf`)

`kardamom-perf` (`crates/bench`, `bin/perf.rs`) automates the saturation campaign against this stack. It does a fresh bring-up, ramps to the sustainable edge, then runs a steady soak. During the soak, async-profiler samples the JVM of the sealer Raft leader. It uses itimer mode, which works inside the nested containers and needs no `perf_events`.

```
cargo build --release -p kardamom-bench --bin kardamom-perf
kardamom-perf up                      # build + purge + fresh KEEP=1 deploy
kardamom-perf run --fresh             # up, then ramp -> profile -> report
kardamom-perf report --dir <outdir>   # re-render summary.md from artifacts
```

Each `run` writes a timestamped directory (default `target/perf/perf-<ts>/`):

- `discovery-report.json`: the ramp.
- `load-report.json`: the profiled soak.
- `flame.html`, `flame.svg`, `stacks.collapsed`: the profile.
- `cpu-snapshot.txt`: a mid-soak snapshot of every node container.
- `summary.md`: the edge tx/s, the delivery verdict, the latency percentiles, the CPU by node, and the leader profile split into service logic and Aeron substrate.

Account budget:

- The discovery ramp signs from genesis accounts #1 to #6. The profiled soak signs from #7 to #15. The smoke gates of the deploy use #0 and #16.
- A `run` therefore needs the fresh chain that `up` deploys. Nonces are managed locally, because the ingress has no `eth_getTransactionCount`.
- `run --fresh` does both steps. The profiling knobs (`--ceiling`, `--soak-fraction`, `--profile-secs` and others) are in `--help`.

## Monitoring

The `monitoring` job (`nomad/monitoring.nomad.hcl`) runs Prometheus, Alertmanager and Grafana on the node with the `monitoring` role (`aux-0` in the local profile).

- Prometheus scrapes the metrics port of every service by its Consul node name. The names come from the node-class counts.
- Grafana provisions the Prometheus datasource by the `prometheus` Consul service. It provisions the dashboards from `deploy/grafana/provisioning/dashboards-json`, the one source for every profile.
- From the host, read the node contract for the address of that node. Prometheus is on port 9090. Alertmanager is on port 9093. Grafana is on port 3000 (anonymous viewer; admin user `admin` with the `grafana_admin_password` job variable, `kardamom` on the local profile).
- The Prometheus APM of the autoscaler reads the same service.
- The `node-exporter` system job exports the host metrics of every node (port 9100). Every Nomad agent publishes its own metrics. Prometheus discovers both through Consul. See [Host and agent metrics](../../docs/observability.md#host-and-agent-metrics).

- Prometheus loads the alert rules of `deploy/alerts.yml`. It sends the firing alerts to the Alertmanager of the same allocation.
- The operator gives the extra rules and the Alertmanager configuration (routes and receivers) in the Nomad variable `nomad/jobs/monitoring`.
  - The item `rules` is a Prometheus rule file. The item `alertmanager` is the complete Alertmanager configuration.
  - A change to the variable reloads both servers in place.
  - Without the variable, Alertmanager sends every alert to a receiver that notifies nobody.
- No job loads `deploy/alertmanager-inhibit.yml`. Copy it into the `alertmanager` item.

See [`../../docs/observability.md`](../../docs/observability.md) for the metrics, the alerts and the readiness rules.

## Sustained-load and chaos suite

The `cluster-e2e` workflow runs the full suite on every trigger. It shards the suite across runners. Each shard brings up its own container cluster from the binaries that one `build` job staged.

Run a shard:

```sh
just shard chaos-cluster                          # bring-up, cases, tear-down, as CI does
just container-up && just container-test          # the load shard against a kept cluster
KARDAMOM_CHAOS_CASES="graceful-executor" just shard chaos-executor   # narrow to some cases
```

- The gates are the `kardamom-chaos` crate: one `#[ignore]` test per shard in `crates/chaos/tests/shards.rs`.
- A shard test brings the cluster up itself. `container-test` runs it with `KARDAMOM_CHAOS_REUSE=1` against the cluster that `container-up` made.
- The knobs (`CHAOS_TPS`, `LOAD_DURATION_S` and others) are the environment variables that `crates/chaos/src/knobs.rs` reads.
- The operator commands are `kardamom-cluster smoke | diagnostics | scale-sequencers` (`cargo run -p kardamom-chaos --bin kardamom-cluster -- ...`).
- `kardamom-deploy formats --base <registry> --root <tree>` prints the format findings of the tree against the registry as JSON. The release gate reads them.
- `kardamom-deploy smoke --rpc-url <ingress> --chain-id <id>` sends one signed transfer from a genesis account (`--account`, default `0`) and waits one minute for its receipt. The canary gate of the deploy runs it.
  - While the ingress answers the nonce read with `account state unavailable`, the read repeats each second, for at most 60 s. Any other error fails the smoke at once.

The shards, their cases, the gates, the load verdict, the L1 fault proxy and the knobs are in [`../../docs/chaos-suite.md`](../../docs/chaos-suite.md).

- `kardamom-load` is the load harness (`crates/bench/src/load/`, run in process).
- `crates/chaos` injects the failures under steady load. It asserts the Nomad auto-recovery, the pipeline progress and the load verdict.
- The untested failure surface is in "Known gaps" of [`../../docs/failure-modes.md`](../../docs/failure-modes.md#known-gaps-untested-failure-surface).

### Routing configuration

`ansible-playbook -i localhost, ansible/shard-map.yml` renders the initial identity map from `partition_count`.

- It refuses to overwrite a map with a version above zero.
- Pass `-e shard_map_output=/path/to/map.toml` to render an independent map.

Ansible parses `config/shard-map.toml` and passes its table to the static sequencer job. The job expands one group for each lane.

- During a resize, the Rust controller passes both tables through `ansible/resize.yml`. It shares the image verification, the variables and the job planner of the deployment.
- Nomad retains the losing groups and starts the gaining slots in shadow mode.
- After the ingress cutover and drain, the controller submits the target table alone.
- A dry run writes no file.

For a manual sequencer submission, pass the current table as the `shard_table` Nomad variable. Without it, the job uses the two-lane development identity map.

Use `just check-contract` for the Ansible contract assertions. Rust tests check the routing hashes, the rebalance behavior and the funded-account coverage.
