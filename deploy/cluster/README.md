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
| `ingress` | 2 | The active/active JSON-RPC front door (`:8545`) and the notifier status feed (`:8547`). Role `redis`. Instance 1 also has role `redis-replica`. |
| `executor` | 3 | State-machine replica appliers (libmdbx state). The state mirror runs here too. |
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

The `update` stanza of each job:

| Job | `max_parallel` | `health_check` | `min_healthy_time` | `auto_revert` | Other |
|-----|----------------|----------------|--------------------|---------------|-------|
| `ingress`, `sequencer` | 1 | `checks` | 15s | `true` | `canary = var.canary`, `auto_promote = false` |
| `cluster` (sealer) | 1 | `checks` | 60s | `false` | One task group for each member |
| `executor`, `validator`, `batcher`, `state-mirror` | 1 | `checks` | 15s | `false` | |
| `notifier` | 1 | `checks` | 10s | `false` | |
| `da-watcher`, `l1-indexer`, `l1-light-client`, `da-proxy`, `da-store` | 1 | `task_states` | 10s | `false` | |
| `aeron` (system job) | 1 | `task_states` | 15s | not set | `stagger = "30s"` |

The sealer roll:

- The sealer has one task group for each Raft member, and the role makes one registration for each member.
- The role rolls the followers first and the leader last. It finds the leader through the admin port (`GET /status`).
- Each step replaces one member. The `update` stanza holds the next step until that member rejoins and catches up.
- A fresh job, or a job that still has one shared group, registers in one step.

The canary:

- The ingress and the sequencer can deploy a canary. Set `KARDAMOM_CANARY=1`. The canary is one allocation more than the count, so it needs one spare node of the class. The container profile has none.
- The role smokes the ingress canary by its node address, and promotes it. Then it replaces the old instances.
- For the sequencer, the canary is healthy by its own check. The smoke then goes through a running ingress and proves that the chain still accepts a transaction.
- A failed smoke fails the play. The deployment stays unpromoted, and the old instances keep serving. `nomad deployment fail` removes the canary.
- `auto_revert` is on for these two stateless classes only.

The deployment record and rollback:

- A successful deploy records its manifest under `deployed/<env>/`. `KARDAMOM_ENV` selects `<env>` (default `local`).
- `images.digests` is what runs. `images.digests.previous` is what it replaced.
- `just rollback <env>` deploys the previous manifest. It is a normal rolling deploy of older images under the same checks. It fails if `deployed/<env>/images.digests.previous` is missing.
- The validator keeps a divergence verdict in a file beside its state. The verdict survives a restart and a deploy, and `/ready` keeps failing. An operator clears it with `kardamom-validator --state-dir <dir> --clear-verdict` inside the allocation.
- The smoke, load and chaos gates of `crates/chaos` follow in CI.

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

### Sealer log purge

Each sealer member purges its own Raft log behind its snapshots.

- The log of the 3 newest snapshots stays. Set `KARDAMOM_CLUSTER_LOG_PURGE_KEEP` to change the count. The value 0 turns the purge off.
- The log of every block that the batcher has not posted also stays.
- A member that stops for longer than this margin seeds from a peer when it comes back.
- See "Raft log purge" and "Follower below the purge point" in [`../../docs/failure-modes.md`](../../docs/failure-modes.md#sealer-the-aeron-cluster-raft).

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
- Upstream Anvil keeps the image policy of its jobspec. The light-client image is built by the image playbook, pinned by digest and signed like the service images.

### Configuration switches

The workloads role reads these environment variables. The defaults are in `ansible/roles/workloads/defaults/main.yml`. An Ansible extra variable can set the matching `workloads_*` variable directly. An empty value means "use the default of the job".

For the behavior of the L1 switches, see [`../../docs/l1-data-path.md`](../../docs/l1-data-path.md).

| Area | Variable | Default | Meaning |
|------|----------|---------|---------|
| Controller | `NOMAD_ADDR` | `http://127.0.0.1:4646` | The Nomad API. The justfile derives it from the node contract. |
| Controller | `NOMAD_TOKEN` | empty | The Nomad ACL token. |
| Controller | `NOMAD_NAMESPACE` | `default` | The Nomad namespace. |
| Controller | `KARDAMOM_ENV` | `local` | The environment name. It selects `deployed/<env>/`. |
| Controller | `KARDAMOM_CLUSTER_BIN` | `target/release/kardamom-cluster` | The operator binary that smokes a canary. |
| Signing | `DIGEST_MANIFEST` | `images.digests` | The image manifest. A relative path starts in `deploy/cluster/`. |
| Signing | `KARDAMOM_REQUIRE_SIGNED` | `0` | `1` requires the signature of the manifest and of each image. |
| Signing | `KARDAMOM_CERT_IDENTITY_RE` | the workflows of the project repository | A regex for the signer identity. |
| Signing | `KARDAMOM_CERT_OIDC_ISSUER` | `https://token.actions.githubusercontent.com` | The OIDC issuer of the signer. |
| Signing | `KARDAMOM_COSIGN_CACHE`, `KARDAMOM_COSIGN_VERSION` | the user cache, the pinned version | Where the controller finds a cosign that is not on `PATH`. |
| Settlement | `SETTLEMENT_ADDRESS` | empty | An existing settlement address. Empty: deploy one with `kardamom-deploy`. |
| Settlement | `DEPLOY_BIN` | `target/release/kardamom-deploy` | The settlement deployer. |
| Settlement | `L1_OWNER`, `L1_OWNER_KEY` | the Anvil dev account 0 | The owner address and key that the deployer uses. |
| Settlement | `BATCHER_EOA` | the Anvil dev account 2 | The batcher address that the deployer registers. |
| Settlement | `L2_CHAIN_ID` | `chain_id` of `all.yml` (412346) | The L2 chain id. |
| Settlement | `SETTLEMENT_DEPLOY_BLOCK` | empty (0) | The L1 block of the settlement deployment. A `BatchPosted` scan starts here. |
| Real L1 | `L1_RPC` | empty | The L1 endpoint. Empty: the in-cluster anvil, found through the Nomad API. Set: the batcher and the da-watcher use it. |
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
| Followers | `L1_FOLLOWERS_RPC` | the fault proxy if deployed, else the light client if deployed, else `L1_RPC` | The L1 that the da-watcher and the indexer walk. A comma-separated list. With two or more entries, a block counts only when two agree. |
| Indexer | `L1_INDEXER_START_BLOCK` | empty (`1` with the fault proxy) | The first L1 block to index on an empty archive. Empty: the finalized block at the first start. |
| Indexer | `L1_INDEXER_POLL_S` | empty (`12` in the binary) | The poll period, in seconds. |
| Indexer | `L1_INDEXER_HOST`, `L1_INDEXER_PORT` | `kardamom-l1-indexer.service.<datacenter>.consul`, `8549` | Where the batcher reaches the indexer. |
| Fault proxy | `KARDAMOM_L1_FAULT_PROXY` | `0` | `1` deploys the lying L1 of the `chaos-l1` shard in front of the in-cluster anvil. All followers read L1 through it. |
| Sealer | `KARDAMOM_CLUSTER_RETENTION` | empty (`65536` frames in the job) | The egress replay retention, in frames. It is a minimum. The sealer keeps a frame above the posted head even past this window. See the [sealer README](../../cluster/sealer-service/README.md). |
| Sealer | `KARDAMOM_DA_LAG_BUDGET_BLOCKS` (job variable `cluster_da_lag_budget_blocks`) | empty (`10000` in the job) | The DA-lag budget, in blocks. The sealer refuses user transactions when the sealed head is more than this far past the posted head. `0` turns the guard off. Every member must use the same value. |
| Sealer | `KARDAMOM_CLUSTER_BOOTSTRAP` | `0` (`1` in `just container-up`) | `1` opens the sealer bootstrap while the role registers a cluster job that Nomad does not know. See [Sealer bootstrap](#sealer-bootstrap). |
| Sealer | `KARDAMOM_CLUSTER_SNAPSHOT_S` | empty (`300` in the job) | The interval of the Raft snapshot, in seconds. `0` disables it. |
| Sealer | `KARDAMOM_CLUSTER_LOG_PURGE_KEEP` | empty (`3` in the job) | How many of the newest Raft snapshots keep their log. `0` turns the log purge off. See [Sealer log purge](#sealer-log-purge). |
| Sealer | `KARDAMOM_CLUSTER_FILE_SYNC_LEVEL` | empty (`1` in the job) | The sync level of the Raft log and the archive. `0` leaves a write in the page cache. `1` syncs the data of each write batch. `2` syncs data and metadata. At `0`, a power loss that takes the members together can drop an entry that a quorum acknowledged. |
| Sealer | `KARDAMOM_REMOTE_ORIGINS` | empty (`412347,412399` in the job) | The peer chain ids whose cross-chain records the sealer seals. All members use the same list. |
| Fees | `PRIORITY_FEES` | empty (`off`) | `on` or `off`. One value sets the sequencer tip, the sealer ordering window and the executor and validator fee schedule. Turning it on for an existing chain is a chain upgrade. See [`../../docs/priority-fees.md`](../../docs/priority-fees.md). |
| Aeron | `AERON_STALL_TOLERANCE_MS` | `10000` | How long every Aeron party waits through a stalled peer, in ms. The container recipes and CI use `30000`. See "Aeron stall tolerance" in [`../../docs/failure-modes.md`](../../docs/failure-modes.md#substrate-the-shared-failure-domain). |
| Deploy | `KARDAMOM_CANARY` | `0` | `1` deploys one canary of the ingress and of the sequencer. |
| Chaos | `KARDAMOM_CHAOS_CASES`, `CHAOS_TPS`, `LOAD_DURATION_S` and others | | The knobs of the chaos suite. See [`../../docs/chaos-suite.md`](../../docs/chaos-suite.md). |

Preflight checks fail the deploy before it changes a job:

- The light client needs the execution RPC, the consensus RPC, the checkpoint and the lockbox address together.
- A supplied settlement address must be a non-zero 20-byte hex address.
- Without a settlement address, `kardamom-deploy` must exist and support `addresses --contract --json`.

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
- The 11 Rust service images of `roles/images/defaults/main.yml`: `ingress`, `sequencer`, `executor`, `validator`, `da-watcher`, `batcher`, `state-mirror`, `l1-indexer`, `da-store`, `l1-fault-proxy` and `notifier`.

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
                            images / deploy / rollback / smoke / validate / check-contract
  ansible/
    ansible.cfg
    containers.yml          inventory from the node contract, host preparation
    cluster.yml             containers.yml + images.yml + deploy.yml
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
    aeron.system.nomad.hcl  ArchivingMediaDriver (driver + archive)
    cluster.nomad.hcl       3-member Aeron Cluster (Raft) sealer
    anvil.nomad.hcl         in-cluster L1 for the smoke test and the da-watcher
    ingress.nomad.hcl  sequencer.nomad.hcl  executor.nomad.hcl
    validator.nomad.hcl  da-watcher.nomad.hcl  batcher.nomad.hcl
    state-mirror.nomad.hcl  redis.nomad.hcl  monitoring.nomad.hcl
    notifier.nomad.hcl      the transaction status feed and webhooks, on the ingress nodes
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
