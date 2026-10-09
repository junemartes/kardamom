# Kardamom

Kardamom is an Ethereum rollup framework. The Rust pipeline services talk to each other over Aeron. A Java Aeron Cluster (Raft) orders the transactions.

See [`docs/img/architecture.jpg`](docs/img/architecture.jpg) for the service architecture diagram. See [`docs/failure-modes.md`](docs/failure-modes.md) for how each actor fails and recovers. See [`docs/runbooks/`](docs/runbooks/README.md) for the steps that clear each halt.

## Overview

The repository has these parts.

- **Rust workspace** (`crates/`). It has the pipeline services, the shared libraries and the tooling. See the tables below.
- **Java sealer** (`cluster/sealer-service/`). It is a clustered service on Aeron Consensus Module, which is JVM-only. It holds the canonical order.
  - The Rust pipeline reaches the sealer through `kardamom-cluster-adapter` and `kardamom-cluster-client`.
- **Contracts** (`contracts/`). The Solidity sources, built with Foundry. `bench-contracts/` and `uniswap-v2-contracts/` hold the contracts of the benchmarks.
- **Chains** (`chains/`). The genesis configs: `dev.toml`, `dev-interop.toml` and `dev-withdrawals.toml`.
- **Deploy** (`deploy/`). The multi-node cluster (Terraform, Ansible, Nomad), the Grafana dashboards and the alert rules.
- **ZK guest and host** (`guest/`). `kardamom-zk-guest` is the SP1 guest. `kardamom-zk-host` is its host. Both are outside the Cargo workspace.
- **Performance gates** (`perf/`). The allocation ceilings and the performance notes.

### Services

| Crate | Role |
|---|---|
| `kardamom-ingress` | The client edge. It serves JSON-RPC over HTTP and WebSocket, recovers senders, and answers receipts. |
| `kardamom-sequencer` | Orders the transactions of each sender by nonce. It offers references to the sealer. |
| `kardamom-executor` | Executes the canonical stream. It commits state to libMDBX and publishes receipts and block access lists. |
| `kardamom-batcher` | Posts the canonical stream to L1 through EigenDA. It also builds `kardamom-da-store`, `kardamom-archive-rereplicate`, `kardamom-proof-submitter`, `kardamom-batch-claimer` and `kardamom-batch-watcher`. |
| `kardamom-da-watcher` | Tails finalized L1 blocks. It republishes deposits into the pipeline. |
| `kardamom-validator` | Re-executes every block. On a divergence, it halts and stays up. |
| `kardamom-notifier` | Serves transaction status events to clients. |
| `kardamom-canary` | Uses the chain as a user does and reports each success and each failure as a metric. |
| `kardamom-state-mirror` | Writes the Redis projection of the account state, next to each executor. |
| `kardamom-l1-indexer` | The L1 follower. It reads the finalized L1 once per finality step, publishes each block on the `l1_blocks` stream, and archives the batches and the epoch inputs. |

### Libraries

| Crate | Role |
|---|---|
| `kardamom-types` | The data types and traits that every subsystem shares. It does no I/O. |
| `kardamom-log` | The Aeron application channels, the archive recorders and the archive refetch client. |
| `kardamom-state` | The libMDBX state database of the L2. |
| `kardamom-obs` | The Prometheus exporter and the `/ready` rule of every service binary. |
| `kardamom-engine` | The execution engine that the executor and the validator share. |
| `kardamom-exec-core` | The pure `no_std` state transition: revm execution, write sets and block environment. |
| `kardamom-cluster-adapter` | The transport layer that puts an Aeron Cluster behind the pipeline seams. |
| `kardamom-cluster-client` | A Rust Aeron Cluster client. |
| `kardamom-cache` | The Redis account projection, the receipt index and the local layer in front of them. |
| `kardamom-stm` | The Block-STM engine. It predicts contention before it executes. |
| `kardamom-footprint` | The footprint prediction core that the Block-STM engine and its offline lab share. |
| `kardamom-interop-feed` | The wire contract of the interop feed between an origin validator and a destination watcher. |

### Tooling

| Crate | Role |
|---|---|
| `kardamom-deployer` | The stateless deployer of the L1 contracts (`kardamom-deploy`, `print-factory-address`). See [`crates/deployer/README.md`](crates/deployer/README.md). |
| `kardamom-bench` | The benchmarks and the load harness (`kardamom-bench`, `kardamom-bench-harness`, `kardamom-load`, `kardamom-perf`, `kardamom-stm-p0`, `kardamom-stm-p2`). |
| `kardamom-reconstruct` | Rebuilds the L2 state from L1 and the DA layer. |
| `kardamom-chaos` | The container cluster chaos suite and the `kardamom-cluster` operator binary. |
| `kardamom-l1-fault-proxy` | An L1 JSON-RPC proxy that lies on command, for the L1 chaos cases. |
| `e2e` | The chain-semantics scenarios and the `kardamom-semantics` runner. |
| `kardamom-test-support` | The shared sign-and-wrap fixture for tests. |

## Building

The default build is pure Rust. It compiles on any platform with a Rust toolchain (edition 2024).

```sh
cargo build --workspace
```

Two parts of the workspace need extra native tooling.

- **The `aeron-live` feature** (pulled in by `--all-features`) compiles the bundled Aeron C sources through the `rusteron-*` crates.
  - It needs a C/C++ compiler, `cmake`, `libclang` (for `bindgen`) and `pkg-config`.
  - It needs a **JDK 17+**. The Aeron archive build runs a Gradle and SBE codegen step that needs JVM 17 or later.
- **`forge` (Foundry)**. The `deployer` build script runs it to compile the Solidity contracts.

Build the Java sealer separately with its Gradle wrapper: `./gradlew build` in `cluster/sealer-service/`. It needs the same JDK 17.

### Quick start

Install [mise](https://mise.jdx.dev/getting-started.html). Then run these commands from the repository root.

```sh
mise trust
mise run setup       # install pinned tools and Ansible collections
mise exec -- just check
```

- The root [`mise.toml`](mise.toml) pins these tools:
  - Rust (with Clippy, rustfmt and rust-src), JDK 17, Python, uv, CMake, Just and Foundry.
  - jq, OpenTofu, Nomad, cosign, Ansible, ansible-lint and yamllint.
- Ansible includes `requests` for the Docker modules.
- The collection versions are in [`requirements.yml`](deploy/cluster/ansible/requirements.yml). Mise installs them in the ignored `.ansible/collections/` directory.
- For automatic tool selection, add `eval "$(mise activate zsh)"` to `~/.zshrc`, or `eval "$(mise activate bash)"` to `~/.bashrc`.
  - After you trust the config, entering the repository installs any missing pinned tool.
  - Then use `just` and `cargo` directly. Mise also sets `JAVA_HOME`.
- Without shell activation, `mise exec -- just <recipe>` installs the missing tools and runs the command with the repository versions.
- Run `mise run setup` again when the collection pins change.

Mise manages user-space tools only.

- Install Docker with Buildx and a running Linux daemon separately for cluster work.
- Native builds still need a C/C++ compiler, libclang, pkg-config and platform libraries. On Debian and Ubuntu these are `uuid-dev`, `libbsd-dev` and `libssl-dev`.
- The Gradle wrapper and the Foundry configuration select the Gradle and Solidity compiler versions.
- CI keeps its own tool installers. The CI Rust channel is `stable`.

The OS bootstrap is also available.

```sh
just bootstrap   # install native prerequisites and system tooling
just check       # cargo check --workspace --all-features
```

`just bootstrap` supports macOS (Homebrew) and Linux (apt, dnf, pacman). On other systems, install the prerequisites by hand.

### Prerequisites (manual install)

Use this table if you do not use `just bootstrap`. Omit CMake and Java when you use mise.

| Platform | Command |
| --- | --- |
| macOS | `brew install cmake pkg-config openjdk@17` (Xcode provides `clang` and `libclang`) |
| Debian / Ubuntu | `sudo apt-get install -y build-essential cmake pkg-config clang libclang-dev openjdk-17-jdk-headless` |
| Fedora / RHEL | `sudo dnf install -y gcc gcc-c++ make cmake pkgconf-pkg-config clang clang-devel java-17-openjdk-devel` |
| Arch | `sudo pacman -S --needed base-devel cmake pkgconf clang jdk17-openjdk` |

Without mise, install Foundry on every platform.

```sh
curl -L https://foundry.paradigm.xyz | bash && foundryup
```

### JAVA_HOME

Mise sets `JAVA_HOME` when its tools are active. For a manual installation, set it yourself.

- The Aeron archive build needs a **JDK 17+**.
- On macOS, the `FindJava` module of cmake resolves through `/usr/libexec/java_home`. That command prefers an older system JDK. The keg-only `openjdk@17` is not registered there.
- Point `JAVA_HOME` at a JDK 17+ so the build finds it.

```sh
# macOS (Homebrew openjdk@17)
export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
```

- Add that line to your shell profile. Editors (rust-analyzer) and ad-hoc `cargo` commands then inherit it.
- The `just` recipes resolve a suitable JDK themselves. They work without a `JAVA_HOME` setting.
- `rust-analyzer.toml` builds with all features. Start your editor from a shell where `JAVA_HOME` is set, so the `aeron-live` build also succeeds in the editor.

## `just` recipes

Run `just` to list the recipes. All build and test recipes set `JAVA_HOME` to a detected JDK 17+.

### Build, lint and test

| Recipe | What it does |
| --- | --- |
| `just bootstrap` | Install the Aeron native toolchain and Foundry for this platform. |
| `just check` | Run `cargo check` on the whole workspace with all features. |
| `just clippy` | Run Clippy across all features with `-D warnings` (mirrors CI). |
| `just style` | Run the mechanical checks of [`docs/STYLE.md`](docs/STYLE.md). Run it before you open a pull request. |
| `just test` | Run the test suite across all features. |
| `just test-e2e-local` | Run the chain-semantics suite against a real local stack. See [Testing](#testing). |
| `just eest` | Run the EEST state-test conformance suite against the pinned fixtures. |
| `just eest-fixtures [dest_root]` | Fetch the pinned EEST fixtures. The default root is `~/.cache/kardamom/eest`. |
| `just eest-tag` | Print the pinned EEST release tag. CI uses it as a cache key. |
| `just alloc-gate` | Run the DHAT allocation harnesses. Fail when a ceiling in `perf/alloc-baselines.env` is exceeded. |
| `just bench-embed` | Regenerate `crates/bench/src/load/defi_bytecode.rs` from the `bench-contracts` artifacts. |
| `just cluster-jar` | Build the Java sealer jar (`:service:shadowJar`). |
| `just check-aeron` | Check that only the Aeron bindings compile. |
| `just aeron-jar` | Download and cache `aeron-all.jar`. |
| `just aeron-driver-up` | Start a host-native Aeron Media Driver. |
| `just aeron-driver-down` | Stop the Media Driver that `aeron-driver-up` started. |

### Cluster

The [cluster recipes](deploy/cluster/README.md#quick-start) run from the repository root. Their relative paths resolve from `deploy/cluster/`.

| Recipe | What it does |
| --- | --- |
| `just cluster-bootstrap` | Install the host tools for `deploy/cluster/` (Ansible, Docker, OpenTofu, the Nomad CLI). |
| `just cluster-doctor` | Check that the host has everything that `deploy/cluster/` needs. |
| `just stage-dist <dir>` | Stage the binaries and the sealer jar that a shard runner needs into one directory. |
| `just images` | Wrap the prebuilt service binaries and the sealer jar, then push the images. |
| `just container-up` | Create and provision the container cluster, publish the images and deploy the workloads. |
| `just deploy` | Converge the workload jobs and the settlement through Ansible. |
| `just rollback <env>` | Deploy the manifest that the last successful deploy of `<env>` replaced. |
| `just smoke` | Submit a signed transfer. `RPC_URL` overrides the node contract address. |
| `just validate` | Run all static deployment checks and report the failures together. |
| `just check-contract` | Verify the values that mirror `group_vars/all.yml`. |
| `just container-test [shard]` | Run a shard against the existing cluster. The default is `SHARD`, or `load`. |
| `just shard [shard]` | Run a shard with its full cluster lifecycle. |
| `just container-diagnostics` | Collect the node and job state after a failure. |
| `just container-down` | Destroy the cluster containers and their volumes. |
| `just container-reset` | Destroy the cluster, then create a fresh chain. |
| `just clean` | Remove the local cluster images and the staged cluster jar. |

## Contracts

The Solidity sources are in `contracts/`. Foundry (`forge`) builds them. See `contracts/foundry.toml`.

- CI compiles, tests and lints the contracts.
- The Rust build scripts run `forge build` to embed or locate the artifacts.

Rust `sol!` bindings read the committed ABIs in `contracts/abi/`, not a
hand-written copy. After you change a contract's interface, run `just abi` and
commit the result. CI runs `just abi-check`, which fails when the committed
ABIs differ from a fresh build. Bytecode still comes from `contracts/out/`,
which the deployer's build script fills.

## Testing

Beyond `cargo test`, two suites answer different questions.

**Chain semantics.** Does the chain mean the right thing?

- One set of scenario drivers (`crates/e2e/src/scenarios/`) covers these areas:
  - L1 to L2 bridge round-trips against a real anvil.
  - Nonce ordering and RPC liveness through `eth_sendRawTransaction`.
  - Validator and executor state parity, down to a byte-level comparison of the two libMDBX databases.
  - DA parity. The suite posts payloads to L1, re-executes them and matches the validator root.
  - State-database integrity across normal operation and an unclean crash.
- The drivers run on two targets:
  - **Target L**: `just test-e2e-local`. Real service binaries, a one-member Java Aeron Cluster sealer and a media driver for each test, on one host. It takes about 45 s. It is also the per-PR `chain-semantics-e2e` CI job.
  - **Target C**: the `semantics` shard of `.github/workflows/cluster-e2e.yml`. The same drivers run against the real 12-node Docker-in-Docker cluster through the `kardamom-semantics` binary.
- The spec and the current coverage are in [`docs/agents/chain-semantics-e2e-suite-spec.md`](docs/agents/chain-semantics-e2e-suite-spec.md).

**Chaos.** Does the pipeline survive faults under load?

- `crates/chaos` injects faults under steady load. It has one test for each shard in `crates/chaos/tests/shards.rs`.
- Run a shard with `just shard <shard>`. CI runs the shards through `.github/workflows/cluster-e2e.yml`.
- [`docs/chaos-suite.md`](docs/chaos-suite.md) lists the shards, the cases, the gates, the load verdict, the persisted-state audit and the knobs.
- [`docs/failure-modes.md`](docs/failure-modes.md) says what each failure does and which case proves the recovery.
- [`docs/runbooks/`](docs/runbooks/README.md) has one runbook for each halt cause.

## Documentation

| Document | What it covers |
| --- | --- |
| [`docs/failure-modes.md`](docs/failure-modes.md) | How each actor fails and recovers, and the chaos case that proves it. |
| [`docs/runbooks/`](docs/runbooks/README.md) | One runbook for each halt cause: cause, confirm, steps and clear. It also covers `POST /halt/clear`. |
| [`docs/chaos-suite.md`](docs/chaos-suite.md) | The chaos shards, the gates, the load verdict, the persisted-state audit, the L1 fault proxy and the knobs. |
| [`docs/observability.md`](docs/observability.md) | The metrics, ports, dashboards, alerts, readiness checks and diagnostic log lines. |
| [`docs/aeron-discovery.md`](docs/aeron-discovery.md) | The Aeron MDC transport and the Consul discovery contract. |
| [`docs/l1-data-path.md`](docs/l1-data-path.md) | The batcher, the EigenDA proxy, the settlement contract, the inbox indexer, the L1 followers and `kardamom-reconstruct`. |
| [`docs/json-rpc.md`](docs/json-rpc.md) | The client JSON-RPC API of the ingress: methods, errors, receipt fields, fee RPCs and subscriptions. |
| [`docs/priority-fees.md`](docs/priority-fees.md) | Priority fees from end to end. |
| [`docs/tx-status-events.md`](docs/tx-status-events.md) | The notifier: the WebSocket status feed and the webhooks. |
| [`docs/STYLE.md`](docs/STYLE.md) | The code style rules and the checks that `just style` runs. |
| [`deploy/cluster/README.md`](deploy/cluster/README.md) | The cluster deploy: the local container profile, recipes, environment switches and images. |
| [`deploy/cluster/PRODUCTION.md`](deploy/cluster/PRODUCTION.md) | The production profile: role placement, bootstrap, enrollment, and host and network settings. |
| [`cluster/sealer-service/README.md`](cluster/sealer-service/README.md) | The Java sealer: wire envelope, settings, dedup window, inclusion deadline, admin port and log lines. |
| [`crates/types/README.md`](crates/types/README.md) | The shared data types. |
| [`crates/deployer/README.md`](crates/deployer/README.md) | The L1 contract deployer. |
| [`crates/log/README.md`](crates/log/README.md) | The canonical log crate: channels, recorders, discovery and refetch. |
| [`perf/README.md`](perf/README.md) | The performance gates and the benchmark notes. |
| [`guest/kardamom-zk-guest/README.md`](guest/kardamom-zk-guest/README.md) | The SP1 zkVM guest program. |

Design records are in `docs/specs/`, `docs/plans/` and `docs/agents/`. They give the reasons behind the design. They are further reading, not reference.
