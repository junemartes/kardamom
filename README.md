# Kardamom

An Ethereum rollup framework. The workspace is a set of Rust crates — the
pipeline services (`kardamom-ingress`, `kardamom-sequencer`, `kardamom-executor`,
`kardamom-batcher`, `kardamom-da-watcher`) wired together over Aeron, the
off-hot-path `kardamom-validator` (re-executes every block and fail-stops on
divergence), shared libraries (`kardamom-types`, `kardamom-log`,
`kardamom-state`, `kardamom-obs`, `kardamom-engine` — the execution core shared
by executor and validator — `kardamom-cluster-adapter`,
`kardamom-cluster-client`), and tooling (`deployer`, `bench`, the `e2e` test
crate). The sealer is not a Rust crate:
canonical ordering runs as a **Java Aeron Cluster (Raft) clustered service**
under `cluster/sealer-service/` (Aeron's Consensus Module is JVM-only); the
Rust pipeline talks to it through `kardamom-cluster-adapter` /
`kardamom-cluster-client`. Solidity contracts live under `contracts/`, chain
genesis configs under `chains/`, and the observability + multi-node deploy
stack under `deploy/`.

See [`docs/img/architecture.jpg`](docs/img/architecture.jpg) for the service
architecture diagram, and [`docs/failure-modes.md`](docs/failure-modes.md) for
how each actor fails and recovers (with the chaos cases that verify it).

## Building

The default build is **pure Rust** and compiles on any platform with just a
Rust toolchain (edition 2024):

```sh
cargo build --workspace
```

Two parts of the workspace need extra native tooling:

- **The `aeron-live` feature** (pulled in by `--all-features`) compiles the
  bundled Aeron C sources through the `rusteron-*` crates. This needs a C/C++
  compiler, `cmake`, `libclang` (for `bindgen`), `pkg-config`, and a **JDK 17+**
  — the Aeron *archive* build runs a Gradle/SBE codegen step that requires JVM
  17 or later.
- **`forge` (Foundry)** is invoked by the `deployer` build script to compile the
  Solidity contracts.

The Java sealer under `cluster/sealer-service/` is built separately with its
Gradle wrapper (`./gradlew build`) and also needs a JDK 17 — the same one the
`aeron-live` build uses.

### Quick start

Install [mise](https://mise.jdx.dev/getting-started.html), then run from the
repository root:

```sh
mise trust
mise run setup       # install pinned tools and Ansible collections
mise exec -- just check
```

The root [`mise.toml`](mise.toml) pins Rust (with Clippy, rustfmt and rust-src),
JDK 17, Python, uv, CMake, Just, Foundry, jq, OpenTofu, Nomad, cosign, Ansible,
ansible-lint, and yamllint. Ansible includes `requests` for the Docker modules.
The collection versions live in
[`requirements.yml`](deploy/cluster/ansible/requirements.yml). Mise installs
them under the repo's ignored `.ansible/collections/` directory.

For automatic tool selection, add `eval "$(mise activate zsh)"` to `~/.zshrc`
(or `eval "$(mise activate bash)"` to `~/.bashrc`). After trusting the config,
entering the repo installs any missing pinned tools. Then use `just` and
`cargo` directly; mise also sets `JAVA_HOME`. Without shell activation,
`mise exec -- just <recipe>` installs missing tools and runs the command with
the repo's versions. Run `mise run setup` again when collection pins change.

Mise manages user-space tools. Install Docker with Buildx and a running Linux
daemon separately for cluster work. Native builds still need a C/C++ compiler,
libclang, pkg-config, and platform libraries (`uuid-dev`, `libbsd-dev`, and
`libssl-dev` on Debian/Ubuntu). The Gradle wrapper and Foundry configuration
already select Gradle and Solidity compiler versions. CI keeps its existing
tool installers; Rust's CI channel remains `stable`.

The existing OS bootstrap is also available:

```sh
just bootstrap   # install native prerequisites and system tooling
just check       # cargo check --workspace --all-features
```

`bootstrap` supports macOS (Homebrew) and Linux (apt / dnf / pacman). On other
systems, install the prerequisites manually (see below).

## Prerequisites (manual install)

If you'd rather not use `just bootstrap` (omit CMake and Java when using mise):

| Platform        | Command |
| --------------- | ------- |
| macOS           | `brew install cmake pkg-config openjdk@17` (Xcode provides `clang`/`libclang`) |
| Debian / Ubuntu | `sudo apt-get install -y build-essential cmake pkg-config clang libclang-dev openjdk-17-jdk-headless` |
| Fedora / RHEL   | `sudo dnf install -y gcc gcc-c++ make cmake pkgconf-pkg-config clang clang-devel java-17-openjdk-devel` |
| Arch            | `sudo pacman -S --needed base-devel cmake pkgconf clang jdk17-openjdk` |

Without mise, also install Foundry on every platform:

```sh
curl -L https://foundry.paradigm.xyz | bash && foundryup
```

### JAVA_HOME

Mise sets `JAVA_HOME` when its tools are active. For a manual installation:

The Aeron archive build needs a **JDK 17+**. On macOS in particular, cmake's
`FindJava` resolves through `/usr/libexec/java_home`, which prefers whatever
older system JDK is registered (and the keg-only `openjdk@17` is not registered
there). Point `JAVA_HOME` at a 17+ JDK so the build picks it up:

```sh
# macOS (Homebrew openjdk@17)
export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
```

Add that to your shell profile so editors (rust-analyzer) and ad-hoc `cargo`
commands inherit it. The `just` recipes below resolve a suitable JDK
automatically, so they work without setting `JAVA_HOME` yourself.

rust-analyzer is configured (via `rust-analyzer.toml`) to build with all
features; launch your editor from a shell where `JAVA_HOME` is set so the
`aeron-live` build succeeds in the editor too.

## `just` recipes

| Recipe                   | What it does |
| ------------------------ | ------------ |
| `just bootstrap`         | Install the Aeron native toolchain + Foundry for this platform. |
| `just check`             | `cargo check` the whole workspace with all features. |
| `just clippy`            | Clippy across all features with `-D warnings` (mirrors CI). |
| `just test`              | Run the test suite across all features. |
| `just test-e2e-local`    | Chain-semantics e2e suite against a real local stack (see below). |
| `just cluster-jar`       | Build the Java Aeron Cluster sealer jar (`:service:shadowJar`). |
| `just check-aeron`       | Targeted check that just the Aeron bindings compile. |
| `just aeron-driver-up`   | Start a host-native Aeron Media Driver (jar cached locally). |
| `just aeron-driver-down` | Stop the Media Driver started by `aeron-driver-up`. |
| `just cluster-bootstrap` | Install the HOST tools for the `deploy/cluster/` workflow (Ansible, Docker, OpenTofu, the Nomad CLI). |
| `just cluster-doctor`    | Check the host has everything `deploy/cluster/` needs. |
| `just container-up`      | Create and provision the container cluster, publish images, and deploy workloads. |
| `just container-test [shard]` | Run a shard against the existing cluster (default: `SHARD` or `load`). |
| `just shard [shard]`     | Run a shard with its full cluster lifecycle. |
| `just container-diagnostics` | Collect node and job state after a failure. |
| `just container-down`    | Destroy the cluster containers and their volumes. |
| `just container-reset`   | Destroy the cluster, then create a fresh chain. |

All [cluster recipes](deploy/cluster/README.md#quick-start) run directly from
the repository root, including `just images`, `just deploy`, `just smoke`,
`just validate`, `just check-contract`, and `just clean`. Their relative paths
are resolved from `deploy/cluster/`.

All build/test recipes set `JAVA_HOME` to a detected JDK 17+ automatically.

## Contracts

Solidity sources live in `contracts/` and are built with Foundry (`forge`). See
`contracts/foundry.toml`. CI compiles, tests, and lints them; the Rust build
scripts run `forge build` to embed/locate artifacts.

## Testing

Beyond `cargo test`, two suites answer different questions.

**Chain semantics** — *does the chain mean the right thing?* One set of
scenario drivers (`crates/e2e/src/scenarios/`) covers L1↔L2 bridge round-trips
against a real anvil, nonce ordering and RPC liveness through
`eth_sendRawTransaction`, validator↔executor state parity (down to a
byte-level comparison of the two libmdbx databases), DA parity (blobs posted
to L1, re-executed, matched against the validator's root), and state-DB
integrity across normal operation and an unclean crash. They run on two
targets:

- **Target L** — `just test-e2e-local`: real service binaries, a 1-member Java
  Aeron Cluster sealer and a per-test media driver on one host. ~45 s; also
  the per-PR `chain-semantics-e2e` CI job.
- **Target C** — the `semantics` shard of `cluster-e2e.yml`: the same drivers
  against the real 12-node DinD cluster, via the `kardamom-semantics` binary.

Spec and current coverage: `docs/agents/chain-semantics-e2e-suite-spec.md`.

**Chaos** — *does the pipeline survive faults under load?*
`crates/chaos` (one test per shard in `tests/shards.rs`), run by the other
`cluster-e2e.yml` shards.
Failure modes and where each is verified: `docs/failure-modes.md`.
