# Kardamom

An Ethereum rollup framework. The workspace is a set of Rust crates — the
cluster services (`kardamom-ingress`, `kardamom-sequencer`, `kardamom-executor`,
`kardamom-sealer`, `kardamom-batcher`, `kardamom-da-watcher`) wired together over
Aeron, plus shared libraries (`kardamom-types`, `kardamom-log`, `kardamom-state`,
`kardamom-obs`) and tooling (`deployer`, `bench`) — plus Solidity contracts under
`contracts/`.

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

### Quick start

[`just`](https://github.com/casey/just) installs everything for your platform:

```sh
just bootstrap   # install the Aeron toolchain + Foundry
just check       # cargo check --workspace --all-features
```

`bootstrap` supports macOS (Homebrew) and Linux (apt / dnf / pacman). On other
systems, install the prerequisites manually (see below).

## Prerequisites (manual install)

If you'd rather not use `just bootstrap`:

| Platform        | Command |
| --------------- | ------- |
| macOS           | `brew install cmake pkg-config openjdk@17` (Xcode provides `clang`/`libclang`) |
| Debian / Ubuntu | `sudo apt-get install -y build-essential cmake pkg-config clang libclang-dev openjdk-17-jdk-headless` |
| Fedora / RHEL   | `sudo dnf install -y gcc gcc-c++ make cmake pkgconf-pkg-config clang clang-devel java-17-openjdk-devel` |
| Arch            | `sudo pacman -S --needed base-devel cmake pkgconf clang jdk17-openjdk` |

Plus Foundry on every platform:

```sh
curl -L https://foundry.paradigm.xyz | bash && foundryup
```

### JAVA_HOME

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

| Recipe            | What it does |
| ----------------- | ------------ |
| `just bootstrap`  | Install the Aeron native toolchain + Foundry for this platform. |
| `just check`      | `cargo check` the whole workspace with all features. |
| `just clippy`     | Clippy across all features with `-D warnings` (mirrors CI). |
| `just test`       | Run the test suite across all features. |
| `just check-aeron`| Targeted check that just the Aeron bindings compile. |

All build/test recipes set `JAVA_HOME` to a detected JDK 17+ automatically.

## Contracts

Solidity sources live in `contracts/` and are built with Foundry (`forge`). See
`contracts/foundry.toml`. CI compiles, tests, and lints them; the Rust build
scripts run `forge build` to embed/locate artifacts.
