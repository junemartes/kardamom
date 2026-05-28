# Kardamom dev tasks. Run `just` to list recipes.
#
# The default Rust build is pure Rust and compiles anywhere with just a Rust
# toolchain. Two things require extra native tooling:
#   * the `aeron-live` feature (pulled in by `--all-features`) compiles the
#     bundled Aeron C sources via the rusteron crates — needs cmake, a C/C++
#     compiler, libclang (for bindgen), pkg-config, and a JDK 17+ (the Aeron
#     archive build runs a Gradle/SBE codegen step that requires JVM 17+);
#   * the `deployer`/`node` build scripts shell out to Foundry's `forge` to
#     compile the Solidity contracts.
# `just bootstrap` installs all of the above for your platform.

_default:
    @just --list

# Install every native dependency needed to build the full workspace
# (Aeron toolchain + Foundry) for the current platform.
bootstrap:
    #!/usr/bin/env bash
    set -euo pipefail
    os="$(uname -s)"
    case "$os" in
    Darwin)
        if ! command -v brew >/dev/null 2>&1; then
            echo "Homebrew not found — install it from https://brew.sh first." >&2
            exit 1
        fi
        echo ">> installing Aeron build toolchain via brew"
        # cmake + pkg-config drive the Aeron C build; openjdk@17 is keg-only so
        # it installs without sudo (the temurin cask needs root). Xcode's clang
        # provides libclang for bindgen; fall back to brew llvm only if absent.
        brew install cmake pkg-config openjdk@17
        if [[ -z "$(xcode-select -p 2>/dev/null)" ]]; then
            echo ">> installing Xcode Command Line Tools"; xcode-select --install || true
        fi
        if ! ls "$(xcode-select -p 2>/dev/null)"/Toolchains/*/usr/lib/libclang.dylib >/dev/null 2>&1 \
            && ! brew list --versions llvm >/dev/null 2>&1; then
            echo ">> no libclang via Xcode — installing llvm"; brew install llvm
        fi
        ;;
    Linux)
        # Detect the package manager from /etc/os-release.
        . /etc/os-release 2>/dev/null || true
        like="${ID:-} ${ID_LIKE:-}"
        echo ">> installing Aeron build toolchain (distro: ${ID:-unknown})"
        if command -v apt-get >/dev/null 2>&1; then
            sudo apt-get update
            sudo apt-get install -y build-essential cmake pkg-config clang libclang-dev openjdk-17-jdk-headless curl
        elif command -v dnf >/dev/null 2>&1; then
            sudo dnf install -y gcc gcc-c++ make cmake pkgconf-pkg-config clang clang-devel java-17-openjdk-devel curl
        elif command -v pacman >/dev/null 2>&1; then
            sudo pacman -S --needed --noconfirm base-devel cmake pkgconf clang jdk17-openjdk curl
        else
            echo "Unsupported Linux distro. Install manually: a C/C++ compiler, cmake," >&2
            echo "pkg-config, clang+libclang, and a JDK 17+ (see README.md)." >&2
            exit 1
        fi
        ;;
    *)
        echo "Unsupported platform: $os. See README.md for manual prerequisites." >&2
        exit 1
        ;;
    esac
    # Foundry (forge) — required by the deployer/node build scripts.
    if ! command -v forge >/dev/null 2>&1; then
        echo ">> installing Foundry"
        curl -L https://foundry.paradigm.xyz | bash
        "${XDG_CONFIG_HOME:-$HOME/.foundry}/bin/foundryup" 2>/dev/null \
            || "$HOME/.foundry/bin/foundryup"
    else
        echo ">> forge already present: $(forge --version | head -1)"
    fi
    echo ">> bootstrap complete. Verify with: just check"

# Resolve the home of a JDK 17+ for the current platform. Used by the recipes
# below so the Aeron archive build finds a suitable JVM regardless of what the
# system default is (macOS /usr/libexec/java_home tends to pick an older JDK,
# and keg-only openjdk@17 isn't registered there). Honors a correct JAVA_HOME.
[private]
java-home:
    #!/usr/bin/env bash
    set -euo pipefail
    ver() { "$1" -version 2>&1 | head -1 | grep -oE '[0-9]+' | head -1; }
    if [[ -n "${JAVA_HOME:-}" && -x "${JAVA_HOME}/bin/java" ]] \
        && [[ "$(ver "${JAVA_HOME}/bin/java")" -ge 17 ]]; then
        echo "$JAVA_HOME"; exit 0
    fi
    case "$(uname -s)" in
    Darwin)
        home="$(brew --prefix openjdk@17 2>/dev/null)/libexec/openjdk.jdk/Contents/Home"
        [[ -x "$home/bin/java" ]] && { echo "$home"; exit 0; } ;;
    Linux)
        for d in /usr/lib/jvm/*; do
            [[ -x "$d/bin/java" ]] || continue
            [[ "$(ver "$d/bin/java")" -ge 17 ]] && { echo "$d"; exit 0; }
        done ;;
    esac
    echo "ERROR: no JDK 17+ found. Run 'just bootstrap'." >&2
    exit 1

# Type-check the whole workspace with every feature (incl. aeron-live).
check:
    JAVA_HOME="$(just java-home)" cargo check --workspace --all-features --locked

# Lint with clippy across all features, mirroring CI (-D warnings).
clippy:
    JAVA_HOME="$(just java-home)" cargo clippy --workspace --all-targets --all-features --locked -- -D warnings

# Run the test suite across all features.
test:
    JAVA_HOME="$(just java-home)" cargo test --workspace --all-targets --all-features --locked

# Targeted check that just the Aeron bindings compile.
check-aeron:
    JAVA_HOME="$(just java-home)" cargo check -p kardamom-log --features aeron-live --locked

# Path where the host-native Aeron Media Driver writes cnc.dat + archive.
# Used by `aeron-driver-up` / `aeron-driver-down` / `test-e2e-local`. Lives
# under /tmp so the layout matches what `crates/log/src/testing.rs` would
# bind-mount when running via Docker.
AERON_LOCAL_ROOT := "/tmp/kardamom-aeron-local"
AERON_JAR_VERSION := "1.45.0"

# Launch the Aeron Archive Media Driver natively on this host (no Docker).
# Used on macOS where Docker's virtiofs breaks host↔container shared-memory
# `cnc.dat` semantics — the natively-running driver shares mmap pages with
# host clients directly, so the e2e tests actually work.
#
# Downloads aeron-all.jar once (cached in {{AERON_LOCAL_ROOT}}). The MD
# runs in the background; its PID lands in `md.pid` for tear-down.
aeron-driver-up:
    #!/usr/bin/env bash
    set -euo pipefail
    JAR={{AERON_LOCAL_ROOT}}/aeron-all-{{AERON_JAR_VERSION}}.jar
    DIR={{AERON_LOCAL_ROOT}}/dir
    ARCHIVE={{AERON_LOCAL_ROOT}}/archive
    PID_FILE={{AERON_LOCAL_ROOT}}/md.pid
    mkdir -p {{AERON_LOCAL_ROOT}}
    if [[ ! -f "$JAR" ]]; then
        echo ">> downloading aeron-all-{{AERON_JAR_VERSION}}.jar"
        curl -fsSL "https://repo1.maven.org/maven2/io/aeron/aeron-all/{{AERON_JAR_VERSION}}/aeron-all-{{AERON_JAR_VERSION}}.jar" -o "$JAR"
    fi
    if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
        echo "MD already running (pid $(cat "$PID_FILE")); reusing"
        echo "KARDAMOM_AERON_DIR=$DIR"
        exit 0
    fi
    rm -rf "$DIR" "$ARCHIVE"
    mkdir -p "$DIR" "$ARCHIVE"
    JH="$(just java-home)"
    nohup "$JH/bin/java" \
        --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
        --add-opens java.base/java.util.zip=ALL-UNNAMED \
        --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
        -Daeron.dir="$DIR" \
        -Daeron.archive.dir="$ARCHIVE" \
        -Daeron.term.buffer.length=4194304 \
        -Daeron.ipc.term.buffer.length=4194304 \
        -Daeron.archive.control.channel=aeron:udp?endpoint=127.0.0.1:8010 \
        -Daeron.archive.control.response.channel=aeron:udp?endpoint=127.0.0.1:8011 \
        -Daeron.archive.replication.channel=aeron:udp?endpoint=127.0.0.1:8021 \
        -cp "$JAR" \
        io.aeron.archive.ArchivingMediaDriver \
        > {{AERON_LOCAL_ROOT}}/md.log 2>&1 &
    echo $! > "$PID_FILE"
    for i in $(seq 1 300); do
        if [[ -f "$DIR/cnc.dat" && -f "$ARCHIVE/archive.catalog" ]]; then
            sleep 0.2
            echo ">> MD ready (pid $(cat "$PID_FILE"))"
            echo "   KARDAMOM_AERON_DIR=$DIR"
            exit 0
        fi
        if ! kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
            echo "MD died during startup; see {{AERON_LOCAL_ROOT}}/md.log" >&2
            exit 1
        fi
        sleep 0.1
    done
    echo "MD did not become ready within 30 s; see {{AERON_LOCAL_ROOT}}/md.log" >&2
    exit 1

# Stop the host-native Media Driver started by `aeron-driver-up`.
aeron-driver-down:
    #!/usr/bin/env bash
    set -euo pipefail
    PID_FILE={{AERON_LOCAL_ROOT}}/md.pid
    if [[ -f "$PID_FILE" ]]; then
        PID="$(cat "$PID_FILE")"
        if kill -0 "$PID" 2>/dev/null; then
            kill -TERM "$PID"
            for i in $(seq 1 30); do
                kill -0 "$PID" 2>/dev/null || break
                sleep 0.1
            done
            kill -KILL "$PID" 2>/dev/null || true
        fi
        rm -f "$PID_FILE"
    fi
    echo ">> MD stopped"

# Run the e2e tests locally against a host-native Aeron Media Driver. Set
# the MD up first, run the in-process + multi-process variants, then tear
# the MD down. Use this on macOS where the Dockerised MD path doesn't work.
test-e2e-local: aeron-driver-up
    #!/usr/bin/env bash
    set -euo pipefail
    DIR={{AERON_LOCAL_ROOT}}/dir
    trap 'just aeron-driver-down' EXIT
    JAVA_HOME="$(just java-home)" KARDAMOM_AERON_DIR="$DIR" \
        cargo test -p e2e --features full-pipeline-e2e \
            --test full_pipeline_e2e --locked -- --ignored --nocapture proof_of_pipeline
    JAVA_HOME="$(just java-home)" KARDAMOM_AERON_DIR="$DIR" \
        cargo test -p e2e --features full-pipeline-e2e \
            --test multiprocess_e2e --locked -- --ignored --nocapture --test-threads=1 \
            multiprocess_e2e_signed_transfer_round_trip multiprocess_e2e_deposit_round_trip
