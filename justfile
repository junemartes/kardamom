# Kardamom dev tasks. Run `just` to list recipes.
#
# The default Rust build is pure Rust and compiles anywhere with just a Rust
# toolchain. Two things require extra native tooling:
#   * the `aeron-live` feature (pulled in by `--all-features`) compiles the
#     bundled Aeron C sources via the rusteron crates — needs cmake, a C/C++
#     compiler, libclang (for bindgen), pkg-config, and a JDK 17+ (the Aeron
#     archive build runs a Gradle/SBE codegen step that requires JVM 17+);
#   * the `deployer` build script shells out to Foundry's `forge` to
#     compile the Solidity contracts.
# `just bootstrap` installs all of the above for your platform.
#
# The multi-node cluster under `deploy/cluster/` needs a different set of HOST
# tools (Ansible, Docker, OpenTofu, the Nomad CLI). `just cluster-bootstrap`
# installs those; `just cluster-doctor` checks them.

# The only execution-spec-tests fixture pin. When you bump it, also update
# `SPEC_ID` in crates/exec-core/src/block_env.rs and `FORK` in
# crates/exec-core/tests/eest_state.rs. Follow the fork-bump procedure in
# docs/agents/l1-client-suite-port-spec.md (hardfork-policy section).
EEST_TAG := "tests@v20.0.1"

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

# Install a cmake wrapper shim under ${TMPDIR:-/tmp}/kardamom-cmake-shim/ that
# forwards to the real cmake but injects -DJava_{JAVA,JAVAC,JAR,JAVADOC}_EXECUTABLE
# pointing at $JAVA_HOME's bin on configure calls (those with -B). Without this,
# the rusteron-archive build invokes cmake which on macOS hard-codes
# /usr/libexec/java_home (returns the system JDK 11) for FindJava — even with
# JAVA_HOME set in the env. The shim also ensures $JAVA_HOME/bin is at the
# front of PATH so the SBE-codegen Gradle step picks up JDK 17 too.
[private]
java-shim:
    #!/usr/bin/env bash
    set -euo pipefail
    JH="$(just java-home)"
    SHIM_DIR="${TMPDIR:-/tmp}/kardamom-cmake-shim"
    mkdir -p "$SHIM_DIR"
    REAL_CMAKE="$(command -v cmake)"
    [[ -x "$REAL_CMAKE" ]] || { echo "ERROR: cmake not found" >&2; exit 1; }
    cat > "$SHIM_DIR/cmake" <<EOF
    #!/usr/bin/env bash
    inject=0
    for a in "\$@"; do
      case "\$a" in
        --build|--install|--version) inject=0; break ;;
        -B) inject=1 ;;
      esac
    done
    if [[ "\$inject" == "1" && -n "\${JAVA_HOME:-}" && -x "\$JAVA_HOME/bin/java" ]]; then
      exec "$REAL_CMAKE" \\
        "-DJava_JAVA_EXECUTABLE=\$JAVA_HOME/bin/java" \\
        "-DJava_JAVAC_EXECUTABLE=\$JAVA_HOME/bin/javac" \\
        "-DJava_JAR_EXECUTABLE=\$JAVA_HOME/bin/jar" \\
        "-DJava_JAVADOC_EXECUTABLE=\$JAVA_HOME/bin/javadoc" \\
        "\$@"
    fi
    exec "$REAL_CMAKE" "\$@"
    EOF
    chmod +x "$SHIM_DIR/cmake"
    echo "$SHIM_DIR:$JH/bin"

# Type-check the whole workspace with every feature (incl. aeron-live).
check:
    PATH="$(just java-shim):$PATH" JAVA_HOME="$(just java-home)" cargo check --workspace --all-features --locked

# Lint with clippy across all features, mirroring CI (-D warnings).
clippy:
    PATH="$(just java-shim):$PATH" JAVA_HOME="$(just java-home)" cargo clippy --workspace --all-targets --all-features --locked -- -D warnings

# The judgment rules (R1, R5, R6, R7, R9, R10, R14, R15, R16) are not
# checked here. Read the diff against docs/STYLE.md for those.
# Mechanical style checks from docs/STYLE.md. Run before you open a PR.
style:
    #!/usr/bin/env bash
    set -uo pipefail
    export PATH="$(just java-shim):$PATH"
    export JAVA_HOME="$(just java-home)"
    failed=0
    step() {
        echo "==> $1"
        if ! "${@:2}"; then
            echo "FAILED: $1" >&2
            failed=1
        fi
    }
    # R11, R2 (too_many_lines at the threshold in clippy.toml), R8 (unreachable_pub).
    step "clippy pedantic" cargo clippy --workspace --all-targets --all-features --locked -- \
        -D warnings -W clippy::pedantic -D unreachable_pub
    step "rustfmt" cargo fmt --all -- --check
    # R9, R13, R6, R11: patterns that a lint cannot express. Test files are exempt.
    forbidden() {
        local hits
        hits="$(grep -rnE --include='*.rs' \
            -e 'debug_assert!' -e '\.max\(1\)' -e '\bdyn\b' -e 'allow\(clippy::too_many_arguments\)' \
            crates guest 2>/dev/null \
            | grep -vE '/tests?/|_tests?\.rs:|/tests\.rs:|/test_support' || true)"
        # `Box<` and `dyn` split over two lines by rustfmt: report the file and
        # the line of the `Box<`.
        local wrapped
        wrapped="$(grep -rnE --include='*.rs' -A1 -e 'Box<$' crates guest 2>/dev/null \
            | grep -E -B1 '^[^:]+-[0-9]+-\s*dyn ' \
            | grep -E ':[0-9]+:' \
            | grep -vE '/tests?/|_tests?\.rs:|/tests\.rs:|/test_support' || true)"
        hits="${hits}${wrapped:+$'\n'$wrapped}"
        if [ -n "$hits" ]; then
            echo "$hits"
            return 1
        fi
    }
    step "forbidden patterns (debug_assert!, .max(1), dyn, allow(too_many_arguments))" forbidden
    if [ "$failed" -ne 0 ]; then
        echo "style check failed. See docs/STYLE.md." >&2
        exit 1
    fi
    echo "style check passed."

# Run the test suite across all features.
test:
    PATH="$(just java-shim):$PATH" JAVA_HOME="$(just java-home)" cargo test --workspace --all-targets --all-features --locked

# EEST state-test conformance (docs/agents/l1-client-suite-port-spec.md W1):
# fetch the pinned fixture release, run the runner against the Osaka posts.
eest:
    #!/usr/bin/env bash
    set -euo pipefail
    fixtures="$(just eest-fixtures | tail -1)"
    KARDAMOM_EEST_FIXTURES="${fixtures}" \
        cargo test -p kardamom-exec-core --release --test eest_state -- --ignored --nocapture

# The pinned execution-spec-tests release. CI reads it for its cache key.
eest-tag:
    @echo "{{ EEST_TAG }}"

# It prints the fixture directory as the last line, and skips the download
# when the files are already there.
# Fetch the pinned EEST fixtures into <dest-root>/<tag>/ (default ~/.cache/kardamom/eest).
eest-fixtures dest_root=(env('HOME') / ".cache/kardamom/eest"):
    #!/usr/bin/env bash
    set -euo pipefail
    dest="{{ dest_root }}/{{ EEST_TAG }}"
    if [[ -d "${dest}/fixtures" ]]; then
        echo "eest fixtures {{ EEST_TAG }} already present" >&2
        echo "${dest}/fixtures"
        exit 0
    fi
    # URL-encode '@' in the release-asset path.
    tag="{{ EEST_TAG }}"
    url="https://github.com/ethereum/execution-specs/releases/download/${tag/@/%40}/fixtures.tar.gz"
    mkdir -p "${dest}"
    echo "fetching ${url}" >&2
    curl -fsSL --retry 3 "${url}" -o "${dest}/fixtures.tar.gz"
    # Keep only state_tests and release metadata. The blockchain and engine
    # formats need a header chain and an Engine API. Kardamom does not have
    # these (see the spec's non-goals). The full download is about 8.1 GB.
    # The filtered set is about 1.5 GB.
    tar -xzf "${dest}/fixtures.tar.gz" -C "${dest}" \
        "fixtures/state_tests" "fixtures/.meta"
    rm "${dest}/fixtures.tar.gz"
    [[ -d "${dest}/fixtures" ]] || {
        echo "unexpected tarball layout under ${dest}" >&2
        exit 1
    }
    echo "${dest}/fixtures"

# It runs the three DHAT harnesses and fails when allocs/op or bytes/op
# exceed their ceilings. Wall time prints for reference only: it depends on
# the machine.
# DHAT allocation ceilings (perf/alloc-baselines.env).
alloc-gate:
    #!/usr/bin/env bash
    set -euo pipefail
    source perf/alloc-baselines.env
    run() { # <name> <dir> <test> <env...>
        local name=$1 dir=$2 test=$3; shift 3
        local raw="/tmp/alloc-gate-$name.log"
        # Keep the full cargo output, including stderr. A compile or runtime
        # failure must show in the log.
        if ! (cd "$dir" && env "$@" cargo test --test "$test" --release -- --ignored --nocapture) >"$raw" 2>&1; then
            # The last 40 lines of a panicking test are its backtrace, so the
            # panic itself scrolls away. Print the failure lines first.
            echo "== $name: HARNESS FAILED (cargo test exit != 0); the failure lines:"
            grep -nE "panicked at|assertion|^error(\[|:)|^thread .* panicked|FAILED" "$raw" | head -20 || true
            echo "== $name: last 40 lines:"
            tail -40 "$raw"
            return 1
        fi
        local out
        out=$(grep -E "allocs/(tx|op)|bytes/(tx|op)|wall/(tx|op)" "$raw" || true)
        if [[ -z "$out" ]]; then
            echo "== $name: NO MEASUREMENT LINES in harness output; last 40 lines:"
            tail -40 "$raw"
            return 1
        fi
        echo "== $name"; echo "$out"
        local allocs bytes
        allocs=$(echo "$out" | grep -E "allocs" | grep -oE "[0-9]+\.?[0-9]*" | head -1)
        bytes=$(echo "$out" | grep -E "bytes" | grep -oE "[0-9]+" | head -1)
        local max_a_var="${name^^}_MAX_ALLOCS" max_b_var="${name^^}_MAX_BYTES"
        local max_a=${!max_a_var} max_b=${!max_b_var}
        awk -v a="$allocs" -v ma="$max_a" -v b="$bytes" -v mb="$max_b" -v n="$name" 'BEGIN {
            bad = 0
            if (a+0 > ma+0) { printf "ALLOC REGRESSION: %s %.2f allocs/op > ceiling %.2f\n", n, a, ma; bad = 1 }
            if (b+0 > mb+0) { printf "ALLOC REGRESSION: %s %d bytes/op > ceiling %d\n", n, b, mb; bad = 1 }
            exit bad
        }'
    }
    run engine    crates/bench     alloc_profile         KARDAMOM_PROFILE_OPS=mix
    run sequencer crates/sequencer alloc_profile
    run ingress   crates/bench     alloc_profile_ingress
    echo "alloc gate: PASS (ceilings: perf/alloc-baselines.env)"

# Run it after you change bench-contracts/src/BenchDefi.sol.
# Regenerate crates/bench/src/load/defi_bytecode.rs from the compiled artifacts.
bench-embed:
    #!/usr/bin/env bash
    set -euo pipefail
    cd bench-contracts
    forge build >/dev/null
    python3 - <<'EMBED'
    import json
    names = ["SwapPool", "Vault", "Clob"]
    with open("../crates/bench/src/load/defi_bytecode.rs", "w") as f:
        f.write("// This file is generated by `just bench-embed`. Do not edit it.\n")
        f.write("// The source of truth is bench-contracts/src/BenchDefi.sol.\n\n")
        for n in names:
            j = json.load(open(f"out/BenchDefi.sol/{n}.json"))
            code = j["bytecode"]["object"].removeprefix("0x")
            f.write(f'pub const {n.upper()}_CREATION_HEX: &str = "{code}";\n')
    print("ok")
    EMBED

# It mirrors the layout a checkout has: the service and operator binaries
# under target/release, the shard test executable as kardamom-chaos-shards,
# the Aeron shared libraries under their rusteron build directories, and the
# sealer jar at its Gradle output path. A shard runner unpacks this over its
# checkout and runs the cluster recipes with KARDAMOM_STAGED=1.
# Stage everything a cluster-e2e shard runner needs into one directory.
stage-dist dist:
    #!/usr/bin/env bash
    set -euo pipefail
    dist="{{ dist }}"
    rel=target/release
    mkdir -p "$dist/$rel/build" "$dist/cluster/sealer-service/service/build/libs"
    # The services the images wrap (the state mirror included), the settlement
    # deployer and the semantics runner the stages spawn, the operator binary,
    # and the archive tool the archive-corruption case runs on the host.
    for bin in ingress sequencer executor validator da-watcher batcher state-mirror reconstruct deploy semantics cluster archive-rereplicate; do
        cp "$rel/kardamom-$bin" "$dist/$rel/"
    done
    # The shard test executable carries a build hash; the newest one is this build's.
    shards=$(ls -t "$rel"/deps/shards-* | grep -v '\.d$' | head -n 1)
    cp "$shards" "$dist/$rel/kardamom-chaos-shards"
    for lib in "$rel"/build/rusteron-archive-*/out/build/lib; do
        build=$(basename "$(dirname "$(dirname "$(dirname "$lib")")")")
        mkdir -p "$dist/$rel/build/$build/out/build/lib"
        cp "$lib"/libaeron*.so "$dist/$rel/build/$build/out/build/lib/"
    done
    cp cluster/sealer-service/service/build/libs/kardamom-cluster-node.jar "$dist/cluster/sealer-service/service/build/libs/"
    find "$dist" -type f | sort

# Targeted check that just the Aeron bindings compile.
check-aeron:
    PATH="$(just java-shim):$PATH" JAVA_HOME="$(just java-home)" cargo check -p kardamom-log --features aeron-live --locked

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
        -Daeron.archive.record.checksum=io.aeron.archive.checksum.Crc32 \
        -Daeron.archive.replay.checksum=io.aeron.archive.checksum.Crc32 \
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

# Ensure the aeron-all jar is cached (the e2e harness spawns its own
# ArchivingMediaDrivers from it; `aeron-driver-up` shares the same cache).
aeron-jar:
    #!/usr/bin/env bash
    set -euo pipefail
    JAR={{AERON_LOCAL_ROOT}}/aeron-all-{{AERON_JAR_VERSION}}.jar
    mkdir -p {{AERON_LOCAL_ROOT}}
    if [[ ! -f "$JAR" ]]; then
        echo ">> downloading aeron-all-{{AERON_JAR_VERSION}}.jar"
        curl -fsSL "https://repo1.maven.org/maven2/io/aeron/aeron-all/{{AERON_JAR_VERSION}}/aeron-all-{{AERON_JAR_VERSION}}.jar" -o "$JAR"
    fi
    echo "$JAR"

# Build the Java Aeron Cluster sealer jar (gradle :service:shadowJar). The
# e2e harness auto-discovers the output path; override with
# KARDAMOM_CLUSTER_JAR.
cluster-jar:
    #!/usr/bin/env bash
    set -euo pipefail
    JH="$(just java-home)"
    cd cluster/sealer-service
    JAVA_HOME="$JH" ./gradlew :service:shadowJar -q
    echo "cluster/sealer-service/service/build/libs/kardamom-cluster-node.jar"

# Run the chain-semantics e2e suite (Target L) locally: real service
# binaries + a 1-member Java Aeron Cluster sealer + per-test media drivers,
# driven through JSON-RPC/metrics only. Spec:
# docs/agents/chain-semantics-e2e-suite-spec.md. Each test brings up its own
# stack (2 JVMs + 4 service processes), so parallelism is capped at 2.
test-e2e-local: aeron-jar cluster-jar
    #!/usr/bin/env bash
    set -euo pipefail
    SHIM="$(just java-shim)"
    export PATH="$SHIM:$PATH" JAVA_HOME="$(just java-home)"
    cargo build --bins --locked \
        -p kardamom-ingress -p kardamom-sequencer -p kardamom-executor \
        -p kardamom-validator -p kardamom-state -p kardamom-da-watcher \
        -p kardamom-reconstruct
    cargo test -p e2e --features full-pipeline-e2e --test chain_semantics \
        --locked -- --ignored --nocapture --test-threads=2 --skip s14_
    # S14 runs TWO full stacks (4 JVMs + 10 service processes) — the
    # heaviest scenario by far. Serialized after the rest so its load never
    # shares the host with another stack's timing-sensitive assertions
    # (S10d's origin-freeze drain in particular).
    cargo test -p e2e --features full-pipeline-e2e --test chain_semantics \
        --locked s14_ -- --ignored --nocapture --test-threads=1

# ---------------------------------------------------------------------------
# Multi-node cluster (deploy/cluster) — HOST dependencies.
#
# These recipes install the tools needed on this machine to run
# `cd deploy/cluster && just container-up`: Ansible (+ the ansible.posix /
# community.docker collections), Docker with BuildKit, OpenTofu, and the
# Nomad CLI (the Ansible workload role uses it to compile HCL locally). Nomad
# *servers/clients* and Consul run inside the node containers and are
# installed by Ansible, not here. See deploy/cluster/README.md.
# ---------------------------------------------------------------------------

# Host-side Nomad CLI version. Mirrors nomad_version in
# deploy/cluster/ansible/group_vars/all.yml (checked by ansible/contract.yml).
NOMAD_VERSION := "1.9.5"

# Install everything the HOST needs for the deploy/cluster workflow.
cluster-bootstrap:
    #!/usr/bin/env bash
    set -euo pipefail
    # Pinned Nomad CLI matching the in-VM agents (Ansible deployment needs it on PATH).
    install_nomad() {
        command -v nomad >/dev/null 2>&1 && return 0
        local ver="{{NOMAD_VERSION}}" os arch zip
        os="$(uname -s | tr '[:upper:]' '[:lower:]')"
        case "$(uname -m)" in
            arm64|aarch64) arch=arm64 ;;
            x86_64)        arch=amd64 ;;
            *) echo "   WARN: unknown arch $(uname -m); install nomad manually" >&2; return 0 ;;
        esac
        zip="nomad_${ver}_${os}_${arch}.zip"
        echo ">> installing nomad ${ver} CLI to /usr/local/bin"
        curl -fsSL "https://releases.hashicorp.com/nomad/${ver}/${zip}" -o "/tmp/${zip}"
        if [ -w /usr/local/bin ]; then unzip -o "/tmp/${zip}" -d /usr/local/bin
        else sudo unzip -o "/tmp/${zip}" -d /usr/local/bin; fi
        rm -f "/tmp/${zip}"
    }
    os="$(uname -s)"
    case "$os" in
    Darwin)
        command -v brew >/dev/null 2>&1 || { echo "Homebrew required — https://brew.sh" >&2; exit 1; }
        echo ">> installing Docker + Ansible + OpenTofu via brew"
        brew install --cask docker || true
        brew install ansible opentofu jq
        install_nomad
        echo "   NOTE: the container cluster needs a Linux Docker daemon with"
        echo "   privileged containers; Docker Desktop's Linux VM serves that."
        ;;
    Linux)
        . /etc/os-release 2>/dev/null || true
        echo ">> installing cluster host deps (distro: ${ID:-unknown})"
        if command -v apt-get >/dev/null 2>&1; then
            sudo apt-get update
            sudo apt-get install -y ansible docker.io docker-buildx jq
        elif command -v dnf >/dev/null 2>&1; then
            sudo dnf install -y ansible docker jq
        elif command -v pacman >/dev/null 2>&1; then
            sudo pacman -S --needed --noconfirm ansible docker
        else
            echo "Unsupported Linux distro. Install manually: ansible, docker," >&2
            echo "opentofu (see deploy/cluster/README.md)." >&2
            exit 1
        fi
        install_nomad
        command -v tofu >/dev/null 2>&1 || echo "   NOTE: install OpenTofu 1.12.6 (https://opentofu.org/docs/intro/install/)"
        # Group membership so docker works without sudo (needs re-login).
        getent group docker >/dev/null 2>&1 && sudo usermod -aG docker "$USER" || true
        sudo systemctl enable --now docker 2>/dev/null || true
        echo "   NOTE: log out/in (or run 'newgrp docker') so the docker group"
        echo "   membership takes effect."
        ;;
    *)
        echo "Unsupported platform: $os. See deploy/cluster/README.md." >&2
        exit 1
        ;;
    esac
    # Ansible Galaxy collections the playbook depends on.
    echo ">> installing ansible collections (ansible.posix, community.docker)"
    ansible-galaxy collection install ansible.posix community.docker community.general
    echo ">> cluster-bootstrap complete. Verify with: just cluster-doctor"
    echo
    echo "   Images are pushed from inside the control node (REGISTRY_PUSH_NODE),"
    echo "   where the registry name registry.service.consul resolves. This host's"
    echo "   Docker daemon needs no insecure-registry entry."

# Check that the HOST has everything deploy/cluster needs.
cluster-doctor:
    #!/usr/bin/env bash
    set -uo pipefail
    rc=0
    have() { command -v "$1" >/dev/null 2>&1; }
    chk() { if have "$1"; then echo "  ok    $1 — $("$1" --version 2>&1 | head -1)"; else echo "  MISS  $1 ($2)"; rc=1; fi; }
    echo ">> deploy/cluster host dependencies:"
    chk ansible "run 'just cluster-bootstrap'"
    chk ansible-galaxy "ships with ansible"
    chk jq "run 'just cluster-bootstrap'"
    chk docker "run 'just cluster-bootstrap'"
    chk nomad "run 'just cluster-bootstrap' — Ansible uses Nomad to compile job specs"
    chk tofu "install OpenTofu 1.12.6 — terraform/containers creates the node containers"
    for col in ansible.posix community.docker; do
        if ansible-galaxy collection list 2>/dev/null | grep -q "^$col "; then
            echo "  ok    ansible collection $col"
        else
            echo "  MISS  ansible collection $col — run 'just cluster-bootstrap'"; rc=1
        fi
    done
    # Images are pushed from inside the control node, so this host's daemon
    # needs no insecure-registry entry; it only has to run.
    if docker info >/dev/null 2>&1; then
        echo "  ok    docker daemon running"
    else
        echo "  WARN  docker daemon not running"
    fi
    if [[ "$rc" == "0" ]]; then
        echo ">> all good — 'cd deploy/cluster && just container-up'"
    else
        echo ">> missing dependencies; run 'just cluster-bootstrap'" >&2
    fi
    exit "$rc"
