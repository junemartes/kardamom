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
#
# The multi-node cluster under `deploy/cluster/` needs a different set of HOST
# tools (Vagrant + a VM provider, Ansible, Docker). `just cluster-bootstrap`
# installs those; `just cluster-doctor` checks them.

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

# Run the test suite across all features.
test:
    PATH="$(just java-shim):$PATH" JAVA_HOME="$(just java-home)" cargo test --workspace --all-targets --all-features --locked

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
    SHIM="$(just java-shim)"
    PATH="$SHIM:$PATH" JAVA_HOME="$(just java-home)" KARDAMOM_AERON_DIR="$DIR" \
        cargo test -p e2e --features full-pipeline-e2e \
            --test full_pipeline_e2e --locked -- --ignored --nocapture proof_of_pipeline
    PATH="$SHIM:$PATH" JAVA_HOME="$(just java-home)" KARDAMOM_AERON_DIR="$DIR" \
        cargo test -p e2e --features full-pipeline-e2e \
            --test multiprocess_e2e --locked -- --ignored --nocapture --test-threads=1 \
            multiprocess_e2e_signed_transfer_round_trip \
            multiprocess_e2e_deposit_round_trip \
            anvil_pipeline_e2e_l1_deposit_and_l2_round_trip
    # NOTE: multiprocess_quorum_e2e_recorder_quorum_and_redundancy is run
    # explicitly (not here): the on-quorum gate keys off the archive's recording
    # position, whose catch-up latency on a single shared host archive is timing
    # -sensitive. True 3-archive quorum + recorder-loss redundancy is validated
    # by the cluster-e2e workflow. Run the single-host variant by name with:
    #   just aeron-driver-up && KARDAMOM_AERON_DIR=/tmp/kardamom-aeron-local/dir \
    #     cargo test -p e2e --features full-pipeline-e2e --test multiprocess_e2e \
    #     -- --ignored --nocapture multiprocess_quorum

# ---------------------------------------------------------------------------
# Multi-node cluster (deploy/cluster) — HOST dependencies.
#
# These recipes install the tools needed on this machine to run
# `cd deploy/cluster && make up`: Vagrant + a VM provider, Ansible (+ the
# ansible.posix / community.docker collections), Docker with BuildKit, and the
# Nomad CLI (deploy/cluster/scripts/deploy.sh drives the cluster's Nomad API
# from the host). Nomad *servers/clients* and Consul run inside the VMs and
# are installed by Ansible, not here. See deploy/cluster/README.md.
# ---------------------------------------------------------------------------

# Host-side Nomad CLI version. Mirrors nomad_version in
# deploy/cluster/ansible/group_vars/all.yml (checked by check-contract.py).
NOMAD_VERSION := "1.9.5"

# Install everything the HOST needs for the deploy/cluster workflow.
cluster-bootstrap:
    #!/usr/bin/env bash
    set -euo pipefail
    # Pinned Nomad CLI matching the in-VM agents (deploy.sh needs it on PATH).
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
        echo ">> installing Vagrant + VirtualBox + Docker + Ansible via brew"
        # VirtualBox is the practical Vagrant provider on macOS (libvirt is
        # Linux-only). Casks may prompt for sudo / a kernel-extension approval.
        brew install --cask vagrant virtualbox docker || true
        brew install ansible
        install_nomad
        echo "   NOTE: VirtualBox support on Apple Silicon is limited; a Linux"
        echo "   host with libvirt/qemu is the best-supported environment."
        echo "   Start Docker Desktop before running 'make up'."
        ;;
    Linux)
        . /etc/os-release 2>/dev/null || true
        echo ">> installing cluster host deps (distro: ${ID:-unknown})"
        if command -v apt-get >/dev/null 2>&1; then
            sudo apt-get update
            # vagrant + libvirt/qemu provider, build deps for the
            # vagrant-libvirt plugin (ruby-dev/libvirt-dev/gcc/make),
            # ansible, and docker + buildx (BuildKit).
            sudo apt-get install -y \
                vagrant qemu-kvm libvirt-daemon-system libvirt-clients libvirt-dev \
                dnsmasq-base ebtables ruby-dev gcc make \
                ansible docker.io docker-buildx
        elif command -v dnf >/dev/null 2>&1; then
            sudo dnf install -y vagrant @virtualization libvirt libvirt-devel qemu-kvm \
                ansible docker gcc make ruby-devel
        elif command -v pacman >/dev/null 2>&1; then
            sudo pacman -S --needed --noconfirm vagrant libvirt qemu-full dnsmasq \
                ansible docker
        else
            echo "Unsupported Linux distro. Install manually: vagrant, libvirt+qemu," >&2
            echo "ansible, docker (see deploy/cluster/README.md)." >&2
            exit 1
        fi
        # vagrant-libvirt provider plugin (idempotent).
        if ! vagrant plugin list 2>/dev/null | grep -q vagrant-libvirt; then
            echo ">> installing vagrant-libvirt plugin"
            vagrant plugin install vagrant-libvirt
        fi
        install_nomad
        # Group membership so libvirt + docker work without sudo (needs re-login).
        for grp in libvirt kvm docker; do
            getent group "$grp" >/dev/null 2>&1 && sudo usermod -aG "$grp" "$USER" || true
        done
        sudo systemctl enable --now libvirtd docker 2>/dev/null || true
        echo "   NOTE: log out/in (or run 'newgrp docker') so the libvirt/kvm/docker"
        echo "   group memberships take effect."
        ;;
    *)
        echo "Unsupported platform: $os. See deploy/cluster/README.md." >&2
        exit 1
        ;;
    esac
    # Ansible Galaxy collections the playbook depends on.
    echo ">> installing ansible collections (ansible.posix, community.docker)"
    ansible-galaxy collection install ansible.posix community.docker
    echo ">> cluster-bootstrap complete. Verify with: just cluster-doctor"
    echo
    echo "   MANUAL STEP: 'make images' pushes over plain HTTP to the in-cluster"
    echo "   registry, so this HOST's Docker daemon must list it as insecure:"
    echo "       { \"insecure-registries\": [\"192.168.56.10:5000\"] }"
    echo "   (Linux: /etc/docker/daemon.json + restart docker; Docker Desktop:"
    echo "   Settings > Docker Engine.) 'just cluster-doctor' checks this."

# Check that the HOST has everything deploy/cluster needs.
cluster-doctor:
    #!/usr/bin/env bash
    set -uo pipefail
    rc=0
    have() { command -v "$1" >/dev/null 2>&1; }
    chk() { if have "$1"; then echo "  ok    $1 — $("$1" --version 2>&1 | head -1)"; else echo "  MISS  $1 ($2)"; rc=1; fi; }
    echo ">> deploy/cluster host dependencies:"
    chk vagrant "run 'just cluster-bootstrap'"
    chk ansible "run 'just cluster-bootstrap'"
    chk ansible-galaxy "ships with ansible"
    chk docker "run 'just cluster-bootstrap'"
    chk nomad "run 'just cluster-bootstrap' — deploy.sh drives the cluster API from the host"
    if have virsh || have VBoxManage; then
        echo "  ok    vm provider (libvirt or virtualbox)"
    else
        echo "  MISS  vm provider — install libvirt+qemu (Linux) or VirtualBox (macOS)"; rc=1
    fi
    for col in ansible.posix community.docker; do
        if ansible-galaxy collection list 2>/dev/null | grep -q "^$col "; then
            echo "  ok    ansible collection $col"
        else
            echo "  MISS  ansible collection $col — run 'just cluster-bootstrap'"; rc=1
        fi
    done
    # Pushing images needs the in-cluster registry allowed as insecure (HTTP)
    # in THIS host's Docker daemon. 192.168.56.10:5000 mirrors registry_host/
    # registry_port in deploy/cluster/ansible/group_vars/all.yml.
    if docker info >/dev/null 2>&1; then
        if docker info 2>/dev/null | grep -qE '^\s*192\.168\.56\.11:5000$'; then
            echo "  ok    docker insecure-registry 192.168.56.10:5000"
        else
            echo "  MISS  docker insecure-registry 192.168.56.10:5000 — add to the daemon's"
            echo "        insecure-registries and restart Docker (see cluster-bootstrap notes)"; rc=1
        fi
    else
        echo "  WARN  docker daemon not running — cannot check insecure-registries"
    fi
    # Smoke test (scripts/smoke.sh) prefers foundry's cast; non-fatal.
    if have cast; then
        echo "  ok    cast — $(cast --version 2>&1 | head -1)"
    else
        echo "  WARN  cast not found — 'make smoke' needs foundry (repo: 'just bootstrap')"
    fi
    if [[ "$rc" == "0" ]]; then
        echo ">> all good — 'cd deploy/cluster && make up'"
    else
        echo ">> missing dependencies; run 'just cluster-bootstrap'" >&2
    fi
    exit "$rc"
