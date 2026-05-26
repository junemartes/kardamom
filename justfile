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
