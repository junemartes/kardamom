# Install the build toolchain needed to compile the Aeron C sources that the
# `rusteron-client` / `rusteron-archive` crates build from source (enabled by
# the `aeron-live` feature on kardamom-log). There is no prebuilt "Aeron C
# library" package — rusteron compiles the bundled C sources via cmake + bindgen,
# so what we install here is the build tooling those crates require.
install-aeron:
    #!/usr/bin/env bash
    set -euo pipefail
    if [[ "$(uname)" != "Darwin" ]]; then
        echo "This recipe targets macOS. On Linux install: cmake, clang/libclang, pkg-config." >&2
        exit 1
    fi
    if ! command -v brew >/dev/null 2>&1; then
        echo "Homebrew not found — install it from https://brew.sh first." >&2
        exit 1
    fi
    # cmake: drives the Aeron C build. The only piece typically missing.
    if ! command -v cmake >/dev/null 2>&1; then
        echo ">> installing cmake"
        brew install cmake
    else
        echo ">> cmake already present: $(cmake --version | head -1)"
    fi
    # JDK 17+: the Aeron *archive* build runs a Gradle/SBE codegen step that
    # requires JVM 17 or later. The keg-only openjdk@17 formula installs without
    # sudo (the temurin cask does not). check-aeron points JAVA_HOME at it.
    if ! brew list --versions openjdk@17 >/dev/null 2>&1; then
        echo ">> installing openjdk@17"
        brew install openjdk@17
    else
        echo ">> openjdk@17 already present"
    fi
    # libclang: required by bindgen. A full Xcode install already provides it;
    # only fall back to Homebrew llvm when no libclang is discoverable.
    if [[ -z "$(xcode-select -p 2>/dev/null)" ]]; then
        echo ">> no Xcode toolchain found — installing Command Line Tools"
        xcode-select --install || true
    fi
    if ! ls "$(xcode-select -p)"/Toolchains/*/usr/lib/libclang.dylib >/dev/null 2>&1 \
        && ! brew list --versions llvm >/dev/null 2>&1; then
        echo ">> no libclang found via Xcode — installing llvm"
        brew install llvm
        echo ">> if bindgen still can't find libclang, export:"
        echo "   LIBCLANG_PATH=$(brew --prefix llvm)/lib"
    fi
    echo ">> toolchain ready. Verify the build with: just check-aeron"

# Confirm the `aeron-live` feature compiles end-to-end (compiles Aeron C + bindings).
# JAVA_HOME is derived from the keg-only openjdk@17 (works on both Intel and
# Apple-silicon brew prefixes) so the Aeron archive build finds a JVM 17+.
check-aeron:
    JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home" \
        cargo check -p kardamom-log --features aeron-live
