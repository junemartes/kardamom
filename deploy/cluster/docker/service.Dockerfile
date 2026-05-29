# syntax=docker/dockerfile:1.7
#
# =============================================================================
# service.Dockerfile — multi-stage build for ONE kardamom service binary.
# =============================================================================
#
# Build context is the WORKSPACE ROOT. The cluster Makefile invokes:
#
#     docker build -f deploy/cluster/docker/service.Dockerfile \
#         --build-arg BIN=kardamom-<svc> -t <registry>/kardamom-<svc>:<tag> <repo-root>
#
# Produces a slim runtime image whose ENTRYPOINT is the requested binary
# (kardamom-ingress, kardamom-sequencer, kardamom-executor, kardamom-sealer,
# kardamom-da-watcher, kardamom-batcher). Nomad supplies all CLI args/config.
#
# -----------------------------------------------------------------------------
# BuildKit is REQUIRED. The builder stage uses `--mount=type=cache` for the
# cargo registry and target dir, and the `# syntax=` directive above. Build with:
#
#     DOCKER_BUILDKIT=1 docker build ...
#
# (or `docker buildx build ...`). Without BuildKit the cache mounts and the
# syntax directive are ignored/error.
# -----------------------------------------------------------------------------
#
# =============================================================================
# !!!  REQUIRED-CHANGE / IMPORTANT UNCERTAINTY — READ BEFORE BUILDING  !!!
# =============================================================================
#
# The task brief asked us to build with the feature that "enables real Aeron"
# (referred to in the repo's README/justfile/source comments as the
# `aeron-live` feature of `kardamom-log`).
#
# INVESTIGATION RESULT: **there is no `aeron-live` cargo feature anywhere in the
# workspace.** As of this writing:
#
#   * `crates/log/Cargo.toml` declares NO `aeron-live` feature. Its
#     `[features]` are only `default`, `testing`, `docker-e2e`.
#   * `rusteron-client` and `rusteron-archive` (the crates that compile the
#     bundled Aeron C/Java sources — the things that need cmake/clang/JDK17)
#     are declared as **non-optional, unconditional dependencies** of
#     `kardamom-log`, and `pub mod aeron_live;` in `crates/log/src/lib.rs` is
#     compiled unconditionally (no `#[cfg(feature = "aeron-live")]` gate exists
#     anywhere in the source tree — only stale doc-comments/test headers
#     mention the name).
#   * None of the service crates (ingress/sequencer/executor/sealer/
#     da_watcher/batcher) expose an `aeron-live` passthrough feature either.
#
# CONSEQUENCE: real Aeron is ALWAYS compiled into every service binary by a
# plain `cargo build`. There is nothing to pass on the `--features` line to
# "turn on" real Aeron; passing `--features aeron-live` would FAIL with
# "none of the selected packages contains these features: aeron-live".
#
# WHAT THIS DOCKERFILE DOES: it builds with NO extra feature flag (real Aeron is
# already in), which is the correct, working invocation today. The native
# toolchain (cmake/clang/libclang/pkg-config/JDK17 + Foundry) is installed
# regardless, because the unconditional rusteron + the deployer/node build
# scripts need it.
#
# >>> REQUIRED FOLLOW-UP (if/when the codebase is refactored to gate real Aeron
# >>> behind a feature, as the docs imply was intended):
# >>>
# >>>   1. Add to `crates/log/Cargo.toml`:
# >>>          [features]
# >>>          aeron-live = ["dep:rusteron-client", "dep:rusteron-archive"]
# >>>      (and make those deps `optional = true`), and `#[cfg]`-gate
# >>>      `pub mod aeron_live;` + the publishers/subscribers/recorder.
# >>>   2. Add a passthrough to EACH service crate's `[features]`:
# >>>          aeron-live = ["kardamom-log/aeron-live"]
# >>>   3. Then change the build line below to:
# >>>          cargo build --release --locked --bin ${BIN} --features aeron-live
# >>>
# >>> The placeholder ARG below (`AERON_FEATURE`) is wired so an operator can
# >>> flip it on WITHOUT editing the RUN line, once (2) above is done:
# >>>          docker build --build-arg AERON_FEATURE="--features aeron-live" ...
# >>> It defaults to empty (the only invocation that works today).
# =============================================================================


# -----------------------------------------------------------------------------
# .dockerignore note
# -----------------------------------------------------------------------------
# A root `.dockerignore` cannot be authored from inside docker/ (it must live at
# the build context root = repo root). Without it, `target/`, `.git/`, and
# `.claude/` are sent to the daemon and slow every build. See the ready-to-copy
# `deploy/cluster/docker/dockerignore.example` — copy it to `<repo-root>/.dockerignore`:
#
#     cp deploy/cluster/docker/dockerignore.example .dockerignore
# -----------------------------------------------------------------------------


# =============================================================================
# Stage 1: builder
# =============================================================================
# rust:1-bookworm pins the latest 1.x toolchain on Debian bookworm. Edition 2024
# requires Rust >= 1.85; rust:1-bookworm tracks the current stable (well past
# 1.85), so edition-2024 + resolver "3" build fine. Pin to a digest in prod.
FROM rust:1-bookworm AS builder

# Native toolchain for the (unconditional) rusteron Aeron build + the Solidity
# build scripts. Mirrors README.md / justfile bootstrap + CI's apt deps:
#   - build-essential, cmake, pkg-config, clang, libclang-dev  → rusteron C build + bindgen
#   - openjdk-17-jdk-headless                                  → Aeron archive SBE/Gradle codegen (JDK 17+)
#   - libbsd-dev, uuid-dev                                     → CI's extra system build deps
# (curl/ca-certificates/git for foundryup + any git-fetched cargo deps.)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        pkg-config \
        clang \
        libclang-dev \
        libbsd-dev \
        uuid-dev \
        openjdk-17-jdk-headless \
        curl \
        ca-certificates \
        git && \
    rm -rf /var/lib/apt/lists/*

# The Aeron archive build runs a JVM (SBE codegen / Gradle) and FindJava needs a
# JDK 17+. On Debian the headless JDK 17 lives here; set JAVA_HOME + PATH so
# both cmake's FindJava and any direct `java` invocation resolve it.
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Foundry (forge) — invoked by the deployer/node build scripts to compile the
# Solidity contracts. Install via the official foundryup bootstrap, then run
# foundryup to fetch the toolchain into /root/.foundry/bin.
ENV FOUNDRY_DIR=/root/.foundry
RUN curl -L https://foundry.paradigm.xyz | bash && \
    "${FOUNDRY_DIR}/bin/foundryup"
ENV PATH="${FOUNDRY_DIR}/bin:${PATH}"

# Which binary to build (e.g. kardamom-ingress). No default — the Makefile
# always passes it; fail loudly if omitted.
ARG BIN
# Real-Aeron feature flag. EMPTY today (real Aeron is unconditional — see the
# REQUIRED-CHANGE block above). When the workspace gains an `aeron-live`
# passthrough feature, pass: --build-arg AERON_FEATURE="--features aeron-live".
ARG AERON_FEATURE=""

WORKDIR /build

# Copy the whole workspace in (a root `.dockerignore` keeps target/.git/.claude
# out of the context — see note above).
COPY . .

# Build the release binary.
#   * --locked: honor Cargo.lock exactly (matches CI).
#   * cache mounts: persist the cargo registry/git and the target dir across
#     builds so rebuilds are incremental. NOTE: because `target/` is a cache
#     mount (not part of the image layer), we must copy the freshly-built
#     binary OUT to a normal path in the SAME RUN before the mount is unmounted.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/target \
    set -eux; \
    test -n "${BIN}" || { echo "ERROR: --build-arg BIN=<kardamom-svc> is required" >&2; exit 1; }; \
    cargo build --release --locked --bin "${BIN}" ${AERON_FEATURE}; \
    mkdir -p /out; \
    cp "target/release/${BIN}" "/out/${BIN}"


# =============================================================================
# Stage 2: runtime
# =============================================================================
FROM debian:bookworm-slim AS runtime

ARG BIN
ENV SERVICE_BIN=${BIN}

# Runtime shared libraries the binary links against:
#   - ca-certificates : TLS roots (da_watcher/batcher reach an L1 RPC over https)
#   - libbsd0         : runtime counterpart of libbsd-dev (Aeron C deps)
#   - libuuid1        : runtime counterpart of uuid-dev
#   - libstdc++6      : C++ runtime for the Aeron C/C++ objects linked in via rusteron
# (libgcc/libc come from the base image.)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        libbsd0 \
        libuuid1 \
        libstdc++6 && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/${BIN} /usr/local/bin/${BIN}

# -----------------------------------------------------------------------------
# Non-root user.
# -----------------------------------------------------------------------------
# These containers run with host networking and BIND-MOUNT the shared Aeron
# directory at /opt/kardamom/aeron-mount, where they read/write the CnC file and
# mmap'd ring buffers created by the media-driver container. The driver image's
# entrypoint sets `umask 0000` so those files are world-writable, so a non-root
# UID can RW them. We create an unprivileged `kardamom` user (uid 10001) and run
# as it.
#
# OPERATOR NOTE: this only works if the host tmpfs mounted at
# /opt/kardamom/aeron-mount is world-writable (the Ansible `common`/`aeron` role
# should `chmod 0777` it, matching the driver's umask 0000). If you hit EACCES on
# cnc.dat / ring buffers, either fix the host mount perms, run the media driver
# with the same uid, or (last resort) drop the USER line to run as root.
RUN groupadd --gid 10001 kardamom && \
    useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin kardamom
USER 10001:10001

# ENTRYPOINT is the service binary. No hardcoded args — Nomad supplies
# --config / --aeron_dir / flags. Use the exec form via a tiny shell indirection
# so ${SERVICE_BIN} (baked from ARG BIN) resolves; `exec` keeps the binary as
# PID 1 for correct signal handling (docker stop -> SIGTERM).
ENTRYPOINT ["/bin/sh", "-c", "exec /usr/local/bin/${SERVICE_BIN} \"$@\"", "--"]
