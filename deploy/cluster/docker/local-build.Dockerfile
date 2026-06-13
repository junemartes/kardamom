# =============================================================================
# local-build.Dockerfile — reproducible builder for the kardamom service
# binaries consumed by the container cluster-e2e.
# =============================================================================
#
# The cluster-e2e workflow builds the binaries on the GitHub ubuntu-24.04 runner
# and only `apt install`s a short list (cmake uuid-dev libbsd-dev libssl-dev
# pkg-config build-essential). That works on the runner ONLY because its image
# also preinstalls, out of band, several things the Aeron C / Archive client
# build script needs:
#   * cmake >= 3.30      — Aeron 1.45's CMakeLists requires it; ubuntu noble apt
#                          ships 3.28, so we install a newer cmake explicitly.
#   * clang + libclang   — rusteron-* run bindgen, which needs libclang.
#   * rustfmt            — rusteron's build.rs formats the generated bindings and
#                          panics if rustfmt is absent.
#   * a JDK              — aeron-archive's C CMake build does find_package(Java).
#   * foundry (forge)    — the deployer build script shells out to `forge build`
#                          to embed contract bytecode (the cluster-e2e workflow
#                          installs it via the foundry-toolchain action).
# A bare ubuntu:24.04 has none of these, so building locally (or anywhere that
# isn't the GitHub runner image) fails one dependency at a time. This image
# captures the FULL set so the build is reproducible off-runner. Used by
# deploy/cluster/scripts/local-cluster.sh.
FROM ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential pkg-config curl ca-certificates git \
        uuid-dev libbsd-dev libssl-dev \
        clang libclang-dev \
        default-jdk \
    && rm -rf /var/lib/apt/lists/*

# Aeron 1.45 needs cmake >= 3.30; ubuntu noble apt has 3.28. Install the upstream
# binary (arch-dynamic: aarch64 / x86_64) ahead of apt's on PATH.
ARG CMAKE_VERSION=3.31.6
RUN m="$(uname -m)" && \
    curl -fsSL "https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-${m}.tar.gz" \
      | tar -xz -C /opt && \
    ln -sf "/opt/cmake-${CMAKE_VERSION}-linux-${m}/bin/"* /usr/local/bin/

# Rust via rustup with the DEFAULT profile (includes rustfmt, unlike minimal).
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --default-toolchain stable --profile default
ENV PATH=/root/.foundry/bin:/root/.cargo/bin:${PATH}
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Foundry — pin the same concrete version as cluster-e2e.yml (a concrete tag
# downloads directly; `stable`/`latest` would hit the GitHub API).
ARG FOUNDRY_VERSION=v1.7.1
RUN curl -L https://foundry.paradigm.xyz | bash && \
    /root/.foundry/bin/foundryup --install "${FOUNDRY_VERSION}"

WORKDIR /work
