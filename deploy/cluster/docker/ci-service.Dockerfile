# syntax=docker/dockerfile:1.7
# =============================================================================
# ci-service.Dockerfile — thin runtime image wrapping a PREBUILT kardamom
# binary, for the container-based cluster e2e only.
# =============================================================================
#
# The production image (service.Dockerfile) compiles the whole workspace inside
# the builder stage — correct, but ~30-60 min on a CI runner. For cluster-e2e
# we instead `cargo build --release` ONCE on the runner (with the shared rust
# cache) and copy the resulting binary in here, so each of the 7 images is a
# fast COPY. Build context is the workspace target dir; invoked as:
#
#   docker build -f deploy/cluster/docker/ci-service.Dockerfile \
#       --build-arg BIN=kardamom-<svc> \
#       -t <registry>/kardamom-<svc>:dev target/release
#
# Runtime libs mirror service.Dockerfile's runtime stage (Aeron C/C++ deps).
FROM debian:bookworm-slim

ARG BIN
ENV SERVICE_BIN=${BIN}

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates libbsd0 libuuid1 libstdc++6 && \
    rm -rf /var/lib/apt/lists/*

# The prebuilt binary (build context = target/release).
COPY ${BIN} /usr/local/bin/${BIN}

# Same world-writable-aeron-dir rationale as service.Dockerfile: these run with
# host networking and bind-mount the shared aeron.dir; run as an unprivileged
# user that can RW the driver's umask-0000 files.
RUN groupadd --gid 10001 kardamom && \
    useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin kardamom
USER 10001:10001

ENTRYPOINT ["/bin/sh", "-c", "exec /usr/local/bin/${SERVICE_BIN} \"$@\"", "--"]
