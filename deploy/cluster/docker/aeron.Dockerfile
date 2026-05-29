# =============================================================================
# aeron.Dockerfile — Aeron Media Driver + Archive image for the cluster.
# =============================================================================
#
# PROVENANCE: this is a verbatim copy of the canonical Aeron image at
#   crates/log/docker/aeron/Dockerfile
# (the one exercised by the e2e tests' testcontainers harness), reproduced here
# so the cluster Makefile can build it without reaching across the tree for the
# Dockerfile while still using that directory as the build CONTEXT. The cluster
# Makefile builds it as:
#
#   docker build -f docker/aeron.Dockerfile \
#       -t <registry>/kardamom-aeron:<tag> <repo-root>/crates/log/docker/aeron
#
# The build context is `crates/log/docker/aeron/`, which is where `entrypoint.sh`
# lives — the `COPY entrypoint.sh ...` below resolves against that context.
# Keep this file IN SYNC with crates/log/docker/aeron/Dockerfile; the entrypoint
# behavior is intentionally identical (it COPYs ./entrypoint.sh from the context).
#
# Aeron Media Driver + Aeron Archive in a single container image. The
# entrypoint script starts an ArchivingMediaDriver in the foreground (Media
# Driver + Archive in one JVM), which keeps container lifecycle in lockstep
# with the Archive.
#
# The image vendors Aeron 1.45.0 (matching what `rusteron-archive` 0.1.x
# targets). Pin the version explicitly — drift in the Aeron wire protocol is
# a guaranteed source of "works on my laptop" bugs.
# =============================================================================

FROM eclipse-temurin:21-jre-jammy AS base

# Pinned to match rusteron-archive 0.1.x and the host-native driver used by the
# justfile (AERON_JAR_VERSION := "1.45.0"). Do not bump without bumping both.
ARG AERON_VERSION=1.45.0
ENV AERON_VERSION=${AERON_VERSION}

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/aeron && \
    curl -L "https://repo1.maven.org/maven2/io/aeron/aeron-all/${AERON_VERSION}/aeron-all-${AERON_VERSION}.jar" \
        -o /opt/aeron/aeron-all.jar

# `aeron.dir` lives in a subdir of a bind-mount root so Aeron can freely
# rmdir + recreate `aeron.dir` on every start without touching the
# mountpoint itself (which can't be `rmdir`'d). The host bind-mounts
# `/aeron-mount` and reads/writes `/aeron-mount/dir` from outside.
#
# CLUSTER NOTE: Nomad bind-mounts the per-node host tmpfs
# (/opt/kardamom/aeron-mount) onto this container's /aeron-mount, and the
# co-located service containers bind-mount the same host path, so they share the
# CnC file + ring buffers. The entrypoint's `umask 0000` makes Aeron's files
# world-writable so the non-root service containers can RW them.
ENV AERON_DIR=/aeron-mount/dir
ENV AERON_ARCHIVE_MOUNT=/aeron-mount/archive
ENV AERON_ARCHIVE_DIR=/aeron-mount/archive/dir
ENV AERON_ARCHIVE_CLASS=io.aeron.archive.ArchivingMediaDriver

# Sensible defaults for testing — small term length to keep RSS low.
ENV AERON_TERM_BUFFER_LENGTH=4194304
ENV AERON_IPC_TERM_BUFFER_LENGTH=4194304

# Expose Archive control + response + replication ports.
EXPOSE 8010/udp 8011/udp 8020/udp 8021/udp

RUN mkdir -p /aeron-mount && chmod 0777 /aeron-mount && \
    mkdir -p /aeron-mount/archive && chmod 0777 /aeron-mount/archive

COPY entrypoint.sh /opt/aeron/entrypoint.sh
RUN chmod +x /opt/aeron/entrypoint.sh

ENTRYPOINT ["/opt/aeron/entrypoint.sh"]
