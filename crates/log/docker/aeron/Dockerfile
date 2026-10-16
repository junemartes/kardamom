# Aeron Media Driver + Aeron Archive in a single container image. The
# entrypoint script starts an ArchivingMediaDriver in the foreground (Media
# Driver + Archive in one JVM), which keeps container lifecycle in lockstep
# with the Archive.
#
# The image vendors Aeron 1.45.0 (matching what `rusteron-archive` 0.1.x
# targets). Pin the version explicitly — drift in the Aeron wire protocol is
# a guaranteed source of "works on my laptop" bugs.

FROM eclipse-temurin:21-jre-jammy AS base

ARG AERON_VERSION=1.45.0
ENV AERON_VERSION=${AERON_VERSION}
# The SHA-256 of aeron-all-1.45.0.jar. Maven Central publishes a SHA-1 for
# it (4c303bbd2ff61f2ff8a74861f733f67416fd6d3f); a download that matched it
# gave this SHA-256. A different AERON_VERSION needs its own hash.
# These two ARG lines are the only pin: .github/actions/aeron-jar reads them.
ARG AERON_JAR_SHA256=80899f3919fad64d5f572a8f7ebe549361eee723df9d28425c8501dea4ae1672

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# CI puts a verified aeron-all.jar into the build context, so the build
# does not depend on Maven Central. The `ja[r]` pattern lets the COPY pass
# with no jar in the context: a local build then downloads it. The script
# checks the SHA-256 in both cases.
COPY aeron-jar.sh aeron-all.ja[r] /opt/aeron/
RUN sh /opt/aeron/aeron-jar.sh "${AERON_VERSION}" "${AERON_JAR_SHA256}" /opt/aeron/aeron-all.jar

# `aeron.dir` lives in a subdir of a bind-mount root so Aeron can freely
# rmdir + recreate `aeron.dir` on every start without touching the
# mountpoint itself (which can't be `rmdir`'d). The host bind-mounts
# `/aeron-mount` and reads/writes `/aeron-mount/dir` from outside.
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
