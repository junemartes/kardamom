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

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/aeron && \
    curl -L "https://repo1.maven.org/maven2/io/aeron/aeron-all/${AERON_VERSION}/aeron-all-${AERON_VERSION}.jar" \
        -o /opt/aeron/aeron-all.jar

ENV AERON_DIR=/dev/shm/aeron
ENV AERON_ARCHIVE_DIR=/var/lib/aeron/archive
ENV AERON_ARCHIVE_CLASS=io.aeron.archive.ArchivingMediaDriver

# Sensible defaults for testing — small term length to keep RSS low.
ENV AERON_TERM_BUFFER_LENGTH=4194304
ENV AERON_IPC_TERM_BUFFER_LENGTH=4194304

# Expose Archive control + response + replication ports.
EXPOSE 8010/udp 8011/udp 8020/udp 8021/udp

RUN mkdir -p ${AERON_ARCHIVE_DIR} && \
    chmod -R 777 ${AERON_ARCHIVE_DIR}

COPY entrypoint.sh /opt/aeron/entrypoint.sh
RUN chmod +x /opt/aeron/entrypoint.sh

ENTRYPOINT ["/opt/aeron/entrypoint.sh"]
