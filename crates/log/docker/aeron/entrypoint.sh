#!/usr/bin/env bash
set -euo pipefail

# ArchivingMediaDriver runs both the Media Driver and the Archive in one JVM,
# which is the simplest deployment for tests.
#
# Aeron 1.45 logs via slf4j; the fat aeron-all jar bundles slf4j-api but no
# backend, so all its log output is silently dropped. We background-launch the
# JVM, poll for the Archive's catalog file (the strongest cheap signal that
# the Archive is up), then print the ready line the test harness waits for,
# then wait on the JVM so signal handling stays correct.

# Permissive umask so files Aeron creates (cnc.dat, archive catalog, segment
# files) are world-writable — the bind-mounted host process needs RW access.
umask 0000

java \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    --add-opens java.base/java.util.zip=ALL-UNNAMED \
    --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
    -Daeron.dir=${AERON_DIR} \
    -Daeron.archive.dir=${AERON_ARCHIVE_DIR} \
    -Daeron.term.buffer.length=${AERON_TERM_BUFFER_LENGTH} \
    -Daeron.ipc.term.buffer.length=${AERON_IPC_TERM_BUFFER_LENGTH} \
    -Daeron.archive.control.channel=aeron:udp?endpoint=0.0.0.0:8010 \
    -Daeron.archive.control.response.channel=aeron:udp?endpoint=0.0.0.0:8011 \
    -Daeron.archive.replication.channel=aeron:udp?endpoint=0.0.0.0:8021 \
    -cp /opt/aeron/aeron-all.jar \
    ${AERON_ARCHIVE_CLASS} &
JAVA_PID=$!

# Forward signals so docker stop terminates the JVM cleanly.
trap "kill -TERM $JAVA_PID 2>/dev/null || true; wait $JAVA_PID" TERM INT

# Wait for the archive to materialize its catalog (Aeron Archive boot complete).
# Bounded retry: 30s * 100ms = 30s max wait.
for i in $(seq 1 300); do
    if [[ -f "${AERON_ARCHIVE_DIR}/archive.catalog" && -f "${AERON_DIR}/cnc.dat" ]]; then
        # Small additional settle to let the ArchiveAgent's poller spin up.
        sleep 0.2
        echo "ArchiveAgent: started"
        break
    fi
    if ! kill -0 $JAVA_PID 2>/dev/null; then
        echo "ArchivingMediaDriver exited during startup (pid $JAVA_PID gone)" >&2
        wait $JAVA_PID
        exit $?
    fi
    sleep 0.1
done

wait $JAVA_PID
