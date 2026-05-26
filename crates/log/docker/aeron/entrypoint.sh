#!/usr/bin/env bash
set -euo pipefail

# ArchivingMediaDriver runs both the Media Driver and the Archive in one JVM,
# which is the simplest deployment for tests.
exec java \
    -Daeron.dir=${AERON_DIR} \
    -Daeron.archive.dir=${AERON_ARCHIVE_DIR} \
    -Daeron.term.buffer.length=${AERON_TERM_BUFFER_LENGTH} \
    -Daeron.ipc.term.buffer.length=${AERON_IPC_TERM_BUFFER_LENGTH} \
    -Daeron.archive.control.channel=aeron:udp?endpoint=0.0.0.0:8010 \
    -Daeron.archive.control.response.channel=aeron:udp?endpoint=0.0.0.0:8011 \
    -Daeron.archive.replication.channel=aeron:udp?endpoint=0.0.0.0:8021 \
    -cp /opt/aeron/aeron-all.jar \
    ${AERON_ARCHIVE_CLASS}
