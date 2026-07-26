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

# CPU thrift. Aeron's default DEDICATED threading runs THREE busy-spinning
# threads per media driver (conductor/sender/receiver), plus the Archive's own
# threads — fine for one low-latency host, but a whole CLUSTER of drivers (the
# container cluster-e2e runs ~6 of them) pins every core. Default to SHARED
# threading so each driver uses ONE thread instead of three.
#
# Idle strategy: deliberately left at Aeron's DEFAULT (a backoff that
# spins→yields→parks). An earlier experiment forced SleepingMillisIdleStrategy
# (sleep 1ms when idle) to save more CPU, but that delayed the driver's
# Status-Message / NAK servicing enough to HURT multicast liveness and made the
# tx_ordering freeze appear *sooner* — and it was never the real cause anyway.
# The freeze was a client-side bug (a back-pressured publish starving the shared
# Aeron thread's subscription polling), fixed in kardamom_log::aeron_live. So we
# keep the responsive default idle here and only trim the thread COUNT.
AERON_THREADING_MODE="${AERON_THREADING_MODE:-SHARED}"
AERON_ARCHIVE_THREADING_MODE="${AERON_ARCHIVE_THREADING_MODE:-SHARED}"
# Per-segment CRC32 on record AND replay-side validation on read. Without a
# record-time checksum, `ArchiveTool verify` only validates frame structure —
# a payload byte-flip in a .rec passes silently (and verify_mirror's length
# check can't see it either). Crc32 is Aeron's own class, so the persisted
# values stay comparable with `ArchiveTool checksum io.aeron.archive.checksum.Crc32`.
AERON_ARCHIVE_CHECKSUM="${AERON_ARCHIVE_CHECKSUM:-io.aeron.archive.checksum.Crc32}"

java \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    --add-opens java.base/java.util.zip=ALL-UNNAMED \
    --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
    -Daeron.dir=${AERON_DIR} \
    -Daeron.archive.dir=${AERON_ARCHIVE_DIR} \
    -Daeron.term.buffer.length=${AERON_TERM_BUFFER_LENGTH} \
    -Daeron.ipc.term.buffer.length=${AERON_IPC_TERM_BUFFER_LENGTH} \
    -Daeron.threading.mode=${AERON_THREADING_MODE} \
    -Daeron.archive.threading.mode=${AERON_ARCHIVE_THREADING_MODE} \
    -Daeron.archive.control.channel=aeron:udp?endpoint=0.0.0.0:8010 \
    -Daeron.archive.control.response.channel=aeron:udp?endpoint=0.0.0.0:8011 \
    -Daeron.archive.replication.channel=aeron:udp?endpoint=0.0.0.0:8021 \
    -Daeron.archive.record.checksum=${AERON_ARCHIVE_CHECKSUM} \
    -Daeron.archive.replay.checksum=${AERON_ARCHIVE_CHECKSUM} \
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
