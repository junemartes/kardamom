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
# Idle strategy: Aeron's backoff (spin, then yield, then park), with the
# longest park capped at KARDAMOM_IDLE_CAP_US (default 50 µs) instead of
# Aeron's 1 ms. A frame that reaches an idle driver waits for the end of
# the park. CappedBackoffIdleStrategy reads the cap from the environment.
# Every driver and archive agent uses it, in each threading mode. A sleep of
# a fixed 1 ms (SleepingMillisIdleStrategy) delays the Status-Message and
# NAK service of the driver, and multicast liveness suffers.
AERON_THREADING_MODE="${AERON_THREADING_MODE:-SHARED}"
AERON_ARCHIVE_THREADING_MODE="${AERON_ARCHIVE_THREADING_MODE:-SHARED}"
export KARDAMOM_IDLE_CAP_US="${KARDAMOM_IDLE_CAP_US:-50}"
IDLE_STRATEGY=io.kardamom.aeron.CappedBackoffIdleStrategy
IDLE_OPTS=""
for agent in conductor sender receiver sharednetwork shared archive archive.recorder archive.replayer; do
    IDLE_OPTS="${IDLE_OPTS} -Daeron.${agent}.idle.strategy=${IDLE_STRATEGY}"
done
# Per-segment CRC32 on record AND replay-side validation on read. Without a
# record-time checksum, `ArchiveTool verify` only validates frame structure —
# a payload byte-flip in a .rec passes silently (and verify_mirror's length
# check can't see it either). Crc32 is Aeron's own class, so the persisted
# values stay comparable with `ArchiveTool checksum io.aeron.archive.checksum.Crc32`.
AERON_ARCHIVE_CHECKSUM="${AERON_ARCHIVE_CHECKSUM:-io.aeron.archive.checksum.Crc32}"
# The archive's UDP ports. The defaults serve the docker test rigs, which map
# fixed container ports. The Nomad job passes the ports it allocated to this
# task, so no archive port is fixed on a cluster node.
AERON_ARCHIVE_CONTROL_PORT="${AERON_ARCHIVE_CONTROL_PORT:-8010}"
AERON_ARCHIVE_CONTROL_RESPONSE_PORT="${AERON_ARCHIVE_CONTROL_RESPONSE_PORT:-8011}"
AERON_ARCHIVE_REPLICATION_PORT="${AERON_ARCHIVE_REPLICATION_PORT:-8021}"

# Timer slack: Linux ends a park up to the timer slack of the thread after
# its deadline, 50 µs by default, so a 50 µs park can take 100 µs. This
# shell sets its own slack to 1 µs. The JVM and each of its threads take
# the slack of the thread that starts them. A kernel that refuses the
# write keeps the default slack.
{ echo 1000 > /proc/self/timerslack_ns; } 2>/dev/null || true

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
    -Daeron.archive.control.channel=aeron:udp?endpoint=0.0.0.0:${AERON_ARCHIVE_CONTROL_PORT} \
    -Daeron.archive.control.response.channel=aeron:udp?endpoint=0.0.0.0:${AERON_ARCHIVE_CONTROL_RESPONSE_PORT} \
    -Daeron.archive.replication.channel=aeron:udp?endpoint=0.0.0.0:${AERON_ARCHIVE_REPLICATION_PORT} \
    -Daeron.archive.record.checksum=${AERON_ARCHIVE_CHECKSUM} \
    -Daeron.archive.replay.checksum=${AERON_ARCHIVE_CHECKSUM} \
    ${IDLE_OPTS} \
    -cp /opt/aeron/aeron-all.jar:/opt/aeron/kardamom-idle.jar \
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
