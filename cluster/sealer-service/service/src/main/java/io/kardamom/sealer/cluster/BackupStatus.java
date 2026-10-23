package io.kardamom.sealer.cluster;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusterBackup;
import io.aeron.cluster.RecordingLog;

/**
 * One sample of a backup node: the state of its cluster backup, the log
 * position that it recorded, and the latest snapshot in its recording log.
 *
 * <p>A backup is steady when it holds a snapshot and is in
 * {@code BACKING_UP}, or in the {@code BACKUP_QUERY} that it starts from
 * there at each interval. A steady backup is ready, and its log line
 * prints only a change of the snapshot or a step out of the steady
 * states: a query every interval is routine.</p>
 *
 * @param state           the state of the cluster backup
 * @param liveLogPosition the log position up to which the backup recorded the live log
 * @param snapshot        the latest snapshot in the recording log of the backup
 */
record BackupStatus(ClusterBackup.State state, long liveLogPosition, Snapshot snapshot) {

    /**
     * A consensus module snapshot in the recording log.
     *
     * @param logPosition      the log position of the snapshot, -1 when there is none
     * @param leadershipTermId the leadership term of the snapshot, -1 when there is none
     */
    record Snapshot(long logPosition, long leadershipTermId) {
        /** The recording log holds no snapshot. */
        static final Snapshot NONE = new Snapshot(AeronArchive.NULL_POSITION, Aeron.NULL_VALUE);

        static Snapshot of(final RecordingLog.Entry entry) {
            return new Snapshot(entry.logPosition, entry.leadershipTermId);
        }
    }

    boolean isReady() {
        return steady();
    }

    /** Whether the backup holds a snapshot, and backs up or queries for the next one. */
    boolean steady() {
        return !snapshot.equals(Snapshot.NONE)
            && (state == ClusterBackup.State.BACKING_UP || state == ClusterBackup.State.BACKUP_QUERY);
    }

    /** Whether {@code other} says nothing new: the same snapshot, and both steady or both in the same state. */
    boolean sameStage(final BackupStatus other) {
        final boolean sameState = steady() ? other.steady() : state == other.state;
        return sameState && snapshot.equals(other.snapshot);
    }

    /** One JSON line. The values are numbers, an enum name, and a boolean. */
    String toJson() {
        return "{\"mode\":\"backup\""
            + ",\"state\":\"" + state + "\""
            + ",\"stateCode\":" + state.code()
            + ",\"liveLogPosition\":" + liveLogPosition
            + ",\"latestSnapshotLogPosition\":" + snapshot.logPosition()
            + ",\"latestSnapshotLeadershipTermId\":" + snapshot.leadershipTermId()
            + ",\"ready\":" + isReady()
            + "}";
    }

    /** The Prometheus text of the gauges. */
    String toMetrics() {
        return gauge("kardamom_backup_state", "The state code of the cluster backup. 5 is BACKING_UP.", state.code())
            + gauge("kardamom_backup_live_log_position", "The log position that the backup recorded.", liveLogPosition)
            + gauge("kardamom_backup_snapshot_log_position",
                "The log position of the latest snapshot that the backup holds, -1 when it holds none.",
                snapshot.logPosition());
    }

    /** The admin endpoint's answer to this sample. */
    AdminServer.Report report() {
        return new AdminServer.Report(isReady(), toJson(), toMetrics());
    }

    /** The log line of this sample. */
    String logLine() {
        return "cluster BACKUP state=" + state + " liveLogPosition=" + liveLogPosition
            + " snapshotLogPosition=" + snapshot.logPosition()
            + " snapshotLeadershipTermId=" + snapshot.leadershipTermId();
    }

    private static String gauge(final String name, final String help, final long value) {
        return "# HELP " + name + " " + help + "\n"
            + "# TYPE " + name + " gauge\n"
            + name + " " + value + "\n";
    }
}
