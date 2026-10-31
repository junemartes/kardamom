package io.kardamom.sealer.cluster;

import io.aeron.cluster.ClusterBackup;

/**
 * Judges one peer seed round from the samples of its ClusterBackup state.
 *
 * <p>The backup reaches {@code BACKING_UP} after it copied the snapshots,
 * started the log recording, and wrote the recording log. A usable seed
 * also needs the copied log to reach the term and snapshot positions in
 * that metadata. In {@code BACKUP_QUERY} and {@code RESET_BACKUP} no peer answers
 * yet. The round gives up when no peer answers for the whole peer
 * timeout. A state between the two shows that a peer answers, so it
 * restarts the timeout. Aeron's own progress timeout sends a stalled
 * transfer back to {@code RESET_BACKUP}. {@code CLOSED} means the backup
 * agent stopped, so the round cannot finish.</p>
 */
final class SeedWatch {

    /** The verdict on one round after one sample. */
    enum Outcome {
        /** The round continues. */
        PENDING,
        /** The member's directories hold the seed. */
        SEEDED,
        /** No peer answered for the whole peer timeout. */
        NO_PEER,
        /** The copied log does not reach its metadata and stops advancing for the peer timeout. */
        LOG_INCOMPLETE,
        /** The backup agent stopped before the seed. */
        BACKUP_CLOSED
    }

    /** The copied log position and whether it covers its recording metadata. */
    record Copy(long position, boolean complete) {
        static final Copy MISSING = new Copy(-1L, false);
    }

    private long highestCopy = -1L;
    private final long peerTimeoutMs;
    private long quietSinceMs;

    SeedWatch(final long peerTimeoutMs, final long nowMs) {
        this.peerTimeoutMs = peerTimeoutMs;
        this.quietSinceMs = nowMs;
    }

    Outcome observe(final ClusterBackup.State state, final Copy copy, final long nowMs) {
        return switch (state) {
            case BACKING_UP -> copied(copy, nowMs);
            case CLOSED -> Outcome.BACKUP_CLOSED;
            case BACKUP_QUERY, RESET_BACKUP ->
                nowMs - quietSinceMs >= peerTimeoutMs ? Outcome.NO_PEER : Outcome.PENDING;
            case SNAPSHOT_RETRIEVE, LIVE_LOG_RECORD, LIVE_LOG_REPLAY, UPDATE_RECORDING_LOG -> progress(nowMs);
        };
    }

    private Outcome copied(final Copy copy, final long nowMs) {
        if (copy.complete()) {
            return Outcome.SEEDED;
        }
        if (copy.position() > highestCopy) {
            highestCopy = copy.position();
            return progress(nowMs);
        }
        return nowMs - quietSinceMs >= peerTimeoutMs ? Outcome.LOG_INCOMPLETE : Outcome.PENDING;
    }

    private Outcome progress(final long nowMs) {
        quietSinceMs = nowMs;
        return Outcome.PENDING;
    }
}
