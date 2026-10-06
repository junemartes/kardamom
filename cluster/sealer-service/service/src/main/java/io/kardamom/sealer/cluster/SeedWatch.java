package io.kardamom.sealer.cluster;

import io.aeron.cluster.ClusterBackup;

/**
 * Judges one peer seed round from the samples of its ClusterBackup state.
 *
 * <p>The backup reaches {@code BACKING_UP} after it copied the snapshots,
 * started the log recording, and wrote the recording log. That is the
 * seed. In {@code BACKUP_QUERY} and {@code RESET_BACKUP} no peer answers
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
        /** The backup agent stopped before the seed. */
        BACKUP_CLOSED
    }

    private final long peerTimeoutMs;
    private long quietSinceMs;

    SeedWatch(final long peerTimeoutMs, final long nowMs) {
        this.peerTimeoutMs = peerTimeoutMs;
        this.quietSinceMs = nowMs;
    }

    Outcome observe(final ClusterBackup.State state, final long nowMs) {
        return switch (state) {
            case BACKING_UP -> Outcome.SEEDED;
            case CLOSED -> Outcome.BACKUP_CLOSED;
            case BACKUP_QUERY, RESET_BACKUP ->
                nowMs - quietSinceMs >= peerTimeoutMs ? Outcome.NO_PEER : Outcome.PENDING;
            case SNAPSHOT_RETRIEVE, LIVE_LOG_RECORD, LIVE_LOG_REPLAY, UPDATE_RECORDING_LOG -> progress(nowMs);
        };
    }

    private Outcome progress(final long nowMs) {
        quietSinceMs = nowMs;
        return Outcome.PENDING;
    }
}
