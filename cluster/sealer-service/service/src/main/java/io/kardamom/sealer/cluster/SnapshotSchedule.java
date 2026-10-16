package io.kardamom.sealer.cluster;

import java.util.Optional;

/**
 * The block interval of the periodic Raft snapshot.
 *
 * <p>A snapshot suspends the consensus module of each member. The module
 * appends no timer and no ingress until the service and the module record
 * their snapshots in the archive. With a file sync level above 0, each
 * recording is a chain of disk syncs. On a loaded disk this lasts many
 * seconds, and no status frame leaves the leader in that time.</p>
 *
 * <p>So the service asks for the snapshot itself, and a status frame with
 * the snapshot notice goes out first. A client that reads the notice waits
 * longer for the next frame. A snapshot is due at each boundary that opens
 * a block whose number is a multiple of the interval: a boundary of the
 * timer, or a boundary that an origin record forces. Each boundary opens
 * one block, so each multiple comes once. The block number is a function
 * of the log, so every member sends the same notice at the same log
 * position, and the next leader keeps the same schedule.</p>
 *
 * @param intervalBlocks the blocks between two snapshots; at least 1, as
 *     {@link #fromSetting} makes it
 */
record SnapshotSchedule(long intervalBlocks) {

    static final String SETTING = "kardamom.cluster.snapshotIntervalS";
    static final long DEFAULT_INTERVAL_S = 300L;

    /**
     * The schedule of an interval of {@code intervalS} seconds at a tick of
     * {@code tickMs}: none for an interval of 0. An interval shorter than
     * one tick is fatal.
     */
    static Optional<SnapshotSchedule> fromSetting(final long intervalS, final long tickMs) {
        if (intervalS == 0) {
            return Optional.empty();
        }
        final long blocks = Math.multiplyExact(intervalS, 1000L) / tickMs;
        if (blocks < 1) {
            throw new IllegalStateException(
                SETTING + ": " + intervalS + " s is shorter than the tick of " + tickMs + " ms");
        }
        return Optional.of(new SnapshotSchedule(blocks));
    }

    /** Whether the boundary that opens {@code blockNumber} asks for a snapshot. */
    boolean due(final long blockNumber) {
        return blockNumber % intervalBlocks == 0;
    }
}
