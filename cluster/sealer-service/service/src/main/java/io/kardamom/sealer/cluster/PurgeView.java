package io.kardamom.sealer.cluster;

import java.util.List;
import java.util.stream.Stream;

/**
 * What the log purge needs from the sealer service: the block number at
 * each snapshot that this service took or restored, and the posted head.
 *
 * <p>The service thread replaces the whole value after each change, and
 * the purge thread reads it. The value is immutable, so one volatile
 * reference carries it between the two threads.</p>
 *
 * <p>The marks live in memory only. A restarted member knows only the
 * snapshot that it restored, and the snapshots that it takes after that.
 * Its purge waits until it knows more snapshots than it keeps. That wait
 * is safe: a purge never moves the log start up while it waits.</p>
 *
 * @param marks the snapshots, oldest first, at most {@link #MAX_MARKS}
 * @param postedHead the last block that the batcher confirmed on L1
 */
record PurgeView(List<Mark> marks, long postedHead) {

    /**
     * The most marks that the view keeps. When the posted head stops, the
     * oldest marks go first. A lost mark only delays a purge: the purge
     * point moves up again when the posted head passes the newer marks.
     */
    static final int MAX_MARKS = 1024;

    static final PurgeView EMPTY = new PurgeView(List.of(), 0L);

    /**
     * One snapshot: its log position, and the block that the next
     * boundary tick stamps. Every record in the log below the position is
     * in a block at or below that block number.
     */
    record Mark(long logPosition, long blockNumber) {
    }

    /** This view with {@code mark} added as the newest snapshot. */
    PurgeView withMark(final Mark mark) {
        final long overflow = Math.max(0L, marks.size() + 1L - MAX_MARKS);
        return new PurgeView(Stream.concat(marks.stream(), Stream.of(mark)).skip(overflow).toList(), postedHead);
    }

    PurgeView withPostedHead(final long head) {
        return new PurgeView(marks, head);
    }
}
