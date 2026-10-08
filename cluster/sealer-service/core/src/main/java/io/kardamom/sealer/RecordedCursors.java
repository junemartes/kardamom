package io.kardamom.sealer;

import java.nio.ByteBuffer;
import java.util.TreeMap;
import java.util.function.IntPredicate;

/**
 * The recorded cursor of each executor, and the record-lag guard on them.
 *
 * <p>An executor records the transactions that it joins in its own
 * archive. Its cursor {@code recorded_through} says that every canonical
 * index at or below it is joined and recorded, or voided. One recorded copy
 * is enough for every other executor and consumer to fetch from, so the
 * guard reads the best cursor: the maximum over the executors. One dead or
 * slow executor does not stop the chain.</p>
 *
 * <p>The guard refuses user records while the last ordered index is more
 * than the budget past the best cursor. A guard with no cursor refuses
 * nothing, so a new cluster, or a state restored from a snapshot that has
 * no cursors, does not halt before the first executor sends its cursor.</p>
 *
 * <p>Every change comes from the replicated log, and the snapshot holds the
 * cursors in executor-id order, so every member has the same cursors.</p>
 */
public final class RecordedCursors {

    /** The best cursor before any executor sent one. The wire carries it as {@code u64::MAX}. */
    public static final long NONE = -1L;

    /** Snapshot bytes for one cursor: executor id, recorded through. */
    private static final int ENTRY_LEN = Byte.BYTES + Long.BYTES;

    private final long budget;
    /** The recorded-through index of each executor, by executor id. */
    private final TreeMap<Integer, Long> recorded = new TreeMap<>();
    /** The maximum of {@link #recorded}, or {@link #NONE}. A cursor only moves up, so the maximum only moves up. */
    private long best = NONE;

    RecordedCursors(long budget) {
        this.budget = budget;
    }

    /**
     * Adopt one cursor. The caller checks that the executor is a voter and
     * that the cursor names an ordered index. A cursor at or below the one
     * this executor sent before changes nothing.
     *
     * @return whether the best cursor moved up
     */
    boolean onCursor(int executorId, long recordedThrough) {
        recorded.merge(executorId, recordedThrough, Math::max);
        if (recordedThrough <= best) {
            return false;
        }
        best = recordedThrough;
        return true;
    }

    /** The best recorded cursor, or {@link #NONE} before the first cursor. */
    long best() {
        return best;
    }

    /** The budget, in canonical records; 0 means the guard is off. */
    long budget() {
        return budget;
    }

    /**
     * Whether the guard refuses user records: the budget is on, a cursor
     * exists, and {@code lastIndex} is more than the budget past it. A
     * cursor names an ordered index, so {@code best <= lastIndex}, and the
     * subtraction cannot overflow.
     *
     * @param lastIndex the last ordered canonical index
     */
    boolean halted(long lastIndex) {
        return budget > 0 && best != NONE && lastIndex - best > budget;
    }

    /** The snapshot length in bytes: {@code count(1) | count * (id(1) | through(8))}. */
    int snapshotLen() {
        return Byte.BYTES + recorded.size() * ENTRY_LEN;
    }

    /** Write the cursors in executor-id order. */
    void writeTo(ByteBuffer buf) {
        buf.put((byte) recorded.size());
        recorded.forEach((id, through) -> {
            buf.put(id.byteValue());
            buf.putLong(through);
        });
    }

    /**
     * Read what {@link #writeTo} wrote. A cursor of an executor that
     * {@code isVoter} does not name drops: the best cursor is the maximum
     * over the configured executors only.
     */
    static RecordedCursors readFrom(ByteBuffer buf, long budget, IntPredicate isVoter) {
        int count = buf.get() & 0xFF;
        if ((long) count * ENTRY_LEN > buf.remaining()) {
            throw new IllegalArgumentException(
                "truncated snapshot: recorded cursor count " + count + ", "
                    + buf.remaining() + " bytes remaining");
        }
        RecordedCursors cursors = new RecordedCursors(budget);
        for (int i = 0; i < count; i++) {
            int executorId = buf.get() & 0xFF;
            long recordedThrough = buf.getLong();
            if (isVoter.test(executorId)) {
                cursors.onCursor(executorId, recordedThrough);
            }
        }
        return cursors;
    }
}
