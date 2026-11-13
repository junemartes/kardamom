package io.kardamom.sealer.cluster;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.LongSupplier;
import org.agrona.concurrent.IdleStrategy;

/**
 * When the log of this member reached each position: the times at which the
 * consensus thread saw the append position and the commit position change.
 * From them, the service thread reads how long a log entry waited for the
 * replication quorum, and how long it then waited for the service thread.
 *
 * <p>The consensus thread takes one sample at each duty cycle
 * ({@link #sampling}), and keeps it when a position changed. It is the thread
 * that moves the commit position, so a wait has the resolution of one duty
 * cycle: at most the idle cap of the member while the thread is idle.</p>
 *
 * <p>The samples are in a ring that one thread writes and one thread reads.
 * The writer publishes a sample with the count of the samples. A reader uses
 * at most half of the ring, and drops its result when the writer came within
 * the other half during the read.</p>
 *
 * <p>The values are member-local. They are not part of the replicated state
 * and never reach the log or a snapshot.</p>
 */
final class LogTimeline {

    /** The receiver of the waits of one log entry, in nanoseconds. */
    interface Waits {
        void accept(long commitWaitNs, long applyWaitNs);
    }

    /** The samples that the ring keeps; a power of two. */
    static final int SLOTS = 16_384;
    /** The most samples that one lookup reads. */
    private static final int LOOKBACK = SLOTS / 2;
    /** The fields of one sample: the time, the append position and the commit position. */
    private static final int FIELDS = 3;
    private static final int APPEND = 1;
    private static final int COMMIT = 2;
    /** The time of a position that no sample holds yet. */
    private static final long UNSEEN = Long.MIN_VALUE;
    /** The position before the first read. */
    private static final long NO_POSITION = -1L;

    private final LongSupplier clock;
    private final AtomicLongArray ring = new AtomicLongArray(SLOTS * FIELDS);
    /** The samples that the consensus thread wrote. */
    private final AtomicLong written = new AtomicLong();
    private volatile LongSupplier append = () -> NO_POSITION;
    private volatile LongSupplier commit = () -> NO_POSITION;
    /** The positions of the newest sample. Only the consensus thread reads them. */
    private long lastAppend = NO_POSITION;
    private long lastCommit = NO_POSITION;

    /** A timeline on the clock of the JVM. It takes no sample before {@link #follow}. */
    LogTimeline() {
        this(System::nanoTime);
    }

    /** A timeline on {@code clock}, in nanoseconds, for a test. */
    LogTimeline(final LongSupplier clock) {
        this.clock = clock;
    }

    /**
     * Follow the position that this member recorded of the log, and the
     * position that the quorum committed. The member calls this after the
     * launch: the counters do not exist before it.
     */
    void follow(final LongSupplier appendPosition, final LongSupplier commitPosition) {
        append = appendPosition;
        commit = commitPosition;
    }

    /** {@code idle}, with one sample at each call. It is the idle strategy of the consensus thread. */
    IdleStrategy sampling(final IdleStrategy idle) {
        return new Sampling(this, idle);
    }

    /** Keep the positions of now, when one of them changed. Only the consensus thread calls this. */
    void sample() {
        final long appended = append.getAsLong();
        final long committed = commit.getAsLong();
        if (appended == lastAppend && committed == lastCommit) {
            return;
        }
        lastAppend = appended;
        lastCommit = committed;
        final long count = written.getPlain();
        final int base = slot(count);
        ring.lazySet(base, clock.getAsLong());
        ring.lazySet(base + APPEND, appended);
        ring.lazySet(base + COMMIT, committed);
        written.lazySet(count + 1);
    }

    /**
     * Give {@code waits} the waits of the log entry that ends at
     * {@code position}, which the service thread applied at {@code appliedNs}.
     * Only the service thread calls this.
     *
     * <p>The commit wait runs from the first sample with the entry recorded
     * to the first sample with the entry committed. The apply wait runs from
     * there to {@code appliedNs}. An entry that the service applied before the
     * consensus thread sampled its commit has an apply wait of zero. An entry
     * that no sample holds yet has both waits at zero: they are shorter than
     * one duty cycle.</p>
     *
     * <p>The entry gives no waits when the ring does not hold the sample
     * before its record: a replayed entry, or an entry older than the
     * lookback.</p>
     */
    void waitsOf(final long position, final long appliedNs, final Waits waits) {
        final long newest = written.get();
        final long oldest = Math.max(0L, newest - LOOKBACK);
        long index = newest;
        long recordedNs = UNSEEN;
        long committedNs = UNSEEN;
        while (index > oldest && ring.get(slot(index - 1) + APPEND) >= position) {
            final int base = slot(index - 1);
            recordedNs = ring.get(base);
            committedNs = ring.get(base + COMMIT) >= position ? recordedNs : committedNs;
            index--;
        }
        if (index == oldest || written.get() - newest >= SLOTS - LOOKBACK) {
            return;
        }
        if (recordedNs == UNSEEN) {
            waits.accept(0L, 0L);
            return;
        }
        final long commitNs = committedNs == UNSEEN ? appliedNs : committedNs;
        waits.accept(Math.max(0L, commitNs - recordedNs), Math.max(0L, appliedNs - commitNs));
    }

    /** The first field of sample {@code count} in the ring. */
    private static int slot(final long count) {
        return (int) (count & (SLOTS - 1)) * FIELDS;
    }

    /** An idle strategy that samples the timeline, then idles as {@code inner}. */
    private record Sampling(LogTimeline timeline, IdleStrategy inner) implements IdleStrategy {
        @Override
        public void idle(final int workCount) {
            timeline.sample();
            inner.idle(workCount);
        }

        @Override
        public void idle() {
            timeline.sample();
            inner.idle();
        }

        @Override
        public void reset() {
            inner.reset();
        }

        @Override
        public String alias() {
            return inner.alias();
        }
    }
}
