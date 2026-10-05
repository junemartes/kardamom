package io.kardamom.sealer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The priority window in front of the record path: a bounded array of
 * records that flushes in {@code (tip descending, arrival ascending)} order,
 * with one sender's records kept in their arrival order.
 *
 * <p>Pure and deterministic: it holds no clock. The service closes a window
 * on log events only (the entry count, a cluster timer, a boundary, or an
 * origin record), so every member flushes the same records in the same
 * order at the same log position.</p>
 *
 * <p>The chaining rule keeps nonce order: a record of a sender the window
 * already holds takes the sender's head key (the tip and the arrival of the
 * sender's first record in the window) and sorts behind the head by its own
 * arrival. So a sender's later nonce never passes an earlier one, whatever
 * the tips. The window is small by design: a sort of at most
 * {@code capacity} elements and a map of at most {@code capacity} senders.
 * A zero capacity is a pass-through window: every add fills it.</p>
 *
 * @param <T> the record the service holds for each entry
 */
public final class OrderingWindow<T> {

    /** The window size with priority fees on. */
    public static final int DEFAULT_CAPACITY = 20;

    /** How long an open window holds its records, in cluster milliseconds. */
    public static final long HOLD_MS = 5L;

    /** One held record and its sort key. */
    private static final class Entry<T> {
        /** The head's tip, as an unsigned 128-bit value in two halves. */
        final long tipHi;
        final long tipLo;
        /** The head's arrival, then this record's own arrival. */
        final long headArrival;
        final long arrival;
        final T record;

        Entry(long tipHi, long tipLo, long headArrival, long arrival, T record) {
            this.tipHi = tipHi;
            this.tipLo = tipLo;
            this.headArrival = headArrival;
            this.arrival = arrival;
            this.record = record;
        }
    }

    /** Highest tip first; then the head's arrival; then the own arrival. */
    private static final Comparator<Entry<?>> FLUSH_ORDER = Comparator
            .<Entry<?>>comparingLong(e -> e.headArrival)
            .thenComparingLong(e -> e.arrival);

    /** Highest tip first, as an unsigned 128-bit comparison; ties keep {@link #FLUSH_ORDER}. */
    private static int byTipDescending(final Entry<?> a, final Entry<?> b) {
        final int hi = Long.compareUnsigned(b.tipHi, a.tipHi);
        if (hi != 0) {
            return hi;
        }
        final int lo = Long.compareUnsigned(b.tipLo, a.tipLo);
        return lo != 0 ? lo : FLUSH_ORDER.compare(a, b);
    }

    private final int capacity;
    private final List<Entry<T>> entries;
    /** Each sender's head entry within the open window. */
    private final Map<ByteBuffer, Entry<T>> heads;
    /** The arrival sequence, never reset: a log-order stamp per record. */
    private long nextArrival;

    public OrderingWindow(int capacity) {
        if (capacity < 0) {
            throw new IllegalArgumentException("ordering window must be >= 0, got " + capacity);
        }
        this.capacity = capacity;
        this.entries = new ArrayList<>(capacity);
        this.heads = new HashMap<>();
    }

    public int capacity() {
        return capacity;
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public int size() {
        return entries.size();
    }

    /**
     * Hold one record. The sort key is the record's own tip, or the head
     * key of a sender the window already holds.
     *
     * @param sender20 the sender; the all-zero sender is never chained,
     *                 since it is the guard-exempt marker, not an account
     * @param tipHi    the high 64 bits of the unsigned 128-bit tip
     * @param tipLo    the low 64 bits of the tip
     * @param record   what the service relays or rejects at flush
     * @return {@code true} when the window is full and must flush now
     */
    public boolean add(byte[] sender20, long tipHi, long tipLo, T record) {
        final long arrival = nextArrival++;
        final ByteBuffer key = ByteBuffer.wrap(sender20.clone()).asReadOnlyBuffer();
        final Entry<T> head = CanonicalSealerState.isZeroSender(sender20) ? null : heads.get(key);
        final Entry<T> entry = head == null
                ? new Entry<>(tipHi, tipLo, arrival, arrival, record)
                : new Entry<>(head.tipHi, head.tipLo, head.headArrival, arrival, record);
        if (head == null && !CanonicalSealerState.isZeroSender(sender20)) {
            heads.put(key, entry);
        }
        entries.add(entry);
        return entries.size() >= capacity;
    }

    /**
     * Take every held record out, in flush order. The window is empty
     * afterwards and the next add opens a new one.
     */
    public List<T> flush() {
        final List<Entry<T>> sorted = new ArrayList<>(entries);
        sorted.sort(OrderingWindow::byTipDescending);
        final List<T> out = new ArrayList<>(sorted.size());
        for (Entry<T> e : sorted) {
            out.add(e.record);
        }
        entries.clear();
        heads.clear();
        return out;
    }
}
