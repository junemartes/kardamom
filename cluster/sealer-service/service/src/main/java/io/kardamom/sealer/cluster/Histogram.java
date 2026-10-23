package io.kardamom.sealer.cluster;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.stream.IntStream;

/**
 * A histogram with fixed buckets that one thread writes and another thread
 * reads. A record allocates nothing: it finds the bucket by a binary search
 * over the bounds and adds one to a preallocated slot.
 *
 * <p>The writer reads each slot plain and writes it with a release store,
 * so it needs no atomic add: it is the only writer. A reader sees each slot
 * whole, but it reads the slots one by one. A read during a record can so
 * see the new bucket count and the old sum, or the reverse. The next scrape
 * corrects that, so a torn read across slots is acceptable here.</p>
 */
final class Histogram {

    /** The bucket bounds, and how the text shows them. */
    enum Layout {
        /**
         * Durations in nanoseconds, shown in seconds: 1 µs to 10 s. The
         * bounds and their text match the buckets of the Rust
         * {@code kardamom_step_seconds}.
         */
        STEP(new long[] {
            1_000L, 2_500L, 5_000L, 10_000L, 25_000L, 50_000L, 100_000L, 250_000L, 500_000L,
            1_000_000L, 2_500_000L, 5_000_000L, 10_000_000L, 25_000_000L, 50_000_000L,
            100_000_000L, 250_000_000L, 500_000_000L,
            1_000_000_000L, 2_500_000_000L, 5_000_000_000L, 10_000_000_000L,
        }, new String[] {
            "0.000001", "0.0000025", "0.000005", "0.00001", "0.000025", "0.00005", "0.0001", "0.00025", "0.0005",
            "0.001", "0.0025", "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5",
            "1", "2.5", "5", "10",
        }, 1e9),
        /** Item counts: the powers of 2 from 1 to 65,536. */
        COUNT(new long[] {
            1L, 2L, 4L, 8L, 16L, 32L, 64L, 128L, 256L, 512L, 1024L, 2048L, 4096L, 8192L, 16384L, 32768L, 65536L,
        }, new String[] {
            "1", "2", "4", "8", "16", "32", "64", "128", "256", "512", "1024", "2048", "4096", "8192", "16384",
            "32768", "65536",
        }, 1.0);

        private final long[] bounds;
        private final String[] le;
        /** The recorded units in one shown unit. */
        private final double unit;

        Layout(final long[] bounds, final String[] le, final double unit) {
            this.bounds = bounds;
            this.le = le;
            this.unit = unit;
        }
    }

    private final Layout layout;
    /** One slot for each bound, then the slot above the last bound. Each slot holds its own count only. */
    private final AtomicLongArray buckets;
    private final AtomicLong sum = new AtomicLong();

    Histogram(final Layout layout) {
        this.layout = layout;
        this.buckets = new AtomicLongArray(layout.bounds.length + 1);
    }

    /**
     * Record one value. A value equal to a bound falls in the bucket of
     * that bound, as the {@code le} label says. Only one thread calls this.
     */
    void record(final long value) {
        final int found = Arrays.binarySearch(layout.bounds, value);
        final int bucket = found >= 0 ? found : -found - 1;
        buckets.lazySet(bucket, buckets.getPlain(bucket) + 1);
        sum.lazySet(sum.getPlain() + value);
    }

    /** Record the nanoseconds from {@code startedNs} to now. */
    void recordSince(final long startedNs) {
        record(System.nanoTime() - startedNs);
    }

    /** Write the cumulative buckets, the sum and the count of the series {@code name} with {@code labels}. */
    void renderTo(final MetricsText text, final String name, final String labels) {
        final long[] counts = new long[buckets.length()];
        Arrays.setAll(counts, buckets::get);
        Arrays.parallelPrefix(counts, Long::sum);
        IntStream.range(0, layout.le.length)
            .forEach(i -> text.sample(name + "_bucket", labels + ",le=\"" + layout.le[i] + "\"", counts[i]));
        final long count = counts[counts.length - 1];
        text.sample(name + "_bucket", labels + ",le=\"+Inf\"", count);
        text.sample(name + "_sum", labels, sum.get() / layout.unit);
        text.sample(name + "_count", labels, count);
    }
}
