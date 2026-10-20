package io.kardamom.sealer.bench;

/**
 * A log-linear histogram of nanosecond values, with a relative error below
 * 3.2%. One thread records. It allocates nothing after construction.
 *
 * <p>A value below 64 has its own bucket. A larger value with its highest
 * set bit at {@code msb} falls in one of 32 buckets of width
 * {@code 2^(msb - 5)}.</p>
 */
final class LatencyHistogram {

    private static final int LINEAR = 64;
    private static final int SUB = 32;
    private static final int BUCKETS = LINEAR + 58 * SUB;

    private final long[] counts = new long[BUCKETS];
    private long total;
    private long max;

    void record(final long valueNs) {
        final long v = Math.max(0L, valueNs);
        counts[bucketOf(v)]++;
        total++;
        max = Math.max(max, v);
    }

    long count() {
        return total;
    }

    long max() {
        return max;
    }

    /** The lower bound of the bucket that holds quantile {@code q}, from 0 to 1. */
    long quantile(final double q) {
        final long rank = Math.max(1L, (long) Math.ceil(q * total));
        long seen = 0;
        int bucket = 0;
        for (; bucket < BUCKETS - 1 && seen + counts[bucket] < rank; bucket++) {
            seen += counts[bucket];
        }
        return lowerBound(bucket);
    }

    private static int bucketOf(final long v) {
        if (v < LINEAR) {
            return (int) v;
        }
        final int shift = (63 - Long.numberOfLeadingZeros(v)) - 5;
        return LINEAR + (shift - 1) * SUB + (int) ((v >>> shift) - SUB);
    }

    private static long lowerBound(final int bucket) {
        if (bucket < LINEAR) {
            return bucket;
        }
        final int shift = (bucket - LINEAR) / SUB + 1;
        final long sub = (bucket - LINEAR) % SUB + SUB;
        return sub << shift;
    }
}
