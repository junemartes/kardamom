package io.kardamom.sealer.bench;

import java.util.concurrent.CountDownLatch;

/**
 * The shared schedule of one run. Batch {@code b} of every twin is due at
 * {@code start + b * interval}. The consumer measures a record's latency
 * from the due time of its batch, so a late offer counts against the
 * cluster, not in its favour.
 */
final class RunClock {

    private final CountDownLatch go = new CountDownLatch(1);
    private final long intervalNs;
    private final long warmupNs;
    private volatile long startNs;

    RunClock(final long intervalNs, final long warmupNs) {
        this.intervalNs = intervalNs;
        this.warmupNs = warmupNs;
    }

    /** Start the run {@code delayMs} from now and release the waiting twins. */
    void start(final long delayMs) {
        startNs = System.nanoTime() + delayMs * 1_000_000L;
        go.countDown();
    }

    void awaitStart() {
        try {
            go.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    long startNs() {
        return startNs;
    }

    long scheduled(final long batchIndex) {
        return startNs + batchIndex * intervalNs;
    }

    /** Whether a batch due at {@code dueNs} belongs to the measured part of the run. */
    boolean measured(final long dueNs) {
        return dueNs >= startNs + warmupNs;
    }
}
