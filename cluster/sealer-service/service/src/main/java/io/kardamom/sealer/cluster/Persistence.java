package io.kardamom.sealer.cluster;

/**
 * How long one condition of the join watchdog has held without a break.
 * The watchdog feeds it one sample a second.
 */
final class Persistence {
    /** The time of the first sample of the current run; -1 when the condition does not hold. */
    private long sinceMs = -1L;

    /**
     * Records one sample, and returns true when the condition has held for
     * longer than {@code limitMs}. A sample where it does not hold ends the run.
     */
    boolean exceeds(final boolean holds, final long nowMs, final long limitMs) {
        if (!holds) {
            sinceMs = -1L;
            return false;
        }
        if (sinceMs < 0) {
            sinceMs = nowMs;
            return false;
        }
        return nowMs - sinceMs > limitMs;
    }

    /** How long the condition has held, in ms, or 0 when it does not hold. */
    long forMs(final long nowMs) {
        return sinceMs < 0 ? 0L : nowMs - sinceMs;
    }
}
