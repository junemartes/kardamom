package io.kardamom.aeron;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

/**
 * Aeron's backoff idle strategy with a short longest park.
 *
 * <p>An idle thread spins, then yields, then parks. The park doubles from
 * 1 µs up to the cap. A message that arrives while the thread parks waits
 * for the end of the park, so a quiet spell delays the next message by up
 * to the cap. Aeron's own backoff caps the park at 1 ms. Aeron 1.45 sets
 * that cap from no property.</p>
 *
 * <p>The media driver and the archive make an idle strategy from its class
 * name, with no arguments. So this class reads its cap from the
 * environment variable {@code KARDAMOM_IDLE_CAP_US}, in whole
 * microseconds. Unset or blank gives 50 µs. A value that is not a whole
 * number above 0 stops the start.</p>
 */
public final class CappedBackoffIdleStrategy implements IdleStrategy {

    static final String ENV = "KARDAMOM_IDLE_CAP_US";
    static final long DEFAULT_CAP_US = 50;

    private final BackoffIdleStrategy backoff = new BackoffIdleStrategy(
        BackoffIdleStrategy.DEFAULT_MAX_SPINS,
        BackoffIdleStrategy.DEFAULT_MAX_YIELDS,
        BackoffIdleStrategy.DEFAULT_MIN_PARK_PERIOD_NS,
        capNs(System.getenv(ENV)));

    static long capNs(final String raw) {
        final String value = raw == null ? "" : raw.trim();
        final long us = value.isEmpty() ? DEFAULT_CAP_US : micros(value);
        if (us < 1) {
            throw new IllegalArgumentException(ENV + " must be at least 1 µs, got " + us);
        }
        return Math.multiplyExact(us, 1_000L);
    }

    private static long micros(final String value) {
        try {
            return Long.parseLong(value);
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException(
                ENV + " must be a whole number of microseconds, got \"" + value + "\"", e);
        }
    }

    @Override
    public void idle(final int workCount) {
        backoff.idle(workCount);
    }

    @Override
    public void idle() {
        backoff.idle();
    }

    @Override
    public void reset() {
        backoff.reset();
    }

    @Override
    public String alias() {
        return "backoff-capped";
    }

    @Override
    public String toString() {
        return "CappedBackoffIdleStrategy{" + backoff + "}";
    }
}
