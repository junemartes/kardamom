package io.kardamom.aeron;

import io.aeron.driver.Configuration;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

/**
 * Aeron's backoff idle strategy with a settable longest park.
 *
 * <p>An idle thread spins {@link Configuration#IDLE_MAX_SPINS} times, then
 * yields {@link Configuration#IDLE_MAX_YIELDS} times, then parks. The park
 * doubles from {@link Configuration#IDLE_MIN_PARK_NS} up to the cap. These
 * are the values of Aeron's own default backoff, which caps the park at
 * 1 ms. A message that arrives while the thread parks waits for the end of
 * the park, so a quiet spell delays the next message by up to the cap.
 * Aeron 1.45 sets that cap from no property.</p>
 *
 * <p>The media driver and the archive make an idle strategy from its class
 * name, with no arguments. So the no-argument constructor reads the cap from
 * the environment variable {@code KARDAMOM_IDLE_CAP_US}, in whole
 * microseconds. Unset or blank gives 1000 µs, the cap of Aeron's own
 * backoff. {@link #capNs} refuses a value that is not a whole number from 1
 * to 1,000,000. The Rust parser of the same variable takes the same values
 * (idle-cap-cases.txt).</p>
 *
 * <p>The sealer compiles this file into its jar and parses its own setting
 * with {@link #capNs}.</p>
 */
public final class CappedBackoffIdleStrategy implements IdleStrategy {

    /** The environment variable of the cap, in whole microseconds. */
    public static final String ENV = "KARDAMOM_IDLE_CAP_US";
    /** The cap when the setting is unset or blank: the cap of Aeron's own backoff. */
    public static final long DEFAULT_CAP_US = 1_000;
    /** The largest cap: 1 s. */
    public static final long MAX_CAP_US = 1_000_000;

    private final BackoffIdleStrategy backoff;

    /** The strategy with the cap from {@link #ENV}. */
    public CappedBackoffIdleStrategy() {
        this(capNs(ENV, System.getenv(ENV)));
    }

    /** The strategy with the longest park {@code maxParkNs}, a value of {@link #capNs}. */
    public CappedBackoffIdleStrategy(final long maxParkNs) {
        backoff = new BackoffIdleStrategy(
            Configuration.IDLE_MAX_SPINS,
            Configuration.IDLE_MAX_YIELDS,
            Configuration.IDLE_MIN_PARK_NS,
            maxParkNs);
    }

    /**
     * The cap in nanoseconds from the raw value {@code raw} of the setting
     * {@code name}, in whole microseconds. Null or blank gives
     * {@link #DEFAULT_CAP_US}.
     *
     * @throws IllegalArgumentException when the value is not a whole number from 1 to {@link #MAX_CAP_US}
     */
    public static long capNs(final String name, final String raw) {
        final String value = raw == null ? "" : raw.trim();
        final long us = value.isEmpty() ? DEFAULT_CAP_US : micros(name, value);
        if (us < 1 || us > MAX_CAP_US) {
            throw new IllegalArgumentException(
                name + " must be from 1 to " + MAX_CAP_US + " µs, got " + us);
        }
        return us * 1_000L;
    }

    private static long micros(final String name, final String value) {
        try {
            return Long.parseLong(value);
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException(
                name + " must be a whole number of microseconds, got \"" + value + "\"", e);
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
