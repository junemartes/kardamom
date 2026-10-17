package io.kardamom.sealer.cluster;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

/**
 * The longest idle park of every hot-path thread of the member: the media
 * driver, the archive, the consensus module, the service container and the
 * feeder.
 *
 * <p>A thread that finds no work spins, then yields, then parks. The park
 * doubles from 1 µs up to this cap. A message that arrives while the thread
 * parks waits for the end of the park, so a quiet spell delays the next
 * message by up to the cap at each thread. A lower cap gives a lower latency
 * and more wake-ups: an idle thread at the cap wakes about once per cap.</p>
 *
 * <p>Linux ends a park up to the timer slack of the thread after its
 * deadline. The default slack is 50 µs. The image entrypoint sets the slack
 * of the JVM to 1 µs, so a park of the cap ends within about 1 µs of its
 * deadline.</p>
 *
 * @param maxParkNs the longest park, in nanoseconds; at least 1 µs, as
 *     {@link #parse} makes it
 */
record IdleCap(long maxParkNs) {

    /** The property of the cap, in whole microseconds. It wins over {@link #ENV}. */
    static final String SETTING = "kardamom.cluster.idleCapUs";
    /** The environment variable of the cap, in whole microseconds. */
    static final String ENV = "KARDAMOM_IDLE_CAP_US";
    static final long DEFAULT_US = 50;
    static final IdleCap DEFAULT = new IdleCap(TimeUnit.MICROSECONDS.toNanos(DEFAULT_US));

    /** The cap from {@link #SETTING}, else {@link #ENV}, else the default. */
    static IdleCap fromSettings() {
        return parse(System.getProperty(SETTING, System.getenv(ENV)));
    }

    /**
     * The cap from its raw value in microseconds. Unset or blank gives the
     * default.
     *
     * @throws IllegalArgumentException when the value is not a whole number above 0
     */
    static IdleCap parse(final String raw) {
        final long us = Optional.ofNullable(raw).map(String::trim).filter(s -> !s.isEmpty())
            .map(IdleCap::micros)
            .orElse(DEFAULT_US);
        if (us < 1) {
            throw new IllegalArgumentException(SETTING + " must be at least 1 µs, got " + us);
        }
        return new IdleCap(TimeUnit.MICROSECONDS.toNanos(us));
    }

    private static long micros(final String value) {
        try {
            return Long.parseLong(value);
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException(
                SETTING + " must be a whole number of microseconds, got \"" + value + "\"", e);
        }
    }

    /** A new backoff strategy: Aeron's default spins, yields and first park, then this cap. */
    IdleStrategy strategy() {
        return new BackoffIdleStrategy(
            BackoffIdleStrategy.DEFAULT_MAX_SPINS,
            BackoffIdleStrategy.DEFAULT_MAX_YIELDS,
            BackoffIdleStrategy.DEFAULT_MIN_PARK_PERIOD_NS,
            maxParkNs);
    }
}
