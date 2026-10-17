package io.kardamom.sealer.cluster;

import io.kardamom.aeron.CappedBackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

/**
 * The longest idle park of every hot-path thread of the member: the media
 * driver, the archive, the consensus module, the service container and the
 * feeder.
 *
 * <p>Each thread runs Aeron's default backoff ({@link CappedBackoffIdleStrategy})
 * with this cap as its longest park. A message that arrives while the thread
 * parks waits for the end of the park, so a quiet spell delays the next
 * message by up to the cap at each thread. A lower cap gives a lower latency
 * and more wake-ups: an idle thread at the cap wakes about once per cap.
 * Unset, the cap is 1 ms, the cap of Aeron's own backoff.</p>
 *
 * <p>Linux ends a park up to the timer slack of the thread after its
 * deadline. The default slack is 50 µs. When the deploy sets the cap, the
 * image entrypoint sets the slack of the JVM to 1 µs.</p>
 *
 * @param maxParkNs the longest park, in nanoseconds, as
 *     {@link CappedBackoffIdleStrategy#capNs} makes it
 */
record IdleCap(long maxParkNs) {

    /** The property of the cap, in whole microseconds. It wins over the environment variable. */
    static final String SETTING = "kardamom.cluster.idleCapUs";
    /** The cap when the setting is unset: 1 ms. */
    static final IdleCap DEFAULT = parse(null);

    /** The cap from {@link #SETTING}, else {@link CappedBackoffIdleStrategy#ENV}, else the default. */
    static IdleCap fromSettings() {
        return parse(System.getProperty(SETTING, System.getenv(CappedBackoffIdleStrategy.ENV)));
    }

    /**
     * The cap from its raw value in microseconds.
     *
     * @throws IllegalArgumentException when {@link CappedBackoffIdleStrategy#capNs} refuses the value
     */
    static IdleCap parse(final String raw) {
        return new IdleCap(CappedBackoffIdleStrategy.capNs(SETTING, raw));
    }

    /** A new idle strategy of one thread with this cap. */
    IdleStrategy strategy() {
        return new CappedBackoffIdleStrategy(maxParkNs);
    }
}
