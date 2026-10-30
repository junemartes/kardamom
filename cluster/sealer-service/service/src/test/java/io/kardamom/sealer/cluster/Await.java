package io.kardamom.sealer.cluster;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;

/** A bounded wait for an observation, for the member-process tests. */
final class Await {

    private static final long POLL_MS = 50;

    private Await() {
    }

    /**
     * Poll {@code probe} until it gives a value, and return the value. Fail
     * with {@code what} and the last observation when {@code bound} passes.
     */
    static <T> T until(final Duration bound, final String what, final Supplier<Optional<T>> probe) {
        final long deadline = System.nanoTime() + bound.toNanos();
        Optional<T> value = probe.get();
        while (value.isEmpty()) {
            if (System.nanoTime() - deadline > 0) {
                throw new AssertionError(what + " within " + bound.toMillis() + " ms");
            }
            pause(POLL_MS);
            value = probe.get();
        }
        return value.get();
    }

    /** Poll until {@code condition} holds; fail with {@code what} when {@code bound} passes. */
    static Duration holds(final Duration bound, final String what, final java.util.function.BooleanSupplier condition) {
        final long start = System.nanoTime();
        until(bound, what, () -> condition.getAsBoolean() ? Optional.of(true) : Optional.empty());
        return Duration.ofNanos(System.nanoTime() - start);
    }

    static void pause(final long ms) {
        try {
            Thread.sleep(ms);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted", e);
        }
    }
}
