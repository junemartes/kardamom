package io.kardamom.sealer.cluster;

import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.concurrent.ShutdownSignalBarrier;

/**
 * The stop request of a member: a shutdown signal, or the termination hook
 * of a component.
 *
 * <p>The barrier wakes the main thread, which then closes the member. The
 * flag tells the {@link JoinWatchdogThread} that a closed component is part
 * of a requested stop. A component that closes with no stop request has
 * failed. The flag is atomic because the hook threads and the main thread
 * set it, and the watchdog thread reads it.</p>
 */
final class StopRequest {
    private final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
    private final AtomicBoolean requested = new AtomicBoolean();

    /** Request the stop from a termination hook. */
    void signal() {
        requested.set(true);
        barrier.signal();
    }

    /** Wait for a shutdown signal or a termination hook, then mark the stop as requested. */
    void await() {
        barrier.await();
        requested.set(true);
    }

    boolean isRequested() {
        return requested.get();
    }
}
