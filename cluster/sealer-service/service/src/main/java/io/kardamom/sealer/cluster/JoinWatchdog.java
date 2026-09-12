package io.kardamom.sealer.cluster;

import io.aeron.cluster.ElectionState;

/**
 * Decides when a member has failed to join the cluster.
 *
 * <p>A member that never becomes active is invisible to the rest of the
 * system. Nomad sees a live container. The other members do not need it
 * while quorum holds. It stays INACTIVE forever, and the danger is the
 * next failure (issue #195). This class turns that state into an exit.</p>
 *
 * <p>The rule is narrow on purpose. It fires only when the election stays
 * in {@link ElectionState#INIT} for longer than the window. INIT normally
 * lasts milliseconds. The known wedge spins in {@code Election.init}, in
 * {@code awaitLocalSocketsClosed}, which has no timeout in Aeron 1.44,
 * and the state counter reads INIT for the whole spin. Every other
 * election state can last a long time for a good reason: a lone member
 * with quorum lost sits in CANVASS, and a blank member catches up for
 * minutes in FOLLOWER_CATCHUP. Those must not restart.</p>
 *
 * <p>This class is pure. {@link ClusterNode} owns the polling thread and
 * the exit.</p>
 */
final class JoinWatchdog {
    private final long windowMs;
    private long initSinceMs = -1L;

    /**
     * @param windowMs how long INIT may persist before {@link #observe}
     *     reports a wedge.
     */
    JoinWatchdog(final long windowMs) {
        if (windowMs <= 0) {
            throw new IllegalArgumentException("windowMs must be positive: " + windowMs);
        }
        this.windowMs = windowMs;
    }

    /**
     * Records one observation of the election state.
     *
     * @param state the election state, or null when the counter is not
     *     allocated yet (the module has not started its first election).
     * @param nowMs the observation time.
     * @return true when INIT has persisted for longer than the window.
     */
    boolean observe(final ElectionState state, final long nowMs) {
        if (state != ElectionState.INIT) {
            initSinceMs = -1L;
            return false;
        }
        if (initSinceMs < 0) {
            initSinceMs = nowMs;
            return false;
        }
        return nowMs - initSinceMs > windowMs;
    }

    /** How long the election has been in INIT, in ms, or 0 when it is not. */
    long initForMs(final long nowMs) {
        return initSinceMs < 0 ? 0L : nowMs - initSinceMs;
    }
}
