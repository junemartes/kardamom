package io.kardamom.sealer.cluster;

import io.aeron.cluster.ElectionState;
import java.util.EnumSet;
import java.util.Set;

/**
 * Decides when a member has failed to join the cluster.
 *
 * <p>A member that never becomes active is invisible to the rest of the
 * system. Nomad sees a live container. The other members do not need it
 * while quorum holds. It stays INACTIVE forever, and the danger is the
 * next failure. This class turns two such states into an exit.</p>
 *
 * <p>The INIT wedge. The election stays in {@link ElectionState#INIT} for
 * longer than the window. INIT normally lasts milliseconds. The known
 * wedge spins in {@code Election.init}, in {@code awaitLocalSocketsClosed},
 * which has no timeout in Aeron 1.44, and the state counter reads INIT
 * for the whole spin. A restart clears it.</p>
 *
 * <p>The catch-up stall. A follower whose log ends below the purge point
 * of the leader cannot catch up: the leader's archive refuses the replay
 * from the follower's log end. The election then cycles. It reaches a
 * catch-up state, waits for the leader heartbeat timeout, and starts again
 * at INIT. The commit position never passes its highest value. A restart
 * replays the same log and sticks the same way, so only a seed from a peer
 * recovers the member. The rule fires when the commit position has not
 * passed its highest value for the stall window, the member reached a
 * catch-up state in that time, and the election still cycles. The rule
 * does not count the failed catch-ups: the states between two of them can
 * last less than one sample interval.</p>
 *
 * <p>Every other long election state has a good reason. A lone member
 * with quorum lost sits in CANVASS, and never reaches a catch-up state
 * without a leader. A catch-up that receives log moves the commit position
 * up. A member that waits for the live log after its catch-up is not in
 * the cycle. Those must not exit.</p>
 *
 * <p>This class is pure. {@link ClusterNode} owns the polling thread and
 * the exit.</p>
 */
final class JoinWatchdog {

    /** What one observation shows. */
    enum Verdict {
        /** The member joins, runs, or waits for a good reason. */
        NONE,
        /** The election stays in INIT past the window. */
        INIT_WEDGE,
        /** The catch-ups bring no commit progress for the stall window. */
        CATCHUP_STALL
    }

    /** The stall window when none is given: 5 minutes, longer than any catch-up of the purge margin. */
    static final long DEFAULT_STALL_WINDOW_S = 300L;

    /** The states in which a follower fetches the leader's log from the leader's archive. */
    private static final Set<ElectionState> CATCHUP_STATES = EnumSet.of(
            ElectionState.FOLLOWER_LOG_REPLICATION,
            ElectionState.FOLLOWER_CATCHUP_INIT,
            ElectionState.FOLLOWER_CATCHUP_AWAIT,
            ElectionState.FOLLOWER_CATCHUP);

    /** The states of the cycle of a member that cannot catch up: its catch-up, its own replay, and a new election. */
    private static final Set<ElectionState> CYCLE_STATES = EnumSet.of(
            ElectionState.INIT,
            ElectionState.CANVASS,
            ElectionState.FOLLOWER_REPLAY,
            ElectionState.FOLLOWER_LOG_REPLICATION,
            ElectionState.FOLLOWER_CATCHUP_INIT,
            ElectionState.FOLLOWER_CATCHUP_AWAIT,
            ElectionState.FOLLOWER_CATCHUP);

    private final long windowMs;
    /** The stall window; 0 turns the stall rule off. */
    private final long stallWindowMs;
    private long initSinceMs = -1L;
    private long highestCommit = Long.MIN_VALUE;
    /** The time of the last commit progress or closed election; -1 before the first observation. */
    private long progressAtMs = -1L;
    /** Whether the member reached a catch-up state since {@link #progressAtMs}. */
    private boolean triedCatchup = false;

    /**
     * @param windowMs how long INIT may persist before {@link #observe}
     *     reports a wedge.
     * @param stallWindowMs how long failed catch-ups may go on without
     *     commit progress before {@link #observe} reports a stall; 0 for never.
     */
    JoinWatchdog(final long windowMs, final long stallWindowMs) {
        if (windowMs <= 0) {
            throw new IllegalArgumentException("windowMs must be positive: " + windowMs);
        }
        this.windowMs = windowMs;
        this.stallWindowMs = stallWindowMs;
    }

    /**
     * Records one observation of the member.
     *
     * @param state the election state, or null when the counter is not
     *     allocated yet (the module has not started its first election).
     * @param commitPosition the member's commit position.
     * @param nowMs the observation time.
     */
    Verdict observe(final ElectionState state, final long commitPosition, final long nowMs) {
        final boolean wedged = initWedged(state, nowMs);
        final boolean stalled = catchupStalled(state, commitPosition, nowMs);
        if (wedged) {
            return Verdict.INIT_WEDGE;
        }
        return stalled ? Verdict.CATCHUP_STALL : Verdict.NONE;
    }

    /** How long the election has been in INIT, in ms, or 0 when it is not. */
    long initForMs(final long nowMs) {
        return initSinceMs < 0 ? 0L : nowMs - initSinceMs;
    }

    /** How long the member has gone without commit progress, in ms, or 0 before it reaches a catch-up state. */
    long stallForMs(final long nowMs) {
        return triedCatchup ? nowMs - progressAtMs : 0L;
    }

    private boolean initWedged(final ElectionState state, final long nowMs) {
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

    private boolean catchupStalled(final ElectionState state, final long commitPosition, final long nowMs) {
        if (progressAtMs < 0 || state == ElectionState.CLOSED || commitPosition > highestCommit) {
            highestCommit = Math.max(highestCommit, commitPosition);
            progressAtMs = nowMs;
            triedCatchup = false;
        }
        triedCatchup |= CATCHUP_STATES.contains(state);
        return stallWindowMs > 0
                && triedCatchup
                && CYCLE_STATES.contains(state)
                && nowMs - progressAtMs >= stallWindowMs;
    }
}
