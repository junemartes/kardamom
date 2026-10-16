package io.kardamom.sealer.cluster;

import io.aeron.cluster.ClusterControl;
import io.aeron.cluster.ElectionState;
import java.util.EnumSet;
import java.util.Set;

/**
 * Decides when a member has failed to join the cluster.
 *
 * <p>A member that never becomes active is invisible to the rest of the
 * system. Nomad sees a live container. The other members do not need it
 * while quorum holds. It stays INACTIVE forever, and the danger is the
 * next failure. This class turns such states into an exit.</p>
 *
 * <p>The INIT wedge. The election stays in {@link ElectionState#INIT} for
 * longer than the window. INIT normally lasts milliseconds. The known
 * wedge spins in {@code Election.init}, in {@code awaitLocalSocketsClosed},
 * which has no timeout in Aeron 1.44, and the state counter reads INIT
 * for the whole spin. A restart clears it.</p>
 *
 * <p>The leader wedge. The completion of an election throws. Aeron 1.44
 * completes a leader's election in this order: it activates the control
 * toggle (INACTIVE to NEUTRAL), makes the recovery plan (archive requests,
 * each up to the archive message timeout), clears the election, and adds
 * the ingress subscription (up to the driver timeout). When the add fails,
 * for example on a name lookup that fails, the member runs as the leader
 * with no ingress, and the state counter keeps the last election state for
 * ever. The rule fires when the toggle is active and the election is not
 * CLOSED for longer than the half-elected limit. The limit is longer than
 * the two blocking waits of a completion that succeeds. A new election
 * first sets the state counter to INIT, and its first step then sets the
 * toggle to INACTIVE. A new consensus module starts with an INACTIVE
 * toggle. These moments last one duty cycle, far below the limit.</p>
 *
 * <p>Progress. The other rules measure the time since the last progress.
 * Progress is a new highest commit position of the member, a new highest
 * position of its log recording while it replicates the leader's log, a
 * new highest commit position of a peer while the member waits as a
 * leader, or a closed election. A replay of the member's own log moves the
 * commit position up from a lower value to the same end, which is no
 * progress.</p>
 *
 * <p>The leader stall. The member waits in LEADER_READY with no progress
 * for longer than the stall limit. Aeron 1.44 gives LEADER_READY no
 * timeout: it waits until each member that voted, or after the leader
 * heartbeat timeout a quorum, appends at its position in its term. A
 * member that voted but can never join keeps it there for ever. A slow
 * follower that replays or catches up moves its own commit position, which
 * the leader reads through the peer probe, so the rule waits for it. A
 * relaunch replays the member's own log, and the members elect again.</p>
 *
 * <p>The follower stall. The member reached a follower state, which means
 * that it found a leader, and it then makes no progress for longer than the
 * stall limit. Two shapes reach it. The election cycles: each join attempt
 * fails, and a new election starts. Or the election stays in log
 * replication: Aeron 1.44 checks the deadline of log replication only after
 * the archive signals both the end of the replication and the stop of the
 * recording, and a replication that fails before the recording starts sends
 * no stop. A relaunch keeps the recording log, replays it, and elects
 * again.</p>
 *
 * <p>The catch-up stall. A follower whose log ends below the purge point
 * of the leader cannot catch up: the leader's archive refuses the replay
 * from the follower's log end. The election then cycles through its
 * catch-up, or waits in log replication, and the commit position never
 * passes its highest value. A relaunch replays the same log and sticks the
 * same way, so only a seed from a peer recovers the member. When the member
 * reached a catch-up state, the follower stall first asks for a peer check.
 * When every peer answers with an open election, the leader that the member
 * found is not live, so its log tail is not the cause, and no peer can seed
 * it: the member relaunches and keeps its log. In all other cases the
 * member waits until the catch-up stall window and then drops its recording
 * log, so that the relaunch seeds from a peer. A peer that does not answer,
 * or a member with no admin port, gives no answer, and the member then
 * seeds as it does with no probe.</p>
 *
 * <p>Every other long election state has a good reason. A lone member
 * with quorum lost sits in CANVASS, and never reaches a follower state
 * without a leader. A catch-up that receives log moves the commit position
 * up. A coordinated restart waits in CANVASS until the members arrive.
 * Those must not exit.</p>
 *
 * <p>This class is pure. {@link JoinWatchdogThread} owns the samples, the
 * peer checks, and the exit.</p>
 */
final class JoinWatchdog {

    /** What one observation shows. */
    enum Verdict {
        /** The member joins, runs, or waits for a good reason. */
        NONE,
        /** The election stays in INIT past the window. */
        INIT_WEDGE,
        /** The catch-ups bring no progress for the catch-up stall window, and a peer has a closed election. */
        CATCHUP_STALL,
        /** The election of a leader did not complete, but its control toggle is active. */
        LEADER_WEDGE,
        /** The member waits in LEADER_READY with no progress for the stall limit. */
        LEADER_STALL,
        /** The member found a leader, and then makes no progress for the stall limit. */
        FOLLOWER_STALL,
        /** The member tried a catch-up with no progress; the decision needs a peer check. */
        PEER_CHECK
    }

    /** The stall window when none is given: 5 minutes, longer than any catch-up of the purge margin. */
    static final long DEFAULT_STALL_WINDOW_S = 300L;

    /**
     * The windows of the rules.
     *
     * @param initMs how long INIT may persist.
     * @param catchupStallMs how long a member that tried a catch-up may go
     *     without progress before it drops its log; 0 never drops it.
     * @param halfElectedMs how long an open election with an active control
     *     toggle may last: longer than the blocking waits of a completion.
     * @param stallMs how long a leader in LEADER_READY, or a member that
     *     found a leader, may go without progress before it relaunches.
     */
    record Limits(long initMs, long catchupStallMs, long halfElectedMs, long stallMs) {
    }

    /**
     * The Aeron timeouts of the member, from which the half-elected limit
     * and the stall limit follow. The job scales the archive message
     * timeout and the driver timeout with the Aeron stall tolerance.
     *
     * @param leaderHeartbeatMs the leader heartbeat timeout.
     * @param archiveMessageMs the archive message timeout.
     * @param driverMs the media driver timeout.
     */
    record Timeouts(long leaderHeartbeatMs, long archiveMessageMs, long driverMs) {

        /** How many join attempts of a follower the stall limit allows. */
        private static final int JOIN_ATTEMPTS = 2;

        /**
         * The half-elected limit of the leader wedge: longer than the two
         * blocking waits of an election completion that succeeds. The
         * archive requests of the recovery plan wait up to the archive
         * message timeout, and the ingress add waits up to the driver
         * timeout.
         */
        long halfElectedLimitMs() {
            return archiveMessageMs + driverMs;
        }

        /**
         * The stall limit of the leader stall and the follower stall: longer
         * than the waits of two join attempts of a follower. One attempt
         * waits for the leader's log image and for the ready notice, each up
         * to the leader heartbeat timeout, for one archive request (a
         * catch-up replay or a log replication), up to the archive message
         * timeout, and for the adds of the log subscription, up to the
         * driver timeout. A first attempt can fail while the leader still
         * replays or joins its own log, so the limit allows a second one.
         * The work between the waits, a replay or a catch-up, moves a commit
         * position, and progress restarts the count.
         */
        long stallLimitMs() {
            return JOIN_ATTEMPTS * (2 * leaderHeartbeatMs + archiveMessageMs + driverMs);
        }

        /** The limits of the rules with these timeouts. */
        Limits limits(final long initMs, final long catchupStallMs) {
            return new Limits(initMs, catchupStallMs, halfElectedLimitMs(), stallLimitMs());
        }
    }

    /**
     * One observation of the member.
     *
     * @param state the election state.
     * @param commitPosition the member's commit position.
     * @param recordingPosition the position of the member's log recording
     *     while it replicates the leader's log, else its last value.
     * @param peerCommitMark the sum of the highest commit positions of the
     *     peers while the member waits as a leader, else its last value.
     * @param toggleCode the value of the control toggle counter of the
     *     consensus module. Any value but INACTIVE means that a leader's
     *     election completed: NEUTRAL, or an action such as SNAPSHOT.
     * @param nowMs the observation time.
     */
    record Sample(
            ElectionState state,
            long commitPosition,
            long recordingPosition,
            long peerCommitMark,
            long toggleCode,
            long nowMs) {
    }

    /**
     * What a peer check shows about the elections of the other members.
     */
    enum PeerElections {
        /** A peer answered with a closed election: it follows a live leader and can seed this member. */
        CLOSED,
        /** The probe knows peers, and every one answered with an open election. */
        NONE_CLOSED,
        /** The probe knows no peer, or a peer did not answer. */
        UNKNOWN
    }

    /** A set of election states. */
    private record States(Set<ElectionState> set) {
        static States of(final ElectionState first, final ElectionState... rest) {
            return new States(EnumSet.of(first, rest));
        }

        States plus(final States other) {
            final Set<ElectionState> both = EnumSet.copyOf(set);
            both.addAll(other.set);
            return new States(both);
        }

        boolean contains(final ElectionState state) {
            return set.contains(state);
        }
    }

    /** The states in which a follower fetches the leader's log from the leader's archive. */
    private static final States CATCHUP_STATES = States.of(
            ElectionState.FOLLOWER_LOG_REPLICATION,
            ElectionState.FOLLOWER_CATCHUP_INIT,
            ElectionState.FOLLOWER_CATCHUP_AWAIT,
            ElectionState.FOLLOWER_CATCHUP);

    /** The states in which a follower joins the leader's live log. */
    private static final States JOIN_STATES = States.of(
            ElectionState.FOLLOWER_LOG_INIT,
            ElectionState.FOLLOWER_LOG_AWAIT,
            ElectionState.FOLLOWER_READY);

    /**
     * The states of the cycle of a member that cannot catch up: its
     * catch-up, its own replay, and a new election. INIT is not one of
     * them: an election that stays in INIT is the INIT wedge, and the INIT
     * rule acts on it at its own window.
     */
    private static final States CYCLE_STATES = CATCHUP_STATES.plus(
            States.of(ElectionState.CANVASS, ElectionState.FOLLOWER_REPLAY));

    /** The states that a member reaches only after it found a leader. */
    private static final States FOLLOWER_STATES = CATCHUP_STATES.plus(JOIN_STATES)
            .plus(States.of(ElectionState.FOLLOWER_REPLAY));

    /** The cycle states, and the states of a join to the live log that fails. */
    private static final States FOLLOWER_CYCLE_STATES = CYCLE_STATES.plus(JOIN_STATES);

    private final Limits limits;
    private final Persistence init = new Persistence();
    private final Persistence halfElected = new Persistence();
    private final Persistence leaderReady = new Persistence();
    private long highestCommit = Long.MIN_VALUE;
    private long highestRecording = Long.MIN_VALUE;
    private long highestPeerMark = Long.MIN_VALUE;
    /** The time of the last progress; -1 before the first observation. */
    private long progressAtMs = -1L;
    /** Whether the member reached a follower state since {@link #progressAtMs}. */
    private boolean triedFollower = false;
    /** Whether the member reached a catch-up state since {@link #progressAtMs}. */
    private boolean triedCatchup = false;
    /** Whether a peer check found a peer with a closed election since {@link #progressAtMs}. */
    private boolean peerSeen = false;
    /** The election state of the last observation. */
    private ElectionState lastState = ElectionState.INIT;

    JoinWatchdog(final Limits limits) {
        if (limits.initMs() <= 0) {
            throw new IllegalArgumentException("the INIT window must be positive: " + limits.initMs());
        }
        this.limits = limits;
    }

    /** Records one observation of the member. */
    Verdict observe(final Sample sample) {
        final long nowMs = sample.nowMs();
        final ElectionState state = sample.state();
        final boolean wedged = init.exceeds(state == ElectionState.INIT, nowMs, limits.initMs());
        final boolean leaderWedged = halfElected.exceeds(
            sample.toggleCode() != ClusterControl.ToggleState.INACTIVE.code() && state != ElectionState.CLOSED,
            nowMs,
            limits.halfElectedMs());
        track(sample);
        final boolean leaderStalled = leaderReady.exceeds(state == ElectionState.LEADER_READY, nowMs, limits.stallMs())
            && sinceProgressMs(nowMs) > limits.stallMs();
        if (wedged) {
            return Verdict.INIT_WEDGE;
        }
        if (leaderWedged) {
            return Verdict.LEADER_WEDGE;
        }
        return leaderStalled ? Verdict.LEADER_STALL : followerVerdict(state, nowMs);
    }

    /**
     * The verdict after a {@link Verdict#PEER_CHECK}.
     *
     * <ul>
     *   <li>A peer has a closed election: the member drops its log once the
     *       catch-up stall window has passed, and waits until then with no
     *       more checks.</li>
     *   <li>Every peer answered with an open election: the leader that the
     *       member found is not live, so its log tail is not the cause. The
     *       member relaunches and keeps its log.</li>
     *   <li>The answer is unknown: the member checks again at the next
     *       sample, and drops its log once the catch-up stall window has
     *       passed. With no admin server, or with a peer that stays silent,
     *       the member seeds as it does with no probe at all.</li>
     * </ul>
     */
    Verdict afterPeerCheck(final PeerElections peers, final long nowMs) {
        if (peers == PeerElections.NONE_CLOSED) {
            return Verdict.FOLLOWER_STALL;
        }
        peerSeen = peers == PeerElections.CLOSED;
        return seedDue(nowMs) ? Verdict.CATCHUP_STALL : Verdict.NONE;
    }

    /** How long the election has been in INIT, in ms, or 0 when it is not. */
    long initForMs(final long nowMs) {
        return init.forMs(nowMs);
    }

    /** How long the election has stayed open with an active toggle, in ms, or 0 when it has not. */
    long halfElectedForMs(final long nowMs) {
        return halfElected.forMs(nowMs);
    }

    /** How long the member has gone without progress, in ms, or 0 before it reaches a follower state. */
    long stallForMs(final long nowMs) {
        return triedFollower ? sinceProgressMs(nowMs) : 0L;
    }

    /** How long the member has gone without progress, in ms. */
    long sinceProgressMs(final long nowMs) {
        return nowMs - progressAtMs;
    }

    private void track(final Sample sample) {
        final boolean progress = progressAtMs < 0
            || sample.state() == ElectionState.CLOSED
            || sample.commitPosition() > highestCommit
            || sample.recordingPosition() > highestRecording
            || sample.peerCommitMark() > highestPeerMark;
        if (progress) {
            highestCommit = Math.max(highestCommit, sample.commitPosition());
            highestRecording = Math.max(highestRecording, sample.recordingPosition());
            highestPeerMark = Math.max(highestPeerMark, sample.peerCommitMark());
            progressAtMs = sample.nowMs();
            triedFollower = false;
            triedCatchup = false;
            peerSeen = false;
        }
        triedFollower |= FOLLOWER_STATES.contains(sample.state());
        triedCatchup |= CATCHUP_STATES.contains(sample.state());
        lastState = sample.state();
    }

    /**
     * The follower stall and the catch-up stall. A member that never tried
     * a catch-up relaunches at the stall limit. A member that tried one asks
     * for a peer check at the stall limit, at each sample while the answer
     * is unknown, and again when the seed is due.
     */
    private Verdict followerVerdict(final ElectionState state, final long nowMs) {
        if (!triedFollower || !FOLLOWER_CYCLE_STATES.contains(state) || stallForMs(nowMs) < limits.stallMs()) {
            return Verdict.NONE;
        }
        if (!triedCatchup) {
            return Verdict.FOLLOWER_STALL;
        }
        return !peerSeen || seedDue(nowMs) ? Verdict.PEER_CHECK : Verdict.NONE;
    }

    /** Whether the catch-up stall window has passed in a cycle state, after a catch-up. */
    private boolean seedDue(final long nowMs) {
        return limits.catchupStallMs() > 0
            && triedCatchup
            && CYCLE_STATES.contains(lastState)
            && stallForMs(nowMs) >= limits.catchupStallMs();
    }
}
