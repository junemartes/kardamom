package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.aeron.cluster.ClusterControl;
import io.aeron.cluster.ElectionState;
import io.kardamom.sealer.cluster.JoinWatchdog.PeerElections;
import io.kardamom.sealer.cluster.JoinWatchdog.Verdict;
import java.util.List;
import java.util.function.LongFunction;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/** Pure-function tests for the join watchdog rules. No Aeron, no cluster. */
final class JoinWatchdogTest {
    private static final long WINDOW_MS = 60_000L;
    private static final long STALL_MS = 300_000L;
    /** The timeouts of the job's CI values: the default heartbeat timeout, 30 s archive and driver timeouts. */
    private static final JoinWatchdog.Timeouts CI = new JoinWatchdog.Timeouts(10_000L, 30_000L, 30_000L);
    private static final long HALF_ELECTED_MS = CI.halfElectedLimitMs();
    private static final long LIMIT_MS = CI.stallLimitMs();
    private static final long INACTIVE = ClusterControl.ToggleState.INACTIVE.code();
    private static final long NEUTRAL = ClusterControl.ToggleState.NEUTRAL.code();
    private static final long NO_RELAUNCH = JoinWatchdog.NO_RELAUNCH_POSITION;
    /** A commit position that the samples of one test do not change. */
    private static final long COMMIT = 4_096L;
    /** The samples of a test, one a second. */
    private static final long SECONDS = 2 * STALL_MS / 1_000L;

    /**
     * One cycle of a follower that cannot catch up, sampled once a second:
     * ten seconds of waiting for the leader's replay, then a new election.
     */
    private static final List<ElectionState> STUCK_CYCLE = List.of(
            ElectionState.CANVASS,
            ElectionState.FOLLOWER_REPLAY,
            ElectionState.FOLLOWER_CATCHUP_INIT,
            ElectionState.FOLLOWER_CATCHUP_AWAIT,
            ElectionState.FOLLOWER_CATCHUP_AWAIT,
            ElectionState.FOLLOWER_CATCHUP_AWAIT,
            ElectionState.FOLLOWER_CATCHUP_AWAIT,
            ElectionState.FOLLOWER_CATCHUP_AWAIT,
            ElectionState.FOLLOWER_CATCHUP_AWAIT,
            ElectionState.FOLLOWER_CATCHUP_AWAIT,
            ElectionState.FOLLOWER_CATCHUP_AWAIT,
            ElectionState.FOLLOWER_CATCHUP_AWAIT,
            ElectionState.INIT);

    /**
     * One cycle of a follower whose service applied a log tail that the
     * leader does not hold: each join to the live log fails at once on the
     * join position, and a new election starts.
     */
    private static final List<ElectionState> JOIN_CYCLE = List.of(
            ElectionState.INIT,
            ElectionState.CANVASS,
            ElectionState.FOLLOWER_LOG_AWAIT,
            ElectionState.INIT,
            ElectionState.FOLLOWER_LOG_AWAIT);

    private static JoinWatchdog watchdog() {
        return watchdog(STALL_MS);
    }

    private static JoinWatchdog watchdog(final long catchupStallMs) {
        return new JoinWatchdog(CI.limits(WINDOW_MS, catchupStallMs));
    }

    private static JoinWatchdog.Sample at(final ElectionState state, final long commit, final long nowMs) {
        return toggled(state, commit, nowMs, INACTIVE);
    }

    private static JoinWatchdog.Sample toggled(
            final ElectionState state, final long commit, final long nowMs, final long toggle) {
        return new JoinWatchdog.Sample(state, commit, -1L, 0L, NO_RELAUNCH, toggle, nowMs);
    }

    /** The state of a cycle at second {@code t}. */
    /** A sample of a member whose recording log fixes the position that a relaunch reaches. */
    private static JoinWatchdog.Sample truncatedAt(final ElectionState state, final long relaunch, final long nowMs) {
        return new JoinWatchdog.Sample(state, COMMIT, -1L, 0L, relaunch, INACTIVE, nowMs);
    }

    private static ElectionState cycleAt(final List<ElectionState> cycle, final long t) {
        return cycle.get((int) (t % cycle.size()));
    }

    /** The first exit of a member that sees {@code samples}, with the peer checks answered by {@code peers}. */
    private record Exit(Verdict verdict, long second) {
        static final Exit NEVER = new Exit(Verdict.NONE, -1L);
    }

    private static Exit firstExit(
            final JoinWatchdog w, final LongFunction<JoinWatchdog.Sample> samples, final PeerElections peers) {
        return LongStream.range(0, SECONDS)
                .mapToObj(t -> new Exit(resolve(w, w.observe(samples.apply(t)), peers, t), t))
                .filter(exit -> exit.verdict() != Verdict.NONE)
                .findFirst()
                .orElse(Exit.NEVER);
    }

    private static Verdict resolve(final JoinWatchdog w, final Verdict verdict, final PeerElections peers, final long t) {
        return verdict == Verdict.PEER_CHECK ? w.afterPeerCheck(peers, t * 1_000L) : verdict;
    }

    @Test
    void theLimitsFollowTheTimeouts() {
        assertEquals(60_000L, HALF_ELECTED_MS);
        assertEquals(2 * (2 * 10_000L + 30_000L + 30_000L), LIMIT_MS);
        final JoinWatchdog.Timeouts defaults = new JoinWatchdog.Timeouts(10_000L, 10_000L, 10_000L);
        assertEquals(80_000L, defaults.stallLimitMs());
        assertEquals(new JoinWatchdog.Limits(WINDOW_MS, STALL_MS, 20_000L, 80_000L),
                defaults.limits(WINDOW_MS, STALL_MS));
    }

    @Test
    void firesWhenInitPersistsPastTheWindow() {
        final JoinWatchdog w = watchdog();
        assertEquals(Verdict.NONE, w.observe(at(ElectionState.INIT, 0L, 0L)));
        assertEquals(Verdict.NONE, w.observe(at(ElectionState.INIT, 0L, WINDOW_MS)));
        assertEquals(Verdict.INIT_WEDGE, w.observe(at(ElectionState.INIT, 0L, WINDOW_MS + 1L)));
        assertEquals(WINDOW_MS + 1L, w.initForMs(WINDOW_MS + 1L));
    }

    @Test
    void aNormalElectionNeverFires() {
        final JoinWatchdog w = watchdog();
        assertEquals(Verdict.NONE, w.observe(at(ElectionState.INIT, 0L, 0L)));
        assertEquals(Verdict.NONE, w.observe(at(ElectionState.CANVASS, 0L, 5L)));
        assertEquals(Verdict.NONE, w.observe(at(ElectionState.FOLLOWER_BALLOT, 0L, 50L)));
        assertEquals(Verdict.NONE, w.observe(at(ElectionState.FOLLOWER_CATCHUP, 1L, 10 * WINDOW_MS)));
        assertEquals(Verdict.NONE, w.observe(at(ElectionState.CLOSED, 1L, 20 * WINDOW_MS)));
        assertEquals(0L, w.initForMs(20 * WINDOW_MS));
    }

    @Test
    void aLoneMemberWithQuorumLostDoesNotFire() {
        // A lone member with quorum lost canvasses for minutes. CANVASS is
        // not INIT, and it never reaches a follower state without a leader.
        assertEquals(Exit.NEVER,
                firstExit(watchdog(), t -> at(ElectionState.CANVASS, COMMIT, t * 1_000L), PeerElections.NONE_CLOSED));
    }

    @Test
    void aCoordinatedRestartDoesNotFire() {
        // The members canvass until all arrive, one wins, and a follower
        // replays its own log up to the leader's log and closes.
        final JoinWatchdog w = watchdog();
        final LongFunction<JoinWatchdog.Sample> restart = t -> t < 120
                ? at(ElectionState.CANVASS, COMMIT, t * 1_000L)
                : t < 180
                ? at(ElectionState.FOLLOWER_REPLAY, COMMIT + (t - 120) * 1_000L, t * 1_000L)
                : at(ElectionState.CLOSED, COMMIT + 60_000L + t, t * 1_000L);
        assertEquals(Exit.NEVER, firstExit(w, restart, PeerElections.NONE_CLOSED));
    }

    @Test
    void leavingInitResetsTheClock() {
        final JoinWatchdog w = watchdog();
        assertEquals(Verdict.NONE, w.observe(at(ElectionState.INIT, 0L, 0L)));
        assertEquals(Verdict.NONE, w.observe(at(ElectionState.CANVASS, 0L, WINDOW_MS / 2)));
        // A second election (a leader change) starts INIT again. The
        // window restarts from this observation, not from the first.
        assertEquals(Verdict.NONE, w.observe(at(ElectionState.INIT, 0L, WINDOW_MS)));
        assertEquals(Verdict.NONE, w.observe(at(ElectionState.INIT, 0L, 2 * WINDOW_MS)));
        assertEquals(Verdict.INIT_WEDGE, w.observe(at(ElectionState.INIT, 0L, 2 * WINDOW_MS + 1L)));
    }

    @Test
    void rejectsANonPositiveWindow() {
        assertThrows(IllegalArgumentException.class, () -> new JoinWatchdog(CI.limits(0L, STALL_MS)));
    }

    @Test
    void aStuckCatchupWithNoLivePeerRelaunchesAtTheStallLimit() {
        // The leader that the member found is not live, so its log tail is
        // not the cause, and a seed would find no peer. The relaunch keeps the log.
        final JoinWatchdog w = watchdog();
        final Exit exit = firstExit(w, t -> at(cycleAt(STUCK_CYCLE, t), COMMIT, t * 1_000L), PeerElections.NONE_CLOSED);
        assertEquals(Verdict.FOLLOWER_STALL, exit.verdict());
        assertEquals(LIMIT_MS / 1_000L, exit.second(), "the first sample at the stall limit relaunches");
    }

    @Test
    void aStuckCatchupWithALivePeerSeedsAtTheCatchupWindow() {
        final JoinWatchdog w = watchdog();
        final Exit exit = firstExit(w, t -> at(cycleAt(STUCK_CYCLE, t), COMMIT, t * 1_000L), PeerElections.CLOSED);
        assertEquals(Verdict.CATCHUP_STALL, exit.verdict());
        assertEquals(STALL_MS / 1_000L, exit.second(), "the first sample at the catch-up window seeds");
        assertEquals(STALL_MS, w.stallForMs(exit.second() * 1_000L));
    }

    @Test
    void aLivePeerDefersTheDecisionWithOneCheck() {
        // The first check finds a live peer. The member then waits, with no
        // more checks, until the seed is due.
        final JoinWatchdog w = watchdog();
        final long checks = LongStream.range(0, STALL_MS / 1_000L)
                .filter(t -> w.observe(at(cycleAt(STUCK_CYCLE, t), COMMIT, t * 1_000L)) == Verdict.PEER_CHECK)
                .peek(t -> assertEquals(Verdict.NONE, w.afterPeerCheck(PeerElections.CLOSED, t * 1_000L)))
                .count();
        assertEquals(1L, checks);
        assertEquals(Verdict.PEER_CHECK, w.observe(at(cycleAt(STUCK_CYCLE, STALL_MS / 1_000L), COMMIT, STALL_MS)));
        assertEquals(Verdict.FOLLOWER_STALL, w.afterPeerCheck(PeerElections.NONE_CLOSED, STALL_MS),
                "a peer that is gone when the seed is due cannot seed the member");
    }

    @Test
    void theReplayOfTheOwnLogIsNotProgress() {
        // Each cycle replays the member's own log again, from a lower
        // commit position up to the same end. Only a new highest position
        // is progress.
        final JoinWatchdog w = watchdog();
        assertEquals(Verdict.NONE, w.observe(at(ElectionState.FOLLOWER_REPLAY, COMMIT, 0L)));
        assertEquals(Verdict.NONE, w.observe(at(ElectionState.FOLLOWER_CATCHUP_AWAIT, COMMIT, 1_000L)));
        assertEquals(Verdict.NONE, w.observe(at(ElectionState.FOLLOWER_REPLAY, 0L, 2_000L)));
        assertEquals(Verdict.NONE, w.observe(at(ElectionState.FOLLOWER_REPLAY, COMMIT, 3_000L)));
        assertEquals(Verdict.PEER_CHECK, w.observe(at(ElectionState.FOLLOWER_CATCHUP_AWAIT, COMMIT, STALL_MS)));
        assertEquals(Verdict.CATCHUP_STALL, w.afterPeerCheck(PeerElections.CLOSED, STALL_MS));
    }

    @Test
    void aCatchupThatReceivesLogNeverStalls() {
        assertEquals(Exit.NEVER, firstExit(watchdog(),
                t -> at(cycleAt(STUCK_CYCLE, t), COMMIT + t / 30L, t * 1_000L), PeerElections.CLOSED));
    }

    @Test
    void aClosedElectionResetsTheStall() {
        final JoinWatchdog w = watchdog();
        assertEquals(Verdict.NONE, w.observe(at(ElectionState.FOLLOWER_CATCHUP_AWAIT, COMMIT, 0L)));
        assertEquals(Verdict.NONE, w.observe(at(ElectionState.CLOSED, COMMIT, STALL_MS / 2)));
        assertEquals(Verdict.NONE, w.observe(at(ElectionState.FOLLOWER_CATCHUP_AWAIT, COMMIT, STALL_MS)));
        assertEquals(Verdict.PEER_CHECK, w.observe(at(ElectionState.CANVASS, COMMIT, STALL_MS + STALL_MS / 2)));
        assertEquals(Verdict.CATCHUP_STALL, w.afterPeerCheck(PeerElections.CLOSED, STALL_MS + STALL_MS / 2));
    }

    @Test
    void aMemberThatWaitsForTheLiveLogIsNeverSeeded() {
        // A member that waits for the live log after its catch-up holds the
        // log up to the leader's position. A seed would drop a full copy of
        // the log, so it must not seed, whatever the wait.
        final JoinWatchdog w = watchdog();
        final LongFunction<JoinWatchdog.Sample> waits = t -> at(
                t == 0 ? ElectionState.FOLLOWER_CATCHUP : ElectionState.FOLLOWER_LOG_AWAIT, COMMIT, t * 1_000L);
        assertEquals(Exit.NEVER, firstExit(w, waits, PeerElections.CLOSED));
        assertEquals(Verdict.FOLLOWER_STALL, firstExit(watchdog(), waits, PeerElections.NONE_CLOSED).verdict(),
                "with no live leader the member relaunches and keeps its log");
    }

    @Test
    void aZeroCatchupWindowNeverDropsTheLog() {
        final LongFunction<JoinWatchdog.Sample> stuck = t -> at(cycleAt(STUCK_CYCLE, t), COMMIT, t * 1_000L);
        assertEquals(Exit.NEVER, firstExit(watchdog(0L), stuck, PeerElections.CLOSED));
        assertEquals(Verdict.FOLLOWER_STALL, firstExit(watchdog(0L), stuck, PeerElections.NONE_CLOSED).verdict());
    }

    @Test
    void aJoinThatFailsOnTheJoinPositionRelaunches() {
        // The member never reaches a catch-up state, so no seed is due: the
        // relaunch replays its own log, and the next join meets the leader's
        // log at the same position. No peer check is needed.
        final JoinWatchdog w = watchdog();
        final Exit exit = firstExit(w, t -> at(cycleAt(JOIN_CYCLE, t), COMMIT, t * 1_000L), PeerElections.CLOSED);
        assertEquals(Verdict.FOLLOWER_STALL, exit.verdict());
        assertEquals(ElectionState.INIT, cycleAt(JOIN_CYCLE, LIMIT_MS / 1_000L));
        assertEquals(LIMIT_MS / 1_000L + 1L, exit.second(), "a sample in INIT leaves the member to the INIT rule");
    }

    @Test
    void aLogReplicationThatEndsWithNoStopRelaunches() {
        // The replication ended before the recording started: no recording
        // is active, and the election waits in FOLLOWER_LOG_REPLICATION for
        // ever. The leader it waits for has no closed election.
        final JoinWatchdog w = watchdog();
        final Exit exit = firstExit(w, t -> new JoinWatchdog.Sample(
                ElectionState.FOLLOWER_LOG_REPLICATION, COMMIT, -1L, 0L, NO_RELAUNCH, INACTIVE, t * 1_000L), PeerElections.NONE_CLOSED);
        assertEquals(Verdict.FOLLOWER_STALL, exit.verdict());
        assertEquals(LIMIT_MS / 1_000L, exit.second());
    }

    @Test
    void withNoAdminPortAPurgedFollowerSeedsAtTheCatchupWindow() {
        // With no admin server the probe knows no peer, so the answer is
        // unknown. The member keeps the seed of a member with no probe.
        final JoinWatchdog w = watchdog();
        final Exit exit = firstExit(w, t -> new JoinWatchdog.Sample(
                ElectionState.FOLLOWER_LOG_REPLICATION, COMMIT, -1L, 0L, NO_RELAUNCH, INACTIVE, t * 1_000L),
                new PeerProbe.Round(0, List.of()).elections());
        assertEquals(Verdict.CATCHUP_STALL, exit.verdict());
        assertEquals(STALL_MS / 1_000L, exit.second());
    }

    @Test
    void aSilentPeerIsCheckedAgainAtEachSample() {
        // One of two peers does not answer, and the other has an open
        // election: no relaunch yet. The check repeats; when the silent peer
        // answers with an open election, the member relaunches and keeps
        // its log. If it never answers, the member seeds at the window.
        final PeerProbe.PeerStatus electing = new PeerProbe.PeerStatus(1, "FOLLOWER", "CANVASS", COMMIT,
            PeerProbe.PeerStatus.TERM_NOT_STATED, true);
        final PeerElections silent = new PeerProbe.Round(2, List.of(electing)).elections();
        final PeerElections answered = new PeerProbe.Round(2, List.of(electing, electing)).elections();
        assertEquals(PeerElections.UNKNOWN, silent);
        assertEquals(PeerElections.NONE_CLOSED, answered);

        final JoinWatchdog w = watchdog();
        final long limit = LIMIT_MS / 1_000L;
        LongStream.range(0, limit).forEach(t ->
                assertEquals(Verdict.NONE, w.observe(at(cycleAt(STUCK_CYCLE, t), COMMIT, t * 1_000L))));
        LongStream.range(limit, limit + 3).forEach(t -> {
            assertEquals(Verdict.PEER_CHECK, w.observe(at(ElectionState.FOLLOWER_CATCHUP_AWAIT, COMMIT, t * 1_000L)));
            assertEquals(Verdict.NONE, w.afterPeerCheck(silent, t * 1_000L));
        });
        assertEquals(Verdict.PEER_CHECK, w.observe(at(ElectionState.FOLLOWER_CATCHUP_AWAIT, COMMIT, (limit + 3) * 1_000L)));
        assertEquals(Verdict.FOLLOWER_STALL, w.afterPeerCheck(answered, (limit + 3) * 1_000L));

        final Exit never = firstExit(watchdog(), t -> at(cycleAt(STUCK_CYCLE, t), COMMIT, t * 1_000L), silent);
        assertEquals(new Exit(Verdict.CATCHUP_STALL, STALL_MS / 1_000L), never);
    }

    @Test
    void theInitWedgeWinsOverTheFollowerStall() {
        // A member that cycled through a catch-up, and then wedges in INIT.
        final JoinWatchdog w = watchdog();
        final long limit = LIMIT_MS / 1_000L;
        LongStream.range(0, limit).forEach(t ->
                assertEquals(Verdict.NONE, w.observe(at(cycleAt(JOIN_CYCLE, t), COMMIT, t * 1_000L))));
        final Exit exit = firstExit(w, t -> at(ElectionState.INIT, COMMIT, (limit + t) * 1_000L),
                PeerElections.NONE_CLOSED);
        assertEquals(Verdict.INIT_WEDGE, exit.verdict());
    }

    @Test
    void aLogReplicationThatReceivesLogNeverStalls() {
        // A follower far behind copies the leader's log: the commit position
        // stays, and the position of its log recording grows.
        assertEquals(Exit.NEVER, firstExit(watchdog(), t -> new JoinWatchdog.Sample(
                ElectionState.FOLLOWER_LOG_REPLICATION, COMMIT, t / 10L, 0L, NO_RELAUNCH, INACTIVE, t * 1_000L), PeerElections.NONE_CLOSED));
    }

    @Test
    void aLeaderThatWaitsWithNoProgressStalls() {
        // A member that voted cannot join, and no peer makes progress: the
        // leader waits in LEADER_READY with its toggle INACTIVE.
        final JoinWatchdog w = watchdog();
        final Exit exit = firstExit(w, t -> at(ElectionState.LEADER_READY, COMMIT, t * 1_000L), PeerElections.NONE_CLOSED);
        assertEquals(Verdict.LEADER_STALL, exit.verdict());
        assertEquals(LIMIT_MS / 1_000L + 1L, exit.second(), "the first sample past the stall limit");
        assertEquals(LIMIT_MS + 1_000L, w.sinceProgressMs(exit.second() * 1_000L));
    }

    @Test
    void aLeaderThatWaitsForASlowFollowerDoesNotStall() {
        // After a total loss on a slow host, a follower loads an old
        // snapshot and replays minutes of log while the leader waits in
        // LEADER_READY. The follower's commit position grows, and the leader
        // reads it every five seconds.
        final JoinWatchdog w = watchdog();
        assertEquals(Exit.NEVER, firstExit(w, t -> new JoinWatchdog.Sample(
                ElectionState.LEADER_READY, COMMIT, -1L, COMMIT + t / 5L, NO_RELAUNCH, INACTIVE, t * 1_000L), PeerElections.NONE_CLOSED));
        assertEquals(Verdict.NONE, w.observe(toggled(ElectionState.CLOSED, COMMIT, SECONDS * 1_000L, NEUTRAL)));
    }

    @Test
    void aLeaderThatLeavesLeaderReadyStartsTheCountAgain() {
        final JoinWatchdog w = watchdog();
        assertEquals(Verdict.NONE, w.observe(at(ElectionState.LEADER_READY, COMMIT, 0L)));
        assertEquals(Verdict.NONE, w.observe(at(ElectionState.LEADER_READY, COMMIT, LIMIT_MS)));
        assertEquals(Verdict.NONE, w.observe(at(ElectionState.CANVASS, COMMIT, LIMIT_MS + 1_000L)));
        assertEquals(Verdict.NONE, w.observe(at(ElectionState.LEADER_READY, COMMIT, LIMIT_MS + 2_000L)));
        assertEquals(Verdict.NONE, w.observe(at(ElectionState.LEADER_READY, COMMIT, 2 * LIMIT_MS + 2_000L)));
        assertEquals(Verdict.LEADER_STALL, w.observe(at(ElectionState.LEADER_READY, COMMIT, 2 * LIMIT_MS + 3_000L)));
    }

    @Test
    void anOpenElectionWithAnActiveToggleIsAWedge() {
        // The completion activated the toggle, then the ingress add threw:
        // the counter keeps LEADER_READY with the toggle at NEUTRAL.
        final JoinWatchdog w = watchdog();
        final long limit = HALF_ELECTED_MS;
        assertEquals(Verdict.NONE, w.observe(toggled(ElectionState.LEADER_READY, COMMIT, 0L, NEUTRAL)));
        assertEquals(Verdict.NONE, w.observe(toggled(ElectionState.LEADER_READY, COMMIT, limit, NEUTRAL)));
        assertEquals(Verdict.LEADER_WEDGE, w.observe(toggled(ElectionState.LEADER_READY, COMMIT, limit + 1L, NEUTRAL)));
        assertEquals(limit + 1L, w.halfElectedForMs(limit + 1L));
    }

    @Test
    void theMomentsAroundACompletedElectionAreNotAWedge() {
        // The toggle turns active one duty cycle before the counter reads
        // CLOSED. A new election sets the counter to INIT first, and its
        // first step then sets the toggle to INACTIVE.
        final JoinWatchdog w = watchdog();
        assertEquals(Verdict.NONE, w.observe(toggled(ElectionState.LEADER_READY, COMMIT, 0L, NEUTRAL)));
        assertEquals(Verdict.NONE, w.observe(toggled(ElectionState.CLOSED, COMMIT, 1_000L, NEUTRAL)));
        assertEquals(Verdict.NONE, w.observe(toggled(ElectionState.INIT, COMMIT, 2_000L, NEUTRAL)));
        assertEquals(Verdict.NONE, w.observe(toggled(ElectionState.CANVASS, COMMIT, 3_000L, INACTIVE)));
        assertEquals(0L, w.halfElectedForMs(3_000L));
    }

    @Test
    void aBrokenRunStartsTheCountAgain() {
        final JoinWatchdog w = watchdog();
        assertEquals(Verdict.NONE, w.observe(toggled(ElectionState.LEADER_READY, COMMIT, 0L, NEUTRAL)));
        assertEquals(Verdict.NONE, w.observe(toggled(ElectionState.CLOSED, COMMIT, HALF_ELECTED_MS / 2, NEUTRAL)));
        assertEquals(Verdict.NONE,
                w.observe(toggled(ElectionState.LEADER_READY, COMMIT, HALF_ELECTED_MS + 1L, NEUTRAL)));
        assertEquals(0L, w.halfElectedForMs(HALF_ELECTED_MS + 1L));
        assertEquals(Verdict.LEADER_WEDGE,
                w.observe(toggled(ElectionState.LEADER_READY, COMMIT, 2 * HALF_ELECTED_MS + 2L, NEUTRAL)));
    }

    @Test
    void aSnapshotAndTheOtherTogglesAreActiveToo() {
        // A leader that runs an action, such as a snapshot, has a toggle out
        // of INACTIVE. With the election CLOSED it is healthy, and no stall
        // rule counts time in a closed election; with an open election it is
        // a wedge.
        Stream.of(ClusterControl.ToggleState.SNAPSHOT, ClusterControl.ToggleState.SUSPEND,
                ClusterControl.ToggleState.SHUTDOWN).forEach(toggle -> {
                    assertEquals(Exit.NEVER, firstExit(watchdog(),
                            t -> toggled(ElectionState.CLOSED, COMMIT, t * 1_000L, toggle.code()), PeerElections.NONE_CLOSED));
                    final JoinWatchdog open = watchdog();
                    assertEquals(Verdict.NONE,
                            open.observe(toggled(ElectionState.LEADER_READY, COMMIT, 0L, toggle.code())));
                    assertEquals(Verdict.LEADER_WEDGE, open.observe(
                            toggled(ElectionState.LEADER_READY, COMMIT, HALF_ELECTED_MS + 1L, toggle.code())));
                });
    }

    @Test
    void aServiceAheadOfItsTruncatedLogRelaunchesAfterTheConfirmWindow() {
        // A new leader truncated the log below the applied position, and
        // each join fails at once on the join position. The member does not
        // wait for the stall limit: the relaunch replays the truncated log.
        final JoinWatchdog w = watchdog();
        final long confirmS = JoinWatchdog.TRUNCATED_CONFIRM_MS / 1_000L;
        final Exit exit = firstExit(w, t -> truncatedAt(cycleAt(JOIN_CYCLE, t), COMMIT - 320L, t * 1_000L),
                PeerElections.CLOSED);
        assertEquals(new Exit(Verdict.LOG_TRUNCATED, confirmS + 1L), exit, "the first sample past the confirm window");
        assertEquals(JoinWatchdog.TRUNCATED_CONFIRM_MS + 1_000L, w.truncatedForMs(exit.second() * 1_000L));
    }

    @Test
    void aServiceAtTheRelaunchPositionIsNoTruncatedLog() {
        // A relaunch that reaches the same position changes nothing: the
        // member joins, or the other rules act on it.
        assertEquals(Verdict.FOLLOWER_STALL, firstExit(watchdog(),
                t -> truncatedAt(cycleAt(JOIN_CYCLE, t), COMMIT, t * 1_000L), PeerElections.CLOSED).verdict());
        assertEquals(Exit.NEVER, firstExit(watchdog(),
                t -> truncatedAt(ElectionState.CLOSED, COMMIT - 320L, t * 1_000L), PeerElections.CLOSED),
                "a closed election joined the leader's log");
    }

    @Test
    void oneReadOfAHalfWrittenEntryDoesNotRelaunch() {
        // The run of samples above the relaunch position breaks on a sample
        // with no fixed log end, and the confirm window starts again.
        final JoinWatchdog w = watchdog();
        final long confirmS = JoinWatchdog.TRUNCATED_CONFIRM_MS / 1_000L;
        LongStream.rangeClosed(0, confirmS).forEach(t -> assertEquals(Verdict.NONE,
                w.observe(truncatedAt(ElectionState.FOLLOWER_LOG_AWAIT, t == confirmS ? NO_RELAUNCH : 0L, t * 1_000L))));
        assertEquals(0L, w.truncatedForMs(confirmS * 1_000L));
        LongStream.rangeClosed(confirmS + 1, 2 * confirmS + 1).forEach(t -> assertEquals(Verdict.NONE,
                w.observe(truncatedAt(ElectionState.CANVASS, 0L, t * 1_000L))));
        assertEquals(Verdict.LOG_TRUNCATED,
                w.observe(truncatedAt(ElectionState.INIT, 0L, (2 * confirmS + 2) * 1_000L)));
    }
}
