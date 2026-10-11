package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.aeron.cluster.ClusterControl;
import io.aeron.cluster.ElectionState;
import io.kardamom.sealer.cluster.JoinWatchdog.Verdict;
import java.util.List;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/** Pure-function tests for the join watchdog rules. No Aeron, no cluster. */
final class JoinWatchdogTest {
    private static final long WINDOW_MS = 60_000L;
    private static final long STALL_MS = 300_000L;
    /** The half-elected limit of the job's CI values: a 30 s archive message timeout and a 30 s driver timeout. */
    private static final long HALF_ELECTED_MS = 60_000L;
    private static final long INACTIVE = ClusterControl.ToggleState.INACTIVE.code();
    private static final long NEUTRAL = ClusterControl.ToggleState.NEUTRAL.code();
    /** A commit position that the samples of one test do not change. */
    private static final long COMMIT = 4_096L;

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

    private static JoinWatchdog watchdog() {
        return new JoinWatchdog(WINDOW_MS, STALL_MS, HALF_ELECTED_MS);
    }

    /** The state of the stuck cycle at second {@code t}. */
    private static ElectionState stuckAt(final long t) {
        return STUCK_CYCLE.get((int) (t % STUCK_CYCLE.size()));
    }

    @Test
    void firesWhenInitPersistsPastTheWindow() {
        final JoinWatchdog w = watchdog();
        assertEquals(Verdict.NONE, w.observe(ElectionState.INIT, 0L, 0L, INACTIVE));
        assertEquals(Verdict.NONE, w.observe(ElectionState.INIT, 0L, WINDOW_MS, INACTIVE));
        assertEquals(Verdict.INIT_WEDGE, w.observe(ElectionState.INIT, 0L, WINDOW_MS + 1L, INACTIVE));
        assertEquals(WINDOW_MS + 1L, w.initForMs(WINDOW_MS + 1L));
    }

    @Test
    void aNormalElectionNeverFires() {
        final JoinWatchdog w = watchdog();
        assertEquals(Verdict.NONE, w.observe(ElectionState.INIT, 0L, 0L, INACTIVE));
        assertEquals(Verdict.NONE, w.observe(ElectionState.CANVASS, 0L, 5L, INACTIVE));
        assertEquals(Verdict.NONE, w.observe(ElectionState.FOLLOWER_BALLOT, 0L, 50L, INACTIVE));
        assertEquals(Verdict.NONE, w.observe(ElectionState.FOLLOWER_CATCHUP, 1L, 10 * WINDOW_MS, INACTIVE));
        assertEquals(Verdict.NONE, w.observe(ElectionState.CLOSED, 1L, 20 * WINDOW_MS, INACTIVE));
        assertEquals(0L, w.initForMs(20 * WINDOW_MS));
    }

    @Test
    void aLoneMemberWithQuorumLostDoesNotFire() {
        // A lone member with quorum lost canvasses for minutes. CANVASS is
        // not INIT, and it never reaches a catch-up state without a leader.
        final JoinWatchdog w = watchdog();
        LongStream.range(0, 2 * STALL_MS / 1_000L).forEach(t ->
                assertEquals(Verdict.NONE, w.observe(ElectionState.CANVASS, COMMIT, t * 1_000L, INACTIVE)));
    }

    @Test
    void leavingInitResetsTheClock() {
        final JoinWatchdog w = watchdog();
        assertEquals(Verdict.NONE, w.observe(ElectionState.INIT, 0L, 0L, INACTIVE));
        assertEquals(Verdict.NONE, w.observe(ElectionState.CANVASS, 0L, WINDOW_MS / 2, INACTIVE));
        // A second election (a leader change) starts INIT again. The
        // window restarts from this observation, not from the first.
        assertEquals(Verdict.NONE, w.observe(ElectionState.INIT, 0L, WINDOW_MS, INACTIVE));
        assertEquals(Verdict.NONE, w.observe(ElectionState.INIT, 0L, 2 * WINDOW_MS, INACTIVE));
        assertEquals(Verdict.INIT_WEDGE, w.observe(ElectionState.INIT, 0L, 2 * WINDOW_MS + 1L, INACTIVE));
    }

    @Test
    void rejectsANonPositiveWindow() {
        assertThrows(IllegalArgumentException.class, () -> new JoinWatchdog(0L, STALL_MS, HALF_ELECTED_MS));
    }

    @Test
    void aStuckCatchupStallsAfterTheWindow() {
        final JoinWatchdog w = watchdog();
        final long fired = LongStream.iterate(0L, t -> t + 1L)
                .filter(t -> w.observe(stuckAt(t), COMMIT, t * 1_000L, INACTIVE) == Verdict.CATCHUP_STALL)
                .findFirst()
                .orElseThrow();
        assertEquals(STALL_MS / 1_000L, fired, "the first sample at the window fires");
        assertEquals(STALL_MS, w.stallForMs(fired * 1_000L));
    }

    @Test
    void theReplayOfTheOwnLogIsNotProgress() {
        // Each cycle replays the member's own log again, from a lower
        // commit position up to the same end. Only a new highest position
        // is progress.
        final JoinWatchdog w = watchdog();
        assertEquals(Verdict.NONE, w.observe(ElectionState.FOLLOWER_REPLAY, COMMIT, 0L, INACTIVE));
        assertEquals(Verdict.NONE, w.observe(ElectionState.FOLLOWER_CATCHUP_AWAIT, COMMIT, 1_000L, INACTIVE));
        assertEquals(Verdict.NONE, w.observe(ElectionState.FOLLOWER_REPLAY, 0L, 2_000L, INACTIVE));
        assertEquals(Verdict.NONE, w.observe(ElectionState.FOLLOWER_REPLAY, COMMIT, 3_000L, INACTIVE));
        assertEquals(Verdict.CATCHUP_STALL,
                w.observe(ElectionState.FOLLOWER_CATCHUP_AWAIT, COMMIT, STALL_MS, INACTIVE));
    }

    @Test
    void aCatchupThatReceivesLogNeverStalls() {
        final JoinWatchdog w = watchdog();
        LongStream.range(0, 2 * STALL_MS / 1_000L).forEach(t -> assertEquals(Verdict.NONE,
                w.observe(stuckAt(t), COMMIT + t / 30L, t * 1_000L, INACTIVE)));
    }

    @Test
    void aClosedElectionResetsTheStall() {
        final JoinWatchdog w = watchdog();
        assertEquals(Verdict.NONE, w.observe(ElectionState.FOLLOWER_CATCHUP_AWAIT, COMMIT, 0L, INACTIVE));
        assertEquals(Verdict.NONE, w.observe(ElectionState.CLOSED, COMMIT, STALL_MS / 2, INACTIVE));
        assertEquals(Verdict.NONE, w.observe(ElectionState.FOLLOWER_CATCHUP_AWAIT, COMMIT, STALL_MS, INACTIVE));
        assertEquals(Verdict.CATCHUP_STALL, w.observe(ElectionState.INIT, COMMIT, STALL_MS + STALL_MS / 2, INACTIVE));
    }

    @Test
    void aMemberThatWaitsForTheLiveLogIsNotInTheCycle() {
        // After a catch-up, a member that waits for the live log is up to
        // date. A seed would drop a full copy of the log, so it must not
        // stall, whatever the wait.
        final JoinWatchdog w = watchdog();
        assertEquals(Verdict.NONE, w.observe(ElectionState.FOLLOWER_CATCHUP, COMMIT, 0L, INACTIVE));
        assertEquals(Verdict.NONE, w.observe(ElectionState.FOLLOWER_LOG_AWAIT, COMMIT, 2 * STALL_MS, INACTIVE));
    }

    @Test
    void aZeroStallWindowTurnsTheStallRuleOff() {
        final JoinWatchdog w = new JoinWatchdog(WINDOW_MS, 0L, HALF_ELECTED_MS);
        LongStream.range(0, 2 * STALL_MS / 1_000L).forEach(t ->
                assertEquals(Verdict.NONE, w.observe(stuckAt(t), COMMIT, t * 1_000L, INACTIVE)));
    }

    @Test
    void aLeaderThatWaitsLongForAFollowerIsNotAWedge() {
        // After a total loss on a slow host, a follower loads an old snapshot
        // and replays minutes of log while the leader waits in LEADER_READY.
        // The leader's toggle stays INACTIVE until its election completes.
        final JoinWatchdog w = watchdog();
        LongStream.range(0, 2 * STALL_MS / 1_000L).forEach(t -> assertEquals(Verdict.NONE,
                w.observe(ElectionState.LEADER_READY, COMMIT, t * 1_000L, INACTIVE)));
        assertEquals(Verdict.NONE, w.observe(ElectionState.CLOSED, COMMIT, 2 * STALL_MS, NEUTRAL));
    }

    @Test
    void anOpenElectionWithAnActiveToggleIsAWedge() {
        // The completion activated the toggle, then the ingress add threw:
        // the counter keeps LEADER_READY with the toggle at NEUTRAL.
        final JoinWatchdog w = watchdog();
        final long limit = HALF_ELECTED_MS;
        assertEquals(Verdict.NONE, w.observe(ElectionState.LEADER_READY, COMMIT, 0L, NEUTRAL));
        assertEquals(Verdict.NONE, w.observe(ElectionState.LEADER_READY, COMMIT, limit, NEUTRAL));
        assertEquals(Verdict.LEADER_WEDGE, w.observe(ElectionState.LEADER_READY, COMMIT, limit + 1L, NEUTRAL));
        assertEquals(limit + 1L, w.halfElectedForMs(limit + 1L));
    }

    @Test
    void theMomentsAroundACompletedElectionAreNotAWedge() {
        // The toggle turns active one duty cycle before the counter reads
        // CLOSED. A new election sets the counter to INIT first, and its
        // first step then sets the toggle to INACTIVE.
        final JoinWatchdog w = watchdog();
        assertEquals(Verdict.NONE, w.observe(ElectionState.LEADER_READY, COMMIT, 0L, NEUTRAL));
        assertEquals(Verdict.NONE, w.observe(ElectionState.CLOSED, COMMIT, 1_000L, NEUTRAL));
        assertEquals(Verdict.NONE, w.observe(ElectionState.INIT, COMMIT, 2_000L, NEUTRAL));
        assertEquals(Verdict.NONE, w.observe(ElectionState.CANVASS, COMMIT, 3_000L, INACTIVE));
        assertEquals(0L, w.halfElectedForMs(3_000L));
    }

    @Test
    void aBrokenRunStartsTheCountAgain() {
        final JoinWatchdog w = watchdog();
        assertEquals(Verdict.NONE, w.observe(ElectionState.LEADER_READY, COMMIT, 0L, NEUTRAL));
        assertEquals(Verdict.NONE, w.observe(ElectionState.CLOSED, COMMIT, HALF_ELECTED_MS / 2, NEUTRAL));
        assertEquals(Verdict.NONE, w.observe(ElectionState.LEADER_READY, COMMIT, HALF_ELECTED_MS + 1L, NEUTRAL));
        assertEquals(0L, w.halfElectedForMs(HALF_ELECTED_MS + 1L));
        assertEquals(Verdict.LEADER_WEDGE,
                w.observe(ElectionState.LEADER_READY, COMMIT, 2 * HALF_ELECTED_MS + 2L, NEUTRAL));
    }

    @Test
    void anActionOfTheToggleIsActiveToo() {
        // A leader that runs an action has a toggle out of INACTIVE. With
        // the election CLOSED it is healthy; with an open election it is a wedge.
        Stream.of(ClusterControl.ToggleState.SNAPSHOT, ClusterControl.ToggleState.SUSPEND,
                ClusterControl.ToggleState.SHUTDOWN).forEach(toggle -> {
                    final JoinWatchdog closed = watchdog();
                    assertEquals(Verdict.NONE, closed.observe(ElectionState.CLOSED, COMMIT, 0L, toggle.code()));
                    assertEquals(Verdict.NONE,
                            closed.observe(ElectionState.CLOSED, COMMIT, 2 * HALF_ELECTED_MS, toggle.code()));
                    final JoinWatchdog open = watchdog();
                    assertEquals(Verdict.NONE, open.observe(ElectionState.LEADER_READY, COMMIT, 0L, toggle.code()));
                    assertEquals(Verdict.LEADER_WEDGE,
                            open.observe(ElectionState.LEADER_READY, COMMIT, HALF_ELECTED_MS + 1L, toggle.code()));
                });
    }
}
