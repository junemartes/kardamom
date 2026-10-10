package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.aeron.cluster.ElectionState;
import io.kardamom.sealer.cluster.JoinWatchdog.Verdict;
import java.util.List;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;

/** Pure-function tests for the join watchdog rules. No Aeron, no cluster. */
final class JoinWatchdogTest {
    private static final long WINDOW_MS = 60_000L;
    private static final long STALL_MS = 300_000L;
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
        return new JoinWatchdog(WINDOW_MS, STALL_MS);
    }

    /** The state of the stuck cycle at second {@code t}. */
    private static ElectionState stuckAt(final long t) {
        return STUCK_CYCLE.get((int) (t % STUCK_CYCLE.size()));
    }

    @Test
    void firesWhenInitPersistsPastTheWindow() {
        final JoinWatchdog w = watchdog();
        assertEquals(Verdict.NONE, w.observe(ElectionState.INIT, 0L, 0L));
        assertEquals(Verdict.NONE, w.observe(ElectionState.INIT, 0L, WINDOW_MS));
        assertEquals(Verdict.INIT_WEDGE, w.observe(ElectionState.INIT, 0L, WINDOW_MS + 1L));
        assertEquals(WINDOW_MS + 1L, w.initForMs(WINDOW_MS + 1L));
    }

    @Test
    void aNormalElectionNeverFires() {
        final JoinWatchdog w = watchdog();
        assertEquals(Verdict.NONE, w.observe(ElectionState.INIT, 0L, 0L));
        assertEquals(Verdict.NONE, w.observe(ElectionState.CANVASS, 0L, 5L));
        assertEquals(Verdict.NONE, w.observe(ElectionState.FOLLOWER_BALLOT, 0L, 50L));
        assertEquals(Verdict.NONE, w.observe(ElectionState.FOLLOWER_CATCHUP, 1L, 10 * WINDOW_MS));
        assertEquals(Verdict.NONE, w.observe(ElectionState.CLOSED, 1L, 20 * WINDOW_MS));
        assertEquals(0L, w.initForMs(20 * WINDOW_MS));
    }

    @Test
    void aLoneMemberWithQuorumLostDoesNotFire() {
        // A lone member with quorum lost canvasses for minutes. CANVASS is
        // not INIT, and it never reaches a catch-up state without a leader.
        final JoinWatchdog w = watchdog();
        LongStream.range(0, 2 * STALL_MS / 1_000L).forEach(t ->
                assertEquals(Verdict.NONE, w.observe(ElectionState.CANVASS, COMMIT, t * 1_000L)));
    }

    @Test
    void leavingInitResetsTheClock() {
        final JoinWatchdog w = watchdog();
        assertEquals(Verdict.NONE, w.observe(ElectionState.INIT, 0L, 0L));
        assertEquals(Verdict.NONE, w.observe(ElectionState.CANVASS, 0L, WINDOW_MS / 2));
        // A second election (a leader change) starts INIT again. The
        // window restarts from this observation, not from the first.
        assertEquals(Verdict.NONE, w.observe(ElectionState.INIT, 0L, WINDOW_MS));
        assertEquals(Verdict.NONE, w.observe(ElectionState.INIT, 0L, 2 * WINDOW_MS));
        assertEquals(Verdict.INIT_WEDGE, w.observe(ElectionState.INIT, 0L, 2 * WINDOW_MS + 1L));
    }

    @Test
    void anUnallocatedCounterIsNotInit() {
        final JoinWatchdog w = watchdog();
        assertEquals(Verdict.NONE, w.observe(null, 0L, 0L));
        assertEquals(Verdict.NONE, w.observe(null, 0L, 5 * STALL_MS));
        assertEquals(0L, w.initForMs(5 * STALL_MS));
    }

    @Test
    void rejectsANonPositiveWindow() {
        assertThrows(IllegalArgumentException.class, () -> new JoinWatchdog(0L, STALL_MS));
    }

    @Test
    void aStuckCatchupStallsAfterTheWindow() {
        final JoinWatchdog w = watchdog();
        final long fired = LongStream.iterate(0L, t -> t + 1L)
                .filter(t -> w.observe(stuckAt(t), COMMIT, t * 1_000L) == Verdict.CATCHUP_STALL)
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
        assertEquals(Verdict.NONE, w.observe(ElectionState.FOLLOWER_REPLAY, COMMIT, 0L));
        assertEquals(Verdict.NONE, w.observe(ElectionState.FOLLOWER_CATCHUP_AWAIT, COMMIT, 1_000L));
        assertEquals(Verdict.NONE, w.observe(ElectionState.FOLLOWER_REPLAY, 0L, 2_000L));
        assertEquals(Verdict.NONE, w.observe(ElectionState.FOLLOWER_REPLAY, COMMIT, 3_000L));
        assertEquals(Verdict.CATCHUP_STALL, w.observe(ElectionState.FOLLOWER_CATCHUP_AWAIT, COMMIT, STALL_MS));
    }

    @Test
    void aCatchupThatReceivesLogNeverStalls() {
        final JoinWatchdog w = watchdog();
        LongStream.range(0, 2 * STALL_MS / 1_000L).forEach(t -> assertEquals(Verdict.NONE,
                w.observe(stuckAt(t), COMMIT + t / 30L, t * 1_000L)));
    }

    @Test
    void aClosedElectionResetsTheStall() {
        final JoinWatchdog w = watchdog();
        assertEquals(Verdict.NONE, w.observe(ElectionState.FOLLOWER_CATCHUP_AWAIT, COMMIT, 0L));
        assertEquals(Verdict.NONE, w.observe(ElectionState.CLOSED, COMMIT, STALL_MS / 2));
        assertEquals(Verdict.NONE, w.observe(ElectionState.FOLLOWER_CATCHUP_AWAIT, COMMIT, STALL_MS));
        assertEquals(Verdict.CATCHUP_STALL, w.observe(ElectionState.INIT, COMMIT, STALL_MS + STALL_MS / 2));
    }

    @Test
    void aMemberThatWaitsForTheLiveLogIsNotInTheCycle() {
        // After a catch-up, a member that waits for the live log is up to
        // date. A seed would drop a full copy of the log, so it must not
        // stall, whatever the wait.
        final JoinWatchdog w = watchdog();
        assertEquals(Verdict.NONE, w.observe(ElectionState.FOLLOWER_CATCHUP, COMMIT, 0L));
        assertEquals(Verdict.NONE, w.observe(ElectionState.FOLLOWER_LOG_AWAIT, COMMIT, 2 * STALL_MS));
    }

    @Test
    void aZeroStallWindowTurnsTheStallRuleOff() {
        final JoinWatchdog w = new JoinWatchdog(WINDOW_MS, 0L);
        LongStream.range(0, 2 * STALL_MS / 1_000L).forEach(t ->
                assertEquals(Verdict.NONE, w.observe(stuckAt(t), COMMIT, t * 1_000L)));
    }

    @Test
    void aLeaderWithNoFollowerWedgesAfterTheWindow() {
        // The member won a vote, but no follower replicates its log or joins its live log.
        final JoinWatchdog w = watchdog();
        assertEquals(Verdict.NONE, w.observe(ElectionState.CANDIDATE_BALLOT, COMMIT, 0L));
        assertEquals(Verdict.NONE, w.observe(ElectionState.LEADER_LOG_REPLICATION, COMMIT, 1_000L));
        assertEquals(Verdict.NONE, w.observe(ElectionState.LEADER_LOG_REPLICATION, COMMIT, 1_000L + WINDOW_MS));
        assertEquals(Verdict.LEADER_WEDGE,
                w.observe(ElectionState.LEADER_LOG_REPLICATION, COMMIT, 1_001L + WINDOW_MS));
        assertEquals(WINDOW_MS + 1L, w.leaderWaitForMs(1_001L + WINDOW_MS));
    }

    @Test
    void aLeaderThatWaitsForTheLiveLogWedgesAfterTheWindow() {
        final JoinWatchdog w = watchdog();
        assertEquals(Verdict.NONE, w.observe(ElectionState.LEADER_READY, COMMIT, 0L));
        assertEquals(Verdict.LEADER_WEDGE, w.observe(ElectionState.LEADER_READY, COMMIT, WINDOW_MS + 1L));
    }

    @Test
    void theOwnReplayOfALeaderIsNotAWait() {
        // A leader replays its own log after a full restart. The replay can
        // take longer than the window on a slow host, and it restarts the wait.
        final JoinWatchdog w = watchdog();
        assertEquals(Verdict.NONE, w.observe(ElectionState.LEADER_LOG_REPLICATION, COMMIT, 0L));
        assertEquals(Verdict.NONE, w.observe(ElectionState.LEADER_REPLAY, COMMIT, WINDOW_MS / 2));
        assertEquals(Verdict.NONE, w.observe(ElectionState.LEADER_REPLAY, COMMIT, 3 * WINDOW_MS));
        assertEquals(Verdict.NONE, w.observe(ElectionState.LEADER_READY, COMMIT, 3 * WINDOW_MS + 1_000L));
        assertEquals(Verdict.NONE, w.observe(ElectionState.CLOSED, COMMIT, 3 * WINDOW_MS + 2_000L));
        assertEquals(0L, w.leaderWaitForMs(3 * WINDOW_MS + 2_000L));
    }
}
