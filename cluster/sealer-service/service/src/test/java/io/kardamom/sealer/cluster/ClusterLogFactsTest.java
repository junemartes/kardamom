package io.kardamom.sealer.cluster;

import static io.kardamom.sealer.cluster.LogCluster.LOG_BYTES_BEFORE_SNAPSHOT;
import static io.kardamom.sealer.cluster.LogCluster.LOG_BYTES_TAIL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.cluster.ElectionState;
import io.aeron.cluster.service.Cluster;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/**
 * Facts about Aeron 1.44 cluster log purge and member rejoin, proven on a
 * 3-member in-JVM {@link TestCluster} that hosts the real sealer service.
 *
 * <p>Every case fills the log past several archive segments, takes a
 * snapshot, and purges each running member's log recording to the segment
 * that holds the snapshot. The leader's archive then holds no log below
 * that segment base.</p>
 *
 * <ul>
 *   <li>A wiped follower cannot rejoin. Its catch-up needs a leader replay
 *       from position 0. The leader's archive refuses it with "requested
 *       replay start position=0 is less than recording start position". The
 *       follower stays in the election: it waits in
 *       {@code FOLLOWER_CATCHUP_AWAIT} until the leader heartbeat timeout
 *       (10 s in TestCluster), then starts again at {@code INIT}. Its commit position stays 0.</li>
 *   <li>{@link PeerSeed}, a ClusterBackup with
 *       {@code ReplayStart.LATEST_SNAPSHOT} that writes into a wiped
 *       member's own directories, records the log from the latest snapshot
 *       position. The member then restores from the snapshot, catches up
 *       from the leader, and reaches the leader's state.</li>
 *   <li>An intact follower whose log ends below the purge point replays its
 *       own log, then sticks the same way as a wiped follower. The leader
 *       refuses the replay from the follower's log end. This case stays in
 *       one leadership term, so the follower takes the catch-up path, not
 *       {@code FOLLOWER_LOG_REPLICATION}.</li>
 * </ul>
 */
@ExtendWith(InterruptingTestCallback.class)
class ClusterLogFactsTest {

    /** The leader's archive error when a replay starts below the purged recording start. */
    static final String REPLAY_BELOW_START = "is less than recording start position=";

    /** Each failed catch-up attempt adds one leader error. Two prove the election retries. */
    private static final long FAILED_CATCHUPS = 2;

    /** The election states that a member with an unrecoverable log gap cycles through. */
    private static final Set<ElectionState> STUCK_JOIN_STATES = EnumSet.of(
            ElectionState.INIT,
            ElectionState.CANVASS,
            ElectionState.FOLLOWER_REPLAY,
            ElectionState.FOLLOWER_CATCHUP_INIT,
            ElectionState.FOLLOWER_CATCHUP_AWAIT);

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @TempDir
    Path baseDir;

    @Test
    @InterruptAfter(value = 90, unit = TimeUnit.SECONDS)
    void wipedFollowerCannotRejoinAfterPurge() {
        systemTestWatcher.ignoreErrorsMatching(error -> error.contains(REPLAY_BELOW_START));
        try (LogCluster run = new LogCluster(systemTestWatcher, baseDir)) {
            run.appendLog(LOG_BYTES_BEFORE_SNAPSHOT);
            final long logStart = snapshotAndPurge(run);
            final TestNode wiped = run.cluster.startStaticNode(run.stopFollower(), true);

            final JoinObserver observer = awaitFailedCatchups(run, wiped, logStart);

            assertTrue(logStart > 0, "the purge must remove the first log segments");
            observer.assertStuckBelow(logStart);
            assertEquals(0L, observer.maxCommitPosition(), "a wiped follower commits nothing");
            assertRefusedCatchups(run, 0L, logStart);
        }
    }

    @Test
    @InterruptAfter(value = 90, unit = TimeUnit.SECONDS)
    void peerSeededMemberRejoinsFromLatestSnapshot() {
        try (LogCluster run = new LogCluster(systemTestWatcher, baseDir)) {
            run.appendLog(LOG_BYTES_BEFORE_SNAPSHOT);
            final long logStart = snapshotAndPurge(run);
            final long snapshotPosition = run.leaderProbe.latestSnapshotLogPosition();
            run.appendLog(LOG_BYTES_TAIL);

            final LogCluster.Seeded seeded = run.seedAndStart(run.stopFollower());

            assertTrue(logStart > 0, "the purge must remove the first log segments");
            assertEquals(snapshotPosition, seeded.snapshotPosition(), "the seed copies the latest snapshot");
            assertEquals(snapshotPosition, new MemberProbe(seeded.member()).logStartPosition(),
                    "the seed records the log from the latest snapshot position");
            assertTrue(((SealerTestService) seeded.member().service()).restoredFromSnapshot(),
                    "the seeded member must start from the seeded snapshot");
            assertEquals(Cluster.Role.FOLLOWER, seeded.member().role());
            run.assertSameStateAsLeader(seeded.member());
        }
    }

    @Test
    @InterruptAfter(value = 90, unit = TimeUnit.SECONDS)
    void intactFollowerBelowPurgePointSticksInCatchup() {
        systemTestWatcher.ignoreErrorsMatching(error -> error.contains(REPLAY_BELOW_START));
        try (LogCluster run = new LogCluster(systemTestWatcher, baseDir)) {
            run.appendLog(LOG_BYTES_TAIL);
            final TestNode lagging = run.cluster.followers().get(0);
            run.cluster.awaitCommitPosition(lagging, run.leader.commitPosition());
            final int memberId = run.stopFollower();
            run.appendLog(LOG_BYTES_BEFORE_SNAPSHOT);
            final long logStart = snapshotAndPurge(run);
            final TestNode restarted = run.cluster.startStaticNode(memberId, false);

            final JoinObserver observer = awaitFailedCatchups(run, restarted, logStart);

            final long logEnd = observer.maxCommitPosition();
            assertTrue(logEnd > 0, "the intact follower replays its own log");
            observer.assertStuckBelow(logStart);
            assertRefusedCatchups(run, logEnd, logStart);
        }
    }

    /**
     * Snapshot every running member, then purge each running member's log
     * to its latest snapshot with the TestCluster purge. Return the leader's
     * new log start position.
     */
    private static long snapshotAndPurge(final LogCluster run) {
        run.snapshotRunning();
        run.cluster.purgeLogToLastSnapshot();
        return run.leaderProbe.logStartPosition();
    }

    /**
     * Sample {@code member} until the leader records
     * {@link #FAILED_CATCHUPS} refused catch-up replays from below
     * {@code logStart}.
     */
    private static JoinObserver awaitFailedCatchups(final LogCluster run, final TestNode member, final long logStart) {
        final JoinObserver observer = new JoinObserver(member);
        while (run.leaderProbe.errorObservations(REPLAY_BELOW_START + logStart) < FAILED_CATCHUPS) {
            observer.sample();
        }
        assertEquals(Cluster.Role.LEADER, run.leader.role(), "the leader must keep its role");
        return observer;
    }

    /** Assert that the leader refused each catch-up replay from {@code replayStart}. */
    private static void assertRefusedCatchups(final LogCluster run, final long replayStart, final long logStart) {
        final String refusal =
                "requested replay start position=" + replayStart + " " + REPLAY_BELOW_START + logStart;
        assertTrue(run.leaderProbe.errorObservations(refusal) >= FAILED_CATCHUPS,
                "the leader must log at least " + FAILED_CATCHUPS + " times: " + refusal);
    }

    /** Records the election states and the highest commit position that one member shows. */
    private static final class JoinObserver {
        private final TestNode member;
        private final Set<ElectionState> seen = EnumSet.noneOf(ElectionState.class);
        private long maxCommitPosition = 0L;

        JoinObserver(final TestNode member) {
            this.member = member;
        }

        void sample() {
            seen.add(member.electionState());
            maxCommitPosition = Math.max(maxCommitPosition, member.commitPosition());
            Tests.yield();
        }

        long maxCommitPosition() {
            return maxCommitPosition;
        }

        /**
         * Assert that the member waited for a catch-up that never came, never
         * left the election, and never committed past {@code logStart}.
         */
        void assertStuckBelow(final long logStart) {
            assertTrue(seen.contains(ElectionState.FOLLOWER_CATCHUP_AWAIT), "states seen: " + seen);
            assertTrue(STUCK_JOIN_STATES.containsAll(seen), "states seen: " + seen);
            assertTrue(maxCommitPosition < logStart,
                    "commit position " + maxCommitPosition + " must stay below " + logStart);
        }
    }
}
