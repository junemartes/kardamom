package io.kardamom.sealer.cluster;

import static io.kardamom.sealer.cluster.LogCluster.LOG_BYTES_TAIL;
import static io.kardamom.sealer.cluster.LogCluster.SEGMENT_LENGTH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.cluster.service.Cluster;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/**
 * The production log purge on a 3-member in-JVM {@link TestCluster} that
 * hosts the real sealer service: {@link LogPurger} on each member, with
 * the snapshot marks and the posted head of the real service.
 *
 * <ul>
 *   <li>The posted-head floor holds the log until the batcher posts. Then
 *       each member purges to the segment of the newest snapshot older than
 *       the kept ones. A wiped member seeds from a peer and rejoins.</li>
 *   <li>An intact follower that stopped inside the margin rejoins from its
 *       own log, without a seed, after the leader purged twice.</li>
 *   <li>An intact follower whose log ends below the purge point stalls in
 *       its catch-up. The {@link JoinWatchdog} names the stall. The
 *       recovery of {@link JoinWatchdogThread} drops the recording log, so
 *       the next start seeds the member from a peer, and it rejoins.</li>
 * </ul>
 */
@ExtendWith(InterruptingTestCallback.class)
class ClusterLogPurgeTest {

    /** The test keeps two snapshots: three snapshots give one purge point. */
    private static final PurgePlanner KEEP_TWO = new PurgePlanner(2);
    private static final long LOG_BYTES_PER_SNAPSHOT = 2L * SEGMENT_LENGTH;
    /** A stall window well past the 10 s leader heartbeat timeout of TestCluster. */
    private static final long STALL_WINDOW_MS = 15_000L;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @TempDir
    Path baseDir;

    @Test
    @InterruptAfter(value = 120, unit = TimeUnit.SECONDS)
    void theFloorHoldsThePurgeThenAWipedMemberRejoinsFromAPeerSeed() {
        try (LogCluster run = new LogCluster(systemTestWatcher, baseDir)) {
            snapshots(run, 3);
            final List<LogPurger> purgers = run.running().stream().map(m -> run.purger(m, KEEP_TWO)).toList();

            final boolean purgedUnposted = purgers.stream().anyMatch(p -> p.runOnce().isPresent());
            run.postNewestSnapshotBlock();
            final List<Optional<LogPurger.Purge>> purges = purgers.stream().map(LogPurger::runOnce).toList();
            final long logStart = run.leaderProbe.logStartPosition();
            run.appendLog(LOG_BYTES_TAIL);
            final LogCluster.Seeded seeded = run.seedAndStart(run.stopFollower());

            assertFalse(purgedUnposted, "no member purges the log of unposted blocks");
            assertTrue(purges.stream().allMatch(Optional::isPresent), "every member purges: " + purges);
            final LogPurger.Purge leaderPurge = purges.get(0).orElseThrow();
            assertEquals(leaderPurge.position(), logStart);
            assertTrue(logStart > 0 && logStart <= leaderPurge.mark().logPosition(),
                    "the log starts at the segment of the purge snapshot: " + leaderPurge);
            assertTrue(leaderPurge.mark().blockNumber() <= leaderPurge.postedHead());
            assertEquals(Cluster.Role.FOLLOWER, seeded.member().role());
            assertTrue(((SealerTestService) seeded.member().service()).restoredFromSnapshot());
            run.assertSameStateAsLeader(seeded.member());
        }
    }

    @Test
    @InterruptAfter(value = 120, unit = TimeUnit.SECONDS)
    void anIntactFollowerInsideTheMarginRejoinsWithoutASeed() {
        try (LogCluster run = new LogCluster(systemTestWatcher, baseDir)) {
            snapshots(run, 3);
            run.postNewestSnapshotBlock();
            final long firstStart = purge(run);
            final int memberId = run.stopFollower();
            snapshots(run, 1);
            run.postNewestSnapshotBlock();
            final long secondStart = purge(run);
            run.appendLog(LOG_BYTES_TAIL);

            final TestNode restarted = run.cluster.startStaticNode(memberId, false);
            TestCluster.awaitElectionClosed(restarted);
            run.snapshot(List.of(run.leader, restarted));

            assertTrue(firstStart > 0 && secondStart > firstStart,
                    "the leader purges twice: " + firstStart + " then " + secondStart);
            assertEquals(Cluster.Role.FOLLOWER, restarted.role());
            run.assertSameStateAsLeader(restarted);
            final List<Long> recorded = run.clusterState(memberId).snapshotPositions();
            final List<PurgeView.Mark> marks = LogCluster.purgeView(restarted).marks();
            assertTrue(marks.size() >= 2 && marks.stream().allMatch(m -> recorded.contains(m.logPosition())),
                    "the restored mark and the new mark sit at recorded snapshots: " + marks + " in " + recorded);
        }
    }

    @Test
    @InterruptAfter(value = 120, unit = TimeUnit.SECONDS)
    void aFollowerBelowThePurgePointStallsAndRecoversFromAPeerSeed() {
        systemTestWatcher.ignoreErrorsMatching(error -> error.contains(ClusterLogFactsTest.REPLAY_BELOW_START));
        try (LogCluster run = new LogCluster(systemTestWatcher, baseDir)) {
            run.appendLog(LOG_BYTES_TAIL);
            run.cluster.awaitCommitPosition(run.cluster.followers().get(0), run.leader.commitPosition());
            final int memberId = run.stopFollower();
            snapshots(run, 3);
            run.postNewestSnapshotBlock();
            final long logStart = purge(run);
            final TestNode stuck = run.cluster.startStaticNode(memberId, false);

            final long stalledAt = awaitStall(stuck);
            run.clusterState(memberId).dropRecordingLog();
            run.cluster.stopNode(stuck);
            final StartMode mode = StartMode.of(run.clusterState(memberId).holdsRecordingLog(), false);
            final LogCluster.Seeded seeded = run.seedAndStart(memberId);

            assertTrue(stalledAt < logStart, "the stalled follower ends below the purge point " + logStart);
            assertEquals(StartMode.SEED_FROM_PEER, mode);
            assertEquals(Cluster.Role.FOLLOWER, seeded.member().role());
            run.assertSameStateAsLeader(seeded.member());
        }
    }

    /** Take {@code count} snapshots on every running member, with log between them. */
    private static void snapshots(final LogCluster run, final int count) {
        IntStream.range(0, count).forEach(i -> {
            run.appendLog(LOG_BYTES_PER_SNAPSHOT);
            run.snapshotRunning();
        });
    }

    /** Run the purger of every running member once. Return the leader's new log start. */
    private static long purge(final LogCluster run) {
        run.running().forEach(member -> run.purger(member, KEEP_TWO).runOnce());
        return run.leaderProbe.logStartPosition();
    }

    /**
     * Feed the watchdog samples of {@code member} until it names a catch-up
     * stall. Return the member's commit position at the stall.
     */
    private static long awaitStall(final TestNode member) {
        final JoinWatchdog watchdog = new JoinWatchdog(STALL_WINDOW_MS * 4, STALL_WINDOW_MS);
        while (watchdog.observe(member.electionState(), member.commitPosition(), System.currentTimeMillis())
                != JoinWatchdog.Verdict.CATCHUP_STALL) {
            Tests.sleep(100);
        }
        return member.commitPosition();
    }
}
