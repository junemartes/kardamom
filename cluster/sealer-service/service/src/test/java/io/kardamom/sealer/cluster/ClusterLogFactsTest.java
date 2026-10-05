package io.kardamom.sealer.cluster;

import static io.kardamom.sealer.cluster.IngressFrames.canonicalId;
import static io.kardamom.sealer.cluster.IngressFrames.offerIngress;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.cluster.ClusterBackup;
import io.aeron.cluster.ClusterBackup.Configuration.ReplayStart;
import io.aeron.cluster.ElectionState;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.service.Cluster;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestBackupNode;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import java.io.IOException;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;
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
 *   <li>A ClusterBackup with {@code ReplayStart.LATEST_SNAPSHOT} records the
 *       log from the latest snapshot position. Its cluster and archive
 *       directories, copied into a wiped member, let that member restore
 *       from the snapshot, catch up from the leader, and reach the leader's
 *       state.</li>
 *   <li>An intact follower whose log ends below the purge point replays its
 *       own log, then sticks the same way as a wiped follower. The leader
 *       refuses the replay from the follower's log end. This case stays in
 *       one leadership term, so the follower takes the catch-up path, not
 *       {@code FOLLOWER_LOG_REPLICATION}.</li>
 * </ul>
 */
@ExtendWith(InterruptingTestCallback.class)
class ClusterLogFactsTest {

    private static final int MEMBER_COUNT = 3;
    private static final int DEDUP_CAPACITY = 8192;
    private static final long TICK_MS = 200L;

    /** A small term and segment, so a few hundred KB of log span several segments. */
    private static final String LOG_CHANNEL = "aeron:udp?term-length=64k|alias=raft";
    private static final int SEGMENT_LENGTH = 64 * 1024;
    private static final long LOG_BYTES_BEFORE_SNAPSHOT = 4L * SEGMENT_LENGTH;
    private static final long LOG_BYTES_TAIL = 8L * 1024;

    /**
     * The prefix of the node directories. TestCluster puts member {@code i}
     * in {@code node-i}, and the backup node in {@code node-3}.
     */
    private static final String NODE_DIR = "node";

    /** The leader's archive error when a replay starts below the purged recording start. */
    private static final String REPLAY_BELOW_START = "is less than recording start position=";

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
        try (Run run = new Run(ReplayStart.BEGINNING)) {
            run.appendLog(LOG_BYTES_BEFORE_SNAPSHOT);
            final long logStart = run.snapshotAndPurge();
            final TestNode wiped = run.cluster.startStaticNode(run.stopFollower(), true);

            final JoinObserver observer = run.awaitFailedCatchups(wiped, logStart);

            assertTrue(logStart > 0, "the purge must remove the first log segments");
            observer.assertStuckBelow(logStart);
            assertEquals(0L, observer.maxCommitPosition(), "a wiped follower commits nothing");
            run.assertRefusedCatchups(0L, logStart);
        }
    }

    @Test
    @InterruptAfter(value = 90, unit = TimeUnit.SECONDS)
    void backupSeededMemberRejoinsFromLatestSnapshot() throws IOException {
        try (Run run = new Run(ReplayStart.LATEST_SNAPSHOT)) {
            run.appendLog(LOG_BYTES_BEFORE_SNAPSHOT);
            final long logStart = run.snapshotAndPurge();
            final long snapshotPosition = run.leaderProbe.latestSnapshotLogPosition();
            run.appendLog(LOG_BYTES_TAIL);
            final int memberId = run.stopFollower();

            final long backupLogStart = run.backUpToLeaderCommit();
            new BackupSeed(nodeDir(MEMBER_COUNT)).copyInto(nodeDir(memberId));
            run.reconnectClient();
            run.appendLog(LOG_BYTES_TAIL);
            final TestNode rejoined = run.cluster.startStaticNode(memberId, false);
            TestCluster.awaitElectionClosed(rejoined);
            run.snapshot(List.of(run.leader, rejoined));

            assertTrue(logStart > 0, "the purge must remove the first log segments");
            assertEquals(snapshotPosition, backupLogStart,
                    "the backup records the log from the latest snapshot position");
            assertTrue(((SealerTestService) rejoined.service()).restoredFromSnapshot(),
                    "the seeded member must start from the backup's snapshot");
            assertEquals(Cluster.Role.FOLLOWER, rejoined.role());
            assertArrayEquals(run.leaderProbe.latestServiceSnapshot(),
                    new MemberProbe(rejoined).latestServiceSnapshot(),
                    "the rejoined member's sealer state must equal the leader's");
        }
    }

    @Test
    @InterruptAfter(value = 90, unit = TimeUnit.SECONDS)
    void intactFollowerBelowPurgePointSticksInCatchup() {
        systemTestWatcher.ignoreErrorsMatching(error -> error.contains(REPLAY_BELOW_START));
        try (Run run = new Run(ReplayStart.BEGINNING)) {
            run.appendLog(LOG_BYTES_TAIL);
            final TestNode lagging = run.cluster.followers().get(0);
            run.cluster.awaitCommitPosition(lagging, run.leader.commitPosition());
            final int memberId = run.stopFollower();
            run.appendLog(LOG_BYTES_BEFORE_SNAPSHOT);
            final long logStart = run.snapshotAndPurge();
            final TestNode restarted = run.cluster.startStaticNode(memberId, false);

            final JoinObserver observer = run.awaitFailedCatchups(restarted, logStart);

            final long logEnd = observer.maxCommitPosition();
            assertTrue(logEnd > 0, "the intact follower replays its own log");
            observer.assertStuckBelow(logStart);
            run.assertRefusedCatchups(logEnd, logStart);
        }
    }

    private Path nodeDir(final int memberId) {
        return baseDir.resolve(NODE_DIR + "-" + memberId);
    }

    /** One cluster for one test: the members, the leader, and a connected client. */
    private final class Run implements AutoCloseable {
        final TestCluster cluster;
        final TestNode leader;
        final MemberProbe leaderProbe;
        private AeronCluster client;
        private int nextRecordId = 0;

        Run(final ReplayStart replayStart) {
            this.cluster = ClusterTestHarness.start(systemTestWatcher,
                    ClusterTestHarness.builder(MEMBER_COUNT, DEDUP_CAPACITY, TICK_MS)
                            .withLogChannel(LOG_CHANNEL)
                            .withSegmentFileLength(SEGMENT_LENGTH)
                            .withClusterBaseDir(baseDir.resolve(NODE_DIR).toString())
                            .replayStart(replayStart));
            this.leader = cluster.awaitLeader();
            this.leaderProbe = new MemberProbe(leader);
            this.client = cluster.connectClient();
        }

        /** Offer records until the leader's commit position grows by {@code bytes}. */
        void appendLog(final long bytes) {
            final long target = leader.commitPosition() + bytes;
            while (leader.commitPosition() < target) {
                offerIngress(client, canonicalId(nextRecordId++));
            }
        }

        /**
         * Snapshot every running member, then purge each running member's log
         * to its latest snapshot. Return the leader's new log start position.
         */
        long snapshotAndPurge() {
            snapshot(Stream.concat(Stream.of(leader), cluster.followers().stream()).toList());
            cluster.purgeLogToLastSnapshot();
            return leaderProbe.logStartPosition();
        }

        /** Take one snapshot and wait until each of {@code members} completes it. */
        void snapshot(final List<TestNode> members) {
            final long[] before = members.stream().mapToLong(cluster::getSnapshotCount).toArray();
            cluster.takeSnapshot(leader);
            IntStream.range(0, before.length)
                    .forEach(i -> cluster.awaitSnapshotCount(members.get(i), before[i] + 1));
        }

        /** Stop one follower and return its member id. */
        int stopFollower() {
            final TestNode follower = cluster.followers().get(0);
            cluster.stopNode(follower);
            return follower.index();
        }

        /**
         * Run a ClusterBackup node until its live log reaches the leader's
         * commit position, then close it. The test waits for
         * {@code BACKING_UP}: the agent enters it only after
         * {@code LIVE_LOG_REPLAY} and {@code UPDATE_RECORDING_LOG}, and
         * {@code LIVE_LOG_REPLAY} is too short to poll. Return the start
         * position of the backup's log recording.
         */
        long backUpToLeaderCommit() {
            try (TestBackupNode backup = cluster.startClusterBackupNode(true)) {
                cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
                cluster.awaitBackupLiveLogPosition(leader.commitPosition());
                return backup.recordingLogStartPosition();
            }
        }

        /** Replace the client, whose session can time out while the test waits on other members. */
        void reconnectClient() {
            client = cluster.reconnectClient();
        }

        /**
         * Sample {@code member} until the leader records
         * {@link #FAILED_CATCHUPS} refused catch-up replays from below
         * {@code logStart}.
         */
        JoinObserver awaitFailedCatchups(final TestNode member, final long logStart) {
            final JoinObserver observer = new JoinObserver(member);
            while (leaderProbe.errorObservations(REPLAY_BELOW_START + logStart) < FAILED_CATCHUPS) {
                observer.sample();
            }
            assertEquals(Cluster.Role.LEADER, leader.role(), "the leader must keep its role");
            return observer;
        }

        /** Assert that the leader refused each catch-up replay from {@code replayStart}. */
        void assertRefusedCatchups(final long replayStart, final long logStart) {
            final String refusal =
                    "requested replay start position=" + replayStart + " " + REPLAY_BELOW_START + logStart;
            assertTrue(leaderProbe.errorObservations(refusal) >= FAILED_CATCHUPS,
                    "the leader must log at least " + FAILED_CATCHUPS + " times: " + refusal);
        }

        @Override
        public void close() {
            cluster.close();
        }
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
