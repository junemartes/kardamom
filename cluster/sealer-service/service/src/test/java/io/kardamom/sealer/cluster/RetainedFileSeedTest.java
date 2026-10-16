package io.kardamom.sealer.cluster;

import static io.kardamom.sealer.cluster.RetainedFiles.assertEqualFiles;
import static io.kardamom.sealer.cluster.RetainedFiles.awaitHeads;
import static io.kardamom.sealer.cluster.RetainedFiles.nextPublishedHead;
import static io.kardamom.sealer.cluster.RetainedFiles.segments;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.cluster.TestNode;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/**
 * A wiped member that a peer seeds gets the retained file with the
 * snapshot: it copies the segments from the admin endpoint of a peer,
 * restores the snapshot, and then holds the same file bytes as the leader.
 */
@ExtendWith(InterruptingTestCallback.class)
class RetainedFileSeedTest {

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @TempDir
    Path baseDir;

    @Test
    @InterruptAfter(value = 120, unit = TimeUnit.SECONDS)
    void a_seeded_member_copies_the_retained_file_from_a_peer() {
        try (LogCluster run = new LogCluster(systemTestWatcher, baseDir, true)) {
            run.appendLog(LogCluster.LOG_BYTES_BEFORE_SNAPSHOT);
            nextPublishedHead(run.leader);
            run.snapshotRunning();
            assertTrue(segments(run.retainedDir(run.leader.index())).size() > 2, "the file spans several segments");

            final LogCluster.Seeded seeded = run.seedAndStart(run.stopFollower());
            final TestNode member = seeded.member();
            assertTrue(((SealerTestService) member.service()).restoredFromSnapshot());
            run.assertSameStateAsLeader(member);
            final long head = nextPublishedHead(run.leader);
            final List<TestNode> compared = List.of(run.leader, member);
            awaitHeads(compared, head);
            assertEqualFiles(compared, run::retainedDir, head);
        }
    }

    @Test
    @InterruptAfter(value = 120, unit = TimeUnit.SECONDS)
    void a_seed_stops_after_its_copies_fail_a_bounded_number_of_rounds() {
        try (LogCluster run = new LogCluster(systemTestWatcher, baseDir, true)) {
            run.appendLog(LogCluster.LOG_BYTES_TAIL);
            run.snapshotRunning();
            final int memberId = run.stopFollower();
            final URI deadPeer = URI.create("http://127.0.0.1:1");

            final IllegalStateException stop =
                assertThrows(IllegalStateException.class, () -> run.seedOnly(memberId, List.of(deadPeer)));
            assertTrue(stop.getMessage().contains(PeerSeed.MAX_COPY_FAILURES + " rounds"), stop.getMessage());
        }
    }
}
