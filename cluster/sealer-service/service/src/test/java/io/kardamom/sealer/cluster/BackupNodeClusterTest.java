package io.kardamom.sealer.cluster;

import static io.kardamom.sealer.cluster.LogCluster.LOG_BYTES_TAIL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestCluster;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/**
 * A {@link BackupNode} against a 3-member in-JVM {@link TestCluster} that
 * hosts the real sealer service. The backup copies the latest snapshot
 * from a follower, and then records the live log as the cluster commits.
 */
@ExtendWith(InterruptingTestCallback.class)
class BackupNodeClusterTest {

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @TempDir
    Path baseDir;

    @Test
    @InterruptAfter(value = 60, unit = TimeUnit.SECONDS)
    void theBackupHoldsTheLatestSnapshotAndFollowsTheLiveLog() {
        try (LogCluster run = new LogCluster(systemTestWatcher, baseDir)) {
            run.appendLog(LOG_BYTES_TAIL);
            run.snapshotRunning();
            final long snapshotPosition = run.leaderProbe.latestSnapshotLogPosition();

            try (BackupNode.Running backup = new BackupNode(run.backupSettings()).start(new StopRequest())) {
                final BackupStatus ready = await(backup, BackupStatus::isReady);
                assertTrue(snapshotPosition > 0, "the cluster must hold a snapshot past the log start");
                assertEquals(snapshotPosition, ready.snapshot().logPosition(),
                        "the backup must hold the latest snapshot in its recording log");
                assertTrue(ready.snapshot().leadershipTermId() >= 0, "the snapshot must name its term");

                run.appendLog(LOG_BYTES_TAIL);
                final long commitPosition = run.leader.commitPosition();
                final BackupStatus caughtUp = await(backup, status -> status.liveLogPosition() >= commitPosition);
                assertTrue(caughtUp.liveLogPosition() > snapshotPosition, "the live log must pass the snapshot");
            }
        }
    }

    /** Sample the backup until {@code done} holds, and return that sample. */
    private static BackupStatus await(final BackupNode.Running backup, final Predicate<BackupStatus> done) {
        BackupStatus status = backup.status();
        while (!done.test(status)) {
            Tests.sleep(10);
            status = backup.status();
        }
        return status;
    }
}
