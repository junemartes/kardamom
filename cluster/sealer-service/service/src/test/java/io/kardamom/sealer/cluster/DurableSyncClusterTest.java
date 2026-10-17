package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestNode;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/**
 * The disk sync of a follower in a running cluster: its synced position
 * follows the log across several archive segments, and never passes the
 * log position that the archive recorded.
 */
@ExtendWith(InterruptingTestCallback.class)
class DurableSyncClusterTest {

    private static final int ROUNDS = 5;
    /** One and a half archive segments of log for each round. */
    private static final long LOG_BYTES = LogCluster.SEGMENT_LENGTH * 3L / 2;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @TempDir
    Path baseDir;

    @Test
    @InterruptAfter(value = 120, unit = TimeUnit.SECONDS)
    void theSyncedPositionFollowsTheLogAndNeverPassesIt() throws InterruptedException {
        try (LogCluster run = new LogCluster(systemTestWatcher, baseDir)) {
            final TestNode follower = run.cluster.followers().get(0);
            final int memberId = follower.index();
            final StateDir clusterDir = run.clusterState(memberId);
            final Supplier<Optional<DurableSync.View>> view = DurableSync.memberView(
                clusterDir, follower.consensusModule().context().aeron().countersReader());
            final List<IOException> failures = new CopyOnWriteArrayList<>();
            final DurableSync sync = new DurableSync(
                new DurableSync.Dirs(run.archiveDir(memberId), clusterDir.root()), view, failures::add);
            sync.start(new Interval(20));
            final List<Long> synced = new ArrayList<>();
            for (int round = 0; round < ROUNDS; round++) {
                synced.add(appendAndAwaitSync(run, view, sync));
            }
            sync.close();
            assertEquals(List.of(), failures);
            assertEquals(0, sync.durability().errors());
            assertTrue(synced.get(ROUNDS - 1) >= ROUNDS * LOG_BYTES, "synced=" + synced);
            assertEquals(synced.stream().sorted().distinct().toList(), synced, "the synced position only grows");
        }
    }

    /**
     * Append log, wait until the synced position reaches the log position
     * of the follower, and check that it is not past the log position that
     * the archive holds after the sync. Return the synced position.
     */
    private static long appendAndAwaitSync(
            final LogCluster run, final Supplier<Optional<DurableSync.View>> view, final DurableSync sync) {
        run.appendLog(LOG_BYTES);
        final long recorded = recordedPosition(view);
        while (sync.durability().position() < recorded) {
            Tests.yield();
        }
        final long position = sync.durability().position();
        final long after = recordedPosition(view);
        assertTrue(position <= after, "synced " + position + " is past the recorded log position " + after);
        return position;
    }

    private static long recordedPosition(final Supplier<Optional<DurableSync.View>> view) {
        while (true) {
            final Optional<DurableSync.View> seen = view.get();
            if (seen.isPresent() && seen.get().position().isPresent()) {
                return seen.get().position().getAsLong();
            }
            Tests.yield();
        }
    }
}
