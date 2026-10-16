package io.kardamom.sealer.cluster;

import static io.kardamom.sealer.cluster.ClusterTestHarness.awaitCondition;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.logbuffer.Header;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.agrona.DirectBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * A slow snapshot stops every frame of the leader, and the status before it
 * carries the snapshot notice.
 *
 * <p>The consensus module appends no timer and no ingress while it takes a
 * snapshot, so the boundary ticks and their status frames stop until every
 * archive recording of the snapshot is complete. The members here hold the
 * service thread at the start of each snapshot, as a slow disk does. A
 * client must find the notice on the last frame before the stall: that
 * frame tells it to wait for the snapshot and not to call the sealer
 * lost.</p>
 */
@ExtendWith(InterruptingTestCallback.class)
class SealerSnapshotStallClusterTest {

    private static final int MEMBER_COUNT = 3;
    private static final int DEDUP_CAPACITY = 8192;
    private static final long TICK_MS = 100L;
    /** The schedule: a snapshot every 20 blocks, about every 2 s. */
    private static final long INTERVAL_BLOCKS = 20L;
    /** The stall of each snapshot. It is many ticks long. */
    private static final long SNAPSHOT_PAUSE_MS = 3_000L;
    /** A gap between two frames this long is a stall: 15 ticks, half the snapshot pause. */
    private static final long STALL_NS = TimeUnit.MILLISECONDS.toNanos(SNAPSHOT_PAUSE_MS / 2);
    private static final int SNAPSHOT_FOLLOWS_OFFSET = SealerWire.STATUS_SNAPSHOT_FLAG_OFFSET;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @Test
    @InterruptAfter(value = 60, unit = TimeUnit.SECONDS)
    void theFramesStopForTheSnapshotAndTheNoticeComesFirst() {
        final FrameTimes frames = new FrameTimes();
        final TestCluster cluster = ClusterTestHarness.start(systemTestWatcher,
                ClusterTestHarness.builder(MEMBER_COUNT, memberId -> new SealerTestService(
                        new SealerClusteredService(DEDUP_CAPACITY, TICK_MS, memberId)
                                .snapshotEvery(new SnapshotSchedule(INTERVAL_BLOCKS)),
                        SNAPSHOT_PAUSE_MS)));
        final TestNode leader = cluster.awaitLeader();
        cluster.egressListener(frames);
        final AeronCluster client = cluster.connectClient();

        // Each snapshot stops the frames. Wait for two stalls, each with a
        // frame after it.
        awaitCondition(client, () -> cluster.getSnapshotCount(leader) >= 2 && frames.stalls().size() >= 2);

        final List<Gap> stalls = frames.stalls();
        assertTrue(stalls.stream().allMatch(Gap::afterNotice),
                "the last frame before each stall is a status with the snapshot notice: " + stalls);
    }

    /** One gap between two frames: its length, and whether the frame before it was the snapshot notice. */
    private record Gap(long lengthNs, boolean afterNotice) {
    }

    /**
     * The arrival time of each egress frame, and whether it is a status with
     * the snapshot notice. The listener runs on the test thread inside
     * {@code pollEgress}, which the await loop calls with no pause, so the
     * poll time is the arrival time.
     */
    private static final class FrameTimes implements EgressListener {
        private final List<Long> arrivals = new ArrayList<>();
        private final List<Boolean> notices = new ArrayList<>();

        @Override
        public void onMessage(
                final long clusterSessionId,
                final long timestamp,
                final DirectBuffer buffer,
                final int offset,
                final int length,
                final Header header) {
            arrivals.add(System.nanoTime());
            notices.add(buffer.getByte(offset) == SealerWire.EGRESS_KIND_STATUS
                    && length > SNAPSHOT_FOLLOWS_OFFSET
                    && buffer.getByte(offset + SNAPSHOT_FOLLOWS_OFFSET) == 1);
        }

        /** The gaps of at least {@link #STALL_NS}. */
        List<Gap> stalls() {
            return IntStream.range(1, arrivals.size())
                    .mapToObj(i -> new Gap(arrivals.get(i) - arrivals.get(i - 1), notices.get(i - 1)))
                    .filter(gap -> gap.lengthNs() >= STALL_NS)
                    .toList();
        }
    }
}
