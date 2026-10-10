package io.kardamom.sealer.cluster;

import static io.kardamom.sealer.cluster.ClusterTestHarness.awaitCondition;
import static io.kardamom.sealer.cluster.IngressFrames.canonicalId;
import static io.kardamom.sealer.cluster.IngressFrames.offerFully;
import static io.kardamom.sealer.cluster.IngressFrames.offerIngress;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.cluster.TestCluster;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * An ingress frame of a kind that no member knows is dropped on every
 * member and ordered on none. The drop is a decision of the replicated
 * state machine, so members of two releases that both do not know the
 * kind stay equal. A frame long enough for a user record is the case that
 * matters: ordered as a record on one member and dropped on another, it
 * would fork the canonical stream.
 */
@ExtendWith(InterruptingTestCallback.class)
class SealerUnknownKindClusterTest {

    private static final int MEMBER_COUNT = 3;
    private static final int DEDUP_CAPACITY = 8192;
    /** No boundary tick fires inside the test, so the state changes only by ingress. */
    private static final long TICK_MS = 600_000L;
    /** A kind no release defines. */
    private static final byte UNKNOWN_KIND = 0x7E;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @Test
    @InterruptAfter(value = 60, unit = TimeUnit.SECONDS)
    void everyMemberDropsAnUnknownKindAndOrdersNothing() {
        final RecordingEgressListener egress = new RecordingEgressListener();
        final TestCluster cluster = ClusterTestHarness.startCluster(
                systemTestWatcher, MEMBER_COUNT, DEDUP_CAPACITY, TICK_MS);
        cluster.awaitLeader();
        cluster.egressListener(egress);
        final AeronCluster client = cluster.connectClient();

        offerUnknownKind(client);
        offerIngress(client, canonicalId(0));
        awaitCondition(client, () -> egress.relayedIndexes.size() >= 1);
        // The record takes index 0: the unknown frame took no index.
        assertEquals(List.of(0L), egress.relayedIndexes);

        final List<SealerClusteredService> members = IntStream.range(0, MEMBER_COUNT)
            .mapToObj(i -> ((SealerTestService) cluster.node(i).service()).delegate())
            .toList();
        final long applied = members.get(cluster.findLeader().index()).servicePosition();
        awaitCondition(client, () -> members.stream().allMatch(m -> m.servicePosition() == applied));

        final byte[] leaderState = members.get(cluster.findLeader().index()).snapshot();
        for (final SealerClusteredService member : members) {
            assertEquals(1L, member.droppedUnknownKindCount(), "every member counts the drop");
            assertArrayEquals(leaderState, member.snapshot(), "the replicated state stays equal");
        }
    }

    /**
     * Offer a frame of {@link #UNKNOWN_KIND} that is long enough for a
     * user record: kind, guard header, canonical id, and a payload byte.
     */
    private static void offerUnknownKind(final AeronCluster client) {
        final int length = SealerWire.MIN_INGRESS_LEN + 1;
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer(length);
        buf.setMemory(0, length, (byte) 0x11);
        buf.putByte(SealerWire.KIND_OFFSET, UNKNOWN_KIND);
        offerFully(client, buf, length);
    }
}
