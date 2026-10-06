package io.kardamom.sealer.cluster;

import static io.kardamom.sealer.cluster.ClusterTestHarness.awaitCondition;
import static io.kardamom.sealer.cluster.IngressFrames.canonicalId;
import static io.kardamom.sealer.cluster.IngressFrames.offerReplayRequest;
import static io.kardamom.sealer.cluster.IngressFrames.originRecordFrame;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.logbuffer.Header;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * An epoch offer that the ingress publication accepts can still be lost:
 * Aeron Cluster ingress is at-most-once across a leader kill and a quorum
 * loss. The sealer must not seal the next epoch over the hole. It answers
 * the next epoch with an origin-gap reject that names the lost origin, and
 * the producer offers the epochs again from there, in order.
 *
 * <p>Three in-JVM members host the real {@link SealerClusteredService}. The
 * client is a real {@link AeronCluster}. Every wait is a yield loop, and
 * {@link InterruptAfter} bounds the test.</p>
 */
@ExtendWith(InterruptingTestCallback.class)
class OriginGapClusterTest {

    private static final int MEMBER_COUNT = 3;
    private static final int DEDUP_CAPACITY = 8192;
    private static final long TICK_MS = 200L;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    /** One origin-gap reject: the origin offered, and the origin the sealer expects. */
    record Gap(long offered, long expected) {
    }

    /**
     * Records the origin of every relayed epoch and every origin-gap
     * reject. A test epoch's canonical id carries its origin in bytes 0..3,
     * so the relayed payload names the origin.
     */
    static final class Origins implements EgressListener {
        final List<Long> relayed = new ArrayList<>();
        final List<Gap> gaps = new ArrayList<>();
        long replayDone = 0;
        /** The L1 origin of the last boundary: the origin the sealer confirmed. */
        long boundaryOrigin = -1;

        @Override
        public void onMessage(
                final long sessionId,
                final long timestamp,
                final DirectBuffer buffer,
                final int offset,
                final int length,
                final Header header) {
            final byte kind = buffer.getByte(offset);
            if (kind == SealerWire.EGRESS_KIND_RELAYED) {
                // kind(1) | index(8) | payloadLen(4) | payload = [canonical_id:32]...
                relayed.add((long) buffer.getInt(offset + 13, ByteOrder.LITTLE_ENDIAN));
            } else if (kind == SealerWire.EGRESS_KIND_ORIGIN_GAP) {
                // kind(1) | offered(8) | expected(8)
                gaps.add(new Gap(
                    buffer.getLong(offset + 1, ByteOrder.LITTLE_ENDIAN),
                    buffer.getLong(offset + 9, ByteOrder.LITTLE_ENDIAN)));
            } else if (kind == SealerWire.EGRESS_KIND_REPLAY_DONE) {
                replayDone++;
            } else if (kind == SealerWire.EGRESS_KIND_BOUNDARY) {
                // kind(1) | block(8) | end_tx_idx(8) | l2_timestamp(8) | l1_origin(8)
                boundaryOrigin = buffer.getLong(offset + 25, ByteOrder.LITTLE_ENDIAN);
            }
        }
    }

    private static long offerOnce(final AeronCluster client, final long origin) {
        final byte[] frame = originRecordFrame(canonicalId((int) origin), origin, 1, new byte[0]);
        return client.offer(new UnsafeBuffer(frame), 0, frame.length);
    }

    private static void offerUntilAccepted(final AeronCluster client, final long origin) {
        while (offerOnce(client, origin) <= 0) {
            client.pollEgress();
            Tests.yield();
        }
    }

    /**
     * The common tail of both cases. Origin 101 was lost. The client offers
     * 102, reads the reject that names 101, and offers 101 and 102 again.
     * The relayed origins have no gap and no duplicate.
     */
    private static void offerAfterTheLossAndRepair(final AeronCluster client, final Origins o) {
        offerUntilAccepted(client, 102);
        awaitCondition(client, () -> !o.gaps.isEmpty());
        assertEquals(new Gap(102, 101), o.gaps.get(0), "the sealer names the lost origin");
        assertEquals(List.of(100L), o.relayed, "the sealer did not seal 102 over the hole");
        offerAgainFrom101(client, o);
    }

    /**
     * Offer 101 and 102 in order. Then read the whole canonical stream
     * again through a replay from genesis: a relay that went to a session
     * the client lost does not show on the live egress. The ordered
     * origins have no gap and no duplicate.
     */
    private static void offerAgainFrom101(final AeronCluster client, final Origins o) {
        offerUntilAccepted(client, 101);
        offerUntilAccepted(client, 102);
        assertEquals(List.of(100L, 101L, 102L), canonicalOrigins(client, o));
    }

    /**
     * The origins of every relayed epoch, read through a replay from
     * genesis. The session's egress follows the log order, so the first
     * replay's DONE marker comes after every frame that the earlier offers
     * caused. The second replay then holds the canonical stream alone.
     */
    private static List<Long> canonicalOrigins(final AeronCluster client, final Origins o) {
        offerReplayRequest(client, 0L, 1L);
        awaitCondition(client, () -> o.replayDone == 1);
        o.relayed.clear();
        offerReplayRequest(client, 0L, 1L);
        awaitCondition(client, () -> o.replayDone == 2);
        return List.copyOf(o.relayed);
    }

    /** Connect a client that records into a new {@link Origins}, and order origin 100. */
    private static AeronCluster connectAndOrder100(final TestCluster cluster, final Origins o) {
        cluster.egressListener(o);
        final AeronCluster client = cluster.connectClient();
        offerUntilAccepted(client, 100);
        awaitCondition(client, () -> o.relayed.contains(100L));
        return client;
    }

    @Test
    @InterruptAfter(value = 120, unit = TimeUnit.SECONDS)
    void an_epoch_lost_in_a_leader_kill_is_refilled() {
        final TestCluster cluster = ClusterTestHarness.startCluster(
            systemTestWatcher, MEMBER_COUNT, DEDUP_CAPACITY, TICK_MS);
        cluster.awaitLeader();
        final Origins o = new Origins();
        final AeronCluster client = connectAndOrder100(cluster, o);

        final TestNode leader = cluster.findLeader();
        cluster.stopNode(leader);
        assertTrue(offerOnce(client, 101) > 0, "the publication accepts the offer it then loses");

        cluster.awaitLeader(leader.index());
        offerAfterTheLossAndRepair(cluster.reconnectClient(), o);
    }

    /**
     * The leader dies with 101 in flight, and the producer that offered it
     * restarts with an empty queue. Nothing offers 101 again, except the
     * da-watcher: the boundaries still carry origin 100, so it publishes
     * again from 101. A copy of an epoch can then reach the sealer before
     * or after the re-publish:
     * <ul>
     *   <li>the re-published 101 first, then a late copy of 101: the copy
     *       is a duplicate, absorbed with no reject;</li>
     *   <li>a later epoch 103 first, then the re-published 102: 103 is
     *       refused as a gap that names 102, and 102 then 103 are
     *       ordered.</li>
     * </ul>
     * A copy of the confirmed 100 is absorbed too. The canonical origins
     * have no gap and no duplicate.
     */
    @Test
    @InterruptAfter(value = 120, unit = TimeUnit.SECONDS)
    void a_republish_from_the_boundary_origin_refills_a_lost_epoch_in_either_order() {
        final TestCluster cluster = ClusterTestHarness.startCluster(
            systemTestWatcher, MEMBER_COUNT, DEDUP_CAPACITY, TICK_MS);
        cluster.awaitLeader();
        final Origins o = new Origins();
        final AeronCluster client = connectAndOrder100(cluster, o);

        final TestNode leader = cluster.findLeader();
        cluster.stopNode(leader);
        assertTrue(offerOnce(client, 101) > 0, "the publication accepts the offer it then loses");

        cluster.awaitLeader(leader.index());
        final AeronCluster again = cluster.reconnectClient();
        o.boundaryOrigin = -1;
        awaitCondition(again, () -> o.boundaryOrigin >= 0);
        assertEquals(100L, o.boundaryOrigin, "the boundaries confirm 100, not the lost 101");

        offerUntilAccepted(again, 101);
        offerUntilAccepted(again, 101);
        offerUntilAccepted(again, 100);
        offerUntilAccepted(again, 103);
        awaitCondition(again, () -> !o.gaps.isEmpty());
        assertEquals(List.of(new Gap(103, 102)), o.gaps, "only the early 103 is refused");

        offerUntilAccepted(again, 102);
        offerUntilAccepted(again, 103);
        awaitCondition(again, () -> o.boundaryOrigin == 103);
        assertEquals(List.of(100L, 101L, 102L, 103L), canonicalOrigins(again, o));
    }

    @Test
    @InterruptAfter(value = 120, unit = TimeUnit.SECONDS)
    void an_epoch_lost_in_a_quorum_loss_is_refilled() {
        final TestCluster cluster = ClusterTestHarness.startCluster(
            systemTestWatcher, MEMBER_COUNT, DEDUP_CAPACITY, TICK_MS);
        cluster.awaitLeader();
        final Origins o = new Origins();
        final AeronCluster client = connectAndOrder100(cluster, o);

        final List<Integer> stopped = cluster.followers().stream().map(TestNode::index).toList();
        cluster.followers().forEach(cluster::stopNode);
        assertTrue(offerOnce(client, 101) > 0, "the publication accepts the offer it then loses");

        stopped.forEach(index -> cluster.startStaticNode(index, false));
        cluster.awaitLeader();
        final AeronCluster again = cluster.reconnectClient();
        offerUntilAccepted(again, 102);
        awaitCondition(again, () -> !o.gaps.isEmpty() || o.relayed.contains(102L));
        // The leader can keep the uncommitted 101 and commit it when the
        // followers return. Then 101 leads 102, and no reject is due. Either
        // way, 102 is never sealed over a hole: the replay below proves it.
        assertTrue(o.gaps.stream().allMatch(gap -> gap.equals(new Gap(102, 101))), o.gaps.toString());
        offerAgainFrom101(again, o);
    }
}
