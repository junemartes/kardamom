package io.kardamom.sealer.cluster;

import static io.kardamom.sealer.cluster.ClusterTestHarness.awaitCondition;
import static io.kardamom.sealer.cluster.IngressFrames.canonicalId;
import static io.kardamom.sealer.cluster.IngressFrames.offerRecord;
import static io.kardamom.sealer.cluster.IngressFrames.offerReplayRequest;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Session messages of up to 1 MiB cross the cluster in both directions. A
 * client offers an ingress record, every member orders it, and the leader
 * relays it on egress. Every channel uses the deployed MTU of 1344 bytes,
 * so each message travels as many Aeron fragments. The cluster log term is
 * 8 MiB, as deployed, so the log takes a message of up to 1 MiB.
 */
@ExtendWith(InterruptingTestCallback.class)
class SealerLargeMessageClusterTest {

    private static final int MEMBER_COUNT = 3;
    private static final int DEDUP_CAPACITY = 8192;
    /** No boundary tick fires inside the test, so only the records reach egress. */
    private static final long TICK_MS = 600_000L;
    /** The longest session message: the 8 MiB term divided by 8. */
    private static final int MAX_MESSAGE_LEN = 1 << 20;
    /** The session header in front of the payload of each session message. */
    private static final int SESSION_HEADER_LEN = 32;
    private static final int KIB_64 = 64 * 1024;
    /** The largest application payload: one session message of exactly 1 MiB. */
    private static final int MAX_PAYLOAD_LEN = MAX_MESSAGE_LEN - SESSION_HEADER_LEN;

    private static final String CONTROL = "aeron:udp?term-length=8m|mtu=1344|sparse=true";
    private static final String LOG_CHANNEL = CONTROL + "|alias=raft";
    private static final String INGRESS_CHANNEL = CONTROL + "|alias=ingress";
    private static final String EGRESS_CHANNEL = CONTROL + "|endpoint=localhost:0|alias=egress";
    /** A 128 KiB egress term takes a message of at most 16 KiB. */
    private static final String SMALL_EGRESS_CHANNEL =
            "aeron:udp?term-length=128k|mtu=1344|endpoint=localhost:0|alias=egress";

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    /** One ingress record: an application payload of {@code length} bytes in total. */
    private record Message(int tag, int length) {

        byte[] id() {
            return canonicalId(tag);
        }

        /** The opaque payload after the canonical id, filled with a pattern of the tag. */
        byte[] payload() {
            final byte[] payload = new byte[length - SealerWire.MIN_INGRESS_LEN];
            IntStream.range(0, payload.length).forEach(i -> payload[i] = (byte) (i * 31 + tag));
            return payload;
        }

        /** What the leader relays: {@code [canonical_id|payload]}. */
        byte[] relayed() {
            final byte[] id = id();
            final byte[] payload = payload();
            final byte[] out = new byte[id.length + payload.length];
            System.arraycopy(id, 0, out, 0, id.length);
            System.arraycopy(payload, 0, out, id.length, payload.length);
            return out;
        }

        void offer(final AeronCluster client) {
            offerRecord(client, id(), payload());
        }
    }

    @Test
    @InterruptAfter(value = 120, unit = TimeUnit.SECONDS)
    void messagesUpToOneMibCrossTheClusterBothWaysIntactAndInOrder() {
        try (TestCluster cluster = startCluster(EGRESS_CHANNEL)) {
            final RecordingEgressListener egress = new RecordingEgressListener();
            cluster.egressListener(egress);
            final AeronCluster client = cluster.connectClient();
            final List<Message> sent = List.of(
                    new Message(1, KIB_64), new Message(2, MAX_PAYLOAD_LEN), new Message(3, KIB_64));

            sent.forEach(message -> message.offer(client));
            awaitCondition(client, () -> egress.relayedPayloads.size() >= sent.size());

            assertEquals(List.of(0L, 1L, 2L), egress.relayedIndexes);
            IntStream.range(0, sent.size()).forEach(i -> assertArrayEquals(
                    sent.get(i).relayed(), egress.relayedPayloads.get(i), "relayed record " + i));
            assertMembersEqual(cluster, client, MAX_MESSAGE_LEN);
        }
    }

    /**
     * A relayed frame above the maximum message length of a session's egress
     * publication closes that session. The leader stays live and equal to
     * the followers, and a session with a large egress term gets the frame
     * by replay.
     */
    @Test
    @InterruptAfter(value = 120, unit = TimeUnit.SECONDS)
    void aFrameAboveTheEgressBoundClosesTheSessionAndTheClusterStaysLive() {
        try (TestCluster cluster = startCluster(SMALL_EGRESS_CHANNEL)) {
            final RecordingEgressListener egress = new RecordingEgressListener();
            cluster.egressListener(egress);
            final AeronCluster small = cluster.connectClient();
            final Message message = new Message(1, KIB_64);

            message.offer(small);
            // The log holds the record once the applied position passes its length.
            assertMembersEqual(cluster, small, KIB_64);

            cluster.egressChannel(EGRESS_CHANNEL);
            final AeronCluster large = cluster.reconnectClient();
            offerReplayRequest(large, 0L, 1L);
            awaitCondition(large, () -> egress.replayDoneCount > 0);

            assertEquals(List.of(0L), egress.relayedIndexes);
            assertArrayEquals(message.relayed(), egress.relayedPayloads.get(0));
            assertMembersEqual(cluster, large, KIB_64);
        }
    }

    private TestCluster startCluster(final String egressChannel) {
        final TestCluster cluster = ClusterTestHarness.start(systemTestWatcher,
                ClusterTestHarness.builder(MEMBER_COUNT, DEDUP_CAPACITY, TICK_MS)
                        .withLogChannel(LOG_CHANNEL));
        cluster.awaitLeader();
        cluster.ingressChannel(INGRESS_CHANNEL);
        cluster.egressChannel(egressChannel);
        return cluster;
    }

    /**
     * Wait until the leader applied the log past {@code minPosition}, and
     * every member applied the leader's log. Then compare their states.
     */
    private static void assertMembersEqual(
            final TestCluster cluster, final AeronCluster client, final long minPosition) {
        final List<SealerClusteredService> members = IntStream.range(0, MEMBER_COUNT)
                .mapToObj(i -> ((SealerTestService) cluster.node(i).service()).delegate())
                .toList();
        final SealerClusteredService leader = members.get(cluster.findLeader().index());
        awaitCondition(client, () -> leader.servicePosition() > minPosition);
        final long applied = leader.servicePosition();
        awaitCondition(client, () -> members.stream().allMatch(m -> m.servicePosition() == applied));
        final byte[] leaderState = leader.snapshot();
        members.forEach(member -> assertArrayEquals(leaderState, member.snapshot()));
    }
}
