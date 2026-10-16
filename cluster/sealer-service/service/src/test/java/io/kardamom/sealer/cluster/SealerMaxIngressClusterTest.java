package io.kardamom.sealer.cluster;

import static io.kardamom.sealer.cluster.ClusterTestHarness.awaitCondition;
import static io.kardamom.sealer.cluster.IngressFrames.canonicalId;
import static io.kardamom.sealer.cluster.IngressFrames.offerFrame;
import static io.kardamom.sealer.cluster.IngressFrames.offerIngress;
import static io.kardamom.sealer.cluster.IngressFrames.offerReplayRequest;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * An ingress message at the ingress cap, of each relayed record type, crosses
 * the cluster and comes back on egress, live and by replay. The relayed
 * frame fits the egress session message, so the session stays open: no
 * close, and no reconnect loop. Every channel has an 8 MiB term and the
 * deployed MTU of 1344 bytes.
 */
@ExtendWith(InterruptingTestCallback.class)
class SealerMaxIngressClusterTest {

    private static final int MEMBER_COUNT = 3;
    private static final int DEDUP_CAPACITY = 8192;
    /**
     * The service sends a back-pressured backlog on each log-driven
     * callback, and the boundary tick is one. A replay of 1 MiB frames meets
     * back-pressure, so the tick runs often.
     */
    private static final long TICK_MS = 200L;
    private static final long CHAIN = 10L;
    private static final int MAX_APP_PAYLOAD_LEN =
        SealerWire.MAX_SESSION_MESSAGE_LEN - SealerWire.SESSION_HEADER_LEN;

    private static final String CONTROL = "aeron:udp?term-length=8m|mtu=1344|sparse=true";
    private static final String LOG_CHANNEL = CONTROL + "|alias=raft";
    private static final String INGRESS_CHANNEL = CONTROL + "|alias=ingress";
    private static final String EGRESS_CHANNEL = CONTROL + "|endpoint=localhost:0|alias=egress";

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    /** One ingress frame at the cap: a relayed kind, its record type, and a filler body. */
    private record MaxFrame(int tag, RelayShape shape, byte recordType) {

        /** The record type, then a filler body that fills the frame to the cap. */
        byte[] body(final int headerLength) {
            final byte[] body = new byte[RelayShape.MAX_INGRESS_PAYLOAD_LEN - headerLength];
            IntStream.range(0, body.length).forEach(i -> body[i] = (byte) (i * 31 + tag));
            body[0] = recordType;
            return body;
        }

        byte[] frame() {
            return switch (shape) {
                case INGRESS_RECORD -> IngressFrames.exemptRecordFrame(
                    canonicalId(tag), body(SealerWire.MIN_INGRESS_LEN));
                case ORIGIN_RECORD -> IngressFrames.originRecordFrame(
                    canonicalId(tag), 1, 1, body(SealerWire.MIN_ORIGIN_RECORD_LEN));
                case REMOTE_ORIGIN_RECORD -> IngressFrames.remoteOriginRecordFrame(
                    canonicalId(tag), CHAIN, 700, 2, 0, 0, body(SealerWire.MIN_REMOTE_ORIGIN_RECORD_LEN));
            };
        }

        /** What the leader relays: the canonical id, then the record type and the body. */
        byte[] relayed() {
            final byte[] body = body(shape.tailOffset());
            final byte[] out = Arrays.copyOf(canonicalId(tag), canonicalId(tag).length + body.length);
            System.arraycopy(body, 0, out, canonicalId(tag).length, body.length);
            return out;
        }
    }

    @Test
    @InterruptAfter(value = 180, unit = TimeUnit.SECONDS)
    void aMessageAtTheCapOfEachRecordTypeRelaysAndReplaysWithNoClose() {
        try (TestCluster cluster = startCluster()) {
            final RecordingEgressListener egress = new RecordingEgressListener();
            cluster.egressListener(egress);
            final AeronCluster client = cluster.connectClient();
            final List<MaxFrame> sent = List.of(
                new MaxFrame(1, RelayShape.INGRESS_RECORD, (byte) 0),
                new MaxFrame(2, RelayShape.INGRESS_RECORD, (byte) 1),
                new MaxFrame(3, RelayShape.INGRESS_RECORD, SealerWire.RT_TX_OFFER),
                new MaxFrame(4, RelayShape.ORIGIN_RECORD, (byte) 2),
                new MaxFrame(5, RelayShape.REMOTE_ORIGIN_RECORD, (byte) 3));
            assertTrue(sent.stream().allMatch(f -> f.frame().length == RelayShape.MAX_INGRESS_PAYLOAD_LEN));

            sent.forEach(frame -> offerFrame(client, frame.frame()));
            awaitCondition(client, () -> egress.relayedPayloads.size() >= sent.size());
            assertRelayed(sent, egress.relayedPayloads.subList(0, sent.size()));

            offerReplayRequest(client, 0L, 1L);
            awaitCondition(client, () -> egress.replayDoneCount > 0);
            assertRelayed(sent, egress.relayedPayloads.subList(sent.size(), 2 * sent.size()));

            // The same session still takes frames: no close, so no reconnect.
            offerIngress(client, canonicalId(99));
            awaitCondition(client, () -> egress.relayedPayloads.size() > 2 * sent.size());
            assertFalse(client.isClosed());
        }
    }

    /** Each relayed payload is the expected one, and its egress frame fits one session message. */
    private static void assertRelayed(final List<MaxFrame> sent, final List<byte[]> relayed) {
        IntStream.range(0, sent.size()).forEach(i -> {
            assertArrayEquals(sent.get(i).relayed(), relayed.get(i), "relayed record " + i);
            assertTrue(SealerWire.RELAYED_HEADER_LEN + relayed.get(i).length <= MAX_APP_PAYLOAD_LEN);
        });
        final byte[] origin = relayed.get(3);
        assertEquals(MAX_APP_PAYLOAD_LEN, SealerWire.RELAYED_HEADER_LEN + origin.length,
            "an origin record at the cap fills one egress session message");
        assertEquals(SealerWire.RT_TX_OFFER, new UnsafeBuffer(relayed.get(2)).getByte(32),
            "the record path relays a full entry as is");
    }

    private TestCluster startCluster() {
        final TestCluster cluster = ClusterTestHarness.start(systemTestWatcher, TestCluster.aCluster()
            .withStaticNodes(MEMBER_COUNT)
            .withLogChannel(LOG_CHANNEL)
            .withServiceSupplier(memberId -> new TestNode.TestService[] {
                new SealerTestService(new SealerClusteredService(
                    DEDUP_CAPACITY, TICK_MS, memberId, Set.of(CHAIN))).index(memberId)
            }));
        cluster.awaitLeader();
        cluster.ingressChannel(INGRESS_CHANNEL);
        cluster.egressChannel(EGRESS_CHANNEL);
        return cluster;
    }
}
