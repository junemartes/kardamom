package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.VoidLedger;
import io.kardamom.sealer.cluster.ClusterStubs.StubCluster;
import io.kardamom.sealer.cluster.ClusterStubs.StubSession;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The void request on the service: the frame offsets that the Rust encoder
 * writes, and the void record on the consumer egress.
 */
class SealerVoidTest {
    private SealerClusteredService service;
    private StubSession publisher;
    private StubSession consumer;

    @BeforeEach
    void start() {
        final StubCluster cluster = new StubCluster();
        service = new SealerClusteredService(64, 250, 0, Set.of(), new VoidLedger.Config(16, 0b11L));
        service.onStart(cluster, null);
        publisher = cluster.addSession(1);
        consumer = cluster.addSession(2);
        deliver(consumer, IngressFrames.subscribeFrame());
    }

    private void deliver(final StubSession from, final byte[] frame) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        buf.putBytes(0, frame);
        service.onSessionMessage(from, 0, buf, 0, frame.length, null);
    }

    private static byte[] sender() {
        final byte[] s = new byte[CanonicalSealerState.SENDER_LEN];
        java.util.Arrays.fill(s, (byte) 0xAA);
        return s;
    }

    private List<byte[]> relayed() {
        return consumer.offered.stream()
            .filter(f -> f[0] == SealerWire.EGRESS_KIND_RELAYED)
            .collect(Collectors.toList());
    }

    @Test
    void the_request_offsets_match_the_rust_encoder() {
        assertEquals(1, SealerWire.VOID_VOTER_OFFSET);
        assertEquals(2, SealerWire.VOID_INDEX_OFFSET);
        assertEquals(10, SealerWire.VOID_HASH_OFFSET);
        assertEquals(42, SealerWire.MIN_VOID_REQUEST_LEN);
        assertEquals(6, SealerWire.KIND_VOID_REQUEST);
        assertEquals(4, SealerWire.RT_VOID);
    }

    @Test
    void the_last_vote_puts_the_void_record_on_the_consumer_egress() {
        deliver(publisher, IngressFrames.recordFrame(7, sender(), 0));
        deliver(consumer, IngressFrames.voidRequestFrame(0, 0, IngressFrames.recordId(7)));
        assertEquals(1, relayed().size(), "one vote of two relays nothing");

        deliver(consumer, IngressFrames.voidRequestFrame(1, 0, IngressFrames.recordId(7)));

        assertEquals(2, relayed().size());
        // [kind:1][index:u64 LE][payload_len:u32 LE][tx_hash:32][record_type:1][void_index:u64 LE]
        final ByteBuffer want = ByteBuffer.allocate(1 + 8 + 4 + 32 + 1 + 8).order(ByteOrder.LITTLE_ENDIAN);
        want.put(SealerWire.EGRESS_KIND_RELAYED).putLong(1L).putInt(41);
        want.put(IngressFrames.recordId(7)).put(SealerWire.RT_VOID).putLong(0L);
        assertArrayEquals(want.array(), relayed().get(1));
    }

    @Test
    void a_short_request_is_dropped() {
        deliver(publisher, IngressFrames.recordFrame(7, sender(), 0));
        final byte[] frame = IngressFrames.voidRequestFrame(0, 0, IngressFrames.recordId(7));
        deliver(consumer, java.util.Arrays.copyOf(frame, frame.length - 1));
        deliver(consumer, IngressFrames.voidRequestFrame(1, 0, IngressFrames.recordId(7)));

        assertEquals(1, relayed().size(), "the short frame is no vote, so the void is not decided");
    }
}
