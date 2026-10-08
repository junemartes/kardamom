package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.LagBudgets;
import io.kardamom.sealer.VoidLedger;
import io.kardamom.sealer.cluster.ClusterStubs.StubCluster;
import io.kardamom.sealer.cluster.ClusterStubs.StubSession;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Set;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The DA-lag guard on the service: the posted-cursor frame the Rust batcher
 * writes, the status frame every session reads, and the reject frame the
 * offering session gets. The byte layouts are the contract with the Rust
 * decoders, so they are pinned at their literal offsets.
 */
class SealerDaLagTest {

    private static final long BUDGET = 2L;

    private StubCluster cluster;
    private SealerClusteredService service;
    private StubSession publisher;
    private StubSession consumer;

    @BeforeEach
    void start() {
        cluster = new StubCluster();
        service = new SealerClusteredService(
            64, 250, 0, Set.of(), VoidLedger.Config.DISABLED,
            CanonicalSealerState.DEFAULT_INCLUSION_HORIZON_BLOCKS,
            CanonicalSealerState.DEFAULT_ORDERING_WINDOW, new LagBudgets(BUDGET, 0L));
        service.onStart(cluster, null);
        publisher = cluster.addSession(1);
        consumer = cluster.addSession(2);
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

    private void tick() {
        service.onTimerEvent(SealerClusteredService.BOUNDARY_TIMER_CORRELATION_ID, 250);
    }

    private static List<byte[]> ofKind(final StubSession s, final byte kind) {
        return s.offered.stream().filter(f -> f[0] == kind).toList();
    }

    private static ByteBuffer le(final byte[] frame) {
        return ByteBuffer.wrap(frame).order(ByteOrder.LITTLE_ENDIAN);
    }

    @Test
    void the_posted_cursor_frame_matches_the_rust_encoder() {
        assertEquals(7, SealerWire.KIND_POSTED_CURSOR);
        assertEquals(1, SealerWire.POSTED_HEAD_OFFSET);
        assertEquals(9, SealerWire.MIN_POSTED_CURSOR_LEN);
        final byte[] frame = IngressFrames.postedCursorFrame(0x0102L);
        assertArrayEquals(new byte[] {7, 0x02, 0x01, 0, 0, 0, 0, 0, 0}, frame);
    }

    @Test
    void a_subscriber_learns_the_status_and_every_tick_broadcasts_it() {
        deliver(consumer, IngressFrames.subscribeFrame());
        final List<byte[]> onSubscribe = ofKind(consumer, SealerWire.EGRESS_KIND_STATUS);
        assertEquals(1, onSubscribe.size(), "one status at the announcement");
        assertEquals(0, ofKind(publisher, SealerWire.EGRESS_KIND_STATUS).size(),
            "the announcement's status goes to the announcing session only");

        tick();
        assertEquals(1, ofKind(publisher, SealerWire.EGRESS_KIND_STATUS).size(),
            "a tick broadcasts the status to every session");
        // [kind:9][posted_head][sealed_head][budget][halted:u8][retained][floor_index][floor_block]
        // then [best_recorded][record_lag_budget][record_lag_halted:u8]
        final byte[] status = ofKind(consumer, SealerWire.EGRESS_KIND_STATUS).get(1);
        assertEquals(1 + 8 * 6 + 1 + 8 * 2 + 1, status.length);
        final ByteBuffer b = le(status);
        assertEquals(9, b.get(0));
        assertEquals(0L, b.getLong(1), "posted head: nothing posted yet");
        assertEquals(1L, b.getLong(9), "sealed head after one tick");
        assertEquals(BUDGET, b.getLong(17));
        assertEquals(0, b.get(25), "lag 1 is within the budget");
        assertEquals(1L, b.getLong(26), "one boundary frame retained");
        assertEquals(0L, b.getLong(34), "floor index");
        assertEquals(1L, b.getLong(42), "floor block");
        assertEquals(-1L, b.getLong(50), "no recorded cursor: u64::MAX");
        assertEquals(0L, b.getLong(58), "the record-lag guard is off");
        assertEquals(0, b.get(66));
    }

    @Test
    void past_the_budget_the_offering_session_gets_the_reject_and_the_status_flips() {
        deliver(consumer, IngressFrames.subscribeFrame());
        tick();
        tick();
        tick(); // sealed head 3, posted head 0: lag 3 > 2
        deliver(publisher, IngressFrames.recordFrame(1, sender(), 0));

        final List<byte[]> rejects = ofKind(publisher, SealerWire.EGRESS_KIND_DA_LAG_REJECT);
        assertEquals(1, rejects.size(), "the offering session gets the reject");
        assertEquals(0, ofKind(consumer, SealerWire.EGRESS_KIND_DA_LAG_REJECT).size());
        assertEquals(0, ofKind(consumer, SealerWire.EGRESS_KIND_RELAYED).size(), "not ordered");
        // [kind:10][sender:20][nonce][sealed_head][posted_head][budget]
        final byte[] reject = rejects.get(0);
        assertEquals(1 + 20 + 8 * 4, reject.length);
        final ByteBuffer r = le(reject);
        assertEquals(10, r.get(0));
        assertArrayEquals(sender(), java.util.Arrays.copyOfRange(reject, 1, 21));
        assertEquals(0L, r.getLong(21), "nonce");
        assertEquals(3L, r.getLong(29), "sealed head");
        assertEquals(0L, r.getLong(37), "posted head");
        assertEquals(BUDGET, r.getLong(45), "budget");

        final List<byte[]> statuses = ofKind(consumer, SealerWire.EGRESS_KIND_STATUS);
        assertEquals(1, le(statuses.get(statuses.size() - 1)).get(25), "the last tick's status says halted");
    }

    @Test
    void the_cursor_resumes_the_guard_and_broadcasts_the_status() {
        deliver(consumer, IngressFrames.subscribeFrame());
        tick();
        tick();
        tick();
        deliver(publisher, IngressFrames.recordFrame(1, sender(), 0));
        assertEquals(1, ofKind(publisher, SealerWire.EGRESS_KIND_DA_LAG_REJECT).size());

        final int before = ofKind(publisher, SealerWire.EGRESS_KIND_STATUS).size();
        deliver(publisher, IngressFrames.postedCursorFrame(2L));
        final List<byte[]> statuses = ofKind(publisher, SealerWire.EGRESS_KIND_STATUS);
        assertEquals(before + 1, statuses.size(), "a cursor broadcasts the status");
        final ByteBuffer s = le(statuses.get(statuses.size() - 1));
        assertEquals(2L, s.getLong(1), "posted head");
        assertEquals(0, s.get(25), "lag 1 is within the budget");

        deliver(publisher, IngressFrames.recordFrame(1, sender(), 0));
        assertEquals(1, ofKind(consumer, SealerWire.EGRESS_KIND_RELAYED).size(), "the resubmit is ordered");
    }

    @Test
    void a_cursor_past_the_sealed_head_and_a_short_frame_are_dropped() {
        deliver(consumer, IngressFrames.subscribeFrame());
        tick();
        final int before = ofKind(consumer, SealerWire.EGRESS_KIND_STATUS).size();
        deliver(publisher, IngressFrames.postedCursorFrame(5L));
        deliver(publisher, java.util.Arrays.copyOf(IngressFrames.postedCursorFrame(1L), 5));
        assertEquals(before, ofKind(consumer, SealerWire.EGRESS_KIND_STATUS).size(),
            "neither frame moved the cursor");
        assertTrue(service.snapshot().length > 0);
    }
}
