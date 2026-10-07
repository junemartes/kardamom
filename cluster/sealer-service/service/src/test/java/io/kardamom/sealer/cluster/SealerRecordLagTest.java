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
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;

/**
 * The record-lag guard on the service: the recorded-cursor frame the Rust
 * executor writes, the status tail every session reads, and the reject
 * frame the offering session gets. The byte layouts are the contract with
 * the Rust codec, so they are pinned at their literal offsets.
 */
class SealerRecordLagTest {

    private static final long BUDGET = 2L;
    /** Executors 0 and 1 are the voters. */
    private static final VoidLedger.Config VOTERS = new VoidLedger.Config(64, 0b11L);

    private final StubCluster cluster = new StubCluster();
    private StubSession publisher;
    private StubSession executor;
    private SealerClusteredService service;

    private void start(final long budget) {
        service = new SealerClusteredService(
            64, 250, 0, Set.of(), VOTERS,
            CanonicalSealerState.DEFAULT_INCLUSION_HORIZON_BLOCKS,
            CanonicalSealerState.DEFAULT_ORDERING_WINDOW, new LagBudgets(0L, budget));
        service.onStart(cluster, null);
        publisher = cluster.addSession(1);
        executor = cluster.addSession(2);
        deliver(executor, IngressFrames.subscribeFrame());
    }

    private void deliver(final StubSession from, final byte[] frame) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        buf.putBytes(0, frame);
        service.onSessionMessage(from, 0, buf, 0, frame.length, null);
    }

    private static byte[] sender() {
        final byte[] s = new byte[CanonicalSealerState.SENDER_LEN];
        Arrays.fill(s, (byte) 0xAA);
        return s;
    }

    /** Order user records with nonces {@code from} to {@code to - 1}. */
    private void users(final int from, final int to) {
        java.util.stream.IntStream.range(from, to)
            .forEach(n -> deliver(publisher, IngressFrames.recordFrame(n, sender(), n)));
    }

    private static List<byte[]> ofKind(final StubSession s, final byte kind) {
        return s.offered.stream().filter(f -> f[0] == kind).toList();
    }

    private static ByteBuffer lastStatus(final StubSession s) {
        final List<byte[]> statuses = ofKind(s, SealerWire.EGRESS_KIND_STATUS);
        return ByteBuffer.wrap(statuses.get(statuses.size() - 1)).order(ByteOrder.LITTLE_ENDIAN);
    }

    @Test
    void the_recorded_cursor_frame_matches_the_rust_encoder() {
        assertEquals(9, SealerWire.KIND_RECORDED_CURSOR);
        assertEquals(13, SealerWire.EGRESS_KIND_RECORD_LAG_REJECT);
        assertEquals(10, SealerWire.RECORDED_CURSOR_LEN);
        assertEquals(85, SealerWire.MIN_INGRESS_LEN);
        assertTrue(SealerWire.RECORDED_CURSOR_LEN < SealerWire.MIN_INGRESS_LEN,
            "a member that does not know kind 9 drops the frame as a short record");
        assertArrayEquals(
            new byte[] {9, 3, 0x02, 0x01, 0, 0, 0, 0, 0, 0},
            IngressFrames.recordedCursorFrame(3, 0x0102L));
    }

    @Test
    void a_cursor_that_moves_the_best_up_broadcasts_the_status_tail() {
        start(0L);
        users(0, 3);
        final int before = ofKind(publisher, SealerWire.EGRESS_KIND_STATUS).size();
        deliver(executor, IngressFrames.recordedCursorFrame(1, 2L));
        assertEquals(before + 1, ofKind(publisher, SealerWire.EGRESS_KIND_STATUS).size(),
            "a cursor that moves the best up broadcasts the status");
        final ByteBuffer s = lastStatus(publisher);
        assertEquals(67, s.capacity());
        assertEquals(2L, s.getLong(50), "best recorded");
        assertEquals(0L, s.getLong(58), "the guard is off");
        assertEquals(0, s.get(66));

        deliver(executor, IngressFrames.recordedCursorFrame(0, 1L));
        assertEquals(before + 1, ofKind(publisher, SealerWire.EGRESS_KIND_STATUS).size(),
            "a cursor below the best sends no status");
    }

    @Test
    void a_short_frame_a_stranger_and_a_cursor_past_the_head_are_dropped() {
        start(BUDGET);
        users(0, 2);
        final int before = ofKind(publisher, SealerWire.EGRESS_KIND_STATUS).size();
        deliver(executor, Arrays.copyOf(IngressFrames.recordedCursorFrame(0, 1L), 9));
        deliver(executor, IngressFrames.recordedCursorFrame(2, 1L));
        deliver(executor, IngressFrames.recordedCursorFrame(0, 2L));
        assertEquals(before, ofKind(publisher, SealerWire.EGRESS_KIND_STATUS).size(),
            "no frame moved a cursor");
        assertEquals(0, ofKind(executor, SealerWire.EGRESS_KIND_RELAYED).stream()
            .filter(f -> f.length == SealerWire.RECORDED_CURSOR_LEN).count(),
            "no cursor frame is ordered as a record");
    }

    @Test
    void past_the_budget_the_offering_session_gets_the_reject_and_the_status_flips() {
        start(BUDGET);
        users(0, 1);
        deliver(executor, IngressFrames.recordedCursorFrame(0, 0L));
        users(1, 4); // last index 3, best 0: lag 3 > 2
        service.onTimerEvent(SealerClusteredService.BOUNDARY_TIMER_CORRELATION_ID, 250);
        assertEquals(1, lastStatus(executor).get(66), "the tick's status says halted");
        users(4, 5);

        final List<byte[]> rejects = ofKind(publisher, SealerWire.EGRESS_KIND_RECORD_LAG_REJECT);
        assertEquals(1, rejects.size(), "the offering session gets the reject");
        assertEquals(0, ofKind(executor, SealerWire.EGRESS_KIND_RECORD_LAG_REJECT).size());
        // [kind:13][sender:20][nonce][sealed_index][recorded_index][budget]
        final byte[] reject = rejects.get(0);
        assertEquals(1 + 20 + 8 * 4, reject.length);
        final ByteBuffer r = ByteBuffer.wrap(reject).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(13, r.get(0));
        assertArrayEquals(sender(), Arrays.copyOfRange(reject, 1, 21));
        assertEquals(4L, r.getLong(21), "nonce");
        assertEquals(3L, r.getLong(29), "sealed index");
        assertEquals(0L, r.getLong(37), "recorded index");
        assertEquals(BUDGET, r.getLong(45), "budget");

        deliver(executor, IngressFrames.recordedCursorFrame(1, 3L));
        assertEquals(0, lastStatus(executor).get(66), "the cursor clears the flag");
        users(4, 5);
        assertEquals(5, ofKind(executor, SealerWire.EGRESS_KIND_RELAYED).size(), "the resubmit is ordered");
    }

    /**
     * The service writes a version-10 state section, so a member of the
     * previous release restores it. The cursors do not survive the restore.
     */
    @Test
    void a_snapshot_is_version_10_and_a_restore_writes_the_same_bytes() {
        start(BUDGET);
        users(0, 3);
        deliver(executor, IngressFrames.recordedCursorFrame(1, 2L));
        final byte[] snapshot = service.snapshot();
        assertEquals(10, ByteBuffer.wrap(snapshot).getInt(4), "the state section is version 10");

        final SealerClusteredService restored = new SealerClusteredService(
            64, 250, 0, Set.of(), VOTERS,
            CanonicalSealerState.DEFAULT_INCLUSION_HORIZON_BLOCKS,
            CanonicalSealerState.DEFAULT_ORDERING_WINDOW, new LagBudgets(0L, BUDGET));
        restored.onStart(new StubCluster(), null);
        restored.restore(snapshot);
        assertArrayEquals(snapshot, restored.snapshot(), "the restored member writes the same bytes");
    }
}
