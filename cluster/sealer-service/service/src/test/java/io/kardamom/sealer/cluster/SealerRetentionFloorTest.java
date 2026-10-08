package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.LagBudgets;
import io.kardamom.sealer.VoidLedger;
import io.kardamom.sealer.cluster.ClusterStubs.StubCluster;
import io.kardamom.sealer.cluster.ClusterStubs.StubSession;
import java.util.Set;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Nothing is pruned below the posted head. The retention window is a
 * minimum: a frame past it stays while its block is unposted, the floor
 * follows the batcher's cursor, and a snapshot carries the retained frames
 * so a restored member keeps the floor instead of raising it to the
 * restore point.
 */
class SealerRetentionFloorTest {

    private StubCluster cluster;
    private SealerClusteredService service;
    private StubSession publisher;

    @BeforeEach
    void start() {
        // A window of three frames; the guard off, so the lag never refuses.
        System.setProperty("kardamom.cluster.retention", "3");
        cluster = new StubCluster();
        service = newService();
        service.onStart(cluster, null);
        publisher = cluster.addSession(1);
    }

    @AfterEach
    void clearRetentionOverride() {
        System.clearProperty("kardamom.cluster.retention");
    }

    private static SealerClusteredService newService() {
        return new SealerClusteredService(
            64, 250, 0, Set.of(), VoidLedger.Config.DISABLED,
            CanonicalSealerState.DEFAULT_INCLUSION_HORIZON_BLOCKS,
            CanonicalSealerState.DEFAULT_ORDERING_WINDOW, new LagBudgets(0L, 0L));
    }

    private void deliver(
            final SealerClusteredService svc, final StubSession from, final byte[] frame) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        buf.putBytes(0, frame);
        svc.onSessionMessage(from, 0, buf, 0, frame.length, null);
    }

    /** One block on {@code svc}: records {@code from..to}, then a seal. */
    private void block(final SealerClusteredService svc, final int from, final int to) {
        final byte[] sender = new byte[CanonicalSealerState.SENDER_LEN];
        java.util.Arrays.fill(sender, (byte) 0xAA);
        for (int n = from; n < to; n++) {
            deliver(svc, publisher, IngressFrames.recordFrame(n, sender, n));
        }
        svc.onTimerEvent(SealerClusteredService.BOUNDARY_TIMER_CORRELATION_ID, 250);
    }

    /** Blocks 1..4 on the service under test, two records each: four windows of frames. */
    private void fourBlocks() {
        block(service, 0, 2);
        block(service, 2, 4);
        block(service, 4, 6);
        block(service, 6, 8);
    }

    private static long count(final StubSession s, final byte kind) {
        return s.offered.stream().filter(f -> f[0] == kind).count();
    }

    private StubSession replay(
            final SealerClusteredService svc, final int sessionId, final long fromIndex, final long fromBlock) {
        final StubSession consumer = cluster.addSession(sessionId);
        deliver(svc, consumer, IngressFrames.replayRequestFrame(fromIndex, fromBlock));
        return consumer;
    }

    @Test
    void a_replay_older_than_the_window_is_served_while_the_range_is_unposted() {
        fourBlocks();
        final StubSession consumer = replay(service, 2, 0, 1);
        assertEquals(0, count(consumer, SealerWire.EGRESS_KIND_REPLAY_UNAVAILABLE));
        assertEquals(8, count(consumer, SealerWire.EGRESS_KIND_RELAYED), "every record from genesis");
        assertEquals(4, count(consumer, SealerWire.EGRESS_KIND_BOUNDARY));
        assertEquals(1, count(consumer, SealerWire.EGRESS_KIND_REPLAY_DONE));
    }

    @Test
    void the_floor_advances_with_the_posted_head() {
        fourBlocks();
        // Blocks 1 and 2 are posted: their six frames may leave, down to the window.
        deliver(service, publisher, IngressFrames.postedCursorFrame(2L));
        final StubSession fromGenesis = replay(service, 2, 0, 1);
        assertEquals(1, count(fromGenesis, SealerWire.EGRESS_KIND_REPLAY_UNAVAILABLE),
            "genesis is below the posted head and past the window");
        // Block 3 starts at index 4: at the posted head, and served.
        final StubSession fromPosted = replay(service, 3, 4, 3);
        assertEquals(0, count(fromPosted, SealerWire.EGRESS_KIND_REPLAY_UNAVAILABLE));
        assertEquals(4, count(fromPosted, SealerWire.EGRESS_KIND_RELAYED), "records 4..7");
        assertEquals(2, count(fromPosted, SealerWire.EGRESS_KIND_BOUNDARY));
    }

    @Test
    void the_window_stays_a_minimum_once_everything_is_posted() {
        fourBlocks();
        deliver(service, publisher, IngressFrames.postedCursorFrame(4L));
        // Everything is posted, so the window shrinks to its three newest frames:
        // record 7 and the boundaries of blocks 3 and 4 are gone from the front.
        final StubSession consumer = replay(service, 2, 6, 4);
        assertEquals(0, count(consumer, SealerWire.EGRESS_KIND_REPLAY_UNAVAILABLE),
            "the newest block is still inside the window");
        final StubSession older = replay(service, 3, 4, 3);
        assertEquals(1, count(older, SealerWire.EGRESS_KIND_REPLAY_UNAVAILABLE),
            "a posted block past the window is gone");
    }

    @Test
    void a_snapshot_restore_keeps_the_floor_at_the_posted_head() {
        fourBlocks();
        deliver(service, publisher, IngressFrames.postedCursorFrame(2L));
        final byte[] snapshot = service.snapshot();

        final SealerClusteredService restored = newService();
        restored.onStart(cluster, null);
        restored.restore(snapshot);

        // The frames above the posted head came with the snapshot, so a
        // replay from the posted head is served, not refused at the restore point.
        final StubSession fromPosted = replay(restored, 4, 4, 3);
        assertEquals(0, count(fromPosted, SealerWire.EGRESS_KIND_REPLAY_UNAVAILABLE));
        assertEquals(4, count(fromPosted, SealerWire.EGRESS_KIND_RELAYED), "records 4..7");
        final StubSession fromGenesis = replay(restored, 5, 0, 1);
        assertEquals(1, count(fromGenesis, SealerWire.EGRESS_KIND_REPLAY_UNAVAILABLE),
            "below the posted head stays refused");
        // The restored member keeps the posted head as its floor afterwards.
        block(restored, 8, 10);
        final StubSession stillServed = replay(restored, 6, 4, 3);
        assertEquals(0, count(stillServed, SealerWire.EGRESS_KIND_REPLAY_UNAVAILABLE));
    }
}
