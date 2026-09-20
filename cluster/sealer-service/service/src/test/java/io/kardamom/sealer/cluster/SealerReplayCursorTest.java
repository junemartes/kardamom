package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.cluster.ClusterStubs.StubCluster;
import io.kardamom.sealer.cluster.ClusterStubs.StubSession;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A replay request names a record index and a block number. The two select
 * frames on separate axes, so a pair that does not name one point of the
 * stream makes the consumer skip records or apply them twice, and the
 * consumer cannot see it: it seeds every counter from the same cursor. The
 * member holds the boundaries, so it refuses a pair outside the block it
 * names. A state rebuilt from L1 with a wrong end index is the case this
 * guards: the refusal routes the consumer into its repair path.
 */
class SealerReplayCursorTest {

    private StubCluster cluster;
    private SealerClusteredService service;
    private StubSession publisher;

    @BeforeEach
    void start() {
        cluster = new StubCluster();
        service = new SealerClusteredService(64, 250, 0);
        service.onStart(cluster, null);
        publisher = cluster.addSession(1);
        // Block 1 holds records 0..2 and ends at index 3. Block 2 holds
        // records 3..4 and ends at index 5.
        records(0, 3);
        tick();
        records(3, 5);
        tick();
    }

    private void deliver(final StubSession from, final byte[] frame) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        buf.putBytes(0, frame);
        service.onSessionMessage(from, 0, buf, 0, frame.length, null);
    }

    private void records(final int from, final int to) {
        final byte[] sender = new byte[CanonicalSealerState.SENDER_LEN];
        java.util.Arrays.fill(sender, (byte) 0xAA);
        for (int n = from; n < to; n++) {
            deliver(publisher, IngressFrames.recordFrame(n, sender, n));
        }
    }

    private void tick() {
        service.onTimerEvent(SealerClusteredService.BOUNDARY_TIMER_CORRELATION_ID, 250);
    }

    private static long count(final StubSession s, final byte kind) {
        return s.offered.stream().filter(f -> f[0] == kind).count();
    }

    private StubSession replay(final int sessionId, final long fromIndex, final long fromBlock) {
        final StubSession consumer = cluster.addSession(sessionId);
        deliver(consumer, IngressFrames.replayRequestFrame(fromIndex, fromBlock));
        return consumer;
    }

    @Test
    void aColdStartAtABlockEndIsServed() {
        final StubSession consumer = replay(2, 3, 2);
        assertEquals(0, count(consumer, SealerWire.EGRESS_KIND_REPLAY_UNAVAILABLE));
        assertEquals(2, count(consumer, SealerWire.EGRESS_KIND_RELAYED), "records 3 and 4");
        assertEquals(1, count(consumer, SealerWire.EGRESS_KIND_BOUNDARY), "the boundary of block 2");
        assertEquals(1, count(consumer, SealerWire.EGRESS_KIND_REPLAY_DONE));
    }

    @Test
    void aReconnectInsideABlockIsServed() {
        final StubSession consumer = replay(2, 4, 2);
        assertEquals(0, count(consumer, SealerWire.EGRESS_KIND_REPLAY_UNAVAILABLE));
        assertEquals(1, count(consumer, SealerWire.EGRESS_KIND_RELAYED), "record 4");
    }

    @Test
    void aStartFromGenesisIsServed() {
        final StubSession consumer = replay(2, 0, 1);
        assertEquals(0, count(consumer, SealerWire.EGRESS_KIND_REPLAY_UNAVAILABLE));
        assertEquals(5, count(consumer, SealerWire.EGRESS_KIND_RELAYED));
    }

    @Test
    void anIndexBelowTheNamedBlockIsRefused() {
        // Block 2 starts at index 3. Index 1 would apply records 1 and 2 twice.
        final StubSession consumer = replay(2, 1, 2);
        assertEquals(1, count(consumer, SealerWire.EGRESS_KIND_REPLAY_UNAVAILABLE));
        assertEquals(0, count(consumer, SealerWire.EGRESS_KIND_RELAYED));
    }

    @Test
    void anIndexPastTheNamedBlockIsRefused() {
        // Block 1 ends at index 3. Index 4 would skip record 3.
        final StubSession consumer = replay(2, 4, 1);
        assertEquals(1, count(consumer, SealerWire.EGRESS_KIND_REPLAY_UNAVAILABLE));
        assertEquals(0, count(consumer, SealerWire.EGRESS_KIND_RELAYED));
    }

    @Test
    void aSideWithNoRetainedBoundaryIsNotChecked() {
        assertTrue(SealerEgress.cursorInsideBlock(7, -1, -1));
        assertTrue(SealerEgress.cursorInsideBlock(3, 3, -1));
        assertTrue(SealerEgress.cursorInsideBlock(5, 3, 5));
        assertFalse(SealerEgress.cursorInsideBlock(2, 3, -1));
        assertFalse(SealerEgress.cursorInsideBlock(6, -1, 5));
    }
}
