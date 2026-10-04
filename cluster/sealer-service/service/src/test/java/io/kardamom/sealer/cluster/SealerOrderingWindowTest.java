package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.OrderingWindow;
import io.kardamom.sealer.cluster.ClusterStubs.StubCluster;
import io.kardamom.sealer.cluster.ClusterStubs.StubSession;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;

/**
 * The ordering window in front of the record path: a window flushes when it
 * is full, when its hold timer fires, or when a boundary closes the block,
 * and it relays in {@code (tip descending, arrival ascending)} order with one
 * sender's records in nonce order. Two members fed the same log events relay
 * the same frames.
 */
class SealerOrderingWindowTest {

    /** A small window, so a test fills it with three records. */
    private static final int WINDOW = 3;

    /** One service over a stub cluster, with its publisher and one consumer. */
    private static final class Member {
        final StubCluster cluster = new StubCluster();
        final SealerClusteredService service;
        final StubSession publisher;
        final StubSession consumer;

        Member(final int window) {
            service = new SealerClusteredService(64, 250, 0, java.util.Set.of(),
                    io.kardamom.sealer.VoidLedger.Config.DISABLED,
                    CanonicalSealerState.DEFAULT_INCLUSION_HORIZON_BLOCKS, window);
            service.onStart(cluster, null);
            publisher = cluster.addSession(1);
            consumer = cluster.addSession(2);
            deliver(consumer, IngressFrames.subscribeFrame());
        }

        void deliver(final StubSession from, final byte[] frame) {
            final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
            buf.putBytes(0, frame);
            service.onSessionMessage(from, 0, buf, 0, frame.length, null);
        }

        /** One record of {@code sender} at {@code nonce}, tagged and bidding {@code tip}. */
        void record(final int idTag, final int sender, final long nonce, final long tip) {
            deliver(publisher, IngressFrames.recordFrame(idTag, sender(sender), nonce, Long.MAX_VALUE, tip));
        }

        void windowTimer() {
            service.onTimerEvent(SealerClusteredService.WINDOW_TIMER_CORRELATION_ID, 5);
        }

        void boundaryTimer() {
            service.onTimerEvent(SealerClusteredService.BOUNDARY_TIMER_CORRELATION_ID, 250);
        }

        /** The id tags of the relayed records, in egress order; a boundary shows as -1. */
        List<Integer> relayed() {
            final List<Integer> out = new ArrayList<>();
            for (final byte[] f : consumer.offered) {
                if (f[0] == SealerWire.EGRESS_KIND_RELAYED) {
                    // [kind:1][index:8][len:4][canonical_id:32]: the id's first byte is the tag.
                    out.add((int) f[1 + 8 + 4]);
                } else if (f[0] == SealerWire.EGRESS_KIND_BOUNDARY) {
                    out.add(-1);
                }
            }
            return out;
        }

        long armed(final long correlationId) {
            return cluster.scheduledTimers.stream().filter(t -> t[0] == correlationId).count();
        }
    }

    private static byte[] sender(final int tag) {
        final byte[] s = new byte[CanonicalSealerState.SENDER_LEN];
        Arrays.fill(s, (byte) 0xAA);
        s[0] = (byte) tag;
        return s;
    }

    @Test
    void aFullWindowFlushesInTipOrder() {
        final Member m = new Member(WINDOW);
        m.record(10, 1, 0, 1);
        m.record(11, 2, 0, 9);
        assertEquals(List.of(), m.relayed(), "two records stay held");
        assertEquals(1, m.armed(SealerClusteredService.WINDOW_TIMER_CORRELATION_ID),
                "the record that opened the window armed the hold timer");
        m.record(12, 3, 0, 5);
        assertEquals(List.of(11, 12, 10), m.relayed(), "the third record fills the window");
        assertEquals(1, m.armed(SealerClusteredService.WINDOW_TIMER_CORRELATION_ID),
                "a flush on the count arms nothing");
    }

    @Test
    void theHoldTimerFlushesAnOpenWindow() {
        final Member m = new Member(WINDOW);
        m.record(10, 1, 0, 1);
        m.record(11, 2, 0, 9);
        m.windowTimer();
        assertEquals(List.of(11, 10), m.relayed());
        // A stale expiry on an empty window changes nothing.
        m.windowTimer();
        assertEquals(List.of(11, 10), m.relayed());
        // The next record opens a new window and arms the timer again.
        m.record(12, 3, 0, 5);
        assertEquals(2, m.armed(SealerClusteredService.WINDOW_TIMER_CORRELATION_ID));
    }

    @Test
    void aBoundaryFlushesTheWindowBeforeItCloses() {
        final Member m = new Member(WINDOW);
        m.record(10, 1, 0, 1);
        m.record(11, 2, 0, 9);
        m.boundaryTimer();
        assertEquals(List.of(11, 10, -1), m.relayed(), "held records land in the block the boundary closes");
    }

    @Test
    void oneSendersRecordsKeepNonceOrderAcrossTips() {
        final Member m = new Member(5);
        m.record(10, 1, 0, 1);
        m.record(11, 2, 0, 5);
        m.record(12, 1, 1, 9);
        m.record(13, 1, 2, 7);
        m.windowTimer();
        assertEquals(List.of(11, 10, 12, 13), m.relayed());
        // Every record relayed: the guard saw nonces 0, 1, 2 in order.
        assertEquals(0, m.publisher.offered.size(), "no reject reached the publisher");
    }

    @Test
    void aZeroWindowRelaysEveryRecordAtOnce() {
        final Member m = new Member(0);
        m.record(10, 1, 0, 1);
        m.record(11, 2, 0, 9);
        assertEquals(List.of(10, 11), m.relayed(), "arrival order, no hold");
        assertEquals(0, m.armed(SealerClusteredService.WINDOW_TIMER_CORRELATION_ID));
    }

    @Test
    void twoMembersRelayTheSameFramesForTheSameLog() {
        final Member a = new Member(WINDOW);
        final Member b = new Member(WINDOW);
        for (final Member m : List.of(a, b)) {
            m.record(10, 1, 0, 3);
            m.record(11, 2, 0, 3);
            m.windowTimer();
            m.record(12, 1, 1, 8);
            m.record(13, 3, 0, 8);
            m.record(14, 2, 1, 0);
            m.boundaryTimer();
            m.record(15, 3, 1, 1);
        }
        assertEquals(a.consumer.offered.size(), b.consumer.offered.size());
        for (int i = 0; i < a.consumer.offered.size(); i++) {
            assertTrue(Arrays.equals(a.consumer.offered.get(i), b.consumer.offered.get(i)),
                    "egress frame " + i + " differs between members");
        }
        assertEquals(List.of(10, 11, 12, 13, 14, -1), a.relayed());
        assertEquals(OrderingWindow.HOLD_MS, 5L);
    }
}
