package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.aeron.Aeron;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import io.kardamom.sealer.CanonicalSealerState;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Egress fan-out targeting: relayed records and boundaries reach the sessions
 * that announced themselves as canonical-stream consumers ({@code SUBSCRIBE}
 * frame or a replay request) — NOT publisher-only sessions — with a
 * broadcast-to-all fallback while no consumer has announced itself (fresh
 * restart, mixed-version deploy). The per-session unicast offer is the
 * dominant leader cost at saturation, so the consumer set directly bounds the
 * sealer's throughput ceiling.
 */
class SealerFanoutTest {

    private StubCluster cluster;
    private SealerClusteredService service;
    private StubSession publisher;
    private StubSession consumerA;
    private StubSession consumerB;

    @BeforeEach
    void start() {
        cluster = new StubCluster();
        service = new SealerClusteredService(64, 250, 0);
        service.onStart(cluster, null);
        publisher = cluster.addSession(1);
        consumerA = cluster.addSession(2);
        consumerB = cluster.addSession(3);
    }

    private static long relayedCount(final StubSession s) {
        return s.offered.stream()
                .filter(f -> f[0] == SealerClusteredService.EGRESS_KIND_RELAYED)
                .count();
    }

    private void record(final StubSession from, final int n) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        int pos = 0;
        buf.putByte(pos, SealerClusteredService.KIND_INGRESS_RECORD);
        pos += Byte.BYTES;
        final byte[] id = new byte[CanonicalSealerState.CANONICAL_ID_LEN];
        id[0] = (byte) n;
        id[31] = 0x5A;
        buf.putBytes(pos, id);
        pos += id.length;
        buf.putByte(pos, (byte) n);
        pos += Byte.BYTES;
        service.onSessionMessage(from, 0, buf, 0, pos, null);
    }

    private void subscribe(final StubSession s) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        buf.putByte(0, SealerClusteredService.KIND_SUBSCRIBE);
        service.onSessionMessage(s, 0, buf, 0, 1, null);
    }

    private void replayRequest(final StubSession s, final long fromIndex, final long fromBlock) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        buf.putByte(0, SealerClusteredService.KIND_REPLAY_REQUEST);
        buf.putLong(1, fromIndex, ByteOrder.LITTLE_ENDIAN);
        buf.putLong(1 + Long.BYTES, fromBlock, ByteOrder.LITTLE_ENDIAN);
        service.onSessionMessage(s, 0, buf, 0, 17, null);
    }

    @Test
    void broadcastsToAllWhileNoConsumerAnnounced() {
        record(publisher, 0);
        assertEquals(1, relayedCount(publisher), "fallback: publisher receives");
        assertEquals(1, relayedCount(consumerA), "fallback: consumerA receives");
        assertEquals(1, relayedCount(consumerB), "fallback: consumerB receives");
    }

    @Test
    void relaysOnlyToAnnouncedConsumers() {
        subscribe(consumerA);
        replayRequest(consumerB, 0, 1); // a replay request announces a consumer too

        record(publisher, 0);
        assertEquals(0, relayedCount(publisher),
                "publisher-only session must not receive the canonical broadcast");
        assertEquals(1, relayedCount(consumerA), "SUBSCRIBE-announced consumer receives");
        assertEquals(1, relayedCount(consumerB), "replay-announced consumer receives");
    }

    @Test
    void boundariesStayBroadcastToEverySession() {
        // Unlike relayed records, boundaries reach every session even after
        // consumers announce: the sequencer's boundary-only lag feed (#93)
        // consumes them without a SUBSCRIBE announcement.
        subscribe(consumerA);
        service.onTimerEvent(SealerClusteredService.BOUNDARY_TIMER_CORRELATION_ID, 250);
        final long pubBoundaries = publisher.offered.stream()
                .filter(f -> f[0] == SealerClusteredService.EGRESS_KIND_BOUNDARY)
                .count();
        final long consBoundaries = consumerA.offered.stream()
                .filter(f -> f[0] == SealerClusteredService.EGRESS_KIND_BOUNDARY)
                .count();
        assertEquals(1, pubBoundaries, "boundaries broadcast to publisher-only sessions too");
        assertEquals(1, consBoundaries, "consumer receives the boundary tick");
    }

    @Test
    void closedConsumerLeavesTheSetAndEmptySetRestoresBroadcast() {
        subscribe(consumerA);
        record(publisher, 0);
        assertEquals(1, relayedCount(consumerA));
        assertEquals(0, relayedCount(publisher));

        // Close the only consumer: the set empties and the fallback broadcast
        // returns, so a restart's pre-subscribe window can never starve.
        service.onSessionClose(consumerA, 0, null);
        record(publisher, 1);
        assertEquals(1, relayedCount(publisher), "empty set falls back to broadcast");
        assertEquals(1, relayedCount(consumerB), "broadcast reaches remaining sessions");
    }

    // --- minimal stubs (same shape as SnapshotRestoreTest's) ----------------

    private static final class StubSession implements ClientSession {
        final long id;
        final List<byte[]> offered = new ArrayList<>();
        boolean closed;

        StubSession(final long id) {
            this.id = id;
        }

        public long id() {
            return id;
        }

        public int responseStreamId() {
            return 0;
        }

        public String responseChannel() {
            return "aeron:ipc";
        }

        public byte[] encodedPrincipal() {
            return new byte[0];
        }

        public void close() {
            closed = true;
        }

        public boolean isClosing() {
            return closed;
        }

        public long offer(final DirectBuffer buffer, final int offset, final int length) {
            final byte[] copy = new byte[length];
            buffer.getBytes(offset, copy);
            offered.add(copy);
            return length;
        }

        public long offer(final io.aeron.DirectBufferVector[] vectors) {
            throw new UnsupportedOperationException();
        }

        public long tryClaim(final int length, final io.aeron.logbuffer.BufferClaim bufferClaim) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class StubCluster implements Cluster {
        final HashMap<Long, ClientSession> sessions = new HashMap<>();
        final IdleStrategy idleStrategy = new YieldingIdleStrategy();

        StubSession addSession(final long id) {
            final StubSession session = new StubSession(id);
            sessions.put(id, session);
            return session;
        }

        public int memberId() {
            return 0;
        }

        public Role role() {
            return Role.LEADER;
        }

        public long logPosition() {
            return 0;
        }

        public Aeron aeron() {
            throw new UnsupportedOperationException();
        }

        public io.aeron.cluster.service.ClusteredServiceContainer.Context context() {
            throw new UnsupportedOperationException();
        }

        public ClientSession getClientSession(final long clusterSessionId) {
            return sessions.get(clusterSessionId);
        }

        public Collection<ClientSession> clientSessions() {
            return sessions.values();
        }

        public void forEachClientSession(final Consumer<? super ClientSession> action) {
            sessions.values().forEach(action);
        }

        public boolean closeClientSession(final long clusterSessionId) {
            return sessions.remove(clusterSessionId) != null;
        }

        public long time() {
            return 0;
        }

        public TimeUnit timeUnit() {
            return TimeUnit.MILLISECONDS;
        }

        public boolean scheduleTimer(final long correlationId, final long deadline) {
            return true;
        }

        public boolean cancelTimer(final long correlationId) {
            return true;
        }

        public long offer(final DirectBuffer buffer, final int offset, final int length) {
            throw new UnsupportedOperationException();
        }

        public long offer(final io.aeron.DirectBufferVector[] vectors) {
            throw new UnsupportedOperationException();
        }

        public long tryClaim(final int length, final io.aeron.logbuffer.BufferClaim bufferClaim) {
            throw new UnsupportedOperationException();
        }

        public IdleStrategy idleStrategy() {
            return idleStrategy;
        }
    }
}
