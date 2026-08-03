package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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

    /** Non-zero test sender for the contiguity guard (0 = guard-exempt). */
    private static byte[] sender(final int tag) {
        final byte[] s = new byte[CanonicalSealerState.SENDER_LEN];
        java.util.Arrays.fill(s, (byte) 0xAA);
        s[0] = (byte) tag;
        return s;
    }

    /**
     * One record from the shared test sender with nonce == {@code n}: tests
     * that emit records 0, 1, … stay guard-contiguous.
     */
    private void record(final StubSession from, final int n) {
        final byte[] frame = recordFrame(n, sender(1), n);
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        buf.putBytes(0, frame);
        service.onSessionMessage(from, 0, buf, 0, frame.length, null);
    }

    private void rawRecord(final StubSession from, final int idTag, final byte[] sender20, final long nonce) {
        final byte[] frame = recordFrame(idTag, sender20, nonce);
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        buf.putBytes(0, frame);
        service.onSessionMessage(from, 0, buf, 0, frame.length, null);
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
    void batchEntriesProcessLikeIndividualRecords() {
        subscribe(consumerA);
        // Batch of 3: two fresh records + a duplicate of the first — the
        // consumer must receive exactly 2 relays (intra-batch dedup holds).
        final byte[][] entries = new byte[3][];
        entries[0] = recordFrame(10);
        entries[1] = recordFrame(11);
        entries[2] = recordFrame(10); // duplicate
        int total = 3;
        for (final byte[] e : entries) total += 4 + e.length;
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        int pos = 0;
        buf.putByte(pos, SealerClusteredService.KIND_BATCH);
        pos += Byte.BYTES;
        buf.putShort(pos, (short) entries.length, ByteOrder.LITTLE_ENDIAN);
        pos += Short.BYTES;
        for (final byte[] e : entries) {
            buf.putInt(pos, e.length, ByteOrder.LITTLE_ENDIAN);
            pos += Integer.BYTES;
            buf.putBytes(pos, e);
            pos += e.length;
        }
        service.onSessionMessage(publisher, 0, buf, 0, pos, null);
        assertEquals(2, relayedCount(consumerA),
                "batch: two fresh entries relay, the duplicate is deduped");
        assertEquals(0, relayedCount(publisher),
                "publisher-only session stays out of the fan-out");
    }

    /** A complete single-record ingress frame (batch-embeddable). */
    private static byte[] recordFrame(final int idTag, final byte[] sender20, final long nonce) {
        final byte[] out = new byte[SealerClusteredService.CANONICAL_ID_OFFSET
                + CanonicalSealerState.CANONICAL_ID_LEN + 1];
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer(out.length);
        buf.putByte(SealerClusteredService.KIND_OFFSET, SealerClusteredService.KIND_INGRESS_RECORD);
        buf.putBytes(SealerClusteredService.SENDER_OFFSET, sender20);
        buf.putLong(SealerClusteredService.NONCE_OFFSET, nonce, ByteOrder.LITTLE_ENDIAN);
        final byte[] id = new byte[CanonicalSealerState.CANONICAL_ID_LEN];
        id[0] = (byte) idTag;
        id[31] = 0x5A;
        buf.putBytes(SealerClusteredService.CANONICAL_ID_OFFSET, id);
        buf.putByte(out.length - 1, (byte) idTag);
        buf.getBytes(0, out);
        return out;
    }

    /** Batch-test entries keep the shared sender, nonce == idTag - 10. */
    private static byte[] recordFrame(final int n) {
        return recordFrame(n, sender(1), n - 10);
    }

    @Test
    void contiguityGapRejectsToOfferingSessionOnly() {
        subscribe(consumerA);
        final byte[] s = sender(7);
        rawRecord(publisher, 20, s, 0); // seeds, accepted
        assertEquals(1, relayedCount(consumerA));

        // Nonce 2 while 1 is expected: the refs for nonce 1 vanished — the
        // service must NOT seal the gap. Reject goes to the OFFERING session.
        rawRecord(publisher, 21, s, 2);
        assertEquals(1, relayedCount(consumerA), "gap record must not relay");
        final List<byte[]> rejects = publisher.offered.stream()
                .filter(f -> f[0] == SealerClusteredService.EGRESS_KIND_CONTIGUITY_REJECT)
                .toList();
        assertEquals(1, rejects.size(), "offering session receives the reject");
        assertEquals(0, consumerA.offered.stream()
                .filter(f -> f[0] == SealerClusteredService.EGRESS_KIND_CONTIGUITY_REJECT)
                .count(), "consumers never see rejects");
        // Frame layout: [kind:1][sender:20][nonce:u64 LE][expected:u64 LE].
        final ExpandableArrayBuffer reject = new ExpandableArrayBuffer(rejects.get(0).length);
        reject.putBytes(0, rejects.get(0));
        final byte[] rejectedSender = new byte[CanonicalSealerState.SENDER_LEN];
        reject.getBytes(1, rejectedSender);
        org.junit.jupiter.api.Assertions.assertArrayEquals(s, rejectedSender);
        assertEquals(2, reject.getLong(1 + CanonicalSealerState.SENDER_LEN, ByteOrder.LITTLE_ENDIAN));
        assertEquals(1, reject.getLong(1 + CanonicalSealerState.SENDER_LEN + Long.BYTES,
                ByteOrder.LITTLE_ENDIAN));

        // Recovery: the gap ref (nonce 1) arrives, then the SAME previously
        // rejected id republishes at nonce 2 — it must be accepted as fresh
        // (a reject leaves no dedup entry behind).
        rawRecord(publisher, 22, s, 1);
        rawRecord(publisher, 21, s, 2);
        assertEquals(3, relayedCount(consumerA), "gap fill + republished reject both relay");
    }

    @Test
    void zeroSenderIsGuardExempt() {
        subscribe(consumerA);
        final byte[] zero = new byte[CanonicalSealerState.SENDER_LEN];
        // Deposits carry the zero sender and a filler nonce — any order, any
        // repetition of nonce values must pass (only the id dedups).
        rawRecord(publisher, 30, zero, 0);
        rawRecord(publisher, 31, zero, 0);
        rawRecord(publisher, 32, zero, 5);
        assertEquals(3, relayedCount(consumerA), "zero sender never contiguity-rejects");
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

    /**
     * The relayed payload of an origin record must be EXACTLY
     * {@code [canonical_id:32][record_type][fields…]} — the same shape a plain
     * record relays in — with no trailing slack. Consumers deserialise the
     * fields with rkyv, which locates its root at the END of the buffer, so
     * even a few extra bytes make every epoch undecodable. Nothing on the Rust
     * side can catch this: it is purely a property of this relay.
     */
    @Test
    void origin_record_relays_id_and_fields_with_no_trailing_slack() {
        subscribe(consumerA);
        final byte[] fields = {0x11, 0x22, 0x33, 0x44, 0x55};
        final byte[] id = new byte[CanonicalSealerState.CANONICAL_ID_LEN];
        id[0] = 0x7E;
        id[31] = 0x5A;

        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        int pos = 0;
        buf.putByte(pos, SealerClusteredService.KIND_ORIGIN_RECORD);
        pos += Byte.BYTES;
        buf.putBytes(pos, id);
        pos += id.length;
        buf.putLong(pos, 4_242L, java.nio.ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putInt(pos, 3, java.nio.ByteOrder.LITTLE_ENDIAN); // slot count
        pos += Integer.BYTES;
        buf.putBytes(pos, fields);
        pos += fields.length;
        service.onSessionMessage(publisher, 0, buf, 0, pos, null);

        final byte[] frame = consumerA.offered.stream()
                .filter(f -> f[0] == SealerClusteredService.EGRESS_KIND_RELAYED)
                .findFirst()
                .orElseThrow();
        // [kind:1][index:8][payloadLen:4][payload…]
        final int payloadLen = new org.agrona.concurrent.UnsafeBuffer(frame)
                .getInt(1 + Long.BYTES, java.nio.ByteOrder.LITTLE_ENDIAN);
        assertEquals(
                CanonicalSealerState.CANONICAL_ID_LEN + fields.length,
                payloadLen,
                "relayed payload must be id + fields exactly");

        final byte[] payload = new byte[payloadLen];
        System.arraycopy(frame, 1 + Long.BYTES + Integer.BYTES, payload, 0, payloadLen);
        final byte[] gotId = java.util.Arrays.copyOfRange(payload, 0, id.length);
        final byte[] gotFields = java.util.Arrays.copyOfRange(payload, id.length, payload.length);
        assertArrayEquals(id, gotId);
        assertArrayEquals(fields, gotFields);
    }

    /** The declared slot count must be consumed, so the next record starts past it. */
    @Test
    void origin_record_consumes_its_declared_slot_range() {
        subscribe(consumerA);
        originRecord(publisher, 1, 100L, 4);
        record(publisher, 9);

        final java.util.List<Long> indices = consumerA.offered.stream()
                .filter(f -> f[0] == SealerClusteredService.EGRESS_KIND_RELAYED)
                .map(f -> new org.agrona.concurrent.UnsafeBuffer(f)
                        .getLong(1, java.nio.ByteOrder.LITTLE_ENDIAN))
                .toList();
        assertEquals(java.util.List.of(0L, 4L), indices, "epoch claims slots 0..3");
    }

    private void originRecord(final StubSession from, final int n, final long origin, final int slots) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        int pos = 0;
        buf.putByte(pos, SealerClusteredService.KIND_ORIGIN_RECORD);
        pos += Byte.BYTES;
        final byte[] id = new byte[CanonicalSealerState.CANONICAL_ID_LEN];
        id[0] = (byte) n;
        id[31] = 0x5A;
        buf.putBytes(pos, id);
        pos += id.length;
        buf.putLong(pos, origin, java.nio.ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putInt(pos, slots, java.nio.ByteOrder.LITTLE_ENDIAN);
        pos += Integer.BYTES;
        buf.putByte(pos, (byte) n);
        pos += Byte.BYTES;
        service.onSessionMessage(from, 0, buf, 0, pos, null);
    }
}
