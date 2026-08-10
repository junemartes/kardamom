package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.cluster.ClusterStubs.StubCluster;
import io.kardamom.sealer.cluster.ClusterStubs.StubSession;
import java.nio.ByteOrder;
import java.util.List;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Decode of the {@code KIND_REMOTE_ORIGIN_RECORD} (kind 5) ingress frame — the
 * cross-chain half of the injection path
 * ({@code docs/specs/interop-outbox-messaging-spec.md} §7).
 *
 * <p>The frame layout is a FIXED contract with the Rust encoder
 * ({@code crates/cluster-adapter/src/wire}), which has no way to catch a drift
 * on this side, so the offsets are pinned to literals here rather than to the
 * {@link SealerWire} constants — a test written in terms of the constants would
 * move with them and prove nothing:</p>
 *
 * <pre>
 * [kind = 5 : u8][canonical_id : 32][origin_chain_id : u64 LE]
 * [anchor_number : u64 LE][slot_count : u32 LE][record_type : u8][payload…]
 * </pre>
 */
class RemoteOriginFrameTest {

    /** Two peers with deliberately different magnitudes (see the swap check). */
    private static final long CHAIN_X = 8_453L;   // 0x2105
    private static final long ANCHOR = 700L;      // 0x02BC

    private StubCluster cluster;
    private SealerClusteredService service;
    private StubSession publisher;
    private StubSession consumer;

    @BeforeEach
    void start() {
        cluster = new StubCluster();
        service = new SealerClusteredService(64, 250, 0);
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

    private static byte[] id(final int tag) {
        final byte[] out = new byte[CanonicalSealerState.CANONICAL_ID_LEN];
        out[0] = (byte) tag;
        out[31] = 0x5A;
        return out;
    }

    /** Egress frame kinds the consumer saw, in order. */
    private List<Byte> kinds() {
        return consumer.offered.stream().map(f -> f[0]).toList();
    }

    /** Canonical indices of the relayed records the consumer saw, in order. */
    private List<Long> relayedIndices() {
        return consumer.offered.stream()
                .filter(f -> f[0] == SealerWire.EGRESS_KIND_RELAYED)
                .map(f -> new UnsafeBuffer(f).getLong(1, ByteOrder.LITTLE_ENDIAN))
                .toList();
    }

    /** Payload of the {@code n}-th relayed egress frame: {@code [kind:1][index:8][len:4][payload]}. */
    private byte[] relayedPayload(final int n) {
        final byte[] frame = consumer.offered.stream()
                .filter(f -> f[0] == SealerWire.EGRESS_KIND_RELAYED)
                .toList()
                .get(n);
        final int len = new UnsafeBuffer(frame).getInt(1 + Long.BYTES, ByteOrder.LITTLE_ENDIAN);
        final byte[] payload = new byte[len];
        System.arraycopy(frame, 1 + Long.BYTES + Integer.BYTES, payload, 0, len);
        return payload;
    }

    /**
     * The frame the Rust encoder writes, byte for byte. Every field is spelled
     * out at its literal offset with its literal little-endian bytes: if either
     * side moves a field or flips an endianness, this fails before any
     * behavioural test gets a chance to be subtly wrong.
     */
    @Test
    void frame_layout_is_byte_for_byte_the_fixed_contract() {
        final byte[] fields = {0x03, 0x11, 0x22}; // RT_REMOTE_EPOCH + opaque bytes
        final byte[] frame =
                IngressFrames.remoteOriginRecordFrame(id(0x7C), CHAIN_X, ANCHOR, 4, fields);

        assertEquals(53 + fields.length, frame.length, "header is 53 bytes, then the payload");
        assertEquals(5, frame[0], "kind byte: remote origin, distinct from the epoch's 4");
        assertArrayEquals(id(0x7C), java.util.Arrays.copyOfRange(frame, 1, 33), "canonical id at 1..33");
        assertArrayEquals(
                new byte[] {0x05, 0x21, 0, 0, 0, 0, 0, 0},
                java.util.Arrays.copyOfRange(frame, 33, 41),
                "origin_chain_id 8453 as u64 LE at 33..41");
        assertArrayEquals(
                new byte[] {(byte) 0xBC, 0x02, 0, 0, 0, 0, 0, 0},
                java.util.Arrays.copyOfRange(frame, 41, 49),
                "anchor_number 700 as u64 LE at 41..49");
        assertArrayEquals(
                new byte[] {0x04, 0, 0, 0},
                java.util.Arrays.copyOfRange(frame, 49, 53),
                "slot_count 4 as u32 LE at 49..53");
        assertArrayEquals(fields, java.util.Arrays.copyOfRange(frame, 53, frame.length),
                "opaque payload follows the header untouched");

        // The constants the service decodes with must agree with those literals.
        assertEquals(5, SealerWire.KIND_REMOTE_ORIGIN_RECORD);
        assertEquals(1, SealerWire.REMOTE_ID_OFFSET);
        assertEquals(33, SealerWire.REMOTE_CHAIN_ID_OFFSET);
        assertEquals(41, SealerWire.REMOTE_ANCHOR_OFFSET);
        assertEquals(49, SealerWire.REMOTE_SLOT_COUNT_OFFSET);
        assertEquals(53, SealerWire.MIN_REMOTE_ORIGIN_RECORD_LEN);
    }

    /**
     * The relayed payload must be EXACTLY {@code [canonical_id:32][record_type]
     * [fields…]} — the same shape every other record relays in — with no
     * trailing slack: consumers deserialise the fields with rkyv, which locates
     * its root at the END of the buffer, so a few extra bytes make every batch
     * undecodable.
     */
    @Test
    void relays_id_and_fields_with_no_trailing_slack() {
        final byte[] id = id(0x7C);
        final byte[] fields = {0x03, 0x11, 0x22, 0x33, 0x44};

        deliver(publisher, IngressFrames.remoteOriginRecordFrame(id, CHAIN_X, ANCHOR, 1, fields));

        final byte[] payload = relayedPayload(0);
        assertEquals(id.length + fields.length, payload.length, "id + fields exactly");
        assertArrayEquals(id, java.util.Arrays.copyOfRange(payload, 0, id.length));
        assertArrayEquals(fields, java.util.Arrays.copyOfRange(payload, id.length, payload.length));
    }

    /** The declared slot count is consumed, so the next record starts past the messages. */
    @Test
    void consumes_the_declared_slot_range() {
        // Marker + 3 messages = 4 slots. A big-endian misread of the u32 would
        // claim 67 million slots and this assertion would be wildly off.
        deliver(publisher, IngressFrames.remoteOriginRecordFrame(
                id(1), CHAIN_X, ANCHOR, 4, new byte[] {0x03}));
        deliver(publisher, IngressFrames.recordFrame(
                9, new byte[CanonicalSealerState.SENDER_LEN], 0L));

        assertEquals(List.of(0L, 4L), relayedIndices(), "batch claims slots 0..3");
    }

    /**
     * The caller contract of {@code RemoteOriginAdvance}: the forced boundary is
     * offered BEFORE the relayed record, or the batch's messages land in the
     * previous block's tail instead of leading the new one.
     */
    @Test
    void forced_boundary_is_offered_before_the_relayed_batch() {
        deliver(publisher, IngressFrames.recordFrame(
                1, new byte[CanonicalSealerState.SENDER_LEN], 0L));
        deliver(publisher, IngressFrames.remoteOriginRecordFrame(
                id(2), CHAIN_X, ANCHOR, 2, new byte[] {0x03}));

        assertEquals(
                List.of(SealerWire.EGRESS_KIND_RELAYED,
                        SealerWire.EGRESS_KIND_BOUNDARY,
                        SealerWire.EGRESS_KIND_RELAYED),
                kinds(),
                "the tx, then the boundary closing its block, then the batch leading the next");
    }

    /** An empty open block needs no boundary — a burst must not emit empty blocks. */
    @Test
    void no_boundary_is_forced_when_the_open_block_is_empty() {
        deliver(publisher, IngressFrames.remoteOriginRecordFrame(
                id(1), CHAIN_X, ANCHOR, 1, new byte[] {0x03}));

        assertEquals(List.of(SealerWire.EGRESS_KIND_RELAYED), kinds());
    }

    /** A duplicate frame (racing watchers re-offering the same batch) relays once. */
    @Test
    void duplicate_frame_is_deduped() {
        final byte[] frame = IngressFrames.remoteOriginRecordFrame(
                id(1), CHAIN_X, ANCHOR, 1, new byte[] {0x03});
        deliver(publisher, frame);
        deliver(publisher, frame);

        assertEquals(List.of(0L), relayedIndices(), "the re-offer is absorbed by dedup");
    }

    /**
     * The two u64s must be read at their own offsets and not confused: a frame
     * that re-anchors chain X BELOW its adopted position is dropped, whereas a
     * decoder that swapped the fields would see an unknown peer 699 and relay.
     * Chain Y meanwhile advances on its own numbering, far below chain X's.
     */
    @Test
    void chain_id_and_anchor_are_read_at_their_own_offsets() {
        deliver(publisher, IngressFrames.remoteOriginRecordFrame(
                id(1), CHAIN_X, ANCHOR, 1, new byte[] {0x03}));
        deliver(publisher, IngressFrames.remoteOriginRecordFrame(
                id(2), CHAIN_X, ANCHOR - 1, 1, new byte[] {0x03}));
        deliver(publisher, IngressFrames.remoteOriginRecordFrame(
                id(3), 10L, 5L, 1, new byte[] {0x03}));

        assertEquals(List.of(0L, 1L), relayedIndices(),
                "chain X's regression is dropped; chain Y's low anchor is not a regression");
    }

    /** A frame too short to carry the header is dropped, never half-decoded. */
    @Test
    void a_truncated_frame_is_dropped() {
        final byte[] full = IngressFrames.remoteOriginRecordFrame(
                id(1), CHAIN_X, ANCHOR, 1, new byte[] {0x03});
        // 45 bytes: the length of a complete EPOCH frame's header, i.e. exactly
        // what a producer that forgot the second u64 would send.
        deliver(publisher, java.util.Arrays.copyOf(full, 45));

        assertEquals(List.of(), kinds(), "nothing relayed from a truncated frame");
    }
}
