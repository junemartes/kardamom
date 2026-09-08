package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.cluster.ClusterStubs.StubCluster;
import io.kardamom.sealer.cluster.ClusterStubs.StubSession;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Set;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Decode of the {@code KIND_REMOTE_ORIGIN_RECORD} (kind 5) ingress frame — the
 * cross-chain half of the injection path
 * ({@code docs/specs/interop-outbox-messaging-spec.md} §7) — and the
 * {@code EGRESS_KIND_REMOTE_ORIGIN_REJECT} (kind 6) answer.
 *
 * <p>The frame layout is a FIXED contract with the Rust encoder
 * ({@code crates/cluster-adapter/src/wire}), which has no way to catch a drift
 * on this side, so the offsets are pinned to literals here rather than to the
 * {@link SealerWire} constants — a test written in terms of the constants would
 * move with them and prove nothing:</p>
 *
 * <pre>
 * [kind = 5 : u8][canonical_id : 32][origin_chain_id : u64 LE]
 * [anchor_number : u64 LE][slot_count : u32 LE][first_seq : u64 LE]
 * [last_seq : u64 LE][record_type : u8][payload…]
 * </pre>
 */
class RemoteOriginFrameTest {

    /** Two peers with deliberately different magnitudes (see the swap check). */
    private static final long CHAIN_X = 8_453L;   // 0x2105
    private static final long CHAIN_Y = 10L;
    private static final long ANCHOR = 700L;      // 0x02BC

    private StubCluster cluster;
    private SealerClusteredService service;
    private StubSession publisher;
    private StubSession consumer;

    @BeforeEach
    void start() {
        cluster = new StubCluster();
        service = new SealerClusteredService(64, 250, 0, Set.of(CHAIN_X, CHAIN_Y));
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

    /** A one-message batch frame: two slots, seq range {@code [seq, seq]}. */
    private static byte[] batch(final byte[] id, final long chain, final long anchor, final long seq) {
        return IngressFrames.remoteOriginRecordFrame(id, chain, anchor, 2, seq, seq, new byte[] {0x03});
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

    /** The remote-origin reject frames the PUBLISHER session received. */
    private List<byte[]> rejects() {
        return publisher.offered.stream()
                .filter(f -> f[0] == SealerWire.EGRESS_KIND_REMOTE_ORIGIN_REJECT)
                .toList();
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
                IngressFrames.remoteOriginRecordFrame(id(0x7C), CHAIN_X, ANCHOR, 4, 9, 11, fields);

        assertEquals(69 + fields.length, frame.length, "header is 69 bytes, then the payload");
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
        assertArrayEquals(
                new byte[] {0x09, 0, 0, 0, 0, 0, 0, 0},
                java.util.Arrays.copyOfRange(frame, 53, 61),
                "first_seq 9 as u64 LE at 53..61");
        assertArrayEquals(
                new byte[] {0x0B, 0, 0, 0, 0, 0, 0, 0},
                java.util.Arrays.copyOfRange(frame, 61, 69),
                "last_seq 11 as u64 LE at 61..69");
        assertArrayEquals(fields, java.util.Arrays.copyOfRange(frame, 69, frame.length),
                "opaque payload follows the header untouched");

        // The constants the service decodes with must agree with those literals.
        assertEquals(5, SealerWire.KIND_REMOTE_ORIGIN_RECORD);
        assertEquals(1, SealerWire.REMOTE_ID_OFFSET);
        assertEquals(33, SealerWire.REMOTE_CHAIN_ID_OFFSET);
        assertEquals(41, SealerWire.REMOTE_ANCHOR_OFFSET);
        assertEquals(49, SealerWire.REMOTE_SLOT_COUNT_OFFSET);
        assertEquals(53, SealerWire.REMOTE_FIRST_SEQ_OFFSET);
        assertEquals(61, SealerWire.REMOTE_LAST_SEQ_OFFSET);
        assertEquals(69, SealerWire.MIN_REMOTE_ORIGIN_RECORD_LEN);
    }

    /**
     * The reject frame the Rust decoder reads, byte for byte:
     * {@code [kind = 6][origin u64 LE][first_seq u64 LE][expected u64 LE][reason u8]}.
     */
    @Test
    void reject_frame_layout_is_byte_for_byte_the_fixed_contract() {
        deliver(publisher, batch(id(1), CHAIN_X, ANCHOR, 0));
        deliver(publisher, batch(id(2), CHAIN_X, ANCHOR + 1, 5)); // skip: cursor is 1

        final List<byte[]> rejects = rejects();
        assertEquals(1, rejects.size(), "one reject to the offering session");
        final byte[] r = rejects.get(0);
        assertEquals(26, r.length, "kind + three u64 + reason");
        assertEquals(6, r[0], "kind byte: remote-origin reject");
        assertEquals(6, SealerWire.EGRESS_KIND_REMOTE_ORIGIN_REJECT);
        assertArrayEquals(
                new byte[] {0x05, 0x21, 0, 0, 0, 0, 0, 0},
                java.util.Arrays.copyOfRange(r, 1, 9),
                "origin_chain_id at 1..9");
        assertArrayEquals(
                new byte[] {0x05, 0, 0, 0, 0, 0, 0, 0},
                java.util.Arrays.copyOfRange(r, 9, 17),
                "first_seq 5 at 9..17");
        assertArrayEquals(
                new byte[] {0x01, 0, 0, 0, 0, 0, 0, 0},
                java.util.Arrays.copyOfRange(r, 17, 25),
                "expected_next_seq 1 at 17..25");
        assertEquals(CanonicalSealerState.REMOTE_REJECT_SEQ_MISMATCH, r[25], "reason at 25");
        assertEquals(1, r[25]);

        assertTrue(consumer.offered.stream().noneMatch(
                f -> f[0] == SealerWire.EGRESS_KIND_REMOTE_ORIGIN_REJECT),
                "the consumer never sees a reject");
        assertEquals(List.of(0L), relayedIndices(), "the skipping record was never relayed");
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

        deliver(publisher, IngressFrames.remoteOriginRecordFrame(id, CHAIN_X, ANCHOR, 2, 0, 0, fields));

        final byte[] payload = relayedPayload(0);
        assertEquals(id.length + fields.length, payload.length, "id + fields exactly");
        assertArrayEquals(id, java.util.Arrays.copyOfRange(payload, 0, id.length));
        assertArrayEquals(fields, java.util.Arrays.copyOfRange(payload, id.length, payload.length));
    }

    /** The declared slot count is consumed, so the next record starts past the messages. */
    @Test
    void consumes_the_declared_slot_range() {
        // Marker + 3 messages (seqs 0..2) = 4 slots. A big-endian misread of
        // the u32 would claim 67 million slots and fail the slot-count rule.
        deliver(publisher, IngressFrames.remoteOriginRecordFrame(
                id(1), CHAIN_X, ANCHOR, 4, 0, 2, new byte[] {0x03}));
        deliver(publisher, IngressFrames.recordFrame(
                9, new byte[CanonicalSealerState.SENDER_LEN], 0L));

        assertEquals(List.of(0L, 4L), relayedIndices(), "batch claims slots 0..3");
    }

    /** Audit H3: a slot count the body cannot fill is rejected, never relayed. */
    @Test
    void a_slot_count_that_disagrees_with_the_seq_range_is_rejected() {
        deliver(publisher, IngressFrames.remoteOriginRecordFrame(
                id(1), CHAIN_X, ANCHOR, 7, 0, 2, new byte[] {0x03}));

        assertEquals(List.of(), relayedIndices(), "nothing relayed");
        assertEquals(1, rejects().size());
        assertEquals(CanonicalSealerState.REMOTE_REJECT_SLOT_COUNT_MISMATCH, rejects().get(0)[25]);
    }

    /** Audit H3: an origin outside the allowlist is rejected. */
    @Test
    void an_unknown_origin_is_rejected() {
        deliver(publisher, batch(id(1), 424_242L, ANCHOR, 0));

        assertEquals(List.of(), relayedIndices());
        assertEquals(1, rejects().size());
        assertEquals(CanonicalSealerState.REMOTE_REJECT_UNKNOWN_ORIGIN, rejects().get(0)[25]);
    }

    /** With no allowlist configured, every kind-5 frame is rejected. */
    @Test
    void an_empty_allowlist_rejects_every_remote_origin_frame() {
        final StubCluster off = new StubCluster();
        final SealerClusteredService disabled = new SealerClusteredService(64, 250, 0);
        disabled.onStart(off, null);
        final StubSession p = off.addSession(1);
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        final byte[] frame = batch(id(1), CHAIN_X, ANCHOR, 0);
        buf.putBytes(0, frame);
        disabled.onSessionMessage(p, 0, buf, 0, frame.length, null);

        assertEquals(1, p.offered.size());
        assertEquals(SealerWire.EGRESS_KIND_REMOTE_ORIGIN_REJECT, p.offered.get(0)[0]);
        assertEquals(CanonicalSealerState.REMOTE_REJECT_UNKNOWN_ORIGIN, p.offered.get(0)[25]);
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
        deliver(publisher, batch(id(2), CHAIN_X, ANCHOR, 0));

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
        deliver(publisher, batch(id(1), CHAIN_X, ANCHOR, 0));

        assertEquals(List.of(SealerWire.EGRESS_KIND_RELAYED), kinds());
    }

    /** A duplicate frame (racing watchers re-offering the same batch) relays once, and is never rejected. */
    @Test
    void duplicate_frame_is_deduped_not_rejected() {
        final byte[] frame = batch(id(1), CHAIN_X, ANCHOR, 0);
        deliver(publisher, frame);
        deliver(publisher, frame);

        assertEquals(List.of(0L), relayedIndices(), "the re-offer is absorbed by dedup");
        assertEquals(0, rejects().size(), "a re-offer is not a lane regression");
    }

    /**
     * The two u64s must be read at their own offsets and not confused: a frame
     * that re-anchors chain X BELOW its adopted position is rejected, whereas a
     * decoder that swapped the fields would see an unknown peer 699 and relay.
     * Chain Y meanwhile advances on its own numbering, far below chain X's.
     */
    @Test
    void chain_id_and_anchor_are_read_at_their_own_offsets() {
        deliver(publisher, batch(id(1), CHAIN_X, ANCHOR, 0));
        deliver(publisher, batch(id(2), CHAIN_X, ANCHOR - 1, 1));
        deliver(publisher, batch(id(3), CHAIN_Y, 5L, 0));

        assertEquals(List.of(0L, 2L), relayedIndices(),
                "chain X's regression is rejected; chain Y's low anchor is not a regression");
        assertEquals(1, rejects().size());
        assertEquals(CanonicalSealerState.REMOTE_REJECT_ANCHOR_REGRESSED, rejects().get(0)[25]);
    }

    /**
     * The lane contiguity guard (audit H2/H9): a skipped seq is rejected and
     * the lane stays intact — the correct next record still seals.
     */
    @Test
    void a_skipped_seq_is_rejected_and_the_lane_stays_intact() {
        deliver(publisher, batch(id(1), CHAIN_X, ANCHOR, 0));
        deliver(publisher, batch(id(2), CHAIN_X, ANCHOR + 1, 2)); // skips seq 1
        deliver(publisher, batch(id(3), CHAIN_X, ANCHOR + 1, 1)); // the real next record

        assertEquals(List.of(0L, 2L), relayedIndices(), "seq 0 and seq 1 sealed; the skip did not");
        assertEquals(1, rejects().size());
        final byte[] r = rejects().get(0);
        assertEquals(2L, new UnsafeBuffer(r).getLong(9, ByteOrder.LITTLE_ENDIAN), "first_seq");
        assertEquals(1L, new UnsafeBuffer(r).getLong(17, ByteOrder.LITTLE_ENDIAN), "expected");
    }

    /**
     * A frame too short to carry the header is dropped, never half-decoded.
     * 53 bytes is exactly what a producer that predates the seq range sends.
     */
    @Test
    void a_truncated_frame_is_dropped() {
        final byte[] full = batch(id(1), CHAIN_X, ANCHOR, 0);
        deliver(publisher, java.util.Arrays.copyOf(full, 53));
        deliver(publisher, java.util.Arrays.copyOf(full, 45));

        assertEquals(List.of(), kinds(), "nothing relayed from a truncated frame");
        assertEquals(0, rejects().size(), "a malformed frame gets no reject, it is dropped");
    }
}
