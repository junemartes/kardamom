package io.kardamom.sealer.cluster;

import io.kardamom.sealer.CanonicalSealerState;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.stream.Stream;

/**
 * A full reference of a test (record type 7): the transaction {@code idTag}
 * at {@code position} of the ingress image {@code (LANE, session)}, with an
 * opaque envelope that the sealer never parses.
 */
record FullRef(int idTag, int session, long position, byte[] envelope) {

    /** The lane of every full reference of a test. */
    static final byte LANE = 3;

    /** The sender of every full reference of a test. */
    static final byte[] SENDER = filled(CanonicalSealerState.SENDER_LEN, 0xCC);

    /** The full reference {@code idTag} of image {@code (LANE, 9)}, with an envelope of 80 + {@code idTag} bytes. */
    static FullRef of(final int idTag, final long position) {
        return new FullRef(idTag, 9, position, filled(SealerWire.ENVELOPE_ROOT_LEN + idTag, 0x40 + idTag));
    }

    static byte[] filled(final int length, final int value) {
        final byte[] out = new byte[length];
        Arrays.fill(out, (byte) value);
        return out;
    }

    byte[] id() {
        return IngressFrames.recordId(idTag);
    }

    /** The kind-0 frame, with the tagged position in the deadline field. */
    byte[] frame() {
        return frame(SealerWire.POSITION_TAG | position, envelope);
    }

    /** The kind-0 frame with "no deadline" in place of the position. */
    byte[] untagged() {
        return frame(Long.MAX_VALUE, envelope);
    }

    /** The kind-0 frame without its envelope: the 99 bytes of the reference. */
    byte[] withoutEnvelope() {
        return frame(SealerWire.POSITION_TAG | position, new byte[0]);
    }

    private byte[] frame(final long deadline, final byte[] env) {
        return ByteBuffer.allocate(SealerWire.TXREF_RECORD_LEN + env.length).order(ByteOrder.LITTLE_ENDIAN)
            .put(SealerWire.KIND_INGRESS_RECORD).put(SENDER).putLong(0L).putLong(deadline).putLong(0L).putLong(0L)
            .put(id()).put(SealerWire.RT_TXREF_OFFER).put(LANE).putInt(0).putInt((int) position).putInt(session)
            .put(env).array();
    }

    /** The relayed payload of the canonical entry: {@code [id:32][6][lane][envelope]}. */
    byte[] canonicalPayload() {
        return ByteBuffer.allocate(CanonicalSealerState.CANONICAL_ID_LEN + 2 + envelope.length)
            .put(id()).put(SealerWire.RT_TX).put(LANE).put(envelope).array();
    }

    /** The egress frame of the canonical entry at {@code index}. */
    byte[] canonicalFrame(final long index) {
        final byte[] payload = canonicalPayload();
        return ByteBuffer.allocate(SealerWire.RELAYED_HEADER_LEN + payload.length).order(ByteOrder.LITTLE_ENDIAN)
            .put(SealerWire.EGRESS_KIND_RELAYED).putLong(index).putInt(payload.length).put(payload).array();
    }

    /** A kind-3 batch of {@code entries}: {@code [kind:3][count:u16]}, then {@code [len:u32][entry]} each. */
    static byte[] batch(final byte[]... entries) {
        final int length = Stream.of(entries).mapToInt(entry -> Integer.BYTES + entry.length).sum();
        final ByteBuffer buf = ByteBuffer.allocate(Byte.BYTES + Short.BYTES + length).order(ByteOrder.LITTLE_ENDIAN)
            .put(SealerWire.KIND_BATCH).putShort((short) entries.length);
        Stream.of(entries).forEach(entry -> buf.putInt(entry.length).put(entry));
        return buf.array();
    }
}
