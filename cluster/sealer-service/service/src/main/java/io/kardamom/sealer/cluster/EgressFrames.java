package io.kardamom.sealer.cluster;

import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.ClusterStatus;
import io.kardamom.sealer.Relayed;
import java.nio.ByteOrder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * The egress frame codec: each method writes one frame at the start of the
 * staging buffer and returns its length. All integers are little-endian.
 * The Rust reader is {@code EgressItem::decode} in
 * {@code crates/cluster-adapter/src/wire/egress.rs}.
 *
 * <p>This class is single-threaded, like {@link SealerEgress}, which owns
 * the staging buffer.</p>
 */
final class EgressFrames {
    /** The offset of the record type in a relayed frame: after the header and the canonical id. */
    private static final int ENTRY_RECORD_TYPE_OFFSET =
        SealerWire.RELAYED_HEADER_LEN + CanonicalSealerState.CANONICAL_ID_LEN;

    private final MutableDirectBuffer buf;

    EgressFrames(final MutableDirectBuffer buf) {
        this.buf = buf;
    }

    /**
     * A relayed record:
     * {@code [kind:1][index:u64][payload_len:u32][payload]}. The payload is
     * copied through as is.
     */
    int relayed(final Relayed relayed) {
        final int pos = relayedHeader(relayed.index, relayed.payload.length);
        buf.putBytes(pos, relayed.payload);
        return pos + relayed.payload.length;
    }

    /** A relayed record whose payload is {@code length} bytes of {@code src} at {@code offset}. */
    int relayed(final long index, final DirectBuffer src, final int offset, final int length) {
        final int pos = relayedHeader(index, length);
        buf.putBytes(pos, src, offset, length);
        return pos + length;
    }

    /**
     * The relayed canonical entry of a full reference: its payload is
     * {@code [canonical_id:32][record_type:6][lane:u8][envelope]}, copied in
     * two parts from the bytes that {@code offer} views.
     */
    int carried(final long index, final TxRefOffer offer) {
        return offer.writeCanonical(buf, relayedHeader(index, offer.canonicalLength()));
    }

    /** {@code [kind:1][index:u64][payload_len:u32]}; returns the offset of the payload. */
    private int relayedHeader(final long index, final int payloadLength) {
        buf.putByte(0, SealerWire.EGRESS_KIND_RELAYED);
        buf.putLong(Byte.BYTES, index, ByteOrder.LITTLE_ENDIAN);
        buf.putInt(Byte.BYTES + Long.BYTES, payloadLength, ByteOrder.LITTLE_ENDIAN);
        return SealerWire.RELAYED_HEADER_LEN;
    }

    /**
     * A relayed reference:
     * {@code [kind:15][index:u64][canonical_id:32][record_type:u8]}.
     */
    int relayedRef(final long index, final byte[] canonicalId, final byte recordType) {
        int pos = 0;
        buf.putByte(pos, SealerWire.EGRESS_KIND_RELAYED_REF);
        pos += Byte.BYTES;
        buf.putLong(pos, index, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putBytes(pos, canonicalId, 0, CanonicalSealerState.CANONICAL_ID_LEN);
        pos += CanonicalSealerState.CANONICAL_ID_LEN;
        buf.putByte(pos, recordType);
        pos += Byte.BYTES;
        return pos;
    }

    /**
     * A retained frame as an egress session gets it: a relayed canonical
     * entry becomes its relayed reference, and every other frame stays as
     * is. A retained frame is one that this member framed: a relayed frame
     * always holds the record type, because the service takes no ingress
     * record without one ({@link RelayShape#minLength()}).
     */
    static byte[] forSession(final byte[] retained) {
        final boolean entry = retained[0] == SealerWire.EGRESS_KIND_RELAYED
            && retained[ENTRY_RECORD_TYPE_OFFSET] == SealerWire.RT_TX;
        if (!entry) {
            return retained;
        }
        final byte[] ref = new byte[SealerWire.RELAYED_REF_LEN];
        ref[0] = SealerWire.EGRESS_KIND_RELAYED_REF;
        System.arraycopy(retained, Byte.BYTES, ref, Byte.BYTES, Long.BYTES);
        System.arraycopy(retained, SealerWire.RELAYED_HEADER_LEN, ref, Byte.BYTES + Long.BYTES,
            CanonicalSealerState.CANONICAL_ID_LEN);
        ref[SealerWire.RELAYED_REF_LEN - 1] = SealerWire.RT_TX;
        return ref;
    }

    /**
     * {@link #forSession(byte[])} in place, on the first {@code length} bytes
     * of {@code staged}: a canonical entry becomes its relayed reference.
     * Returns the new length.
     */
    static int forSession(final MutableDirectBuffer staged, final int length) {
        final boolean entry = staged.getByte(0) == SealerWire.EGRESS_KIND_RELAYED
            && staged.getByte(ENTRY_RECORD_TYPE_OFFSET) == SealerWire.RT_TX;
        if (!entry) {
            return length;
        }
        final byte[] id = new byte[CanonicalSealerState.CANONICAL_ID_LEN];
        staged.getBytes(SealerWire.RELAYED_HEADER_LEN, id);
        staged.putByte(0, SealerWire.EGRESS_KIND_RELAYED_REF);
        staged.putBytes(Byte.BYTES + Long.BYTES, id);
        staged.putByte(SealerWire.RELAYED_REF_LEN - 1, SealerWire.RT_TX);
        return SealerWire.RELAYED_REF_LEN;
    }

    /** A control message {@code [kind][a:u64][b:u64]}: the replay answers and the origin gap. */
    int control(final byte kind, final long a, final long b) {
        int pos = 0;
        buf.putByte(pos, kind);
        pos += Byte.BYTES;
        buf.putLong(pos, a, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, b, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        return pos;
    }

    /** The sealer's rules: {@code [kind:17][decision_version:u8][cluster_session_id:i64]}. */
    int sealerInfo(final int decisionVersion, final long sessionId) {
        buf.putByte(0, SealerWire.EGRESS_KIND_SEALER_INFO);
        buf.putByte(Byte.BYTES, (byte) decisionVersion);
        buf.putLong(Byte.BYTES + Byte.BYTES, sessionId, ByteOrder.LITTLE_ENDIAN);
        return SealerWire.SEALER_INFO_LEN;
    }

    /**
     * The refusal of a transaction reference:
     * {@code [kind:18][canonical_id:32][sender:20][nonce:u64][decision_version:u8]}.
     */
    int refRefused(final byte[] canonicalId, final byte[] sender20, final long nonce, final int decisionVersion) {
        int pos = 0;
        buf.putByte(pos, SealerWire.EGRESS_KIND_REF_REFUSED);
        pos += Byte.BYTES;
        buf.putBytes(pos, canonicalId, 0, CanonicalSealerState.CANONICAL_ID_LEN);
        pos += CanonicalSealerState.CANONICAL_ID_LEN;
        buf.putBytes(pos, sender20, 0, CanonicalSealerState.SENDER_LEN);
        pos += CanonicalSealerState.SENDER_LEN;
        buf.putLong(pos, nonce, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putByte(pos, (byte) decisionVersion);
        pos += Byte.BYTES;
        return pos;
    }

    /**
     * An answer on the member feed: {@code [kind][a:u64][b:u64][request_id:u64]}.
     * It is the control message of {@link #control} with the id of the
     * request that it answers as a tail.
     */
    int feedAnswer(final byte kind, final long a, final long b, final long requestId) {
        final int pos = control(kind, a, b);
        buf.putLong(pos, requestId, ByteOrder.LITTLE_ENDIAN);
        return pos + Long.BYTES;
    }

    /** A feed gap: {@code [kind:16][next_index:u64][next_block:u64][request_id:u64]}. */
    int feedGap(final long nextIndex, final long nextBlock, final long requestId) {
        return feedAnswer(SealerWire.EGRESS_KIND_FEED_GAP, nextIndex, nextBlock, requestId);
    }

    /**
     * The chain's status with the record-lag tail and the flag byte:
     * {@code kind(1) | posted_head(8) | sealed_head(8) | budget(8) |
     * halted(1) | retained(8) | floor_index(8) | floor_block(8) |
     * best_recorded(8) | record_lag_budget(8) | record_lag_halted(1) |
     * snapshot_follows(1)}. {@code snapshotFollows} is true on the status
     * before a snapshot.
     */
    int status(final ClusterStatus status, final boolean snapshotFollows) {
        final int pos = recordLagStatus(status);
        buf.putByte(pos, snapshotFollows ? (byte) 1 : (byte) 0);
        return SealerWire.STATUS_WITH_SNAPSHOT_FLAG_LEN;
    }

    /**
     * {@link #status(ClusterStatus, boolean)}, then the byte tail
     * {@code [retained_bytes:u64]}.
     */
    int status(final ClusterStatus status, final boolean snapshotFollows, final long retainedBytes) {
        status(status, snapshotFollows);
        buf.putLong(SealerWire.STATUS_RETAINED_BYTES_OFFSET, retainedBytes, ByteOrder.LITTLE_ENDIAN);
        return SealerWire.STATUS_WITH_RETAINED_BYTES_LEN;
    }

    /** The status up to the end of the record-lag tail. Returns the next offset. */
    private int recordLagStatus(final ClusterStatus status) {
        int pos = 0;
        buf.putByte(pos, SealerWire.EGRESS_KIND_STATUS);
        pos += Byte.BYTES;
        buf.putLong(pos, status.postedHead(), ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, status.sealedHead(), ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, status.budgetBlocks(), ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putByte(pos, status.halted() ? (byte) 1 : (byte) 0);
        pos += Byte.BYTES;
        buf.putLong(pos, status.retainedFrames(), ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, status.floorIndex(), ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, status.floorBlock(), ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, status.bestRecorded(), ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, status.recordLagBudget(), ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putByte(pos, status.recordLagHalted() ? (byte) 1 : (byte) 0);
        pos += Byte.BYTES;
        return pos;
    }

    /** A copy of the first {@code length} bytes of the staging buffer. */
    byte[] copy(final int length) {
        final byte[] frame = new byte[length];
        buf.getBytes(0, frame);
        return frame;
    }
}
