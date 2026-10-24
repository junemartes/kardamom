package io.kardamom.sealer.cluster;

import io.kardamom.sealer.CanonicalSealerState;
import java.nio.ByteOrder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * A view of the relayed payload of a full reference
 * ({@link SealerWire#RT_TXREF_OFFER}):
 * {@code [canonical_id:32][record_type:7][lane:u8][term_id:i32][term_offset:i32][session_id:i32][envelope]}.
 * The payload starts at the canonical id of a kind-0 frame. The deadline
 * field of the frame carries the position of the reference in its image.
 *
 * <p>One view serves every record: {@link #wrap} points it at the bytes,
 * and each read goes to a fixed offset. It copies nothing and allocates
 * nothing. The service is single-threaded, so one instance is enough.</p>
 */
final class TxRefOffer {
    /** The offset of the record type in the payload. */
    static final int RECORD_TYPE_OFFSET = CanonicalSealerState.CANONICAL_ID_LEN;
    /** The offset of the lane in the payload. */
    static final int LANE_OFFSET = RECORD_TYPE_OFFSET + Byte.BYTES;
    /** The offset of the session id in the payload. */
    static final int SESSION_OFFSET = LANE_OFFSET + Byte.BYTES + Integer.BYTES + Integer.BYTES;
    /** The offset of the envelope in the payload: the length of the payload of a reference. */
    static final int ENVELOPE_OFFSET = SESSION_OFFSET + Integer.BYTES;
    /** The shortest payload of a full reference: the reference, then the root of the envelope. */
    static final int MIN_PAYLOAD_LEN = ENVELOPE_OFFSET + SealerWire.ENVELOPE_ROOT_LEN;
    /** The bytes that the canonical entry keeps of the payload: the id, the record type and the lane. */
    private static final int CANONICAL_HEAD_LEN = LANE_OFFSET + Byte.BYTES;

    private DirectBuffer buffer;
    private int offset;
    private int length;

    /** Point this view at {@code length} bytes of payload at {@code offset} of {@code buffer}. */
    TxRefOffer wrap(final DirectBuffer buffer, final int offset, final int length) {
        this.buffer = buffer;
        this.offset = offset;
        this.length = length;
        return this;
    }

    /** Whether the payload holds the reference and an envelope of at least its root. */
    boolean isWhole() {
        return length >= MIN_PAYLOAD_LEN;
    }

    /** The record type of the payload. */
    byte recordType() {
        return buffer.getByte(offset + RECORD_TYPE_OFFSET);
    }

    /** The lane of the ingress image. */
    int lane() {
        return buffer.getByte(offset + LANE_OFFSET) & 0xFF;
    }

    /** The Aeron session id of the ingress image. */
    int session() {
        return buffer.getInt(offset + SESSION_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /** The length of the archived envelope. */
    int envelopeLength() {
        return length - ENVELOPE_OFFSET;
    }

    /** The length of the canonical entry: the id, the record type, the lane and the envelope. */
    int canonicalLength() {
        return CANONICAL_HEAD_LEN + envelopeLength();
    }

    /**
     * Write the canonical entry at {@code at} of {@code dst}:
     * {@code [canonical_id:32][record_type:6][lane:u8][envelope]}. It copies
     * the id and the envelope from the payload, and drops the position.
     *
     * @return the offset after the entry
     */
    int writeCanonical(final MutableDirectBuffer dst, final int at) {
        dst.putBytes(at, buffer, offset, CanonicalSealerState.CANONICAL_ID_LEN);
        dst.putByte(at + RECORD_TYPE_OFFSET, SealerWire.RT_TX);
        dst.putByte(at + LANE_OFFSET, buffer.getByte(offset + LANE_OFFSET));
        dst.putBytes(at + CANONICAL_HEAD_LEN, buffer, offset + ENVELOPE_OFFSET, envelopeLength());
        return at + canonicalLength();
    }
}
