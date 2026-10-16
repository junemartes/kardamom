package io.kardamom.sealer.cluster;

import io.kardamom.sealer.CanonicalSealerState;
import java.util.Arrays;
import org.agrona.DirectBuffer;

/**
 * How the service turns one ingress frame into the relayed payload of one
 * egress kind-1 frame. Each relay keeps the canonical id and the record type
 * with its fields. It drops the bytes in front of the canonical id and the
 * service-only header after it. The egress header
 * ({@link SealerWire#RELAYED_HEADER_LEN} bytes) goes in front. A
 * {@link SealerWire#KIND_BATCH} entry relays as a kind-0 frame. The Rust
 * mirror is {@code RELAY_SHAPES} in {@code crates/cluster-adapter/src/wire/relay.rs}.
 */
enum RelayShape {
    /** {@code [kind:0][guard header][canonical_id:32][record_type][fields…]}. */
    INGRESS_RECORD(SealerWire.KIND_INGRESS_RECORD, SealerWire.CANONICAL_ID_OFFSET, 0),
    /** {@code [kind:4][canonical_id:32][l1_origin:u64][slot_count:u32][record_type][fields…]}. */
    ORIGIN_RECORD(SealerWire.KIND_ORIGIN_RECORD, SealerWire.ORIGIN_ID_OFFSET,
        SealerWire.MIN_ORIGIN_RECORD_LEN - SealerWire.ORIGIN_OFFSET),
    /** {@code [kind:5][canonical_id:32][origin, anchor, slots, seq range][record_type][fields…]}. */
    REMOTE_ORIGIN_RECORD(SealerWire.KIND_REMOTE_ORIGIN_RECORD, SealerWire.REMOTE_ID_OFFSET,
        SealerWire.MIN_REMOTE_ORIGIN_RECORD_LEN - SealerWire.REMOTE_CHAIN_ID_OFFSET);

    /**
     * The longest application payload of one ingress session message: the
     * longest payload of one session message, less the largest growth of a
     * relay. So the relayed frame of an accepted ingress message fits one
     * egress session message.
     */
    static final int MAX_INGRESS_PAYLOAD_LEN = SealerWire.MAX_SESSION_MESSAGE_LEN
        - SealerWire.SESSION_HEADER_LEN
        - Arrays.stream(values()).mapToInt(RelayShape::growth).max().orElse(0);

    /** The ingress kind. */
    final byte kind;
    /** The ingress bytes in front of the canonical id. */
    final int idOffset;
    /** The service-only header between the canonical id and the record type. */
    final int afterId;

    RelayShape(final byte kind, final int idOffset, final int afterId) {
        this.kind = kind;
        this.idOffset = idOffset;
        this.afterId = afterId;
    }

    /** The offset of the record type: the start of the relayed tail. */
    int tailOffset() {
        return idOffset + CanonicalSealerState.CANONICAL_ID_LEN + afterId;
    }

    /**
     * How many bytes the egress frame adds to the ingress frame. A relay
     * that drops more than its egress header adds makes the frame shorter,
     * so its growth is 0.
     */
    int growth() {
        return Math.max(0, SealerWire.RELAYED_HEADER_LEN - idOffset - afterId);
    }

    /**
     * The relayed payload of the frame at {@code offset}: the canonical id,
     * then everything from the record type on. The caller checked that the
     * frame holds {@link #tailOffset()} bytes.
     */
    byte[] payload(final DirectBuffer buffer, final int offset, final int length) {
        final int tailLength = length - tailOffset();
        final byte[] payload = new byte[CanonicalSealerState.CANONICAL_ID_LEN + tailLength];
        buffer.getBytes(offset + idOffset, payload, 0, CanonicalSealerState.CANONICAL_ID_LEN);
        buffer.getBytes(offset + tailOffset(), payload, CanonicalSealerState.CANONICAL_ID_LEN, tailLength);
        return payload;
    }
}
