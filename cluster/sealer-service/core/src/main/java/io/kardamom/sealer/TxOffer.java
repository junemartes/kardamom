package io.kardamom.sealer;

import java.util.Arrays;
import java.util.Optional;

/**
 * A full entry: one transaction with its archived envelope, as a sequencer
 * offers it on ingress. Its relayed payload is
 * {@code [canonical_id:32][record_type=5][lane:u8][envelope]}. The sealer
 * relays it as a canonical entry: the same bytes with record type
 * {@link #RT_TX}. The envelope is the archived {@code TxEnvelope} in its
 * frozen layout, and the sealer never parses it.
 *
 * <p>An instance holds a payload that has the fixed part and record type
 * {@link #RT_TX_OFFER}: {@link #parse} is the only way to get one.</p>
 */
public final class TxOffer {
    /** Record type of a full entry, on ingress only. Matches Rust {@code RT_TX_OFFER}. */
    public static final byte RT_TX_OFFER = 5;
    /** Record type of a canonical entry, on egress only. Matches Rust {@code RT_TX}. */
    public static final byte RT_TX = 6;

    /** Offset of the record type in a relayed payload. */
    static final int RECORD_TYPE_OFFSET = CanonicalSealerState.CANONICAL_ID_LEN;
    /** Offset of the lane in a relayed payload. */
    static final int LANE_OFFSET = RECORD_TYPE_OFFSET + Byte.BYTES;
    /** Offset of the envelope in a relayed payload: the length of the fixed part. */
    static final int ENVELOPE_OFFSET = LANE_OFFSET + Byte.BYTES;

    /** The relayed payload, record type {@link #RT_TX_OFFER}. */
    private final byte[] payload;

    private TxOffer(final byte[] payload) {
        this.payload = payload;
    }

    /**
     * Parse the relayed payload of a kind-0 record. Empty when the payload
     * is not a full entry: another record type, or a payload shorter than
     * the fixed part.
     */
    public static Optional<TxOffer> parse(final byte[] payload) {
        return payload.length >= ENVELOPE_OFFSET && payload[RECORD_TYPE_OFFSET] == RT_TX_OFFER
            ? Optional.of(new TxOffer(payload))
            : Optional.empty();
    }

    /** The transaction hash. */
    public byte[] canonicalId() {
        return Arrays.copyOf(payload, CanonicalSealerState.CANONICAL_ID_LEN);
    }

    /** The lane of the sender. */
    public byte lane() {
        return payload[LANE_OFFSET];
    }

    /** The length of the archived envelope. */
    public int envelopeLength() {
        return payload.length - ENVELOPE_OFFSET;
    }

    /**
     * The relayed payload of the canonical entry:
     * {@code [canonical_id:32][record_type=6][lane:u8][envelope]}.
     */
    public byte[] canonicalPayload() {
        final byte[] canonical = payload.clone();
        canonical[RECORD_TYPE_OFFSET] = RT_TX;
        return canonical;
    }
}
