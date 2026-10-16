package io.kardamom.sealer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import org.junit.jupiter.api.Test;

/** The parse of a full entry and the payload of its canonical entry. */
class TxOfferTest {

    /** A relayed payload: an id of {@code 0xAB}, the record type, lane 3, and {@code envelope}. */
    private static byte[] payload(final byte recordType, final byte[] envelope) {
        final byte[] out = new byte[TxOffer.ENVELOPE_OFFSET + envelope.length];
        Arrays.fill(out, 0, CanonicalSealerState.CANONICAL_ID_LEN, (byte) 0xAB);
        out[TxOffer.RECORD_TYPE_OFFSET] = recordType;
        out[TxOffer.LANE_OFFSET] = 3;
        System.arraycopy(envelope, 0, out, TxOffer.ENVELOPE_OFFSET, envelope.length);
        return out;
    }

    @Test
    void aFullEntryParsesAndRelaysAsTypeSix() {
        final byte[] envelope = {1, 2, 3, 4};
        final TxOffer offer = TxOffer.parse(payload(TxOffer.RT_TX_OFFER, envelope)).orElseThrow();

        final byte[] id = new byte[CanonicalSealerState.CANONICAL_ID_LEN];
        Arrays.fill(id, (byte) 0xAB);
        assertArrayEquals(id, offer.canonicalId());
        assertEquals(3, offer.lane());
        assertEquals(envelope.length, offer.envelopeLength());
        assertArrayEquals(payload(TxOffer.RT_TX, envelope), offer.canonicalPayload());
    }

    @Test
    void anotherRecordTypeOrAShortPayloadIsNotAFullEntry() {
        assertTrue(TxOffer.parse(payload((byte) 0, new byte[13])).isEmpty());
        assertTrue(TxOffer.parse(payload(TxOffer.RT_TX, new byte[4])).isEmpty());
        final byte[] fixedPart = payload(TxOffer.RT_TX_OFFER, new byte[0]);
        assertTrue(TxOffer.parse(fixedPart).isPresent(), "the sealer does not parse the envelope");
        assertTrue(TxOffer.parse(Arrays.copyOf(fixedPart, fixedPart.length - 1)).isEmpty());
    }
}
