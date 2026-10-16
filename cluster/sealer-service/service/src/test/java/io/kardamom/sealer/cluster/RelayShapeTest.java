package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/** The relay of each ingress kind, and the ingress cap against the growth of each relay. */
class RelayShapeTest {

    /** The longest application payload of one session message. */
    private static final int MAX_APP_PAYLOAD_LEN =
        SealerWire.MAX_SESSION_MESSAGE_LEN - SealerWire.SESSION_HEADER_LEN;

    private static byte[] id() {
        return IngressFrames.canonicalId(7);
    }

    /** One frame of each relayed kind, with the body {@code {0x03, 0x04}} after the record type. */
    private static Map<RelayShape, byte[]> frames() {
        final byte[] body = {0x03, 0x04};
        return Map.of(
            RelayShape.INGRESS_RECORD, IngressFrames.exemptRecordFrame(id(), body),
            RelayShape.ORIGIN_RECORD, IngressFrames.originRecordFrame(id(), 9, 1, body),
            RelayShape.REMOTE_ORIGIN_RECORD,
                IngressFrames.remoteOriginRecordFrame(id(), 10, 700, 2, 0, 0, body));
    }

    @Test
    void eachShapeRelaysTheIdThenTheTail() {
        final byte[] expected = {
            7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x5A, 0x03, 0x04};
        frames().forEach((shape, frame) -> {
            assertEquals(shape.kind, frame[0]);
            assertArrayEquals(expected, shape.payload(new UnsafeBuffer(frame), 0, frame.length), shape.name());
        });
    }

    /** A kind-0 frame loses 40 bytes in the relay, a remote-origin frame 24, and an origin frame none. */
    @Test
    void theRelayMarginsAreFortyTwentyFourAndZero() {
        final Map<RelayShape, Integer> margins = frames().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().length
                - SealerWire.RELAYED_HEADER_LEN
                - entry.getKey().payload(new UnsafeBuffer(entry.getValue()), 0, entry.getValue().length).length));
        assertEquals(Map.of(
            RelayShape.INGRESS_RECORD, 40,
            RelayShape.ORIGIN_RECORD, 0,
            RelayShape.REMOTE_ORIGIN_RECORD, 24), margins);
    }

    /**
     * An ingress frame at the cap relays to an egress frame that fits one
     * session message, for each shape. The relay with the largest growth
     * fills the message exactly: today the origin record.
     */
    @Test
    void theIngressCapFitsTheRelayOfEachKind() {
        Arrays.stream(RelayShape.values()).forEach(shape -> {
            final byte[] frame = new byte[RelayShape.MAX_INGRESS_PAYLOAD_LEN];
            frame[0] = shape.kind;
            final int egress = SealerWire.RELAYED_HEADER_LEN
                + shape.payload(new UnsafeBuffer(frame), 0, frame.length).length;
            assertTrue(egress <= MAX_APP_PAYLOAD_LEN, shape + " relays " + egress + " bytes");
            assertTrue(RelayShape.MAX_INGRESS_PAYLOAD_LEN + shape.growth() <= MAX_APP_PAYLOAD_LEN);
        });
        final List<Integer> growths = Arrays.stream(RelayShape.values()).map(RelayShape::growth).toList();
        assertEquals(List.of(0, 0, 0), growths);
        assertEquals(MAX_APP_PAYLOAD_LEN, RelayShape.MAX_INGRESS_PAYLOAD_LEN);
    }
}
