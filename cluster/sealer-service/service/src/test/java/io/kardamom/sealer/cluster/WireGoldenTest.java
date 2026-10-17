package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.kardamom.sealer.ClusterStatus;
import io.kardamom.sealer.RecordedCursors;
import io.kardamom.sealer.Relayed;
import io.kardamom.sealer.TxOffer;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The Java codecs on the golden frames that the Rust wire tests also read.
 * The build passes the path of the file in the {@code kardamom.wireGolden}
 * system property. The service reads each golden ingress frame and builds
 * the golden egress frame from it, byte for byte.
 */
final class WireGoldenTest {
    private static final long INDEX = 5;
    private static final long RETAINED_BYTES = 1_048_583;

    /** The golden frames by name. */
    private static Map<String, byte[]> golden;

    private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    private final EgressFrames frames = new EgressFrames(buffer);

    @BeforeAll
    static void readGolden() throws IOException {
        golden = Files.readAllLines(Path.of(System.getProperty("kardamom.wireGolden"))).stream()
            .filter(line -> !line.isEmpty() && !line.startsWith("#"))
            .map(line -> line.split(" ", 2))
            .collect(Collectors.toMap(parts -> parts[0], parts -> HexFormat.of().parseHex(parts[1])));
    }

    /** The relayed payload of a golden kind-0 frame, as the service builds it. */
    private static byte[] relayedPayload(final String name) {
        final byte[] ingress = golden.get(name);
        return RelayShape.INGRESS_RECORD.payload(new UnsafeBuffer(ingress), 0, ingress.length);
    }

    private static ClusterStatus status() {
        return new ClusterStatus(100, 160, 50, false, 7000, 90_000, 95, RecordedCursors.NONE, 0, false);
    }

    @Test
    void aFullEntryRelaysAsTheGoldenCanonicalEntry() {
        final TxOffer offer = TxOffer.parse(relayedPayload("tx_offer_ingress")).orElseThrow();

        assertEquals(3, offer.lane());
        assertArrayEquals(golden.get("tx_egress"),
            frames.copy(frames.relayed(new Relayed(INDEX, offer.canonicalPayload()))));
        assertArrayEquals(golden.get("relayed_ref"),
            frames.copy(frames.relayedRef(INDEX, offer.canonicalId(), SealerWire.RT_TX)));
    }

    @Test
    void theRelayedReferenceAndTheFeedAnswersHaveTheirFixedLengths() {
        assertEquals(SealerWire.RELAYED_REF_LEN, golden.get("relayed_ref").length);
        assertArrayEquals(golden.get("feed_gap"), frames.copy(frames.feedGap(9, 4, 7)));
        assertArrayEquals(golden.get("feed_replay_done"),
            frames.copy(frames.feedAnswer(SealerWire.EGRESS_KIND_REPLAY_DONE, 9, 4, 7)));
        assertEquals(SealerWire.FEED_ANSWER_LEN, golden.get("feed_gap").length);
        final UnsafeBuffer request = new UnsafeBuffer(new byte[SealerWire.FEED_REQUEST_LEN]);
        new FeedRequest(9, 4, 7).write(request);
        assertArrayEquals(golden.get("feed_request"), request.byteArray());
        assertEquals(Optional.of(new FeedRequest(9, 4, 7)),
            FeedRequest.parse(new UnsafeBuffer(golden.get("feed_request")), 0, SealerWire.FEED_REQUEST_LEN));
    }

    @Test
    void theStatusFramesMatchWithAndWithoutTheByteTail() {
        assertArrayEquals(golden.get("status_68"), frames.copy(frames.status(status(), false)));
        assertEquals(SealerWire.STATUS_WITH_SNAPSHOT_FLAG_LEN, golden.get("status_68").length);
        assertArrayEquals(golden.get("status_76"), frames.copy(frames.status(status(), false, RETAINED_BYTES)));
        assertArrayEquals(golden.get("status_76_snapshot"),
            frames.copy(frames.status(status(), true, RETAINED_BYTES)));
        assertEquals(SealerWire.STATUS_WITH_RETAINED_BYTES_LEN, golden.get("status_76").length);
        // The frame of the release before the flag is the 68-byte frame without its last byte.
        assertArrayEquals(golden.get("status_67"),
            java.util.Arrays.copyOf(golden.get("status_68"), SealerWire.STATUS_WITH_RECORD_LAG_LEN));
    }

    @Test
    void theFlagByteSitsBetweenTheRecordLagTailAndTheByteTail() {
        final UnsafeBuffer status = new UnsafeBuffer(golden.get("status_76"));
        assertEquals(0, status.getByte(SealerWire.STATUS_SNAPSHOT_FLAG_OFFSET));
        assertEquals(1, new UnsafeBuffer(golden.get("status_76_snapshot"))
            .getByte(SealerWire.STATUS_SNAPSHOT_FLAG_OFFSET), "the snapshot notice");
        assertEquals(RETAINED_BYTES,
            status.getLong(SealerWire.STATUS_RETAINED_BYTES_OFFSET, ByteOrder.LITTLE_ENDIAN));
        assertEquals(67, SealerWire.STATUS_SNAPSHOT_FLAG_OFFSET);
        assertEquals(68, SealerWire.STATUS_WITH_SNAPSHOT_FLAG_LEN);
        assertEquals(68, SealerWire.STATUS_RETAINED_BYTES_OFFSET);
        assertEquals(76, SealerWire.STATUS_WITH_RETAINED_BYTES_LEN);
    }

    @Test
    void aReferenceRecordRelaysAsToday() {
        assertArrayEquals(golden.get("txref_egress"),
            frames.copy(frames.relayed(new Relayed(INDEX, relayedPayload("txref_ingress")))));
    }
}
