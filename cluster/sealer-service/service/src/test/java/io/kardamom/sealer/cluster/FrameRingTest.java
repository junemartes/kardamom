package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.stream.IntStream;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;
import org.junit.jupiter.api.Test;

/**
 * The reserve of the frame ring holds the gap marker at every position of
 * the ring, the wrap point included: from each start offset, fill the ring
 * with frames until one does not fit, and the marker still goes in.
 */
class FrameRingTest {

    private static final int CAPACITY = 4096;
    /** A record of a one-byte message: 8 bytes of header and 8 of alignment. */
    private static final int STEP = 16;
    /** Up to the largest message of the ring, an eighth of it. */
    private static final int[] LENGTHS = {1, 17, 33, 57, 120, 200, 511};
    private static final UnsafeBuffer FRAME = new UnsafeBuffer(new byte[CAPACITY / 8]);
    private static final UnsafeBuffer MARKER = new UnsafeBuffer(new byte[SealerWire.FEED_ANSWER_LEN]);
    private static final MessageHandler SKIP = (type, buffer, index, length) -> { };

    @Test
    void the_marker_fits_after_a_full_ring_at_each_start_offset() {
        IntStream.range(0, CAPACITY / STEP).forEach(steps ->
            IntStream.of(LENGTHS).forEach(length -> assertMarkerFits(steps, length)));
    }

    /** Move the ring by {@code steps} records, fill it with frames of {@code length}, and write the marker. */
    private static void assertMarkerFits(final int steps, final int length) {
        final RingBuffer ring = new OneToOneRingBuffer(
            new UnsafeBuffer(new byte[CAPACITY + RingBufferDescriptor.TRAILER_LENGTH]));
        IntStream.range(0, steps).forEach(i -> ring.write(1, MARKER, 0, 1));
        ring.read(SKIP, Integer.MAX_VALUE);
        final FrameRing frames = new FrameRing(ring);
        boolean more = true;
        while (more) {
            more = frames.put(1, FRAME, length);
        }
        assertFalse(frames.put(1, FRAME, length), "the ring is full");
        assertTrue(frames.putMarker(1, MARKER, MARKER.capacity()),
            "the marker fits at offset " + steps * STEP + " after frames of " + length);
    }
}
