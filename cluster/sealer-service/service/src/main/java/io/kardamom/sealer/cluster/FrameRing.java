package io.kardamom.sealer.cluster;

import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.ringbuffer.RecordDescriptor;
import org.agrona.concurrent.ringbuffer.RingBuffer;

/**
 * The write side of the frame ring of the member feed. A frame goes in only
 * while the ring keeps {@link #RESERVE} bytes free after it, so the one gap
 * marker that ends an answer always fits.
 *
 * <p>The marker is 25 bytes: a record of 40 bytes with its header and its
 * alignment. When the marker does not fit before the end of the ring, the
 * ring writes a padding record there, of at most 32 bytes, and needs the
 * record plus 8 bytes at the start of the ring: 48 bytes. So the marker
 * needs at most 80 bytes. The reserve is 128.</p>
 */
final class FrameRing {

    /** The free bytes that each frame leaves for the gap marker and its padding. */
    static final int RESERVE = 128;

    private final RingBuffer ring;

    FrameRing(final RingBuffer ring) {
        this.ring = ring;
    }

    /**
     * Write the first {@code length} bytes of {@code frame} with the type
     * {@code type}, when the ring keeps {@link #RESERVE} free after it. The
     * check counts the record twice: a record that does not fit before the
     * end of the ring takes a padding record of at most its own size.
     * False when the ring has no room.
     */
    boolean put(final int type, final DirectBuffer frame, final int length) {
        final long record = BitUtil.align(length + RecordDescriptor.HEADER_LENGTH, RecordDescriptor.ALIGNMENT);
        final long free = (long) ring.capacity() - ring.size();
        return free >= 2 * record + RESERVE && ring.write(type, frame, 0, length);
    }

    /** Write the gap marker into the reserve. False only when the reserve did not hold. */
    boolean putMarker(final int type, final DirectBuffer marker, final int length) {
        return ring.write(type, marker, 0, length);
    }
}
