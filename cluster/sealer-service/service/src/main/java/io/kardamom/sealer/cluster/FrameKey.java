package io.kardamom.sealer.cluster;

import java.nio.ByteOrder;
import org.agrona.DirectBuffer;

/**
 * The replay key of one retained egress frame. A relayed frame and a
 * boundary frame both put their key after the kind byte: the record index,
 * or the block number.
 *
 * @param boundary whether the frame is a boundary
 * @param key      the record index, or the block number of a boundary
 */
record FrameKey(boolean boundary, long key) {

    /** The offset of the key in a frame: after the kind byte. */
    static final int KEY_OFFSET = Byte.BYTES;
    /** The offset of {@code endTxIdx} in a boundary frame: after the kind and the block number. */
    static final int BOUNDARY_END_OFFSET = KEY_OFFSET + Long.BYTES;

    /** The key of the frame at the start of {@code frame}. */
    static FrameKey of(final DirectBuffer frame) {
        return new FrameKey(
            frame.getByte(0) == SealerWire.EGRESS_KIND_BOUNDARY,
            frame.getLong(KEY_OFFSET, ByteOrder.LITTLE_ENDIAN));
    }

    /** Whether a replay from {@code (fromIndex, fromBlock)} includes this frame. */
    boolean wanted(final long fromIndex, final long fromBlock) {
        return boundary ? key >= fromBlock : key >= fromIndex;
    }
}
