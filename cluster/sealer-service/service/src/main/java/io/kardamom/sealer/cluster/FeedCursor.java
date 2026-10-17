package io.kardamom.sealer.cluster;

import java.nio.ByteOrder;
import org.agrona.DirectBuffer;

/**
 * The point of the canonical stream after the frames that a feed answer
 * carried: the next record index and the next block. A replay request from
 * it is valid, and it carries every frame that the answer did not carry.
 *
 * <ul>
 *   <li>After a boundary, the index is its {@code end_tx_idx}, and the block
 *       is one above its block.</li>
 *   <li>After a relayed frame, the index is one above its index. An origin
 *       record takes more slots, so the next relayed index can be higher;
 *       the boundary of its block still ends at or above this index, so the
 *       cursor stays inside its block.</li>
 * </ul>
 *
 * <p>The cursor is mutable, so the walk moves it with no allocation.</p>
 */
final class FeedCursor {

    private long index;
    private long block;

    /** Start at the cursor of a request. */
    void reset(final long fromIndex, final long fromBlock) {
        index = fromIndex;
        block = fromBlock;
    }

    /** Move past the retained frame at the start of {@code frame}. */
    void pass(final DirectBuffer frame) {
        final long key = frame.getLong(FrameKey.KEY_OFFSET, ByteOrder.LITTLE_ENDIAN);
        if (frame.getByte(0) == SealerWire.EGRESS_KIND_BOUNDARY) {
            index = frame.getLong(FrameKey.BOUNDARY_END_OFFSET, ByteOrder.LITTLE_ENDIAN);
            block = Math.addExact(key, 1);
        } else {
            index = Math.addExact(key, 1);
        }
    }

    long index() {
        return index;
    }

    long block() {
        return block;
    }

    @Override
    public String toString() {
        return "(" + index + "," + block + ")";
    }
}
