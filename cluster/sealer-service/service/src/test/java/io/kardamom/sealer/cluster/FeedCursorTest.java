package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteOrder;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * The cursor after an epoch with deposits, which takes the slots
 * {@code k..k+n}, and the boundary after it: the end index of the boundary.
 * The cursor names one point of the stream, so a replay from it passes the
 * check of the egress.
 */
class FeedCursorTest {

    private static UnsafeBuffer relayed(final long index) {
        final UnsafeBuffer frame = new UnsafeBuffer(new byte[SealerWire.RELAYED_HEADER_LEN]);
        frame.putByte(0, SealerWire.EGRESS_KIND_RELAYED);
        frame.putLong(FrameKey.KEY_OFFSET, index, ByteOrder.LITTLE_ENDIAN);
        return frame;
    }

    private static UnsafeBuffer boundary(final long block, final long endTxIdx) {
        final UnsafeBuffer frame = new UnsafeBuffer(new byte[RetainedWalk.BOUNDARY_LEN]);
        frame.putByte(0, SealerWire.EGRESS_KIND_BOUNDARY);
        frame.putLong(FrameKey.KEY_OFFSET, block, ByteOrder.LITTLE_ENDIAN);
        frame.putLong(FrameKey.BOUNDARY_END_OFFSET, endTxIdx, ByteOrder.LITTLE_ENDIAN);
        return frame;
    }

    @Test
    void after_an_epoch_and_its_boundary_the_cursor_is_the_end_index() {
        final long k = 7;
        final long slots = 5;
        final long block = 3;
        final FeedCursor cursor = new FeedCursor();
        cursor.reset(0, 1);
        cursor.pass(relayed(k));
        assertEquals(k + 1, cursor.index(), "after a relayed frame: one above its index");
        cursor.pass(boundary(block, k + slots));
        assertEquals(k + slots, cursor.index());
        assertEquals(block + 1, cursor.block());
        assertTrue(SealerEgress.cursorInsideBlock(cursor.index(), k + slots, -1),
            "the cursor lies inside the next block");
    }

    @Test
    void after_an_epoch_the_cursor_stays_inside_its_block() {
        final FeedCursor cursor = new FeedCursor();
        cursor.reset(7, 3);
        cursor.pass(relayed(7));
        assertTrue(SealerEgress.cursorInsideBlock(cursor.index(), 7, 12), "below the end of the block");
    }
}
