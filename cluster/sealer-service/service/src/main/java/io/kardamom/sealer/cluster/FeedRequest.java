package io.kardamom.sealer.cluster;

import java.nio.ByteOrder;
import java.util.Optional;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * A replay request on the feed control stream:
 * {@code [kind:1][from_index:u64][from_block:u64][request_id:u64]}. The
 * feeder thread checks each request at the boundary and passes the bytes to
 * the service thread through the control ring, so both threads read one
 * layout. The ring also carries {@link #IDLE}: one byte 0, sent when the
 * feed loses its subscriber.
 *
 * @param fromIndex the record index of the cursor
 * @param fromBlock the block of the cursor
 * @param requestId the id that each answer to the request carries
 */
record FeedRequest(long fromIndex, long fromBlock, long requestId) {

    /** The control message that turns the feed idle. */
    static final byte IDLE = 0;
    private static final int FROM_INDEX_OFFSET = Byte.BYTES;
    private static final int FROM_BLOCK_OFFSET = FROM_INDEX_OFFSET + Long.BYTES;
    private static final int REQUEST_ID_OFFSET = FROM_BLOCK_OFFSET + Long.BYTES;

    /**
     * The request in the first {@code length} bytes of {@code buffer} at
     * {@code offset}, or empty for a message of another kind or a short
     * one. A longer message is a request: the reader ignores the tail.
     */
    static Optional<FeedRequest> parse(final DirectBuffer buffer, final int offset, final int length) {
        if (length < SealerWire.FEED_REQUEST_LEN || buffer.getByte(offset) != SealerWire.KIND_REPLAY_REQUEST) {
            return Optional.empty();
        }
        return Optional.of(new FeedRequest(
            buffer.getLong(offset + FROM_INDEX_OFFSET, ByteOrder.LITTLE_ENDIAN),
            buffer.getLong(offset + FROM_BLOCK_OFFSET, ByteOrder.LITTLE_ENDIAN),
            buffer.getLong(offset + REQUEST_ID_OFFSET, ByteOrder.LITTLE_ENDIAN)));
    }

    /** Write the request into {@code buffer} at 0, and return its length. */
    int write(final MutableDirectBuffer buffer) {
        buffer.putByte(0, SealerWire.KIND_REPLAY_REQUEST);
        buffer.putLong(FROM_INDEX_OFFSET, fromIndex, ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(FROM_BLOCK_OFFSET, fromBlock, ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(REQUEST_ID_OFFSET, requestId, ByteOrder.LITTLE_ENDIAN);
        return SealerWire.FEED_REQUEST_LEN;
    }
}
