package io.kardamom.sealer.cluster;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.IntStream;

/**
 * The retention section of a snapshot with a retained file, version 12 or
 * 17: the floors, the frame count, the front and head positions of the
 * retained file, and the index of its retained boundaries. It is the last part of the snapshot:
 * {@code floor_index(8) | floor_block(8) | count(8) | front(8) | head(8) |
 * n(4) | n * (block(8) | end_tx_idx(8) | end_pos(8)) | len(4)},
 * big-endian like the state. {@code len} is the length of the section
 * before it, so a peer seed finds the section at the end of the snapshot
 * without a parse of the state.
 *
 * @param floors the oldest retained record index and boundary block
 * @param count  the number of retained frames
 * @param front  the position of the oldest retained frame
 * @param head   the position after the newest frame
 * @param marks  the retained boundaries by block number
 */
record RetainedSection(RetainedFrames.Floors floors, long count, long front, long head,
        NavigableMap<Long, Mark> marks) {

    /** One retained boundary: its end index, and the position after its record. */
    record Mark(long endTxIdx, long endPos) {
    }

    private static final int FIXED_BYTES = 5 * Long.BYTES + Integer.BYTES;
    private static final int MARK_BYTES = 3 * Long.BYTES;

    RetainedSection {
        marks = Collections.unmodifiableNavigableMap(new TreeMap<>(marks));
    }

    /** The section, with its length after it. */
    byte[] write() {
        final int length = Math.addExact(FIXED_BYTES, Math.multiplyExact(MARK_BYTES, marks.size()));
        final ByteBuffer buf = ByteBuffer.allocate(Math.addExact(length, Integer.BYTES)).order(ByteOrder.BIG_ENDIAN);
        buf.putLong(floors.index()).putLong(floors.block())
            .putLong(count).putLong(front).putLong(head).putInt(marks.size());
        marks.forEach((block, mark) -> buf.putLong(block).putLong(mark.endTxIdx()).putLong(mark.endPos()));
        buf.putInt(length);
        return buf.array();
    }

    /**
     * Read a section at the position of {@code buf}, and its length after
     * it. The section must end the buffer, and the length must equal the
     * bytes of the section.
     *
     * @throws IllegalStateException when the length does not match, or bytes follow it
     */
    static RetainedSection read(final ByteBuffer buf) {
        final int start = buf.position();
        final RetainedFrames.Floors floors = new RetainedFrames.Floors(buf.getLong(), buf.getLong());
        final long count = buf.getLong();
        final long front = buf.getLong();
        final long head = buf.getLong();
        final TreeMap<Long, Mark> marks = new TreeMap<>();
        IntStream.range(0, buf.getInt()).forEach(i -> marks.put(buf.getLong(), new Mark(buf.getLong(), buf.getLong())));
        final int read = buf.position() - start;
        final int length = buf.getInt();
        if (length != read || buf.hasRemaining()) {
            throw new IllegalStateException("the retained section has " + read + " bytes and the length " + length
                + ", with " + buf.remaining() + " bytes after it");
        }
        return new RetainedSection(floors, count, front, head, marks);
    }

    /** The section at the end of a snapshot payload with a retained file. */
    static RetainedSection ofSnapshot(final byte[] snapshot) {
        final ByteBuffer buf = ByteBuffer.wrap(snapshot).order(ByteOrder.BIG_ENDIAN);
        final int length = buf.getInt(snapshot.length - Integer.BYTES);
        return read(buf.position(snapshot.length - Integer.BYTES - length));
    }
}
