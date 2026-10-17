package io.kardamom.sealer.cluster;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;

/**
 * The check of a retained file against the section of a snapshot. The walk
 * reads each record from the front position to the head position. It
 * passes only when every record is a whole relayed or boundary frame inside
 * its segment, each boundary matches the index, the last record ends
 * exactly at the head, and the frame count and the boundary count match.
 *
 * <p>The walk also checks the record indexes, because the eviction copies
 * them into the floor. An index is at least the floor index, and above the
 * index before it. The first record after a boundary has the end index of
 * that boundary, and every record before a boundary is below its end
 * index. An index can step up by more than one: an origin record takes one
 * index for each of its slots.</p>
 *
 * <p>A preallocated segment holds zeros, and zeros never pass: a record of
 * length 0 is not a frame. So a file that ends below the head of the
 * snapshot, or a segment with a gap, fails the walk. After a passed walk,
 * every read of the file from the front to the head finds a whole frame,
 * so the eviction and the replay that read the file act alike on every
 * member.</p>
 */
final class RetainedWalk {

    /** The length of a boundary frame: the kind, the block, the end index, the timestamp and the L1 origin. */
    static final int BOUNDARY_LEN = Byte.BYTES + 4 * Long.BYTES;

    /** One position of the walk, or the reason why the walk stops. */
    private record Step(long pos, Optional<String> problem) {
        boolean goesOn(final long head) {
            return problem.isEmpty() && pos < head;
        }
    }

    /** The segment buffers by start position. A segment ends where the next one starts, or at its capacity. */
    private final NavigableMap<Long, ? extends ByteBuffer> segments;
    private final RetainedSection section;
    private long frames;
    private long marks;
    /** The lowest index that the next record can have. */
    private long nextIndex;
    /** Whether the next record must have exactly {@link #nextIndex}: it is the first after a boundary. */
    private boolean afterBoundary;

    RetainedWalk(final NavigableMap<Long, ? extends ByteBuffer> segments, final RetainedSection section) {
        this.segments = segments;
        this.section = section;
        this.nextIndex = section.floors().index();
    }

    /** Empty when the file holds exactly the frames of the section; else the first mismatch. */
    Optional<String> problem() {
        Step step = new Step(section.front(), Optional.empty());
        while (step.goesOn(section.head())) {
            step = next(step.pos());
        }
        final Step last = step;
        return last.problem().or(() -> totals(last.pos()));
    }

    private Optional<String> totals(final long end) {
        if (end != section.head() || frames != section.count() || marks != section.marks().size()) {
            return Optional.of("the walk ends at " + end + " with " + frames + " frames and " + marks
                + " boundaries; the snapshot has head " + section.head() + ", " + section.count()
                + " frames and " + section.marks().size() + " boundaries");
        }
        return Optional.empty();
    }

    /** Read the record at {@code pos}. Return the position after it, or the problem. */
    private Step next(final long pos) {
        final Map.Entry<Long, ? extends ByteBuffer> entry = segments.floorEntry(pos);
        if (entry == null) {
            return fail(pos, "no segment holds the position");
        }
        final long start = entry.getKey();
        final ByteBuffer buffer = entry.getValue().duplicate().order(ByteOrder.LITTLE_ENDIAN);
        final long end = Optional.ofNullable(segments.higherKey(start))
            .map(next -> Math.min(next, start + buffer.capacity()))
            .orElse(start + buffer.capacity());
        if (pos + RetainedFile.LENGTH_PREFIX > end) {
            return fail(pos, "the record header passes the end of its segment at " + end);
        }
        final int offset = Math.toIntExact(pos - start);
        final int length = buffer.getInt(offset);
        final long after = pos + RetainedFile.LENGTH_PREFIX + (long) length;
        if (length < Byte.BYTES + Long.BYTES || after > end) {
            return fail(pos, "the record length " + length + " is not a frame inside its segment");
        }
        return frame(pos, buffer, offset + RetainedFile.LENGTH_PREFIX, length, after);
    }

    /** Check one frame: a relayed frame, or a boundary that matches the index. */
    private Step frame(final long pos, final ByteBuffer buffer, final int at, final int length, final long after) {
        final byte kind = buffer.get(at);
        frames++;
        if (kind == SealerWire.EGRESS_KIND_RELAYED && length >= SealerWire.RELAYED_HEADER_LEN) {
            return record(pos, buffer.getLong(at + FrameKey.KEY_OFFSET), after);
        }
        if (kind != SealerWire.EGRESS_KIND_BOUNDARY || length != BOUNDARY_LEN) {
            return fail(pos, "the frame kind " + kind + " with length " + length + " is not retained");
        }
        final long block = buffer.getLong(at + FrameKey.KEY_OFFSET);
        final RetainedSection.Mark expected = new RetainedSection.Mark(
            buffer.getLong(at + FrameKey.BOUNDARY_END_OFFSET), after);
        if (!expected.equals(section.marks().get(block))) {
            return fail(pos, "the boundary of block " + block + " " + expected + " is not in the index");
        }
        if (nextIndex > expected.endTxIdx()) {
            return fail(pos, "the boundary of block " + block + " ends at index " + expected.endTxIdx()
                + ", below the records before it");
        }
        marks++;
        nextIndex = expected.endTxIdx();
        afterBoundary = true;
        return new Step(after, Optional.empty());
    }

    /** Check the index of one relayed record against the records and the boundary before it. */
    private Step record(final long pos, final long index, final long after) {
        final boolean fits = afterBoundary ? index == nextIndex : index >= nextIndex;
        if (!fits) {
            return fail(pos, "the record index " + index + " does not follow: the next index is "
                + (afterBoundary ? "" : "at least ") + nextIndex);
        }
        nextIndex = Math.addExact(index, 1);
        afterBoundary = false;
        return new Step(after, Optional.empty());
    }

    private static Step fail(final long pos, final String why) {
        return new Step(pos, Optional.of("position " + pos + ": " + why));
    }
}
