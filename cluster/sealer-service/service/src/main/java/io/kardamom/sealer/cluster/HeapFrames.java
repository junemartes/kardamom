package io.kardamom.sealer.cluster;

import io.kardamom.sealer.ClusterStatus;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * The retained frames on the heap: one array for each frame. The heap
 * grows with the retained bytes. The snapshot carries every frame:
 * {@code count(4) | count * (boundary(1) | key(8) | len(4) | frame)},
 * big-endian like the state.
 */
final class HeapFrames implements RetainedFrames {

    /** One retained frame and its replay key. A retained array never changes. */
    private record Frame(byte[] bytes, FrameKey key) {
    }

    private final ArrayDeque<Frame> frames = new ArrayDeque<>();

    @Override
    public byte[] append(final DirectBuffer staged, final int length) {
        final byte[] bytes = new byte[length];
        staged.getBytes(0, bytes);
        frames.addLast(new Frame(bytes, FrameKey.of(staged)));
        return bytes;
    }

    @Override
    public long count() {
        return frames.size();
    }

    @Override
    public FrameKey front() {
        return frames.peekFirst().key();
    }

    @Override
    public void evictFront() {
        frames.removeFirst();
    }

    @Override
    public long boundaryEnd(final long block) {
        return frames.stream()
            .filter(f -> f.key().boundary() && f.key().key() == block)
            .findFirst()
            .map(f -> new UnsafeBuffer(f.bytes()).getLong(FrameKey.BOUNDARY_END_OFFSET, ByteOrder.LITTLE_ENDIAN))
            .orElse(-1L);
    }

    @Override
    public Stream<byte[]> wanted(final long fromIndex, final long fromBlock) {
        return frames.stream().filter(f -> f.key().wanted(fromIndex, fromBlock)).map(Frame::bytes);
    }

    @Override
    public byte[] writeSnapshot(final long floorIndex, final long floorBlock) {
        final int size = frames.stream()
            .mapToInt(f -> Byte.BYTES + Long.BYTES + Integer.BYTES + f.bytes().length)
            .reduce(Integer.BYTES, Math::addExact);
        final ByteBuffer buf = ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN);
        buf.putInt(frames.size());
        frames.forEach(f -> buf
            .put(f.key().boundary() ? (byte) 1 : (byte) 0)
            .putLong(f.key().key())
            .putInt(f.bytes().length)
            .put(f.bytes()));
        return buf.array();
    }

    /**
     * Read the frames of a heap section, and move the floors down to the
     * oldest of them. A section-less snapshot (a version before 8) keeps
     * the floors at the restore point.
     */
    @Override
    public Floors restore(final ByteBuffer section, final Floors restorePoint) {
        final int count = section.hasRemaining() ? section.getInt() : 0;
        return IntStream.range(0, count)
            .mapToObj(i -> restoreFrame(section))
            .reduce(restorePoint, HeapFrames::lower, HeapFrames::lowerBoth);
    }

    /** Read one frame, {@code boundary(1) | key(8) | len(4) | frame}, retain it, and return its key. */
    private FrameKey restoreFrame(final ByteBuffer section) {
        section.position(section.position() + Byte.BYTES + Long.BYTES);
        final byte[] bytes = new byte[section.getInt()];
        section.get(bytes);
        final FrameKey key = FrameKey.of(new UnsafeBuffer(bytes));
        frames.addLast(new Frame(bytes, key));
        return key;
    }

    /** The floors moved down to {@code key}, on its axis. */
    private static Floors lower(final Floors floors, final FrameKey key) {
        return key.boundary()
            ? new Floors(floors.index(), Math.min(floors.block(), key.key()))
            : new Floors(Math.min(floors.index(), key.key()), floors.block());
    }

    private static Floors lowerBoth(final Floors a, final Floors b) {
        return new Floors(Math.min(a.index(), b.index()), Math.min(a.block(), b.block()));
    }

    @Override
    public void clear() {
        frames.clear();
    }

    @Override
    public int status(
            final EgressFrames codec, final ClusterStatus status, final boolean snapshotFollows, final long postedHead) {
        return codec.status(status, snapshotFollows);
    }

    @Override
    public long publishedHead() {
        return 0;
    }

    @Override
    public byte[] forSession(final byte[] frame) {
        return frame;
    }

    @Override
    public void checkHealth() {
        // The heap store has no helper thread and no file.
    }

    @Override
    public void close() {
        frames.clear();
    }
}
