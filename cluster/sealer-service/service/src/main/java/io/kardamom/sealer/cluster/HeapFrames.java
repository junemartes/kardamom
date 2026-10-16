package io.kardamom.sealer.cluster;

import io.kardamom.sealer.ClusterStatus;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
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
     * The heap cannot hold a retained file: its frames are on the disk of
     * the member that wrote the snapshot. The member stops, so it never
     * serves a partial retention.
     */
    @Override
    public Floors restore(final ByteBuffer section) {
        throw new IllegalStateException("the snapshot holds a retained file, and this member keeps the retained"
            + " frames on the heap; start every member with kardamom.cluster.carryBytes=true");
    }

    @Override
    public void clear() {
        frames.clear();
    }

    @Override
    public int status(final EgressFrames codec, final ClusterStatus status, final long postedHead) {
        return codec.status(status);
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
