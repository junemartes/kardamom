package io.kardamom.sealer.cluster;

import io.kardamom.sealer.ClusterStatus;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * The retained frames on the heap, in one byte ring and one index ring. A
 * frame takes the next bytes of the byte ring, or the start of the ring when
 * it does not fit before the end. The index ring holds the offset, the
 * length and the replay key of each frame, oldest first. Both rings double
 * when they are full, so an append allocates nothing in steady state.
 *
 * <p>The walk key of a frame is its frame number: the count of the frames
 * that this store appended before it, since the store started. The front
 * key is the appended count minus the frame count, and the frame at key
 * {@code k} is in the index slot {@code (first + k - front) % slots}. So an
 * eviction, a growth of the byte ring and a growth of the index ring keep
 * the key of each frame. {@link #clear()} keeps the appended count, so a
 * key from before it is below the front.</p>
 *
 * <p>The snapshot carries every frame:
 * {@code count(4) | count * (boundary(1) | key(8) | len(4) | frame)},
 * big-endian like the state.</p>
 */
final class HeapFrames implements RetainedFrames {

    private static final int INITIAL_BYTES = 1 << 20;
    private static final int INITIAL_FRAMES = 1 << 14;

    private byte[] data = new byte[INITIAL_BYTES];
    private final UnsafeBuffer view = new UnsafeBuffer(data);
    /** The offset of the oldest frame, and the offset after the newest one. */
    private int head;
    private int tail;

    private int[] offsets = new int[INITIAL_FRAMES];
    private int[] lengths = new int[INITIAL_FRAMES];
    private long[] keys = new long[INITIAL_FRAMES];
    private boolean[] boundaries = new boolean[INITIAL_FRAMES];
    /** The index slot of the oldest frame, and the number of frames. */
    private int first;
    private int count;
    /** The bytes of the retained frames. */
    private long bytes;
    /** The frames that this store appended since its start: the walk key after the newest frame. */
    private long appended;

    @Override
    public void append(final DirectBuffer staged, final int length) {
        final int at = placeFor(length);
        staged.getBytes(0, data, at, length);
        final int slot = slotFor();
        offsets[slot] = at;
        lengths[slot] = length;
        keys[slot] = staged.getLong(FrameKey.KEY_OFFSET, ByteOrder.LITTLE_ENDIAN);
        boundaries[slot] = staged.getByte(0) == SealerWire.EGRESS_KIND_BOUNDARY;
        tail = at + length;
        count++;
        bytes += length;
        appended++;
    }

    /** The offset where a frame of {@code length} bytes goes. The byte ring grows when no free run holds it. */
    private int placeFor(final int length) {
        if (count == 0) {
            head = 0;
            tail = 0;
        }
        final boolean wrapped = count > 0 && tail <= head;
        if (!wrapped && data.length - tail >= length) {
            return tail;
        }
        if (!wrapped && head >= length) {
            return 0;
        }
        if (wrapped && head - tail >= length) {
            return tail;
        }
        grow(length);
        return tail;
    }

    /** The index slot of the next frame. The index ring grows when it is full. */
    private int slotFor() {
        if (count == offsets.length) {
            growIndex();
        }
        return (first + count) % offsets.length;
    }

    /** Copy the frames, oldest first, to the start of a byte ring with room for {@code length} more bytes. */
    private void grow(final int length) {
        final long need = (long) usedBytes() + length;
        final byte[] bigger = new byte[(int) Math.min(Integer.MAX_VALUE - 8, Math.max(2L * data.length, 2 * need))];
        int at = 0;
        for (int i = 0; i < count; i++) {
            at = move(bigger, slot(i), at);
        }
        data = bigger;
        view.wrap(data);
        head = 0;
        tail = at;
    }

    private int move(final byte[] to, final int slot, final int at) {
        System.arraycopy(data, offsets[slot], to, at, lengths[slot]);
        offsets[slot] = at;
        return at + lengths[slot];
    }

    private int usedBytes() {
        return IntStream.range(0, count).map(i -> lengths[slot(i)]).sum();
    }

    private void growIndex() {
        final int size = offsets.length * 2;
        final int[] o = new int[size];
        final int[] l = new int[size];
        final long[] k = new long[size];
        final boolean[] b = new boolean[size];
        for (int i = 0; i < count; i++) {
            final int s = slot(i);
            o[i] = offsets[s];
            l[i] = lengths[s];
            k[i] = keys[s];
            b[i] = boundaries[s];
        }
        offsets = o;
        lengths = l;
        keys = k;
        boundaries = b;
        first = 0;
    }

    /** The index slot of the {@code i}-th oldest frame. */
    private int slot(final int i) {
        return (first + i) % offsets.length;
    }

    @Override
    public long count() {
        return count;
    }

    @Override
    public long bytes() {
        return bytes;
    }

    @Override
    public boolean carriesBytes() {
        return false;
    }

    @Override
    public boolean frontIsBoundary() {
        return boundaries[first];
    }

    @Override
    public long frontKey() {
        return keys[first];
    }

    @Override
    public void evictFront() {
        bytes -= lengths[first];
        first = (first + 1) % offsets.length;
        count--;
        head = count == 0 ? tail : offsets[first];
    }

    @Override
    public long boundaryEnd(final long block) {
        return IntStream.range(0, count)
            .map(this::slot)
            .filter(s -> boundaries[s] && keys[s] == block)
            .mapToLong(s -> view.getLong(offsets[s] + FrameKey.BOUNDARY_END_OFFSET, ByteOrder.LITTLE_ENDIAN))
            .findFirst()
            .orElse(-1L);
    }

    /** The scan of the index runs once for each feed request, and requests are rare. */
    @Override
    public long walkStart(final long fromBlock) {
        final long front = walkFront();
        return IntStream.range(0, count)
            .filter(i -> boundaries[slot(i)] && keys[slot(i)] == fromBlock - 1)
            .mapToLong(i -> front + i + 1)
            .findFirst()
            .orElse(front);
    }

    @Override
    public long walkFront() {
        return appended - count;
    }

    @Override
    public long walkHead() {
        return appended;
    }

    /**
     * A growth of the byte ring moves the bytes, so the walk wraps the view
     * again at each step. The key minus the front key is below the frame
     * count, an {@code int}.
     */
    @Override
    public void view(final long key, final UnsafeBuffer view) {
        final int slot = slot((int) (key - walkFront()));
        view.wrap(data, offsets[slot], lengths[slot]);
    }

    @Override
    public long walkNext(final long key) {
        return key + 1;
    }

    @Override
    public Stream<byte[]> wanted(final long fromIndex, final long fromBlock) {
        return IntStream.range(0, count)
            .map(this::slot)
            .filter(s -> boundaries[s] ? keys[s] >= fromBlock : keys[s] >= fromIndex)
            .mapToObj(this::copyOf);
    }

    private byte[] copyOf(final int slot) {
        final byte[] out = new byte[lengths[slot]];
        System.arraycopy(data, offsets[slot], out, 0, out.length);
        return out;
    }

    @Override
    public byte[] writeSnapshot(final long floorIndex, final long floorBlock) {
        final int size = IntStream.range(0, count)
            .map(i -> Byte.BYTES + Long.BYTES + Integer.BYTES + lengths[slot(i)])
            .reduce(Integer.BYTES, Math::addExact);
        final ByteBuffer buf = ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN);
        buf.putInt(count);
        IntStream.range(0, count).map(this::slot).forEach(s -> buf
            .put(boundaries[s] ? (byte) 1 : (byte) 0)
            .putLong(keys[s])
            .putInt(lengths[s])
            .put(data, offsets[s], lengths[s]));
        return buf.array();
    }

    /**
     * Read the frames of a heap section, and move the floors down to the
     * oldest of them. A section-less snapshot (a version before 8) keeps
     * the floors at the restore point.
     */
    @Override
    public Floors restore(final ByteBuffer section, final Floors restorePoint) {
        final int frames = section.hasRemaining() ? section.getInt() : 0;
        return IntStream.range(0, frames)
            .mapToObj(i -> restoreFrame(section))
            .reduce(restorePoint, HeapFrames::lower, HeapFrames::lowerBoth);
    }

    /** Read one frame, {@code boundary(1) | key(8) | len(4) | frame}, retain it, and return its key. */
    private FrameKey restoreFrame(final ByteBuffer section) {
        section.position(section.position() + Byte.BYTES + Long.BYTES);
        final byte[] bytes = new byte[section.getInt()];
        section.get(bytes);
        final UnsafeBuffer frame = new UnsafeBuffer(bytes);
        append(frame, bytes.length);
        return FrameKey.of(frame);
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
        first = 0;
        count = 0;
        bytes = 0;
        head = 0;
        tail = 0;
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
    public int forSession(final MutableDirectBuffer staged, final int length) {
        return length;
    }

    @Override
    public void checkHealth() {
        // The heap store has no helper thread and no file.
    }

    @Override
    public void close() {
        clear();
    }
}
