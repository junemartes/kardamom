package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * The heap store against a deque of frames: random appends and evictions
 * wrap the byte ring and grow both rings, and every read gives the frames of
 * the deque, oldest first.
 */
class HeapFramesTest {

    /** A relayed frame of record {@code index}, or a boundary frame of block {@code index}, of {@code length} bytes. */
    private static byte[] frame(final boolean boundary, final long index, final int length, final Random random) {
        final byte[] out = new byte[length];
        random.nextBytes(out);
        final ByteBuffer buf = ByteBuffer.wrap(out).order(ByteOrder.LITTLE_ENDIAN);
        buf.put(0, boundary ? SealerWire.EGRESS_KIND_BOUNDARY : SealerWire.EGRESS_KIND_RELAYED);
        buf.putLong(FrameKey.KEY_OFFSET, index);
        return out;
    }

    @Test
    void the_store_matches_a_deque_under_random_appends_and_evictions() {
        final Random random = new Random(3);
        final HeapFrames store = new HeapFrames();
        final ArrayDeque<byte[]> model = new ArrayDeque<>();
        long index = 0;
        for (int step = 0; step < 200_000; step++) {
            final boolean evict = !model.isEmpty() && random.nextInt(100) < 45 + (model.size() > 40_000 ? 10 : 0);
            if (evict) {
                assertEquals(FrameKey.of(new UnsafeBuffer(model.peekFirst())).key(), store.frontKey());
                store.evictFront();
                model.removeFirst();
            } else {
                final byte[] f = frame(random.nextInt(50) == 0, index++, 20 + random.nextInt(400), random);
                store.append(new UnsafeBuffer(f), f.length);
                model.addLast(f);
            }
            assertEquals(model.size(), store.count());
        }
        assertFrames(List.copyOf(model), store.wanted(0, 0).toList());
    }

    @Test
    void a_snapshot_restores_the_same_frames() {
        final Random random = new Random(5);
        final HeapFrames store = new HeapFrames();
        for (int i = 0; i < 30_000; i++) {
            final byte[] f = frame(i % 100 == 0, i, 60 + random.nextInt(100), random);
            store.append(new UnsafeBuffer(f), f.length);
            if (i % 3 == 0) {
                store.evictFront();
            }
        }
        final byte[] snapshot = store.writeSnapshot(0, 0);
        final HeapFrames restored = new HeapFrames();
        restored.restore(ByteBuffer.wrap(snapshot), new RetainedFrames.Floors(Long.MAX_VALUE, Long.MAX_VALUE));
        assertFrames(store.wanted(0, 0).toList(), restored.wanted(0, 0).toList());
        assertArrayEquals(snapshot, restored.writeSnapshot(0, 0));
    }

    @Test
    void the_end_of_a_boundary_is_read_from_its_frame() {
        final HeapFrames store = new HeapFrames();
        final byte[] boundary = new byte[FrameKey.BOUNDARY_END_OFFSET + 3 * Long.BYTES];
        ByteBuffer.wrap(boundary).order(ByteOrder.LITTLE_ENDIAN)
            .put(0, SealerWire.EGRESS_KIND_BOUNDARY).putLong(FrameKey.KEY_OFFSET, 7L)
            .putLong(FrameKey.BOUNDARY_END_OFFSET, 42L);
        store.append(new UnsafeBuffer(boundary), boundary.length);
        assertEquals(42L, store.boundaryEnd(7L));
        assertEquals(-1L, store.boundaryEnd(8L));
    }

    /**
     * The walk keys against a list of every appended frame: the key of a
     * frame is its place in that list. Random appends and evictions wrap
     * the byte ring and grow both rings, and a walker that holds one key
     * reads the same bytes at it until the front passes it.
     */
    @Test
    void the_frame_numbers_stay_the_same_across_growth_and_eviction() {
        final NumberedWalk walk = new NumberedWalk(new Random(11));
        IntStream.range(0, 100_000).forEach(i -> walk.step());
        assertTrue(walk.mostFrames > 1 << 14, "the index ring grew past its first size");
        assertTrue(walk.mostBytes > 1 << 20, "the byte ring grew past its first size");
        final HeapFrames store = walk.store;
        final List<byte[]> walked = LongStream.iterate(store.walkFront(), k -> k < store.walkHead(), store::walkNext)
            .mapToObj(walk::bytesAt)
            .toList();
        assertFrames(store.wanted(0, 0).toList(), walked);
    }

    /** One store, every frame that it appended, and a walker that holds one key. */
    private static final class NumberedWalk {
        private final Random random;
        private final HeapFrames store = new HeapFrames();
        private final List<byte[]> appended = new ArrayList<>();
        private final UnsafeBuffer view = new UnsafeBuffer(0, 0);
        private long walker;
        private long mostFrames;
        private long mostBytes;

        NumberedWalk(final Random random) {
            this.random = random;
        }

        /** Append or evict one frame, check the keys, and move the walker on at random. */
        void step() {
            if (store.count() > 0 && random.nextInt(100) < 40) {
                store.evictFront();
            } else {
                append();
            }
            assertEquals(appended.size(), store.walkHead());
            assertEquals(appended.size() - store.count(), store.walkFront());
            mostFrames = Math.max(mostFrames, store.count());
            mostBytes = Math.max(mostBytes, store.bytes());
            walker = Math.max(walker, store.walkFront());
            if (walker < store.walkHead()) {
                assertArrayEquals(appended.get((int) walker), bytesAt(walker), "key " + walker);
                walker = random.nextInt(4) == 0 ? store.walkNext(walker) : walker;
            }
        }

        /** A rare large frame makes the byte ring grow. */
        private void append() {
            final int length = random.nextInt(1_000) == 0 ? 300_000 : 20 + random.nextInt(400);
            final byte[] f = frame(random.nextInt(50) == 0, appended.size(), length, random);
            store.append(new UnsafeBuffer(f), f.length);
            appended.add(f);
        }

        byte[] bytesAt(final long key) {
            store.view(key, view);
            final byte[] out = new byte[view.capacity()];
            view.getBytes(0, out);
            return out;
        }
    }

    @Test
    void the_walk_starts_after_the_boundary_of_the_block_before_and_a_clear_keeps_the_numbers() {
        final Random random = new Random(13);
        final HeapFrames store = new HeapFrames();
        List.of(frame(false, 0, 40, random), frame(true, 4, 40, random), frame(false, 1, 40, random),
                frame(true, 5, 40, random), frame(false, 2, 40, random))
            .forEach(f -> store.append(new UnsafeBuffer(f), f.length));
        assertEquals(2L, store.walkStart(5), "after the boundary of block 4");
        assertEquals(4L, store.walkStart(6), "after the boundary of block 5");
        assertEquals(0L, store.walkStart(9), "no boundary of block 8: the front");
        store.evictFront();
        store.evictFront();
        assertEquals(2L, store.walkStart(5), "the boundary of block 4 left: the front");
        store.clear();
        assertEquals(5L, store.walkFront(), "a key from before the clear is below the front");
        assertEquals(5L, store.walkHead());
    }

    private static void assertFrames(final List<byte[]> expected, final List<byte[]> actual) {
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertArrayEquals(expected.get(i), actual.get(i), "frame " + i);
        }
    }
}
