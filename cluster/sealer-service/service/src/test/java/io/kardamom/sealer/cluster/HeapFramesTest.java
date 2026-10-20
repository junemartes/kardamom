package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Random;
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

    private static void assertFrames(final List<byte[]> expected, final List<byte[]> actual) {
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertArrayEquals(expected.get(i), actual.get(i), "frame " + i);
        }
    }
}
