package io.kardamom.sealer;

import static io.kardamom.sealer.SessionWindows.DUPLICATE;
import static io.kardamom.sealer.SessionWindows.EXPIRED;
import static io.kardamom.sealer.SessionWindows.FRESH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

final class SessionWindowsTest {

    private static final int UNIT = 1 << SessionWindows.UNIT_SHIFT;

    @Test
    void a_second_copy_of_a_reference_is_a_duplicate() {
        SessionWindows w = new SessionWindows(4, 64);
        assertEquals(FRESH, w.offer(0, 9, 10 * UNIT, 0));
        assertEquals(DUPLICATE, w.offer(0, 9, 10 * UNIT, 1));
    }

    @Test
    void the_same_position_of_another_image_is_fresh() {
        SessionWindows w = new SessionWindows(4, 64);
        assertEquals(FRESH, w.offer(0, 9, 10 * UNIT, 0));
        assertEquals(FRESH, w.offer(1, 9, 10 * UNIT, 1));
        assertEquals(FRESH, w.offer(0, 8, 10 * UNIT, 2));
    }

    @Test
    void a_reference_a_width_below_the_highest_is_expired() {
        SessionWindows w = new SessionWindows(4, 64);
        assertEquals(FRESH, w.offer(0, 9, 100 * UNIT, 0));
        assertEquals(FRESH, w.offer(0, 9, 37 * UNIT, 1));
        assertEquals(EXPIRED, w.offer(0, 9, 36 * UNIT, 2));
    }

    @Test
    void a_unit_that_reenters_the_ring_is_fresh() {
        SessionWindows w = new SessionWindows(4, 64);
        assertEquals(FRESH, w.offer(0, 9, 5 * UNIT, 0));
        assertEquals(FRESH, w.offer(0, 9, (5 + 64) * UNIT, 1));
        assertEquals(EXPIRED, w.offer(0, 9, 5 * UNIT, 2));
        assertEquals(DUPLICATE, w.offer(0, 9, (5 + 64) * UNIT, 3));
    }

    @Test
    void a_new_image_takes_the_slot_of_the_image_silent_longest() {
        SessionWindows w = new SessionWindows(2, 64);
        w.offer(0, 1, UNIT, 0);
        w.offer(0, 2, UNIT, 1);
        w.offer(0, 1, 2 * UNIT, 2);
        assertEquals(FRESH, w.offer(0, 3, UNIT, 3));
        assertEquals(DUPLICATE, w.offer(0, 1, 2 * UNIT, 4));
        assertEquals(FRESH, w.offer(0, 2, UNIT, 5));
        assertEquals(2, w.used());
    }

    @Test
    void the_windows_match_a_model_under_random_offers() {
        Random random = new Random(7);
        int width = 256;
        SessionWindows w = new SessionWindows(16, width);
        Model model = new Model(width);
        long[] tops = new long[8];
        for (int i = 0; i < 400_000; i++) {
            int image = random.nextInt(tops.length);
            long unit = nextUnit(random, tops, image, width);
            assertEquals(model.offer(image, unit), w.offer(image, 1000 + image, unit * UNIT, i), "offer " + i);
        }
    }

    @Test
    void a_snapshot_restores_the_same_decisions() {
        Random random = new Random(11);
        SessionWindows w = new SessionWindows(8, 128);
        long[] tops = new long[6];
        for (int i = 0; i < 20_000; i++) {
            int image = random.nextInt(tops.length);
            w.offer(image, 7, nextUnit(random, tops, image, 128) * UNIT, i);
        }
        ByteBuffer buf = ByteBuffer.allocate(w.snapshotLen());
        w.writeTo(buf);
        buf.flip();
        SessionWindows restored = SessionWindows.readFrom(buf, 8, 128);
        for (int i = 20_000; i < 40_000; i++) {
            int image = random.nextInt(tops.length);
            long position = nextUnit(random, tops, image, 128) * UNIT;
            assertEquals(w.offer(image, 7, position, i), restored.offer(image, 7, position, i), "offer " + i);
        }
    }

    @Test
    void a_snapshot_with_other_settings_is_refused() {
        SessionWindows w = new SessionWindows(8, 128);
        ByteBuffer buf = ByteBuffer.allocate(w.snapshotLen());
        w.writeTo(buf);
        buf.flip();
        assertThrows(IllegalStateException.class, () -> SessionWindows.readFrom(buf, 8, 256));
    }

    /** A unit near the image's highest: mostly just above or below it, sometimes far ahead. */
    private static long nextUnit(Random random, long[] tops, int image, int width) {
        int roll = random.nextInt(100);
        long unit = roll < 50 ? tops[image] + 1 + random.nextInt(3)
            : roll < 95 ? tops[image] - random.nextInt(width + width / 2)
            : tops[image] + random.nextInt(3 * width);
        unit = Math.max(0, unit);
        tops[image] = Math.max(tops[image], unit);
        return unit;
    }

    /** The rules of the windows with a set per image. */
    private static final class Model {
        private final int width;
        private final Map<Integer, TreeSet<Long>> seen = new HashMap<>();
        private final Map<Integer, Long> top = new HashMap<>();

        Model(int width) {
            this.width = width;
        }

        int offer(int image, long unit) {
            long highest = top.getOrDefault(image, -1L);
            if (highest - unit >= width) {
                return EXPIRED;
            }
            TreeSet<Long> units = seen.computeIfAbsent(image, k -> new TreeSet<>());
            if (unit > highest) {
                top.put(image, unit);
                units.headSet(unit - width + 1).clear();
            }
            return units.add(unit) ? FRESH : DUPLICATE;
        }
    }
}
