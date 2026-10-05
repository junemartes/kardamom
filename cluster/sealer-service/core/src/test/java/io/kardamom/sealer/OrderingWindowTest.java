package io.kardamom.sealer;

import static io.kardamom.sealer.SealerStateFixtures.sender;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

/** The ordering window's flush order, its sender chaining, and its size. */
class OrderingWindowTest {

    /** Add a record with a 64-bit tip, under the sender's name. */
    private static boolean add(OrderingWindow<String> w, int sender, long tip, String record) {
        return w.add(sender(sender), 0L, tip, record);
    }

    @Test
    void flushes_highest_tip_first_then_arrival() {
        OrderingWindow<String> w = new OrderingWindow<>(OrderingWindow.DEFAULT_CAPACITY);
        add(w, 1, 5, "a5");
        add(w, 2, 9, "b9");
        add(w, 3, 5, "c5");
        add(w, 4, 0, "d0");
        add(w, 5, 9, "e9");
        assertEquals(List.of("b9", "e9", "a5", "c5", "d0"), w.flush());
        assertTrue(w.isEmpty(), "a flush empties the window");
    }

    @Test
    void a_senders_records_stay_in_arrival_order_whatever_their_tips() {
        OrderingWindow<String> w = new OrderingWindow<>(OrderingWindow.DEFAULT_CAPACITY);
        add(w, 1, 1, "a-n0");
        add(w, 2, 5, "b-n0");
        add(w, 1, 9, "a-n1");
        add(w, 1, 7, "a-n2");
        // Sender 1's head bid 1, so its chain sorts after sender 2's 5, and
        // the chain keeps nonce order 0, 1, 2 inside.
        assertEquals(List.of("b-n0", "a-n0", "a-n1", "a-n2"), w.flush());
    }

    @Test
    void a_chain_is_keyed_on_its_head_within_the_window_only() {
        OrderingWindow<String> w = new OrderingWindow<>(OrderingWindow.DEFAULT_CAPACITY);
        add(w, 1, 1, "a-n0");
        w.flush();
        // A new window: the sender's next record is its own head.
        add(w, 1, 9, "a-n1");
        add(w, 2, 5, "b-n0");
        assertEquals(List.of("a-n1", "b-n0"), w.flush());
    }

    @Test
    void the_zero_sender_is_never_chained() {
        OrderingWindow<String> w = new OrderingWindow<>(OrderingWindow.DEFAULT_CAPACITY);
        byte[] zero = new byte[CanonicalSealerState.SENDER_LEN];
        w.add(zero, 0L, 1L, "z1");
        w.add(zero, 0L, 9L, "z9");
        assertEquals(List.of("z9", "z1"), w.flush());
    }

    @Test
    void the_tip_compares_as_an_unsigned_128_bit_value() {
        OrderingWindow<String> w = new OrderingWindow<>(OrderingWindow.DEFAULT_CAPACITY);
        w.add(sender(1), 0L, -1L, "lo-max");
        w.add(sender(2), 1L, 0L, "hi-one");
        w.add(sender(3), 0L, Long.MAX_VALUE, "lo-half");
        assertEquals(List.of("hi-one", "lo-max", "lo-half"), w.flush());
    }

    @Test
    void the_window_is_full_at_its_capacity() {
        OrderingWindow<String> w = new OrderingWindow<>(OrderingWindow.DEFAULT_CAPACITY);
        for (int i = 1; i < OrderingWindow.DEFAULT_CAPACITY; i++) {
            assertFalse(add(w, i, i, "r" + i), "record " + i + " leaves room");
        }
        assertTrue(add(w, 20, 20, "r20"), "the twentieth record fills the window");
        assertEquals(OrderingWindow.DEFAULT_CAPACITY, w.size());
        List<String> flushed = w.flush();
        assertEquals(OrderingWindow.DEFAULT_CAPACITY, flushed.size());
        assertEquals("r20", flushed.get(0));
        assertEquals("r1", flushed.get(19));
    }

    @Test
    void a_zero_capacity_window_passes_every_record_through() {
        OrderingWindow<String> w = new OrderingWindow<>(0);
        assertTrue(add(w, 1, 0, "r1"), "every add fills a window of size zero");
        assertEquals(List.of("r1"), w.flush());
        assertThrows(IllegalArgumentException.class, () -> new OrderingWindow<>(-1));
    }

    /** A snapshot carries the window the cluster runs; a member with another value halts. */
    @Test
    void snapshot_load_halts_on_an_ordering_window_mismatch() {
        CanonicalSealerState twenty = new CanonicalSealerState(
                16, 1, java.util.Set.of(), VoidLedger.Config.DISABLED,
                CanonicalSealerState.DEFAULT_INCLUSION_HORIZON_BLOCKS, 20);
        byte[] snapshot = twenty.takeSnapshot();
        CanonicalSealerState restored = CanonicalSealerState.load(
                snapshot, 16, java.util.Set.of(), VoidLedger.Config.DISABLED,
                CanonicalSealerState.DEFAULT_INCLUSION_HORIZON_BLOCKS, 20);
        assertEquals(20, restored.orderingWindow());
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> CanonicalSealerState.load(
                        snapshot, 16, java.util.Set.of(), VoidLedger.Config.DISABLED,
                        CanonicalSealerState.DEFAULT_INCLUSION_HORIZON_BLOCKS, 0));
        assertTrue(e.getMessage().contains("orderingWindow"), e.getMessage());
    }
}
