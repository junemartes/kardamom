package io.kardamom.sealer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Deterministic unit tests for {@link CanonicalSealerState}. These depend ONLY
 * on JUnit 5 — no Aeron jars — so the canonical logic stays verifiable even
 * when the cluster transport cannot be built.
 */
class CanonicalSealerStateTest {

    /** Build a distinct 32-byte canonical id whose every byte is {@code b}. */
    private static byte[] id(int b) {
        byte[] out = new byte[CanonicalSealerState.CANONICAL_ID_LEN];
        java.util.Arrays.fill(out, (byte) b);
        return out;
    }

    private static byte[] payload(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    @Test
    void dedup_first_seen_only_relays_once() {
        CanonicalSealerState state = new CanonicalSealerState(8);

        Optional<Relayed> first = state.onRecord(id(1), payload("a"));
        assertTrue(first.isPresent(), "first sighting must relay");
        assertEquals(0L, first.get().index);
        assertEquals(1L, state.canonicalCount());

        Optional<Relayed> dup = state.onRecord(id(1), payload("a-again"));
        assertFalse(dup.isPresent(), "duplicate id must be dropped");
        assertEquals(1L, state.canonicalCount(), "duplicate must NOT bump the count");
    }

    @Test
    void dedup_window_evicts_fifo() {
        // Capacity 2, mirrors Rust DedupWindow::first_seen semantics.
        CanonicalSealerState state = new CanonicalSealerState(2);

        assertTrue(state.firstSeen(id(1)));
        assertFalse(state.firstSeen(id(1)), "second sighting is a duplicate");
        assertTrue(state.firstSeen(id(2)));
        // Window is [1, 2]; inserting 3 evicts 1 (oldest first).
        assertTrue(state.firstSeen(id(3)));
        assertFalse(state.firstSeen(id(2)), "2 still inside the window");
        assertFalse(state.firstSeen(id(3)), "3 still inside the window");
        // id1 was evicted, so it is fresh again.
        assertTrue(state.firstSeen(id(1)), "evicted id is fresh again");
        assertEquals(2, state.dedupSize(), "window stays at capacity");
    }

    @Test
    void count_matches_deduped_records() {
        // N=7 records, D=3 of which are duplicates of earlier ids ⇒ 4 unique.
        CanonicalSealerState state = new CanonicalSealerState(1024);
        int[] ids = {1, 2, 1, 3, 2, 4, 3}; // duplicates: positions 2(id1), 4(id2), 6(id3)
        int n = ids.length;
        int duplicates = 3;
        for (int i : ids) {
            state.onRecord(id(i), payload("p" + i));
        }
        assertEquals((long) (n - duplicates), state.canonicalCount());
    }

    @Test
    void boundary_stamps_current_count() {
        CanonicalSealerState state = new CanonicalSealerState(64);
        state.onRecord(id(1), payload("x"));
        state.onRecord(id(2), payload("y"));
        state.onRecord(id(2), payload("y-dup")); // dropped, not counted
        state.onRecord(id(3), payload("z"));
        // 3 unique records counted at tick time.
        Boundary boundary = state.onTick(1000);
        assertEquals(state.canonicalCount(), boundary.endTxIdx);
        assertEquals(3L, boundary.endTxIdx);
    }

    @Test
    void boundary_block_number_monotonic() {
        CanonicalSealerState state = new CanonicalSealerState(64, 1);
        long previous = -1;
        for (int i = 0; i < 5; i++) {
            Boundary b = state.onTick(1000 + i * 250L);
            if (previous >= 0) {
                assertEquals(previous + 1, b.blockNumber, "block number must increment by exactly 1");
            }
            previous = b.blockNumber;
        }
        // 5 ticks starting at genesis block 1 ⇒ stamped 1..5, next would be 6.
        assertEquals(6L, state.blockNumber());
    }

    @Test
    void l2_timestamp_floored_to_interval() {
        CanonicalSealerState state = new CanonicalSealerState(64);
        // 1123 / 250 = 4 ⇒ 1000.
        assertEquals(1000L, state.onTick(1123).l2Timestamp);
        // 1250 is exactly on an interval boundary ⇒ 1250.
        assertEquals(1250L, state.onTick(1250).l2Timestamp);
    }

    @Test
    void payload_relayed_opaque() {
        CanonicalSealerState state = new CanonicalSealerState(64);
        byte[] input = new byte[] {0, 1, 2, (byte) 0xFF, 0x7F, (byte) 0x80, 42, -7};
        Relayed relayed = state.onRecord(id(9), input).orElseThrow();
        assertArrayEquals(input, relayed.payload, "payload must be byte-identical");
        // The state never wraps/parses the payload: the same reference is relayed.
        assertSame(input, relayed.payload);
    }

    @Test
    void determinism_same_inputs_same_egress() {
        CanonicalSealerState a = new CanonicalSealerState(16, 1);
        CanonicalSealerState b = new CanonicalSealerState(16, 1);

        // A fixed interleaving of records (with duplicates) and ticks.
        List<Object> aEgress = drive(a);
        List<Object> bEgress = drive(b);

        assertEquals(aEgress, bEgress, "identical inputs ⇒ identical egress");
        assertEquals(a.canonicalCount(), b.canonicalCount());
        assertEquals(a.blockNumber(), b.blockNumber());
    }

    @Test
    void snapshot_roundtrip_preserves_state() {
        CanonicalSealerState original = new CanonicalSealerState(4, 1);
        original.onRecord(id(1), payload("a"));
        original.onRecord(id(2), payload("b"));
        original.onTick(500);
        original.onRecord(id(3), payload("c"));

        byte[] snapshot = original.takeSnapshot();
        CanonicalSealerState restored = CanonicalSealerState.load(snapshot, 4);
        assertNotSame(original, restored);

        // Identical continuation inputs ⇒ identical outputs from both.
        List<Object> origCont = continuation(original);
        List<Object> restCont = continuation(restored);
        assertEquals(origCont, restCont, "restored state must continue identically");
        assertEquals(original.canonicalCount(), restored.canonicalCount());
        assertEquals(original.blockNumber(), restored.blockNumber());
    }

    @Test
    void snapshot_preserves_block_and_count() {
        CanonicalSealerState original = new CanonicalSealerState(8, 1);
        original.onRecord(id(1), payload("a"));
        original.onRecord(id(2), payload("b"));
        original.onTick(750); // stamps block 1, endTxIdx 2; block ⇒ 2
        original.onRecord(id(3), payload("c"));
        original.onTick(1000); // stamps block 2, endTxIdx 3; block ⇒ 3

        long expectedCount = original.canonicalCount();
        long expectedBlock = original.blockNumber();

        CanonicalSealerState restored = CanonicalSealerState.load(original.takeSnapshot(), 8);
        // The next tick on the restored state resumes block and count exactly.
        Boundary next = restored.onTick(1234);
        assertEquals(expectedBlock, next.blockNumber, "block number resumes exactly");
        assertEquals(expectedCount, next.endTxIdx, "endTxIdx resumes from snapshotted count");

        // Dedup window also survived: a snapshotted id is still a duplicate.
        assertFalse(restored.firstSeen(id(3)), "snapshotted id must still dedup");
    }

    // --- helpers ------------------------------------------------------------

    /** A fixed, reproducible script of records and ticks; returns the egress. */
    private static List<Object> drive(CanonicalSealerState state) {
        List<Object> egress = new ArrayList<>();
        record(state, egress, 1, "a");
        record(state, egress, 2, "b");
        record(state, egress, 1, "a-dup"); // duplicate
        egress.add(state.onTick(1123));
        record(state, egress, 3, "c");
        record(state, egress, 2, "b-dup"); // duplicate
        egress.add(state.onTick(1500));
        record(state, egress, 4, "d");
        return egress;
    }

    /** A reproducible continuation script used to compare post-snapshot states. */
    private static List<Object> continuation(CanonicalSealerState state) {
        List<Object> egress = new ArrayList<>();
        record(state, egress, 3, "c-maybe-dup"); // dup if window still holds id3
        record(state, egress, 5, "e");
        egress.add(state.onTick(900));
        record(state, egress, 6, "f");
        egress.add(state.onTick(1200));
        return egress;
    }

    private static void record(CanonicalSealerState state, List<Object> egress, int idByte, String p) {
        state.onRecord(id(idByte), payload(p)).ifPresent(egress::add);
    }
}
