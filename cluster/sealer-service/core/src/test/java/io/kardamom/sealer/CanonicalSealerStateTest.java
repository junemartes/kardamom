package io.kardamom.sealer;

import static io.kardamom.sealer.SealerStateFixtures.id;
import static io.kardamom.sealer.SealerStateFixtures.payload;
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
 * Deterministic unit tests for {@link CanonicalSealerState}.
 * The tests check dedup, counting, boundaries, determinism, and snapshot
 * round trips.
 * The tests use only JUnit 5, not Aeron. This keeps the canonical logic
 * testable even when the cluster transport does not build.
 * {@link ContiguityGuardTest} covers the contiguity guard.
 * {@link OriginRecordTest} covers L1-origin and epoch handling.
 */
class CanonicalSealerStateTest {

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
        // Capacity is 2. This matches the Rust DedupWindow::first_seen behavior.
        CanonicalSealerState state = new CanonicalSealerState(2);

        assertTrue(state.firstSeen(id(1)));
        assertFalse(state.firstSeen(id(1)), "second sighting is a duplicate");
        assertTrue(state.firstSeen(id(2)));
        // The window holds [1, 2]. Adding 3 evicts 1, the oldest id.
        assertTrue(state.firstSeen(id(3)));
        assertFalse(state.firstSeen(id(2)), "2 still inside the window");
        assertFalse(state.firstSeen(id(3)), "3 still inside the window");
        // Id 1 was evicted. It is fresh again.
        assertTrue(state.firstSeen(id(1)), "evicted id is fresh again");
        assertEquals(2, state.dedupSize(), "window stays at capacity");
    }

    @Test
    void count_matches_deduped_records() {
        // 7 records include 3 duplicates of earlier ids, so 4 records are unique.
        CanonicalSealerState state = new CanonicalSealerState(1024);
        int[] ids = {1, 2, 1, 3, 2, 4, 3}; // duplicates at position 2 (id 1), 4 (id 2), 6 (id 3)
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
        // The tick counts 3 unique records.
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
        // 5 ticks starting at genesis block 1 stamp blocks 1 to 5. The next block is 6.
        assertEquals(6L, state.blockNumber());
    }

    @Test
    void l2_timestamp_floored_to_interval() {
        CanonicalSealerState state = new CanonicalSealerState(64);
        // 1123 / 250 = 4, so the floored value is 1000.
        assertEquals(1000L, state.onTick(1123).l2Timestamp);
        // 1250 is exactly on an interval boundary, so the floored value is 1250.
        assertEquals(1250L, state.onTick(1250).l2Timestamp);
    }

    @Test
    void payload_relayed_opaque() {
        CanonicalSealerState state = new CanonicalSealerState(64);
        byte[] input = new byte[] {0, 1, 2, (byte) 0xFF, 0x7F, (byte) 0x80, 42, -7};
        Relayed relayed = state.onRecord(id(9), input).orElseThrow();
        assertArrayEquals(input, relayed.payload, "payload must be byte-identical");
        // The state does not wrap or parse the payload. It relays the same reference.
        assertSame(input, relayed.payload);
    }

    @Test
    void determinism_same_inputs_same_egress() {
        CanonicalSealerState a = new CanonicalSealerState(16, 1);
        CanonicalSealerState b = new CanonicalSealerState(16, 1);

        // This is a fixed sequence of records, including duplicates, and ticks.
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

        // Identical continuation inputs produce identical outputs from both states.
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
        original.onTick(750); // stamps block 1 with endTxIdx 2; block number becomes 2
        original.onRecord(id(3), payload("c"));
        original.onTick(1000); // stamps block 2 with endTxIdx 3; block number becomes 3

        long expectedCount = original.canonicalCount();
        long expectedBlock = original.blockNumber();

        CanonicalSealerState restored = CanonicalSealerState.load(original.takeSnapshot(), 8);
        // The next tick on the restored state resumes block and count exactly.
        Boundary next = restored.onTick(1234);
        assertEquals(expectedBlock, next.blockNumber, "block number resumes exactly");
        assertEquals(expectedCount, next.endTxIdx, "endTxIdx resumes from snapshotted count");

        // The dedup window also survives. A snapshotted id is still a duplicate.
        assertFalse(restored.firstSeen(id(3)), "snapshotted id must still dedup");
    }

    @Test
    void load_rejects_id_count_above_capacity() {
        // The snapshot was taken with a window of 8 ids.
        CanonicalSealerState original = new CanonicalSealerState(8, 1);
        for (int i = 1; i <= 8; i++) {
            original.onRecord(id(i), payload("p" + i));
        }
        byte[] snapshot = original.takeSnapshot();
        // The state must not silently load into a smaller configured window.
        // Otherwise, dedup behavior would diverge from a fresh state with the same config.
        IllegalArgumentException e = org.junit.jupiter.api.Assertions.assertThrows(
                IllegalArgumentException.class, () -> CanonicalSealerState.load(snapshot, 4));
        assertTrue(e.getMessage().contains("idCount"), "message names the field: " + e.getMessage());
        // The same snapshot loads correctly at or above the original capacity.
        assertEquals(8, CanonicalSealerState.load(snapshot, 8).dedupSize());
        assertEquals(8, CanonicalSealerState.load(snapshot, 16).dedupSize());
    }

    @Test
    void load_rejects_truncated_snapshot() {
        CanonicalSealerState original = new CanonicalSealerState(8, 1);
        for (int i = 1; i <= 4; i++) {
            original.onRecord(id(i), payload("p" + i));
        }
        byte[] snapshot = original.takeSnapshot();
        // Remove half of the id section to truncate the snapshot.
        // The load must fail with a clear error message, not a raw BufferUnderflowException.
        byte[] truncated = java.util.Arrays.copyOf(snapshot, snapshot.length - 2 * CanonicalSealerState.CANONICAL_ID_LEN - 7);
        IllegalArgumentException e = org.junit.jupiter.api.Assertions.assertThrows(
                IllegalArgumentException.class, () -> CanonicalSealerState.load(truncated, 8));
        assertTrue(e.getMessage().contains("truncated"), "message says truncated: " + e.getMessage());
    }

    // --- helpers ------------------------------------------------------------

    /** Run a fixed, repeatable script of records and ticks. Return the egress. */
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

    /** Run a repeatable continuation script to compare states after a snapshot. */
    private static List<Object> continuation(CanonicalSealerState state) {
        List<Object> egress = new ArrayList<>();
        record(state, egress, 3, "c-maybe-dup"); // duplicate only if window still holds id 3
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
