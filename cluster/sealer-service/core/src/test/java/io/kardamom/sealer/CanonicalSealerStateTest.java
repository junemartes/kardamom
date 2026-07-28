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

    @Test
    void load_rejects_id_count_above_capacity() {
        // Snapshot taken with a window of 8 ids…
        CanonicalSealerState original = new CanonicalSealerState(8, 1);
        for (int i = 1; i <= 8; i++) {
            original.onRecord(id(i), payload("p" + i));
        }
        byte[] snapshot = original.takeSnapshot();
        // …must NOT silently load into a smaller configured window: dedup
        // behaviour would diverge from a fresh state with the same config.
        IllegalArgumentException e = org.junit.jupiter.api.Assertions.assertThrows(
                IllegalArgumentException.class, () -> CanonicalSealerState.load(snapshot, 4));
        assertTrue(e.getMessage().contains("idCount"), "message names the field: " + e.getMessage());
        // The same snapshot loads fine at (or above) the original capacity.
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
        // Chop off half of the id section (the F12.1 truncated-fragment shape):
        // must fail with a DESCRIPTIVE error, not a raw BufferUnderflowException.
        byte[] truncated = java.util.Arrays.copyOf(snapshot, snapshot.length - 2 * CanonicalSealerState.CANONICAL_ID_LEN - 7);
        IllegalArgumentException e = org.junit.jupiter.api.Assertions.assertThrows(
                IllegalArgumentException.class, () -> CanonicalSealerState.load(truncated, 8));
        assertTrue(e.getMessage().contains("truncated"), "message says truncated: " + e.getMessage());
    }

    // --- contiguity guard (#85 fix B) ---------------------------------------

    /** Build a distinct 20-byte sender whose every byte is {@code b}. */
    private static byte[] sender(int b) {
        byte[] out = new byte[CanonicalSealerState.SENDER_LEN];
        java.util.Arrays.fill(out, (byte) b);
        return out;
    }

    @Test
    void guard_unknown_sender_seeds_at_any_nonce() {
        CanonicalSealerState state = new CanonicalSealerState(8);
        CanonicalSealerState.RecordOutcome first = state.onRecord(id(1), sender(1), 41, payload("a"));
        assertTrue(first.relayed.isPresent(), "unknown sender seeds at any nonce");
        assertEquals(Optional.of(42L), state.expectedNonceOf(sender(1)));
    }

    @Test
    void guard_rejects_known_sender_gap_without_state_change() {
        CanonicalSealerState state = new CanonicalSealerState(8);
        state.onRecord(id(1), sender(1), 0, payload("a"));
        // Nonce 2 while 1 is expected: reject, and NOTHING changes — no count,
        // no dedup entry, no expected-nonce movement.
        CanonicalSealerState.RecordOutcome gap = state.onRecord(id(2), sender(1), 2, payload("b"));
        assertTrue(gap.rejected);
        assertEquals(1L, gap.expectedNonce);
        assertFalse(gap.relayed.isPresent());
        assertEquals(1L, state.canonicalCount(), "reject must not count");
        assertEquals(Optional.of(1L), state.expectedNonceOf(sender(1)), "reject must not advance");
        // The gap fills, then the SAME rejected id republishes and is fresh
        // (a reject leaves no dedup entry).
        assertTrue(state.onRecord(id(3), sender(1), 1, payload("gap")).relayed.isPresent());
        assertTrue(state.onRecord(id(2), sender(1), 2, payload("b")).relayed.isPresent());
        assertEquals(3L, state.canonicalCount());
    }

    @Test
    void guard_dedup_absorbs_republished_copies_before_the_nonce_check() {
        // #114 interplay: the sequencer republishes unconfirmed refs; copies
        // that DID commit re-arrive with a by-then stale nonce and MUST be
        // absorbed as duplicates, never contiguity-rejected.
        CanonicalSealerState state = new CanonicalSealerState(8);
        state.onRecord(id(1), sender(1), 0, payload("a"));
        state.onRecord(id(2), sender(1), 1, payload("b"));
        CanonicalSealerState.RecordOutcome copy = state.onRecord(id(1), sender(1), 0, payload("a"));
        assertFalse(copy.rejected, "committed copy is a duplicate, not a gap");
        assertFalse(copy.relayed.isPresent());
        assertEquals(2L, state.canonicalCount());
    }

    @Test
    void guard_zero_sender_is_exempt() {
        CanonicalSealerState state = new CanonicalSealerState(8);
        assertTrue(state.onRecord(id(1), sender(0), 0, payload("d1")).relayed.isPresent());
        assertTrue(state.onRecord(id(2), sender(0), 0, payload("d2")).relayed.isPresent());
        assertTrue(state.onRecord(id(3), sender(0), 7, payload("d3")).relayed.isPresent());
        assertEquals(0, state.trackedSenders(), "zero sender is never tracked");
    }

    @Test
    void guard_map_is_lru_bounded_and_evicted_sender_reseeds() {
        // Capacity 2 (shared with the dedup window): tracking a third sender
        // evicts the LEAST RECENTLY USED one, which then re-seeds like a new
        // sender — honest degradation, never a false reject.
        CanonicalSealerState state = new CanonicalSealerState(2);
        state.onRecord(id(1), sender(1), 10, payload("a"));
        state.onRecord(id(2), sender(2), 20, payload("b"));
        state.onRecord(id(3), sender(1), 11, payload("a2")); // touches sender1
        state.onRecord(id(4), sender(3), 30, payload("c")); // evicts sender2 (LRU)
        assertEquals(2, state.trackedSenders());
        assertEquals(Optional.empty(), state.expectedNonceOf(sender(2)), "sender2 evicted");
        // Evicted sender2 reappears at an arbitrary nonce: seeds, not rejected.
        CanonicalSealerState.RecordOutcome reseed = state.onRecord(id(5), sender(2), 99, payload("b2"));
        assertTrue(reseed.relayed.isPresent(), "evicted sender re-seeds");
    }

    @Test
    void guard_map_survives_snapshot_roundtrip() {
        CanonicalSealerState original = new CanonicalSealerState(8, 1);
        original.onRecord(id(1), sender(1), 5, payload("a"));
        original.onRecord(id(2), sender(2), 0, payload("b"));

        CanonicalSealerState restored = CanonicalSealerState.load(original.takeSnapshot(), 8);
        assertEquals(2, restored.trackedSenders());
        assertEquals(Optional.of(6L), restored.expectedNonceOf(sender(1)));
        // The restored guard still rejects a gap…
        CanonicalSealerState.RecordOutcome gap = restored.onRecord(id(3), sender(1), 9, payload("x"));
        assertTrue(gap.rejected, "restored guard must keep rejecting gaps");
        assertEquals(6L, gap.expectedNonce);
        // …and still accepts the contiguous next nonce.
        assertTrue(restored.onRecord(id(3), sender(1), 6, payload("x")).relayed.isPresent());
    }

    @Test
    void guard_v1_snapshot_loads_with_empty_map() {
        // A pre-guard (v1) snapshot restores an EMPTY guard map: every sender
        // re-seeds trust-on-first-sight. Synthesize a v1 snapshot by patching
        // the version field and dropping the sender section.
        CanonicalSealerState original = new CanonicalSealerState(8, 1);
        original.onRecord(id(1), sender(1), 5, payload("a"));
        byte[] v2 = original.takeSnapshot();
        int senderSection = 4 + 1 * (CanonicalSealerState.SENDER_LEN + 8);
        byte[] v1 = java.util.Arrays.copyOf(v2, v2.length - senderSection);
        v1[7] = 1; // version int (big-endian) at bytes 4..8

        CanonicalSealerState restored = CanonicalSealerState.load(v1, 8);
        assertEquals(0, restored.trackedSenders(), "v1 snapshot has no guard map");
        assertFalse(restored.firstSeen(id(1)), "dedup window still restored");
        assertTrue(restored.onRecord(id(2), sender(1), 99, payload("x")).relayed.isPresent(),
                "post-v1-restore senders re-seed at any nonce");
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
