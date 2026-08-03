package io.kardamom.sealer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

    // ---- L1 origin / epoch handling -------------------------------------

    @Test
    void origin_record_closes_the_open_block_with_the_OLD_origin() {
        CanonicalSealerState state = new CanonicalSealerState(8);
        state.onRecord(id(1), payload("tx"));

        Optional<OriginAdvance> advance =
                state.onOriginRecord(id(2), 100L, 1L, payload("epoch-100"), 1_000L);

        assertTrue(advance.isPresent());
        Boundary forced = advance.get().forcedBoundary().orElseThrow();
        // The forced boundary closes a block belonging to the PREVIOUS epoch,
        // so it must NOT carry the incoming origin.
        assertEquals(0L, forced.l1Origin, "forced boundary keeps the old origin");
        assertEquals(1L, forced.endTxIdx, "closes the block holding the tx");
        // The epoch record itself lands AFTER the boundary: it leads the new block.
        assertEquals(1L, advance.get().relayed().index);
        assertEquals(100L, state.l1Origin());

        // Only from the next boundary on does the new origin appear.
        assertEquals(100L, state.onTick(2_000L).l1Origin);
    }

    @Test
    void origin_record_on_an_empty_block_forces_no_boundary() {
        CanonicalSealerState state = new CanonicalSealerState(8);

        Optional<OriginAdvance> advance =
                state.onOriginRecord(id(1), 100L, 1L, payload("epoch-100"), 1_000L);

        assertTrue(advance.isPresent());
        assertTrue(
                advance.get().forcedBoundary().isEmpty(),
                "an empty open block already lets the epoch lead — no empty block");
        assertEquals(0L, advance.get().relayed().index);
        assertEquals(100L, state.l1Origin());
    }

    @Test
    void back_to_back_epochs_emit_one_block_each_and_no_empty_blocks() {
        CanonicalSealerState state = new CanonicalSealerState(8);

        // A catch-up burst: three epochs with no L2 traffic in between.
        Optional<OriginAdvance> a = state.onOriginRecord(id(1), 100L, 1L, payload("e100"), 1_000L);
        Optional<OriginAdvance> b = state.onOriginRecord(id(2), 101L, 1L, payload("e101"), 1_000L);
        Optional<OriginAdvance> c = state.onOriginRecord(id(3), 102L, 1L, payload("e102"), 1_000L);

        assertTrue(a.orElseThrow().forcedBoundary().isEmpty(), "nothing open yet");
        // b and c each close the block their predecessor opened — which is
        // non-empty (it holds that epoch's record), so each yields exactly one.
        assertEquals(100L, b.orElseThrow().forcedBoundary().orElseThrow().l1Origin);
        assertEquals(101L, c.orElseThrow().forcedBoundary().orElseThrow().l1Origin);
        assertEquals(102L, state.l1Origin());
    }

    @Test
    void duplicate_epoch_neither_seals_a_block_nor_moves_the_origin() {
        CanonicalSealerState state = new CanonicalSealerState(8);
        state.onRecord(id(1), payload("tx"));
        state.onOriginRecord(id(2), 100L, 1L, payload("epoch-100"), 1_000L);
        long blockAfterFirst = state.blockNumber();
        long countAfterFirst = state.canonicalCount();

        // What M racing sequencers actually produce: the SAME epoch re-offered
        // with the SAME origin the state already adopted. Dedup must absorb
        // it — treating a non-advancing origin as a fault here would reject
        // every sequencer but the first.
        Optional<OriginAdvance> dup =
                state.onOriginRecord(id(2), 100L, 1L, payload("epoch-100"), 2_000L);

        assertTrue(dup.isEmpty(), "duplicate epoch is dropped");
        assertEquals(blockAfterFirst, state.blockNumber(), "no block sealed");
        assertEquals(countAfterFirst, state.canonicalCount(), "nothing counted");
        assertEquals(100L, state.l1Origin(), "origin unmoved");
    }

    @Test
    void non_advancing_origin_is_rejected() {
        CanonicalSealerState state = new CanonicalSealerState(8);
        state.onOriginRecord(id(1), 100L, 1L, payload("e100"), 1_000L);

        assertThrows(
                IllegalArgumentException.class,
                () -> state.onOriginRecord(id(2), 100L, 1L, payload("e100-again"), 1_000L),
                "same origin under a fresh id is a producer bug");
        assertThrows(
                IllegalArgumentException.class,
                () -> state.onOriginRecord(id(3), 99L, 1L, payload("e99"), 1_000L),
                "going backwards would break derivation");
        assertEquals(100L, state.l1Origin());
    }

    @Test
    void forced_boundary_keeps_timestamps_strictly_increasing_within_a_tick() {
        CanonicalSealerState state = new CanonicalSealerState(8);
        // Two boundaries inside ONE 250 ms window: the tick, then a forced one.
        Boundary tick = state.onTick(1_000L);
        state.onRecord(id(1), payload("tx"));
        Boundary forced =
                state.onOriginRecord(id(2), 100L, 1L, payload("e100"), 1_010L)
                        .orElseThrow()
                        .forcedBoundary()
                        .orElseThrow();

        assertEquals(1_000L, tick.l2Timestamp, "plain ticks stay floored");
        assertTrue(
                forced.l2Timestamp > tick.l2Timestamp,
                "two blocks must never share a timestamp: " + forced.l2Timestamp);
        // The next aligned tick is far enough ahead to resume plain flooring.
        assertEquals(1_250L, state.onTick(1_250L).l2Timestamp);
    }

    @Test
    void snapshot_round_trips_origin_state() {
        CanonicalSealerState state = new CanonicalSealerState(8);
        state.onRecord(id(1), payload("tx"));
        state.onOriginRecord(id(2), 100L, 1L, payload("e100"), 1_000L);
        state.onTick(1_500L);

        CanonicalSealerState restored = CanonicalSealerState.load(state.takeSnapshot(), 8);

        assertEquals(state.l1Origin(), restored.l1Origin());
        assertEquals(state.canonicalCount(), restored.canonicalCount());
        assertEquals(state.blockNumber(), restored.blockNumber());
        // The two must stay indistinguishable: same next boundary, and the
        // restored member must still refuse a stale origin.
        assertEquals(state.onTick(2_000L), restored.onTick(2_000L));
        assertThrows(
                IllegalArgumentException.class,
                () -> restored.onOriginRecord(id(9), 100L, 1L, payload("stale"), 2_000L));
    }

    @Test
    void older_snapshots_load_as_pre_origin_state() {
        // What an in-place upgrade finds on disk. v1 predates BOTH the
        // contiguity-guard sender map and the origin trio; v2 has the sender
        // map but no origin. Both mean origin 0 — exactly the state those
        // chains were in — so neither needs a migration pass.
        CanonicalSealerState pre = new CanonicalSealerState(8);
        pre.onRecord(id(1), sender(1), 0L, payload("tx"));
        pre.onTick(1_000L);
        long count = pre.canonicalCount();
        long block = pre.blockNumber();

        // v1: magic | version=1 | canonicalCount | blockNumber | idCount | ids
        java.nio.ByteBuffer v1 = java.nio.ByteBuffer
                .allocate(4 + 4 + 8 + 8 + 4 + CanonicalSealerState.CANONICAL_ID_LEN)
                .order(java.nio.ByteOrder.BIG_ENDIAN);
        v1.putInt(0x4B53_4541);
        v1.putInt(1);
        v1.putLong(count);
        v1.putLong(block);
        v1.putInt(1);
        v1.put(id(1));
        CanonicalSealerState fromV1 = CanonicalSealerState.load(v1.array(), 8);
        assertEquals(0L, fromV1.l1Origin());
        assertEquals(count, fromV1.canonicalCount());
        assertEquals(block, fromV1.blockNumber());

        // v2: the same, plus an (empty) sender map and no origin trio.
        java.nio.ByteBuffer v2 = java.nio.ByteBuffer
                .allocate(4 + 4 + 8 + 8 + 4 + CanonicalSealerState.CANONICAL_ID_LEN + 4)
                .order(java.nio.ByteOrder.BIG_ENDIAN);
        v2.putInt(0x4B53_4541);
        v2.putInt(2);
        v2.putLong(count);
        v2.putLong(block);
        v2.putInt(1);
        v2.put(id(1));
        v2.putInt(0); // senderCount
        CanonicalSealerState fromV2 = CanonicalSealerState.load(v2.array(), 8);
        assertEquals(0L, fromV2.l1Origin());
        assertEquals(count, fromV2.canonicalCount());
        assertEquals(block, fromV2.blockNumber());

        // Both adopt an origin normally from there.
        assertTrue(fromV1.onOriginRecord(id(2), 100L, 1L, payload("e100"), 1_500L).isPresent());
        assertTrue(fromV2.onOriginRecord(id(2), 100L, 1L, payload("e100"), 1_500L).isPresent());
    }

    @Test
    void epoch_claims_a_contiguous_slot_range() {
        CanonicalSealerState state = new CanonicalSealerState(8);
        state.onRecord(id(1), payload("tx"));

        // Marker + 3 deposits = 4 slots, relayed at the FIRST of them.
        Optional<OriginAdvance> advance =
                state.onOriginRecord(id(2), 100L, 4L, payload("e100"), 1_000L);

        assertEquals(1L, advance.orElseThrow().relayed().index);
        assertEquals(5L, state.canonicalCount(), "range is consumed, not just the marker");
        // The next ordinary record starts past the deposits — no slot is shared.
        assertEquals(5L, state.onRecord(id(3), payload("tx2")).orElseThrow().index);
        // ...and the boundary that closes the block agrees with the count, which
        // is exactly what the executor's alignment check compares against.
        assertEquals(6L, state.onTick(1_500L).endTxIdx);
    }

    @Test
    void zero_slot_record_is_rejected() {
        CanonicalSealerState state = new CanonicalSealerState(8);

        assertThrows(
                IllegalArgumentException.class,
                () -> state.onOriginRecord(id(1), 100L, 0L, payload("e100"), 1_000L),
                "a zero-width record would let the next record reuse its index");
        assertEquals(0L, state.l1Origin(), "rejected before any state moved");
        assertEquals(0L, state.canonicalCount());
    }

    /// The M-sequencer fan-in: every sequencer forwards every epoch, so all
    /// but the first offer of each is a duplicate carrying the origin already
    /// adopted. That is normal traffic, not a regression.
    @Test
    void racing_sequencers_reoffering_the_same_epoch_is_not_a_regression() {
        CanonicalSealerState state = new CanonicalSealerState(64);
        for (int epoch = 1; epoch <= 3; epoch++) {
            long origin = 100L + epoch;
            for (int sequencer = 0; sequencer < 3; sequencer++) {
                // Same epoch id from three sequencers, same declared origin.
                Optional<OriginAdvance> r =
                        state.onOriginRecord(id(epoch), origin, 1L, payload("e" + epoch), 1_000L);
                assertEquals(
                        sequencer == 0,
                        r.isPresent(),
                        "only the first offer of an epoch may be relayed");
            }
            assertEquals(origin, state.l1Origin());
        }
        assertEquals(3L, state.canonicalCount(), "three epochs ordered, not nine");
    }
}
