package io.kardamom.sealer;

import static io.kardamom.sealer.SealerStateFixtures.id;
import static io.kardamom.sealer.SealerStateFixtures.payload;
import static io.kardamom.sealer.SealerStateFixtures.sender;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * L1-origin / epoch-handling unit tests for {@link CanonicalSealerState}:
 * origin adoption, forced boundaries, slot-range claiming, duplicate-epoch
 * absorption, origin monotonicity, and snapshot compatibility across the
 * pre-origin versions. Split out of {@link CanonicalSealerStateTest}.
 */
class OriginRecordTest {

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
