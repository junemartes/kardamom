package io.kardamom.sealer;

import static io.kardamom.sealer.SealerStateFixtures.id;
import static io.kardamom.sealer.SealerStateFixtures.payload;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kardamom.sealer.CanonicalSealerState.RemoteOriginOutcome;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Remote-origin (cross-chain message batch) unit tests for
 * {@link CanonicalSealerState}: per-peer adoption and independence, the lane
 * contiguity guard, the slot-count rule, the allowlist, per-peer anchor
 * monotonicity, duplicate absorption, the check-before-insert order, forced
 * boundaries, slot-range claiming, snapshot round-trip (v5) and the v4
 * upgrade path, and the standing guarantee that boundaries do NOT grow a
 * per-peer stamp. Mirrors {@link OriginRecordTest}, which covers the L1 half.
 * See {@code docs/specs/interop-outbox-messaging-spec.md} §7.
 */
class RemoteOriginRecordTest {

    /** Two peers, so every test can prove one never gates the other. */
    private static final long CHAIN_X = 8453L;
    private static final long CHAIN_Y = 10L;
    private static final Set<Long> ALLOW = Set.of(CHAIN_X, CHAIN_Y);

    private static CanonicalSealerState state(int capacity) {
        return new CanonicalSealerState(capacity, CanonicalSealerState.GENESIS_BLOCK_NUMBER, ALLOW);
    }

    /** A one-message batch at {@code seq} for {@code chain}, anchored at {@code anchor}. */
    private static RemoteOriginOutcome one(
            CanonicalSealerState s, byte[] id, long chain, long anchor, long seq, String tag) {
        return s.onRemoteOriginRecord(id, chain, anchor, 2L, seq, seq, payload(tag), 1_000L);
    }

    @Test
    void remote_record_closes_the_open_block_so_the_batch_leads() {
        CanonicalSealerState state = state(8);
        state.onRecord(id(1), payload("tx"));

        RemoteOriginOutcome out = one(state, id(2), CHAIN_X, 700L, 0L, "batch");

        assertTrue(out.advance.isPresent());
        Boundary forced = out.advance.get().forcedBoundary().orElseThrow();
        assertEquals(1L, forced.endTxIdx, "closes the block holding the tx");
        // The batch record itself lands AFTER the boundary: it leads the new
        // block, so its contiguous slot range cannot straddle two blocks.
        assertEquals(1L, out.advance.get().relayed().index);
        assertEquals(Optional.of(700L), state.remoteOriginOf(CHAIN_X));
        assertEquals(Optional.of(1L), state.remoteNextSeqOf(CHAIN_X), "cursor is one past lastSeq");
    }

    @Test
    void remote_record_on_an_empty_block_forces_no_boundary() {
        CanonicalSealerState state = state(8);

        RemoteOriginOutcome out = one(state, id(1), CHAIN_X, 700L, 0L, "batch");

        assertTrue(out.advance.isPresent());
        assertTrue(
                out.advance.get().forcedBoundary().isEmpty(),
                "an empty open block already lets the batch lead — no empty block");
        assertEquals(0L, out.advance.get().relayed().index);
    }

    @Test
    void duplicate_batch_is_dropped_and_moves_nothing() {
        CanonicalSealerState state = state(8);
        state.onRecord(id(1), payload("tx"));
        one(state, id(2), CHAIN_X, 700L, 0L, "batch");
        long blockAfterFirst = state.blockNumber();
        long countAfterFirst = state.canonicalCount();

        // What M racing watchers produce: the SAME batch re-offered at the
        // SAME position the state already adopted. Dedup absorbs it BEFORE
        // the lane guard runs, so it is a silent drop, never a reject.
        RemoteOriginOutcome dup = one(state, id(2), CHAIN_X, 700L, 0L, "batch");

        assertTrue(dup.advance.isEmpty(), "duplicate batch is dropped");
        assertFalse(dup.rejected, "a re-offer is not a lane regression");
        assertEquals(blockAfterFirst, state.blockNumber(), "no block sealed");
        assertEquals(countAfterFirst, state.canonicalCount(), "nothing counted");
        assertEquals(Optional.of(700L), state.remoteOriginOf(CHAIN_X), "anchor unmoved");
        assertEquals(Optional.of(1L), state.remoteNextSeqOf(CHAIN_X), "cursor unmoved");
    }

    @Test
    void batch_claims_a_contiguous_slot_range() {
        CanonicalSealerState state = state(8);
        state.onRecord(id(1), payload("tx"));

        // Marker + 3 messages (seqs 0..2) = 4 slots, relayed at the FIRST.
        RemoteOriginOutcome out =
                state.onRemoteOriginRecord(id(2), CHAIN_X, 700L, 4L, 0L, 2L, payload("batch"), 1_000L);

        assertEquals(1L, out.advance.orElseThrow().relayed().index);
        assertEquals(5L, state.canonicalCount(), "range is consumed, not just the marker");
        // The next ordinary record starts past the messages — no slot is shared.
        assertEquals(5L, state.onRecord(id(3), payload("tx2")).orElseThrow().index);
        assertEquals(6L, state.onTick(1_500L).endTxIdx);
        assertEquals(Optional.of(3L), state.remoteNextSeqOf(CHAIN_X));
    }

    /** The slot count must equal 2 + lastSeq - firstSeq. The sealer never trusts it. */
    @Test
    void slot_count_that_disagrees_with_the_seq_range_is_rejected() {
        CanonicalSealerState state = state(8);

        // Too few slots for three messages.
        RemoteOriginOutcome few =
                state.onRemoteOriginRecord(id(1), CHAIN_X, 700L, 3L, 0L, 2L, payload("b"), 1_000L);
        assertTrue(few.rejected);
        assertEquals(CanonicalSealerState.REMOTE_REJECT_SLOT_COUNT_MISMATCH, few.reason);
        // Too many slots for one message.
        RemoteOriginOutcome many =
                state.onRemoteOriginRecord(id(2), CHAIN_X, 700L, 3L, 0L, 0L, payload("b"), 1_000L);
        assertTrue(many.rejected);
        assertEquals(CanonicalSealerState.REMOTE_REJECT_SLOT_COUNT_MISMATCH, many.reason);
        // Zero slots.
        RemoteOriginOutcome zero =
                state.onRemoteOriginRecord(id(3), CHAIN_X, 700L, 0L, 0L, 0L, payload("b"), 1_000L);
        assertTrue(zero.rejected);
        assertEquals(CanonicalSealerState.REMOTE_REJECT_SLOT_COUNT_MISMATCH, zero.reason);

        assertEquals(0L, state.canonicalCount(), "nothing moved");
        assertTrue(state.remoteOriginOf(CHAIN_X).isEmpty(), "rejected before any state moved");
        assertEquals(0, state.dedupSize(), "a rejected id never enters the window");
    }

    @Test
    void a_reversed_seq_range_is_rejected() {
        CanonicalSealerState state = state(8);

        RemoteOriginOutcome out =
                state.onRemoteOriginRecord(id(1), CHAIN_X, 700L, 2L, 5L, 4L, payload("b"), 1_000L);

        assertTrue(out.rejected);
        assertEquals(CanonicalSealerState.REMOTE_REJECT_BAD_RANGE, out.reason);
        assertEquals(0, state.dedupSize());
    }

    /** The whole point of the lane cursor: a skipped or repeated seq never seals. */
    @Test
    void lane_contiguity_guard_rejects_a_skip_and_a_repeat() {
        CanonicalSealerState state = state(32);
        assertTrue(one(state, id(1), CHAIN_X, 700L, 0L, "x0").advance.isPresent());
        assertTrue(one(state, id(2), CHAIN_X, 701L, 1L, "x1").advance.isPresent());
        assertEquals(Optional.of(2L), state.remoteNextSeqOf(CHAIN_X));

        // Skip: seq 3 while the cursor is 2.
        RemoteOriginOutcome skip = one(state, id(3), CHAIN_X, 702L, 3L, "x3");
        assertTrue(skip.rejected);
        assertEquals(CanonicalSealerState.REMOTE_REJECT_SEQ_MISMATCH, skip.reason);
        assertEquals(2L, skip.expectedNextSeq, "the reject carries the lane cursor");

        // Repeat under a fresh id: seq 1 again with a new anchor.
        RemoteOriginOutcome repeat = one(state, id(4), CHAIN_X, 703L, 1L, "x1-again");
        assertTrue(repeat.rejected);
        assertEquals(CanonicalSealerState.REMOTE_REJECT_SEQ_MISMATCH, repeat.reason);
        assertEquals(2L, repeat.expectedNextSeq);

        // The lane is intact: seq 2 still seals, and the cursor moves to 3.
        assertTrue(one(state, id(5), CHAIN_X, 704L, 2L, "x2").advance.isPresent());
        assertEquals(Optional.of(3L), state.remoteNextSeqOf(CHAIN_X));
        assertEquals(Optional.of(704L), state.remoteOriginOf(CHAIN_X));
        assertEquals(3L, state.canonicalCount() / 2, "three two-slot batches ordered");
    }

    /**
     * Audit M6: a rejected id must not enter the dedup window. A racing copy
     * of the SAME bad record must be rejected the same way, not absorbed as
     * a silent "duplicate", and it must evict nothing.
     */
    @Test
    void a_rejected_id_never_enters_the_dedup_window() {
        CanonicalSealerState state = state(2);
        state.onRecord(id(1), payload("tx-a"));
        state.onRecord(id(2), payload("tx-b"));
        assertEquals(2, state.dedupSize(), "window full");
        one(state, id(3), CHAIN_X, 700L, 0L, "x0"); // evicts id(1), window = {2, 3}

        RemoteOriginOutcome first = one(state, id(4), CHAIN_X, 701L, 5L, "x5-skip");
        assertTrue(first.rejected);
        RemoteOriginOutcome racingCopy = one(state, id(4), CHAIN_X, 701L, 5L, "x5-skip");
        assertTrue(racingCopy.rejected, "the racing copy is rejected the same way");
        assertEquals(CanonicalSealerState.REMOTE_REJECT_SEQ_MISMATCH, racingCopy.reason);

        // The window still holds exactly the two legitimate ids.
        assertEquals(2, state.dedupSize());
        assertFalse(state.firstSeen(id(2)), "id 2 was not evicted by a rejected id");
        assertFalse(state.firstSeen(id(3)), "id 3 was not evicted by a rejected id");
    }

    /** Audit H3: an origin outside the allowlist never grows the peer map. */
    @Test
    void an_origin_outside_the_allowlist_is_rejected() {
        CanonicalSealerState state = state(8);

        RemoteOriginOutcome out = one(state, id(1), 999_999L, 700L, 0L, "stranger");

        assertTrue(out.rejected);
        assertEquals(CanonicalSealerState.REMOTE_REJECT_UNKNOWN_ORIGIN, out.reason);
        assertEquals(0, state.trackedRemoteOrigins(), "the map did not grow");
        assertEquals(0, state.dedupSize());
        assertEquals(0L, state.canonicalCount());
    }

    /** The default constructors carry an empty allowlist: interop is off. */
    @Test
    void an_empty_allowlist_disables_interop() {
        CanonicalSealerState state = new CanonicalSealerState(8);

        RemoteOriginOutcome out = one(state, id(1), CHAIN_X, 700L, 0L, "x0");

        assertTrue(out.rejected);
        assertEquals(CanonicalSealerState.REMOTE_REJECT_UNKNOWN_ORIGIN, out.reason);
        assertTrue(state.remoteOriginAllowlist().isEmpty());
    }

    /**
     * The property the whole per-peer map exists for: two peers interleaved,
     * each advancing on its own numbering. Chain Y's anchors here are FAR below
     * chain X's, which a single shared scalar would reject outright.
     */
    @Test
    void two_peers_advance_independently_when_interleaved() {
        CanonicalSealerState state = state(32);

        assertTrue(one(state, id(1), CHAIN_X, 700L, 0L, "x1").advance.isPresent());
        assertTrue(one(state, id(2), CHAIN_Y, 5L, 0L, "y1").advance.isPresent(),
                "a far lower anchor from another peer is not a regression");
        assertTrue(one(state, id(3), CHAIN_X, 701L, 1L, "x2").advance.isPresent());
        assertTrue(one(state, id(4), CHAIN_Y, 6L, 1L, "y2").advance.isPresent());

        assertEquals(Optional.of(701L), state.remoteOriginOf(CHAIN_X));
        assertEquals(Optional.of(6L), state.remoteOriginOf(CHAIN_Y));
        assertEquals(Optional.of(2L), state.remoteNextSeqOf(CHAIN_X));
        assertEquals(Optional.of(2L), state.remoteNextSeqOf(CHAIN_Y));
        assertEquals(2, state.trackedRemoteOrigins());
        assertEquals(8L, state.canonicalCount(), "all four two-slot batches ordered");
    }

    @Test
    void a_peers_non_advancing_anchor_is_rejected_without_touching_other_peers() {
        CanonicalSealerState state = state(32);
        one(state, id(1), CHAIN_X, 700L, 0L, "x1");
        one(state, id(2), CHAIN_Y, 5L, 0L, "y1");

        // The contiguous seq with a stale anchor: the second guard fires.
        RemoteOriginOutcome same = one(state, id(3), CHAIN_X, 700L, 1L, "x-again");
        assertTrue(same.rejected, "the same anchor under a fresh id is a producer bug");
        assertEquals(CanonicalSealerState.REMOTE_REJECT_ANCHOR_REGRESSED, same.reason);
        RemoteOriginOutcome back = one(state, id(4), CHAIN_X, 699L, 1L, "x-back");
        assertTrue(back.rejected, "going backwards would break destination-side ordering");
        assertEquals(CanonicalSealerState.REMOTE_REJECT_ANCHOR_REGRESSED, back.reason);

        // Chain Y is untouched by chain X's rejects — position AND liveness.
        assertEquals(Optional.of(5L), state.remoteOriginOf(CHAIN_Y));
        assertTrue(one(state, id(5), CHAIN_Y, 6L, 1L, "y2").advance.isPresent(),
                "one peer's regression must not stall another peer");
        assertEquals(Optional.of(700L), state.remoteOriginOf(CHAIN_X), "X's position unmoved");
        assertEquals(Optional.of(1L), state.remoteNextSeqOf(CHAIN_X), "X's cursor unmoved");
    }

    @Test
    void an_unknown_peer_seeds_at_whatever_position_arrives() {
        CanonicalSealerState state = state(8);

        // The sealer has no more access to a peer chain than it has to L1, so
        // the first position it can know is the first one ordered — here a
        // mid-lane resume at seq 40, anchor 0.
        assertTrue(one(state, id(1), CHAIN_X, 0L, 40L, "x40").advance.isPresent());
        assertEquals(Optional.of(0L), state.remoteOriginOf(CHAIN_X));
        assertEquals(Optional.of(41L), state.remoteNextSeqOf(CHAIN_X));
        RemoteOriginOutcome again = one(state, id(2), CHAIN_X, 0L, 41L, "x41-stale-anchor");
        assertTrue(again.rejected, "every position after the seed must strictly advance");
        assertEquals(CanonicalSealerState.REMOTE_REJECT_ANCHOR_REGRESSED, again.reason);
    }

    /** Seq and anchor values above {@code Long.MAX_VALUE} compare as u64. */
    @Test
    void comparisons_are_unsigned() {
        CanonicalSealerState state = state(8);
        long big = 0xFFFF_FFFF_FFFF_FFF0L; // negative as a signed long
        assertTrue(state.onRemoteOriginRecord(
                id(1), CHAIN_X, big, 2L, big, big, payload("x"), 1_000L).advance.isPresent());
        // big + 1 advances the anchor and continues the lane under u64 rules.
        assertTrue(state.onRemoteOriginRecord(
                id(2), CHAIN_X, big + 1, 2L, big + 1, big + 1, payload("x"), 1_000L)
                .advance.isPresent());
        // A small (signed-positive) anchor is a regression under u64 rules.
        RemoteOriginOutcome small = state.onRemoteOriginRecord(
                id(3), CHAIN_X, 5L, 2L, big + 2, big + 2, payload("x"), 1_000L);
        assertTrue(small.rejected);
        assertEquals(CanonicalSealerState.REMOTE_REJECT_ANCHOR_REGRESSED, small.reason);
    }

    /**
     * REGRESSION GUARD. Boundaries must never grow a per-peer stamp: one
     * {@code l1Origin} rides every boundary because there is exactly one L1,
     * whereas per-peer stamps would grow EVERY boundary by the peer count for
     * data already recoverable from the relayed markers. Asserted structurally
     * (the exact field set) as well as behaviourally, because the tempting
     * "just add a field" change compiles fine.
     */
    @Test
    void boundaries_carry_only_the_l1_origin_never_a_per_peer_stamp() {
        // Sorted, because getDeclaredFields() makes no ordering promise.
        List<String> fields = java.util.Arrays.stream(Boundary.class.getDeclaredFields())
                .filter(f -> !f.isSynthetic())
                .map(java.lang.reflect.Field::getName)
                .sorted()
                .toList();
        assertEquals(
                List.of("blockNumber", "endTxIdx", "l1Origin", "l2Timestamp"),
                fields,
                "Boundary must keep exactly these fields — no per-peer remote origin");

        CanonicalSealerState state = state(32);
        state.onOriginRecord(id(1), 100L, 1L, payload("e100"), 1_000L);
        one(state, id(2), CHAIN_X, 700L, 0L, "x1");
        one(state, id(3), CHAIN_Y, 5L, 0L, "y1");

        // Remote batches move no boundary field but endTxIdx: the L1 origin is
        // untouched, and there is nowhere for a peer position to appear.
        Boundary tick = state.onTick(2_000L);
        assertEquals(100L, tick.l1Origin, "remote batches never move the boundary's origin");
        assertEquals(new Boundary(tick.blockNumber, 5L, tick.l2Timestamp, 100L), tick);
    }

    @Test
    void forced_boundary_keeps_timestamps_strictly_increasing_within_a_tick() {
        CanonicalSealerState state = state(8);
        Boundary tick = state.onTick(1_000L);
        state.onRecord(id(1), payload("tx"));
        Boundary forced = state
                .onRemoteOriginRecord(id(2), CHAIN_X, 700L, 2L, 0L, 0L, payload("x1"), 1_010L)
                .advance
                .orElseThrow()
                .forcedBoundary()
                .orElseThrow();

        assertTrue(
                forced.l2Timestamp > tick.l2Timestamp,
                "two blocks must never share a timestamp: " + forced.l2Timestamp);
    }

    @Test
    void snapshot_round_trips_the_per_peer_map_with_cursors() {
        CanonicalSealerState state = state(8);
        state.onRemoteOriginRecord(id(1), CHAIN_X, 700L, 3L, 0L, 1L, payload("x1"), 1_000L);
        one(state, id(2), CHAIN_Y, 5L, 7L, "y1");

        CanonicalSealerState restored = CanonicalSealerState.load(state.takeSnapshot(), 8, ALLOW);

        assertEquals(state.canonicalCount(), restored.canonicalCount());
        assertEquals(Optional.of(700L), restored.remoteOriginOf(CHAIN_X));
        assertEquals(Optional.of(2L), restored.remoteNextSeqOf(CHAIN_X));
        assertEquals(Optional.of(5L), restored.remoteOriginOf(CHAIN_Y));
        assertEquals(Optional.of(8L), restored.remoteNextSeqOf(CHAIN_Y));
        // The two must stay indistinguishable, or a restored member accepts a
        // record its peers reject — state-machine divergence.
        RemoteOriginOutcome skip = one(restored, id(9), CHAIN_X, 701L, 3L, "skip");
        assertTrue(skip.rejected);
        assertEquals(CanonicalSealerState.REMOTE_REJECT_SEQ_MISMATCH, skip.reason);
        assertEquals(2L, skip.expectedNextSeq);
        assertTrue(one(restored, id(8), CHAIN_X, 701L, 2L, "x2").advance.isPresent());
    }

    /**
     * The in-place upgrade path: a version-4 snapshot has the per-peer anchor
     * but no lane cursor. Each peer keeps its anchor guard and seeds its
     * cursor from its next record (trust-on-first-sight). The v4 bytes are
     * built explicitly, because a v5 entry is 9 bytes wider than a v4 one.
     */
    @Test
    void a_v4_snapshot_loads_with_unknown_cursors() {
        CanonicalSealerState pre = state(8);
        pre.onRecord(id(1), payload("tx"));
        pre.onOriginRecord(id(2), 100L, 1L, payload("e100"), 1_000L);
        byte[] v5 = pre.takeSnapshot(); // no peers yet: remoteCount = 0
        // Append one v4 peer entry (origin 8 + anchor 8) and re-tag as v4.
        ByteBuffer v4 = ByteBuffer.allocate(v5.length + 16).order(ByteOrder.BIG_ENDIAN);
        v4.put(v5, 0, v5.length - Integer.BYTES); // everything before remoteCount
        v4.putInt(1);
        v4.putLong(CHAIN_X);
        v4.putLong(700L);
        v4.putInt(4, 4);

        CanonicalSealerState fromV4 = CanonicalSealerState.load(v4.array(), 8, ALLOW);

        assertEquals(1, fromV4.trackedRemoteOrigins());
        assertEquals(Optional.of(700L), fromV4.remoteOriginOf(CHAIN_X), "the anchor still parses");
        assertTrue(fromV4.remoteNextSeqOf(CHAIN_X).isEmpty(), "the cursor is unknown");
        assertEquals(100L, fromV4.l1Origin(), "the v3 origin trio still parses");
        assertEquals(pre.canonicalCount(), fromV4.canonicalCount());
        // The anchor guard still holds.
        RemoteOriginOutcome stale = one(fromV4, id(3), CHAIN_X, 700L, 40L, "stale");
        assertTrue(stale.rejected);
        assertEquals(CanonicalSealerState.REMOTE_REJECT_ANCHOR_REGRESSED, stale.reason);
        // The cursor seeds from the first advancing record, then guards.
        assertTrue(one(fromV4, id(4), CHAIN_X, 701L, 40L, "x40").advance.isPresent());
        assertEquals(Optional.of(41L), fromV4.remoteNextSeqOf(CHAIN_X));
        assertTrue(one(fromV4, id(5), CHAIN_X, 702L, 43L, "x43").rejected);
        // A v4-loaded state re-snapshots as v5 and round-trips.
        CanonicalSealerState again = CanonicalSealerState.load(fromV4.takeSnapshot(), 8, ALLOW);
        assertEquals(Optional.of(41L), again.remoteNextSeqOf(CHAIN_X));
    }

    @Test
    void pre_interop_snapshots_load_with_an_empty_peer_map() {
        // What an in-place upgrade finds on disk: a v3 snapshot has the origin
        // trio but no peer map. Every peer re-seeds on its next batch —
        // trust-on-first-sight, exactly as a fresh state would.
        CanonicalSealerState pre = state(8);
        pre.onRecord(id(1), payload("tx"));
        pre.onOriginRecord(id(2), 100L, 1L, payload("e100"), 1_000L);
        byte[] v5 = pre.takeSnapshot();
        // Re-tag as v3 and drop the 4-byte peer-map count the tail added.
        byte[] v3 = java.util.Arrays.copyOf(v5, v5.length - Integer.BYTES);
        ByteBuffer.wrap(v3).order(ByteOrder.BIG_ENDIAN).putInt(4, 3);

        CanonicalSealerState fromV3 = CanonicalSealerState.load(v3, 8, ALLOW);

        assertEquals(0, fromV3.trackedRemoteOrigins());
        assertEquals(100L, fromV3.l1Origin(), "the v3 origin trio still parses");
        assertEquals(pre.canonicalCount(), fromV3.canonicalCount());
        assertTrue(one(fromV3, id(3), CHAIN_X, 700L, 0L, "x1").advance.isPresent(),
                "peers seed normally from there");
    }
}
