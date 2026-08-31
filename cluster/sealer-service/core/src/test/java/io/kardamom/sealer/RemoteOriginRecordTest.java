package io.kardamom.sealer;

import static io.kardamom.sealer.SealerStateFixtures.id;
import static io.kardamom.sealer.SealerStateFixtures.payload;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Remote-origin (cross-chain message batch) unit tests for
 * {@link CanonicalSealerState}: per-peer adoption and independence, per-peer
 * monotonicity, duplicate absorption, forced boundaries, slot-range claiming,
 * snapshot round-trip, and the standing guarantee that boundaries do NOT grow a
 * per-peer stamp. Mirrors {@link OriginRecordTest}, which covers the L1 half.
 * See {@code docs/specs/interop-outbox-messaging-spec.md} §7.
 */
class RemoteOriginRecordTest {

    /** Two peers, so every test can prove one never gates the other. */
    private static final long CHAIN_X = 8453L;
    private static final long CHAIN_Y = 10L;

    @Test
    void remote_record_closes_the_open_block_so_the_batch_leads() {
        CanonicalSealerState state = new CanonicalSealerState(8);
        state.onRecord(id(1), payload("tx"));

        Optional<RemoteOriginAdvance> advance =
                state.onRemoteOriginRecord(id(2), CHAIN_X, 700L, 1L, payload("batch"), 1_000L);

        assertTrue(advance.isPresent());
        Boundary forced = advance.get().forcedBoundary().orElseThrow();
        assertEquals(1L, forced.endTxIdx, "closes the block holding the tx");
        // The batch record itself lands AFTER the boundary: it leads the new
        // block, so its contiguous slot range cannot straddle two blocks.
        assertEquals(1L, advance.get().relayed().index);
        assertEquals(Optional.of(700L), state.remoteOriginOf(CHAIN_X));
    }

    @Test
    void remote_record_on_an_empty_block_forces_no_boundary() {
        CanonicalSealerState state = new CanonicalSealerState(8);

        Optional<RemoteOriginAdvance> advance =
                state.onRemoteOriginRecord(id(1), CHAIN_X, 700L, 1L, payload("batch"), 1_000L);

        assertTrue(advance.isPresent());
        assertTrue(
                advance.get().forcedBoundary().isEmpty(),
                "an empty open block already lets the batch lead — no empty block");
        assertEquals(0L, advance.get().relayed().index);
    }

    @Test
    void duplicate_batch_is_dropped_and_moves_nothing() {
        CanonicalSealerState state = new CanonicalSealerState(8);
        state.onRecord(id(1), payload("tx"));
        state.onRemoteOriginRecord(id(2), CHAIN_X, 700L, 1L, payload("batch"), 1_000L);
        long blockAfterFirst = state.blockNumber();
        long countAfterFirst = state.canonicalCount();

        // What M racing watchers actually produce: the SAME batch re-offered at
        // the SAME anchor the state already adopted. The canonical id already
        // mixes the origin chain id in, so one dedup window absorbs it —
        // treating the non-advancing anchor as a fault here would reject every
        // watcher but the first.
        Optional<RemoteOriginAdvance> dup =
                state.onRemoteOriginRecord(id(2), CHAIN_X, 700L, 1L, payload("batch"), 2_000L);

        assertTrue(dup.isEmpty(), "duplicate batch is dropped");
        assertEquals(blockAfterFirst, state.blockNumber(), "no block sealed");
        assertEquals(countAfterFirst, state.canonicalCount(), "nothing counted");
        assertEquals(Optional.of(700L), state.remoteOriginOf(CHAIN_X), "anchor unmoved");
    }

    @Test
    void batch_claims_a_contiguous_slot_range() {
        CanonicalSealerState state = new CanonicalSealerState(8);
        state.onRecord(id(1), payload("tx"));

        // Marker + 3 messages = 4 slots, relayed at the FIRST of them.
        Optional<RemoteOriginAdvance> advance =
                state.onRemoteOriginRecord(id(2), CHAIN_X, 700L, 4L, payload("batch"), 1_000L);

        assertEquals(1L, advance.orElseThrow().relayed().index);
        assertEquals(5L, state.canonicalCount(), "range is consumed, not just the marker");
        // The next ordinary record starts past the messages — no slot is shared.
        assertEquals(5L, state.onRecord(id(3), payload("tx2")).orElseThrow().index);
        assertEquals(6L, state.onTick(1_500L).endTxIdx);
    }

    @Test
    void zero_slot_batch_is_rejected() {
        CanonicalSealerState state = new CanonicalSealerState(8);

        assertThrows(
                IllegalArgumentException.class,
                () -> state.onRemoteOriginRecord(id(1), CHAIN_X, 700L, 0L, payload("batch"), 1_000L),
                "a zero-width record would let the next record reuse its index");
        assertEquals(0L, state.canonicalCount());
        assertTrue(state.remoteOriginOf(CHAIN_X).isEmpty(), "rejected before any state moved");
    }

    /**
     * The property the whole per-peer map exists for: two peers interleaved,
     * each advancing on its own numbering. Chain Y's anchors here are FAR below
     * chain X's, which a single shared scalar would reject outright.
     */
    @Test
    void two_peers_advance_independently_when_interleaved() {
        CanonicalSealerState state = new CanonicalSealerState(32);

        assertTrue(state.onRemoteOriginRecord(id(1), CHAIN_X, 700L, 1L, payload("x1"), 1_000L)
                .isPresent());
        assertTrue(state.onRemoteOriginRecord(id(2), CHAIN_Y, 5L, 1L, payload("y1"), 1_000L)
                .isPresent(), "a far lower anchor from another peer is not a regression");
        assertTrue(state.onRemoteOriginRecord(id(3), CHAIN_X, 701L, 1L, payload("x2"), 1_000L)
                .isPresent());
        assertTrue(state.onRemoteOriginRecord(id(4), CHAIN_Y, 6L, 1L, payload("y2"), 1_000L)
                .isPresent());

        assertEquals(Optional.of(701L), state.remoteOriginOf(CHAIN_X));
        assertEquals(Optional.of(6L), state.remoteOriginOf(CHAIN_Y));
        assertEquals(2, state.trackedRemoteOrigins());
        assertEquals(4L, state.canonicalCount(), "all four batches ordered");
    }

    @Test
    void a_peers_non_advancing_anchor_is_rejected_without_touching_other_peers() {
        CanonicalSealerState state = new CanonicalSealerState(32);
        state.onRemoteOriginRecord(id(1), CHAIN_X, 700L, 1L, payload("x1"), 1_000L);
        state.onRemoteOriginRecord(id(2), CHAIN_Y, 5L, 1L, payload("y1"), 1_000L);

        assertThrows(
                IllegalArgumentException.class,
                () -> state.onRemoteOriginRecord(id(3), CHAIN_X, 700L, 1L, payload("x-again"), 1_000L),
                "the same anchor under a fresh id is a producer bug");
        assertThrows(
                IllegalArgumentException.class,
                () -> state.onRemoteOriginRecord(id(4), CHAIN_X, 699L, 1L, payload("x-back"), 1_000L),
                "going backwards would break destination-side ordering");

        // Chain Y is untouched by chain X's rejects — position AND liveness.
        assertEquals(Optional.of(5L), state.remoteOriginOf(CHAIN_Y));
        assertTrue(state.onRemoteOriginRecord(id(5), CHAIN_Y, 6L, 1L, payload("y2"), 1_000L)
                .isPresent(), "one peer's regression must not stall another peer");
        assertEquals(Optional.of(700L), state.remoteOriginOf(CHAIN_X), "X's position unmoved");
    }

    @Test
    void an_unknown_peer_seeds_at_whatever_anchor_arrives() {
        CanonicalSealerState state = new CanonicalSealerState(8);

        // The sealer has no more access to a peer chain than it has to L1, so
        // the first position it can know is the first one ordered — including 0.
        assertTrue(state.onRemoteOriginRecord(id(1), CHAIN_X, 0L, 1L, payload("x0"), 1_000L)
                .isPresent());
        assertEquals(Optional.of(0L), state.remoteOriginOf(CHAIN_X));
        assertThrows(
                IllegalArgumentException.class,
                () -> state.onRemoteOriginRecord(id(2), CHAIN_X, 0L, 1L, payload("x0-again"), 1_000L),
                "every position after the seed must strictly advance");
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

        CanonicalSealerState state = new CanonicalSealerState(32);
        state.onOriginRecord(id(1), 100L, 1L, payload("e100"), 1_000L);
        state.onRemoteOriginRecord(id(2), CHAIN_X, 700L, 1L, payload("x1"), 1_000L);
        state.onRemoteOriginRecord(id(3), CHAIN_Y, 5L, 1L, payload("y1"), 1_000L);

        // Remote batches move no boundary field but endTxIdx: the L1 origin is
        // untouched, and there is nowhere for a peer position to appear.
        Boundary tick = state.onTick(2_000L);
        assertEquals(100L, tick.l1Origin, "remote batches never move the boundary's origin");
        assertEquals(new Boundary(tick.blockNumber, 3L, tick.l2Timestamp, 100L), tick);
    }

    @Test
    void forced_boundary_keeps_timestamps_strictly_increasing_within_a_tick() {
        CanonicalSealerState state = new CanonicalSealerState(8);
        Boundary tick = state.onTick(1_000L);
        state.onRecord(id(1), payload("tx"));
        Boundary forced =
                state.onRemoteOriginRecord(id(2), CHAIN_X, 700L, 1L, payload("x1"), 1_010L)
                        .orElseThrow()
                        .forcedBoundary()
                        .orElseThrow();

        assertTrue(
                forced.l2Timestamp > tick.l2Timestamp,
                "two blocks must never share a timestamp: " + forced.l2Timestamp);
    }

    @Test
    void snapshot_round_trips_the_per_peer_map() {
        CanonicalSealerState state = new CanonicalSealerState(8);
        state.onRemoteOriginRecord(id(1), CHAIN_X, 700L, 2L, payload("x1"), 1_000L);
        state.onRemoteOriginRecord(id(2), CHAIN_Y, 5L, 1L, payload("y1"), 1_000L);

        CanonicalSealerState restored = CanonicalSealerState.load(state.takeSnapshot(), 8);

        assertEquals(state.canonicalCount(), restored.canonicalCount());
        assertEquals(Optional.of(700L), restored.remoteOriginOf(CHAIN_X));
        assertEquals(Optional.of(5L), restored.remoteOriginOf(CHAIN_Y));
        // The two must stay indistinguishable, or a restored member accepts a
        // stale anchor its peers reject — state-machine divergence.
        assertThrows(
                IllegalArgumentException.class,
                () -> restored.onRemoteOriginRecord(id(9), CHAIN_X, 700L, 1L, payload("stale"), 2_000L));
        assertTrue(restored.onRemoteOriginRecord(id(8), CHAIN_X, 701L, 1L, payload("x2"), 2_000L)
                .isPresent());
    }

    @Test
    void pre_interop_snapshots_load_with_an_empty_peer_map() {
        // What an in-place upgrade finds on disk: a v3 snapshot has the origin
        // trio but no peer map. Every peer re-seeds on its next batch —
        // trust-on-first-sight, exactly as a fresh state would.
        CanonicalSealerState pre = new CanonicalSealerState(8);
        pre.onRecord(id(1), payload("tx"));
        pre.onOriginRecord(id(2), 100L, 1L, payload("e100"), 1_000L);
        byte[] v4 = pre.takeSnapshot();
        // Re-tag as v3 and drop the 4-byte peer-map count the v4 tail added.
        byte[] v3 = java.util.Arrays.copyOf(v4, v4.length - Integer.BYTES);
        java.nio.ByteBuffer.wrap(v3).order(java.nio.ByteOrder.BIG_ENDIAN).putInt(4, 3);

        CanonicalSealerState fromV3 = CanonicalSealerState.load(v3, 8);

        assertEquals(0, fromV3.trackedRemoteOrigins());
        assertEquals(100L, fromV3.l1Origin(), "the v3 origin trio still parses");
        assertEquals(pre.canonicalCount(), fromV3.canonicalCount());
        assertTrue(fromV3.onRemoteOriginRecord(id(3), CHAIN_X, 700L, 1L, payload("x1"), 1_500L)
                .isPresent(), "peers seed normally from there");
    }
}
