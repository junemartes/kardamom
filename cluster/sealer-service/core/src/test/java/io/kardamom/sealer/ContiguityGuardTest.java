package io.kardamom.sealer;

import static io.kardamom.sealer.SealerStateFixtures.NO_DEADLINE;
import static io.kardamom.sealer.SealerStateFixtures.id;
import static io.kardamom.sealer.SealerStateFixtures.payload;
import static io.kardamom.sealer.SealerStateFixtures.sender;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Contiguity-guard unit tests for {@link CanonicalSealerState}.
 * The tests cover per-sender nonce tracking, gap rejection without state
 * movement, dedup absorption of republished copies before the nonce check,
 * the zero-sender exemption, LRU bounding of the sender map, and guard
 * survival across snapshot versions.
 * These tests were split out of {@link CanonicalSealerStateTest}.
 */
class ContiguityGuardTest {

    @Test
    void guard_unknown_sender_seeds_at_any_nonce() {
        CanonicalSealerState state = new CanonicalSealerState(8);
        CanonicalSealerState.RecordOutcome first = state.onRecord(id(1), sender(1), 41, NO_DEADLINE, payload("a"));
        assertTrue(first.relayed.isPresent(), "unknown sender seeds at any nonce");
        assertEquals(Optional.of(42L), state.expectedNonceOf(sender(1)));
    }

    @Test
    void guard_rejects_known_sender_gap_without_state_change() {
        CanonicalSealerState state = new CanonicalSealerState(8);
        state.onRecord(id(1), sender(1), 0, NO_DEADLINE, payload("a"));
        // The state expects nonce 1, but this record has nonce 2. The state
        // rejects it. Nothing changes: no count, no dedup entry, no nonce update.
        CanonicalSealerState.RecordOutcome gap = state.onRecord(id(2), sender(1), 2, NO_DEADLINE, payload("b"));
        assertTrue(gap.kind == CanonicalSealerState.RecordOutcome.Kind.CONTIGUITY_REJECT);
        assertEquals(1L, gap.expectedNonce);
        assertFalse(gap.relayed.isPresent());
        assertEquals(1L, state.canonicalCount(), "reject must not count");
        assertEquals(Optional.of(1L), state.expectedNonceOf(sender(1)), "reject must not advance");
        // The gap is filled. The same rejected id republishes and is fresh,
        // because a reject leaves no dedup entry.
        assertTrue(state.onRecord(id(3), sender(1), 1, NO_DEADLINE, payload("gap")).relayed.isPresent());
        assertTrue(state.onRecord(id(2), sender(1), 2, NO_DEADLINE, payload("b")).relayed.isPresent());
        assertEquals(3L, state.canonicalCount());
    }

    @Test
    void guard_dedup_absorbs_republished_copies_before_the_nonce_check() {
        // The sequencer republishes unconfirmed refs.
        // Copies that already committed re-arrive with a now-stale nonce.
        // The guard must absorb these as duplicates, not reject them as gaps.
        CanonicalSealerState state = new CanonicalSealerState(8);
        state.onRecord(id(1), sender(1), 0, NO_DEADLINE, payload("a"));
        state.onRecord(id(2), sender(1), 1, NO_DEADLINE, payload("b"));
        CanonicalSealerState.RecordOutcome copy = state.onRecord(id(1), sender(1), 0, NO_DEADLINE, payload("a"));
        assertFalse(copy.kind == CanonicalSealerState.RecordOutcome.Kind.CONTIGUITY_REJECT, "committed copy is a duplicate, not a gap");
        assertFalse(copy.relayed.isPresent());
        assertEquals(2L, state.canonicalCount());
    }

    @Test
    void guard_zero_sender_is_exempt() {
        CanonicalSealerState state = new CanonicalSealerState(8);
        assertTrue(state.onRecord(id(1), sender(0), 0, NO_DEADLINE, payload("d1")).relayed.isPresent());
        assertTrue(state.onRecord(id(2), sender(0), 0, NO_DEADLINE, payload("d2")).relayed.isPresent());
        assertTrue(state.onRecord(id(3), sender(0), 7, NO_DEADLINE, payload("d3")).relayed.isPresent());
        assertEquals(0, state.trackedSenders(), "zero sender is never tracked");
    }

    @Test
    void guard_map_is_lru_bounded_and_evicted_sender_reseeds() {
        // Capacity 2 is shared with the dedup window. The window refuses a
        // record once it is full, so the records that exercise the sender
        // map's eviction arrive across a tick that prunes the window. The
        // sender map is not pruned by deadline: it stays LRU-bounded.
        // Tracking a third sender evicts the least recently used sender.
        // The evicted sender then re-seeds like a new sender. This is
        // graceful degradation, not a false reject.
        CanonicalSealerState state = new CanonicalSealerState(2);
        long open = state.blockNumber();
        state.onRecord(id(1), sender(1), 10, open, payload("a"));
        state.onRecord(id(2), sender(2), 20, open, payload("b"));
        state.onTick(1_000L); // the open block passes both deadlines
        state.onTick(2_000L);
        assertEquals(0, state.dedupSize(), "the window pruned, the sender map did not");
        long second = state.blockNumber();
        state.onRecord(id(3), sender(1), 11, second, payload("a2")); // touches sender 1
        state.onRecord(id(4), sender(3), 30, second, payload("c")); // evicts sender 2, the least recently used sender
        assertEquals(2, state.trackedSenders());
        assertEquals(Optional.empty(), state.expectedNonceOf(sender(2)), "sender2 evicted");
        // The evicted sender 2 reappears at an arbitrary nonce. It seeds again, and is not rejected.
        state.onTick(3_000L);
        state.onTick(4_000L);
        CanonicalSealerState.RecordOutcome reseed =
                state.onRecord(id(5), sender(2), 99, state.blockNumber(), payload("b2"));
        assertTrue(reseed.relayed.isPresent(), "evicted sender re-seeds");
    }

    @Test
    void guard_map_survives_snapshot_roundtrip() {
        CanonicalSealerState original = new CanonicalSealerState(8, 1);
        original.onRecord(id(1), sender(1), 5, NO_DEADLINE, payload("a"));
        original.onRecord(id(2), sender(2), 0, NO_DEADLINE, payload("b"));

        CanonicalSealerState restored = CanonicalSealerState.load(original.takeSnapshot(), 8);
        assertEquals(2, restored.trackedSenders());
        assertEquals(Optional.of(6L), restored.expectedNonceOf(sender(1)));
        // The restored guard still rejects a gap.
        CanonicalSealerState.RecordOutcome gap = restored.onRecord(id(3), sender(1), 9, NO_DEADLINE, payload("x"));
        assertTrue(gap.kind == CanonicalSealerState.RecordOutcome.Kind.CONTIGUITY_REJECT, "restored guard must keep rejecting gaps");
        assertEquals(6L, gap.expectedNonce);
        // The restored guard still accepts the next contiguous nonce.
        assertTrue(restored.onRecord(id(3), sender(1), 6, NO_DEADLINE, payload("x")).relayed.isPresent());
    }

    @Test
    void guard_v1_snapshot_loads_with_empty_map() {
        // A pre-guard (v1) snapshot restores an empty guard map.
        // Every sender re-seeds on trust at first sight.
        // This test builds a v1 snapshot by patching the version field and
        // removing the sender section.
        CanonicalSealerState original = new CanonicalSealerState(8, 1);
        original.onRecord(id(1), sender(1), 5, NO_DEADLINE, payload("a"));
        byte[] v2 = original.takeSnapshot();
        int senderSection = 4 + 1 * (CanonicalSealerState.SENDER_LEN + 8);
        byte[] v1 = java.util.Arrays.copyOf(v2, v2.length - senderSection);
        v1[7] = 1; // version int (big-endian) at bytes 4..8

        CanonicalSealerState restored = CanonicalSealerState.load(v1, 8);
        assertEquals(0, restored.trackedSenders(), "v1 snapshot has no guard map");
        assertEquals(
                CanonicalSealerState.Admission.DUPLICATE,
                restored.firstSeen(id(1), NO_DEADLINE),
                "dedup window still restored");
        assertTrue(restored.onRecord(id(2), sender(1), 99, NO_DEADLINE, payload("x")).relayed.isPresent(),
                "post-v1-restore senders re-seed at any nonce");
    }
}
