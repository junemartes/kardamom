package io.kardamom.sealer;

import static io.kardamom.sealer.SealerStateFixtures.NO_DEADLINE;
import static io.kardamom.sealer.SealerStateFixtures.id;
import static io.kardamom.sealer.SealerStateFixtures.payload;
import static io.kardamom.sealer.SealerStateFixtures.sender;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Void-request unit tests for {@link CanonicalSealerState}: the vote rule,
 * the void record, what a void does to the dedup window and to the
 * contiguity guard, the window bound, and the snapshot.
 */
class VoidRequestTest {
    /** Voters 0, 1 and 2, with a window of 4 indices. */
    private static final VoidLedger.Config THREE_VOTERS = new VoidLedger.Config(4, 0b111L);

    private static CanonicalSealerState stateWith(VoidLedger.Config config) {
        return new CanonicalSealerState(8, CanonicalSealerState.GENESIS_BLOCK_NUMBER, Set.of(), config);
    }

    /** Order one reference from sender 1 at {@code nonce}, with id {@code nonce}. */
    private static long order(CanonicalSealerState state, int nonce) {
        return state.onRecord(id(nonce), sender(1), nonce, NO_DEADLINE, payload("ref"))
                .relayed
                .orElseThrow()
                .index;
    }

    @Test
    void no_void_until_every_voter_has_voted() {
        CanonicalSealerState state = stateWith(THREE_VOTERS);
        long index = order(state, 5);

        assertEquals(VoidLedger.Vote.COUNTED, state.onVoidRequest(0, index, id(5)).vote);
        CanonicalSealerState.VoidOutcome second = state.onVoidRequest(1, index, id(5));
        assertEquals(VoidLedger.Vote.COUNTED, second.vote);
        assertFalse(second.relayed.isPresent(), "two of three voters must not remove an entry");
        assertEquals(1, state.canonicalCount(), "a vote takes no canonical slot");

        CanonicalSealerState.VoidOutcome last = state.onVoidRequest(2, index, id(5));
        assertEquals(VoidLedger.Vote.DECIDED, last.vote);
        assertEquals(1L, last.relayed.orElseThrow().index, "the void record takes the next slot");
        assertEquals(2, state.canonicalCount());
    }

    @Test
    void the_void_record_is_the_hash_the_type_and_the_index() {
        CanonicalSealerState state = stateWith(new VoidLedger.Config(4, 0b1L));
        order(state, 4);
        long index = order(state, 5);

        byte[] record = state.onVoidRequest(0, index, id(5)).relayed.orElseThrow().payload;

        ByteBuffer want = ByteBuffer.allocate(32 + 1 + 8).order(ByteOrder.LITTLE_ENDIAN);
        want.put(id(5)).put(VoidLedger.RT_VOID).putLong(index);
        assertArrayEquals(want.array(), record, "matches the Rust RT_VOID layout");
    }

    @Test
    void a_repeated_vote_does_not_count_twice() {
        CanonicalSealerState state = stateWith(THREE_VOTERS);
        long index = order(state, 5);

        state.onVoidRequest(0, index, id(5));
        assertEquals(VoidLedger.Vote.REPEATED, state.onVoidRequest(0, index, id(5)).vote);
        assertEquals(1, state.voids().votesFor(index));
    }

    @Test
    void a_vote_is_refused_for_a_stranger_a_wrong_hash_and_a_slot_that_is_no_reference() {
        CanonicalSealerState state = stateWith(THREE_VOTERS);
        long index = order(state, 5);
        long deposit = state.onRecord(id(9), payload("deposit")).orElseThrow().index;

        assertEquals(VoidLedger.Vote.REFUSED, state.onVoidRequest(3, index, id(5)).vote, "not a voter");
        assertEquals(VoidLedger.Vote.REFUSED, state.onVoidRequest(0, index, id(6)).vote, "wrong hash");
        assertEquals(VoidLedger.Vote.REFUSED, state.onVoidRequest(0, deposit, id(9)).vote, "no reference");
        assertEquals(VoidLedger.Vote.REFUSED, state.onVoidRequest(0, 99, id(5)).vote, "not ordered yet");
    }

    @Test
    void an_entry_has_one_void_only() {
        CanonicalSealerState state = stateWith(new VoidLedger.Config(4, 0b1L));
        long index = order(state, 5);

        assertEquals(VoidLedger.Vote.DECIDED, state.onVoidRequest(0, index, id(5)).vote);
        assertEquals(VoidLedger.Vote.REFUSED, state.onVoidRequest(0, index, id(5)).vote);
    }

    @Test
    void the_sender_can_submit_the_same_bytes_again_after_a_void() {
        CanonicalSealerState state = stateWith(new VoidLedger.Config(8, 0b1L));
        long index = order(state, 5);
        order(state, 6);
        assertFalse(
            state.onRecord(id(5), sender(1), 5, NO_DEADLINE, payload("ref")).relayed.isPresent(),
            "before the void the window absorbs the same bytes as a duplicate");

        state.onVoidRequest(0, index, id(5));

        CanonicalSealerState.RecordOutcome again = state.onRecord(id(5), sender(1), 5, NO_DEADLINE, payload("ref"));
        assertTrue(again.relayed.isPresent(), "same id and same nonce are fresh after the void");
        assertFalse(again.kind == CanonicalSealerState.RecordOutcome.Kind.CONTIGUITY_REJECT);
    }

    @Test
    void an_entry_that_left_the_window_cannot_be_removed() {
        CanonicalSealerState state = stateWith(new VoidLedger.Config(2, 0b11L));
        long old = order(state, 5);
        state.onVoidRequest(0, old, id(5));
        order(state, 6);
        order(state, 7);

        assertEquals(VoidLedger.Vote.REFUSED, state.onVoidRequest(1, old, id(5)).vote);
        assertEquals(0, state.voids().votesFor(old), "the open vote left with the entry");
    }

    @Test
    void no_voters_refuses_every_vote() {
        CanonicalSealerState state = new CanonicalSealerState(8);
        long index = order(state, 5);

        assertEquals(VoidLedger.Vote.REFUSED, state.onVoidRequest(0, index, id(5)).vote);
    }

    @Test
    void an_open_vote_survives_a_snapshot() {
        CanonicalSealerState state = stateWith(THREE_VOTERS);
        long index = order(state, 5);
        state.onVoidRequest(0, index, id(5));
        state.onVoidRequest(1, index, id(5));

        CanonicalSealerState restored =
            CanonicalSealerState.load(state.takeSnapshot(), 8, Set.of(), THREE_VOTERS);

        CanonicalSealerState.VoidOutcome last = restored.onVoidRequest(2, index, id(5));
        assertEquals(VoidLedger.Vote.DECIDED, last.vote, "the restored member decides as its peers do");
        assertArrayEquals(
            state.onVoidRequest(2, index, id(5)).relayed.orElseThrow().payload,
            last.relayed.orElseThrow().payload);
        assertArrayEquals(state.takeSnapshot(), restored.takeSnapshot());
    }

    @Test
    void a_version_5_snapshot_restores_an_empty_ledger() {
        CanonicalSealerState old = new CanonicalSealerState(8);
        long index = order(old, 5);
        // A disabled ledger writes two zero counts. Strip the version-7
        // per-id deadlines, cut those counts, and set the version field
        // back, which is the exact version-5 byte layout.
        byte[] v6 = SealerStateFixtures.downgradeToVersion(old.takeSnapshot(), 5);
        byte[] v5 = java.util.Arrays.copyOf(v6, v6.length - 8);

        CanonicalSealerState restored = CanonicalSealerState.load(v5, 8, Set.of(), THREE_VOTERS);

        assertEquals(1, restored.canonicalCount());
        assertEquals(VoidLedger.Vote.REFUSED, restored.onVoidRequest(0, index, id(5)).vote);
    }
}
