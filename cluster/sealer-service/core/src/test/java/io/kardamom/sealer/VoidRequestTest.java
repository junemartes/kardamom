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
import java.util.Arrays;
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

    /** Order references of sender 1 at {@code nonces}, each with id = nonce, and return their indices. */
    private static long[] orderAll(CanonicalSealerState state, int... nonces) {
        return Arrays.stream(nonces).mapToLong(nonce -> order(state, nonce)).toArray();
    }

    private static long expectedOf(CanonicalSealerState state) {
        return state.expectedNonceOf(sender(1)).orElseThrow();
    }

    @Test
    void a_single_void_sets_the_expected_nonce_back_to_the_entry() {
        CanonicalSealerState state = stateWith(new VoidLedger.Config(8, 0b1L));
        long[] index = orderAll(state, 5, 6, 7);

        state.onVoidRequest(0, index[1], id(6));

        assertEquals(6L, expectedOf(state), "a void below a later ordered nonce opens the voided nonce");
    }

    @Test
    void two_voids_in_nonce_order_keep_the_lower_nonce_open() {
        CanonicalSealerState state = stateWith(new VoidLedger.Config(8, 0b1L));
        long[] index = orderAll(state, 5, 6);

        state.onVoidRequest(0, index[0], id(5));
        state.onVoidRequest(0, index[1], id(6));

        assertEquals(5L, expectedOf(state), "the second void must not skip the first voided nonce");
    }

    @Test
    void two_voids_in_reverse_nonce_order_keep_the_lower_nonce_open() {
        CanonicalSealerState state = stateWith(new VoidLedger.Config(8, 0b1L));
        long[] index = orderAll(state, 5, 6);

        state.onVoidRequest(0, index[1], id(6));
        state.onVoidRequest(0, index[0], id(5));

        assertEquals(5L, expectedOf(state));
    }

    @Test
    void after_two_voids_the_sender_fills_both_nonces_in_order() {
        CanonicalSealerState state = stateWith(new VoidLedger.Config(8, 0b1L));
        long[] index = orderAll(state, 5, 6);
        state.onVoidRequest(0, index[0], id(5));
        state.onVoidRequest(0, index[1], id(6));

        CanonicalSealerState.RecordOutcome early = state.onRecord(id(6), sender(1), 6, NO_DEADLINE, payload("ref"));
        assertEquals(CanonicalSealerState.RecordOutcome.Kind.CONTIGUITY_REJECT, early.kind);
        assertEquals(5L, early.expectedNonce, "the guard asks for the lowest voided nonce");

        long five = order(state, 5);
        long six = order(state, 6);
        assertTrue(six > five, "the resubmit of 6 is ordered after the resubmit of 5");
        assertEquals(7L, expectedOf(state));
    }

    @Test
    void a_void_of_an_unknown_sender_adds_no_expected_nonce() {
        CanonicalSealerState state = new CanonicalSealerState(
            1, CanonicalSealerState.GENESIS_BLOCK_NUMBER, Set.of(), new VoidLedger.Config(8, 0b1L));
        long index = state.onRecord(
                id(5), sender(1), 5, CanonicalSealerState.GENESIS_BLOCK_NUMBER, payload("ref"))
            .relayed
            .orElseThrow()
            .index;
        state.onTick(0L);
        state.onRecord(id(9), sender(2), 0, NO_DEADLINE, payload("ref"));
        assertFalse(state.expectedNonceOf(sender(1)).isPresent(), "the second sender evicts the first");

        state.onVoidRequest(0, index, id(5));

        assertFalse(state.expectedNonceOf(sender(1)).isPresent(), "the void does not add the evicted sender");
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

    /** Voters 0 and 1: the voter set of {@link #THREE_VOTERS} with voter 2 removed. */
    private static final VoidLedger.Config TWO_VOTERS = new VoidLedger.Config(4, 0b011L);

    @Test
    void a_removed_voter_does_not_block_a_vote_from_a_snapshot() {
        CanonicalSealerState state = stateWith(THREE_VOTERS);
        long index = order(state, 5);
        state.onVoidRequest(0, index, id(5));
        state.onVoidRequest(2, index, id(5));
        byte[] snapshot = state.takeSnapshot();

        CanonicalSealerState a = CanonicalSealerState.load(snapshot, 8, Set.of(), TWO_VOTERS);
        CanonicalSealerState b = CanonicalSealerState.load(snapshot, 8, Set.of(), TWO_VOTERS);
        assertEquals(1, a.voids().votesFor(index), "the bit of the removed voter is dropped");
        assertArrayEquals(a.takeSnapshot(), b.takeSnapshot(), "every member restores the same ledger");

        CanonicalSealerState.VoidOutcome last = a.onVoidRequest(1, index, id(5));
        assertEquals(VoidLedger.Vote.DECIDED, last.vote, "the remaining voters decide the void");
        assertEquals(VoidLedger.Vote.DECIDED, b.onVoidRequest(1, index, id(5)).vote);
        assertArrayEquals(a.takeSnapshot(), b.takeSnapshot());
    }

    @Test
    void a_vote_from_a_snapshot_that_holds_every_remaining_voter_decides_on_the_next_vote() {
        CanonicalSealerState state = stateWith(THREE_VOTERS);
        long index = order(state, 5);
        state.onVoidRequest(0, index, id(5));
        state.onVoidRequest(1, index, id(5));

        CanonicalSealerState restored =
            CanonicalSealerState.load(state.takeSnapshot(), 8, Set.of(), TWO_VOTERS);

        CanonicalSealerState.VoidOutcome again = restored.onVoidRequest(0, index, id(5));
        assertEquals(VoidLedger.Vote.DECIDED, again.vote, "a repeated vote decides a full mask");
        assertEquals(1L, again.relayed.orElseThrow().index);
    }

    @Test
    void a_vote_from_a_snapshot_with_only_removed_voters_is_no_vote() {
        CanonicalSealerState state = stateWith(THREE_VOTERS);
        long index = order(state, 5);
        state.onVoidRequest(2, index, id(5));

        CanonicalSealerState restored =
            CanonicalSealerState.load(state.takeSnapshot(), 8, Set.of(), TWO_VOTERS);

        assertEquals(0, restored.voids().votesFor(index));
        assertEquals(VoidLedger.Vote.COUNTED, restored.onVoidRequest(0, index, id(5)).vote);
        assertEquals(VoidLedger.Vote.DECIDED, restored.onVoidRequest(1, index, id(5)).vote);
    }

    @Test
    void a_version_5_snapshot_restores_an_empty_ledger() {
        CanonicalSealerState old = new CanonicalSealerState(8);
        long index = order(old, 5);
        // A disabled ledger writes two zero counts. Strip the version-7
        // per-id deadlines, cut those counts, and set the version field
        // back, which is the exact version-5 byte layout.
        byte[] v6 = SealerStateFixtures.downgradeToVersion(old.takeSnapshot(), 5);
        byte[] v5 = Arrays.copyOf(v6, v6.length - 8);

        CanonicalSealerState restored = CanonicalSealerState.load(v5, 8, Set.of(), THREE_VOTERS);

        assertEquals(1, restored.canonicalCount());
        assertEquals(VoidLedger.Vote.REFUSED, restored.onVoidRequest(0, index, id(5)).vote);
    }
}
