package io.kardamom.sealer;

import static io.kardamom.sealer.SealerStateFixtures.NO_DEADLINE;
import static io.kardamom.sealer.SealerStateFixtures.id;
import static io.kardamom.sealer.SealerStateFixtures.payload;
import static io.kardamom.sealer.SealerStateFixtures.sender;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kardamom.sealer.CanonicalSealerState.RecordOutcome.Kind;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The nonce release: an executor skipped an ordered entry, and the guard
 * goes back to the nonce that the chain expects, once for each skip.
 */
class NonceReleaseTest {

    /** A state that ordered nonces 0, 1 and 2 of sender 1, as ids 1, 2 and 3. */
    private static CanonicalSealerState orderedThree() {
        CanonicalSealerState state = new CanonicalSealerState(8);
        for (int n = 0; n < 3; n++) {
            assertEquals(Kind.RELAYED, state.onRecord(id(n + 1), sender(1), n, NO_DEADLINE, payload("t")).kind);
        }
        return state;
    }

    @Test
    void a_release_after_a_skip_lets_a_new_transaction_in_at_the_released_nonce() {
        CanonicalSealerState state = orderedThree();
        // The executor skipped nonce 1 (id 2), so the chain expects 1.
        assertEquals(Kind.CONTIGUITY_REJECT, state.onRecord(id(9), sender(1), 1, NO_DEADLINE, payload("n")).kind);
        assertTrue(state.onRelease(sender(1), 1, id(2)));
        assertEquals(Optional.of(1L), state.expectedNonceOf(sender(1)));
        assertEquals(Kind.RELAYED, state.onRecord(id(9), sender(1), 1, NO_DEADLINE, payload("n")).kind);
        assertEquals(Optional.of(2L), state.expectedNonceOf(sender(1)));
    }

    @Test
    void the_twin_release_drops() {
        CanonicalSealerState state = orderedThree();
        assertTrue(state.onRelease(sender(1), 1, id(2)));
        state.onRecord(id(9), sender(1), 1, NO_DEADLINE, payload("n"));
        // The copy of the other twin comes after the sender used nonce 1 again.
        assertFalse(state.onRelease(sender(1), 1, id(2)), "the mark drops the second copy");
        assertEquals(Optional.of(2L), state.expectedNonceOf(sender(1)), "the guard stays at the chain");
    }

    @Test
    void the_same_signed_bytes_stay_a_duplicate() {
        CanonicalSealerState state = orderedThree();
        state.onRelease(sender(1), 1, id(2));
        assertEquals(Kind.DUPLICATE, state.onRecord(id(2), sender(1), 1, NO_DEADLINE, payload("t")).kind);
        assertEquals(3L, state.canonicalCount(), "one hash has one canonical entry");
    }

    @Test
    void a_release_after_the_deadline_drops() {
        CanonicalSealerState state = new CanonicalSealerState(8);
        long deadline = state.blockNumber();
        state.onRecord(id(1), sender(1), 0, deadline, payload("t"));
        state.onTick(250);
        assertEquals(0, state.dedupSize(), "the id left at its deadline");
        assertFalse(state.onRelease(sender(1), 0, id(1)), "a stale release changes nothing");
        assertEquals(Optional.of(1L), state.expectedNonceOf(sender(1)));
    }

    @Test
    void the_mark_leaves_with_its_entry() {
        CanonicalSealerState state = new CanonicalSealerState(8);
        long deadline = state.blockNumber();
        state.onRecord(id(1), sender(1), 0, deadline, payload("t"));
        assertTrue(state.onRelease(sender(1), 0, id(1)));
        assertEquals(2, state.dedupSize(), "the id and its mark");
        state.onTick(250);
        assertEquals(0, state.dedupSize());
    }

    @Test
    void a_release_at_or_above_the_guard_changes_nothing() {
        CanonicalSealerState state = orderedThree();
        assertTrue(state.onRelease(sender(1), 1, id(2)));
        assertFalse(state.onRelease(sender(1), 2, id(3)), "the guard already expects 1");
        assertFalse(state.onRelease(sender(1), 3, id(1)), "a nonce at the guard changes nothing");
        assertEquals(Optional.of(1L), state.expectedNonceOf(sender(1)));
    }

    /**
     * The run n (skip), n + 1 (skip as a nonce too high): only n sends a
     * release. It applies, n' is ordered at n, and no second lowering
     * follows: the twin copy of the release finds the mark.
     */
    @Test
    void a_run_of_skips_recovers_with_the_release_of_its_first_entry() {
        CanonicalSealerState state = new CanonicalSealerState(8);
        state.onRecord(id(1), sender(1), 5, NO_DEADLINE, payload("n"));
        state.onRecord(id(2), sender(1), 6, NO_DEADLINE, payload("n+1"));
        assertTrue(state.onRelease(sender(1), 5, id(1)));
        assertEquals(Kind.RELAYED, state.onRecord(id(3), sender(1), 5, NO_DEADLINE, payload("n'")).kind);
        assertFalse(state.onRelease(sender(1), 5, id(1)), "the twin copy drops");
        assertEquals(Optional.of(6L), state.expectedNonceOf(sender(1)));
        assertEquals(Kind.RELAYED, state.onRecord(id(4), sender(1), 6, NO_DEADLINE, payload("6")).kind);
    }

    @Test
    void a_release_of_an_unknown_sender_changes_no_guard() {
        CanonicalSealerState state = orderedThree();
        assertFalse(state.onRelease(sender(7), 0, id(2)));
        assertEquals(3, state.dedupSize(), "no mark");
        assertEquals(Optional.empty(), state.expectedNonceOf(sender(7)));
        assertEquals(Optional.of(3L), state.expectedNonceOf(sender(1)));
    }

    @Test
    void the_mark_skips_the_capacity_check() {
        CanonicalSealerState state = new CanonicalSealerState(2);
        state.onRecord(id(1), sender(1), 0, NO_DEADLINE, payload("t"));
        state.onRecord(id(2), sender(1), 1, NO_DEADLINE, payload("t"));
        assertTrue(state.onRelease(sender(1), 1, id(2)), "a full window still takes the mark");
        assertEquals(3, state.dedupSize());
    }

    /** Both snapshot layouts of this release carry the mark, with equal bytes on every member. */
    @ParameterizedTest
    @ValueSource(ints = {10, 12})
    void the_snapshot_carries_the_mark(final int version) {
        CanonicalSealerState a = orderedThree();
        CanonicalSealerState b = orderedThree();
        a.onRelease(sender(1), 1, id(2));
        b.onRelease(sender(1), 1, id(2));
        byte[] snapshot = a.takeSnapshot(version);
        assertArrayEquals(snapshot, b.takeSnapshot(version), "every member writes the same bytes");

        CanonicalSealerState restored = CanonicalSealerState.load(snapshot, 8);
        assertEquals(version, restored.restoredSnapshotVersion());
        assertFalse(restored.onRelease(sender(1), 1, id(2)), "the restored mark drops the twin copy");
        assertEquals(Optional.of(1L), restored.expectedNonceOf(sender(1)));
        assertArrayEquals(snapshot, restored.takeSnapshot(version), "a restore writes the same bytes");
    }

    /** Version 12 is version 10 without the void ledger. */
    @Test
    void version_twelve_holds_no_void_ledger() {
        VoidLedger.Config voids = new VoidLedger.Config(4, 1L);
        CanonicalSealerState state = new CanonicalSealerState(8, 1, Set.of(), voids);
        state.onRecord(id(1), sender(1), 0, NO_DEADLINE, payload("t"));
        int ledger = state.voids().snapshotLen(state.canonicalCount());
        assertEquals(state.takeSnapshot(10).length - ledger, state.takeSnapshot(12).length);
    }
}
