package io.kardamom.sealer;

import static io.kardamom.sealer.SealerStateFixtures.NO_DEADLINE;
import static io.kardamom.sealer.SealerStateFixtures.id;
import static io.kardamom.sealer.SealerStateFixtures.payload;
import static io.kardamom.sealer.SealerStateFixtures.sender;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kardamom.sealer.CanonicalSealerState.RecordOutcome;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * The DA-lag guard: the sealer refuses user records while the sealed head
 * is more than the budget past the batcher's posted cursor, lets deposits
 * and boundaries through, and resumes on its own when the cursor advances.
 * The cursor is ordered input and the budget is shared configuration, so
 * two members fed the same log take the same decisions.
 */
class DaLagGuardTest {

    private static final long BUDGET = 3L;

    private static CanonicalSealerState state(long budget) {
        return new CanonicalSealerState(
                64, 1, Set.of(), VoidLedger.Config.DISABLED,
                CanonicalSealerState.DEFAULT_INCLUSION_HORIZON_BLOCKS, budget);
    }

    /** One user record of the shared sender, by nonce. */
    private static RecordOutcome user(CanonicalSealerState s, int n) {
        return s.onRecord(id(n), sender(1), n, NO_DEADLINE, payload("u" + n));
    }

    /** Seal {@code n} blocks. */
    private static void seal(CanonicalSealerState s, int n) {
        for (int i = 0; i < n; i++) {
            s.onTick((s.blockNumber() + 1) * 1000L);
        }
    }

    @Test
    void a_record_past_the_budget_is_refused_and_moves_nothing() {
        CanonicalSealerState s = state(BUDGET);
        seal(s, 4); // sealed head 4, posted head 0: lag 4 > 3
        assertTrue(s.daLagHalted());
        RecordOutcome out = user(s, 0);
        assertEquals(RecordOutcome.Kind.DA_LAG_REJECT, out.kind);
        assertEquals(0L, s.canonicalCount(), "a refused record is not counted");
        assertEquals(0, s.dedupSize(), "a refused record does not enter the window");
        assertTrue(s.expectedNonceOf(sender(1)).isEmpty(), "the guard did not move");
    }

    @Test
    void a_record_within_the_budget_is_accepted() {
        CanonicalSealerState s = state(BUDGET);
        seal(s, 3); // lag 3, not past the budget
        assertFalse(s.daLagHalted());
        assertEquals(RecordOutcome.Kind.RELAYED, user(s, 0).kind);
    }

    @Test
    void the_guard_resumes_when_the_cursor_advances() {
        CanonicalSealerState s = state(BUDGET);
        seal(s, 6); // sealed head 6
        assertEquals(RecordOutcome.Kind.DA_LAG_REJECT, user(s, 0).kind);
        assertTrue(s.onPostedCursor(3L), "the cursor advances");
        assertFalse(s.daLagHalted(), "lag 3 is within the budget");
        RecordOutcome out = user(s, 0);
        assertEquals(RecordOutcome.Kind.RELAYED, out.kind, "the resubmit is fresh");
        assertEquals(0L, out.relayed.orElseThrow().index);
    }

    @Test
    void zero_turns_the_guard_off() {
        CanonicalSealerState s = state(0L);
        seal(s, 50);
        assertFalse(s.daLagHalted());
        assertEquals(RecordOutcome.Kind.RELAYED, user(s, 0).kind);
    }

    @Test
    void deposits_and_boundaries_still_enter() {
        CanonicalSealerState s = state(BUDGET);
        seal(s, 6);
        assertTrue(s.daLagHalted());
        // A zero-sender record (a deposit reference) is exempt.
        assertTrue(s.onRecord(id(7), payload("deposit")).isPresent());
        // An origin record (an L1 epoch with its deposits) is exempt.
        assertTrue(s.onOriginRecord(id(8), 100L, 2L, payload("epoch"), 9000L).isPresent());
        long before = s.blockNumber();
        s.onTick(10_000L);
        assertEquals(before + 1, s.blockNumber(), "boundaries keep sealing");
        assertTrue(s.daLagHalted(), "user records stay refused");
    }

    @Test
    void the_cursor_only_moves_up_and_never_past_the_sealed_head() {
        CanonicalSealerState s = state(BUDGET);
        seal(s, 5);
        assertTrue(s.onPostedCursor(4L));
        assertFalse(s.onPostedCursor(4L), "a re-offer changes nothing");
        assertFalse(s.onPostedCursor(2L), "a stale cursor changes nothing");
        assertEquals(4L, s.postedHead());
        assertThrows(IllegalArgumentException.class, () -> s.onPostedCursor(6L));
    }

    @Test
    void two_members_fed_the_same_log_refuse_the_same_records() {
        List<Object> a = drive(state(BUDGET));
        List<Object> b = drive(state(BUDGET));
        assertEquals(a, b, "identical inputs give identical decisions");
        assertTrue(a.contains(RecordOutcome.Kind.DA_LAG_REJECT), "the script trips the guard");
        assertTrue(a.indexOf(RecordOutcome.Kind.RELAYED) >= 0, "and sees it resume");
    }

    /** A script of records, ticks and posted cursors; the outcomes in order. */
    private static List<Object> drive(CanonicalSealerState s) {
        List<Object> out = new ArrayList<>();
        out.add(user(s, 0).kind);
        seal(s, 5);
        out.add(user(s, 1).kind); // refused: lag 5
        out.add(s.onPostedCursor(2L));
        out.add(user(s, 1).kind); // accepted: lag 3 is within the budget
        seal(s, 2);
        out.add(user(s, 2).kind); // lag 5 again: refused
        out.add(s.onPostedCursor(7L));
        out.add(user(s, 2).kind); // accepted
        out.add(s.canonicalCount());
        out.add(s.blockNumber());
        out.add(s.postedHead());
        return out;
    }

    @Test
    void the_snapshot_keeps_the_posted_head_and_an_older_one_restores_zero() {
        CanonicalSealerState s = state(BUDGET);
        seal(s, 5);
        s.onPostedCursor(4L);
        byte[] v8 = s.takeSnapshot();
        CanonicalSealerState restored = CanonicalSealerState.load(v8, 64);
        assertEquals(4L, restored.postedHead());
        assertEquals(5L, restored.sealedHead());

        // A version-7 snapshot: the same bytes without the trailing cursor,
        // tagged 7.
        byte[] v7 = java.util.Arrays.copyOf(v8, v8.length - 8);
        java.nio.ByteBuffer.wrap(v7).order(java.nio.ByteOrder.BIG_ENDIAN).putInt(4, 7);
        assertEquals(0L, CanonicalSealerState.load(v7, 64).postedHead(),
                "a snapshot before version 8 carries no cursor");
    }
}
