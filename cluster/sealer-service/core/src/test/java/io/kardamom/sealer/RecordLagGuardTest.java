package io.kardamom.sealer;

import static io.kardamom.sealer.SealerStateFixtures.NO_DEADLINE;
import static io.kardamom.sealer.SealerStateFixtures.id;
import static io.kardamom.sealer.SealerStateFixtures.payload;
import static io.kardamom.sealer.SealerStateFixtures.sender;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kardamom.sealer.CanonicalSealerState.RecordOutcome;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * The record-lag guard: the sealer keeps the recorded cursor of each
 * executor, and refuses user records while the last ordered index is more
 * than the budget past the best cursor. Deposits, origin records, votes,
 * void records and boundaries still enter. The cursors are ordered input
 * and the budget is shared configuration, so two members fed the same log
 * take the same decisions.
 */
class RecordLagGuardTest {

    private static final long BUDGET = 2L;
    /** Executors 0, 1 and 2 are the voters. */
    private static final VoidLedger.Config VOTERS = new VoidLedger.Config(64, 0b111L);
    /** Voters 0 and 1: executor 2 is removed. */
    private static final VoidLedger.Config TWO_VOTERS = new VoidLedger.Config(64, 0b011L);

    private static CanonicalSealerState state(long budget) {
        return state(budget, VOTERS);
    }

    private static CanonicalSealerState state(long budget, VoidLedger.Config voters) {
        return new CanonicalSealerState(
            64, 1, Set.of(), voters, CanonicalSealerState.DEFAULT_INCLUSION_HORIZON_BLOCKS,
            CanonicalSealerState.DEFAULT_ORDERING_WINDOW, new LagBudgets(0L, budget));
    }

    private static CanonicalSealerState reload(CanonicalSealerState s, byte[] snapshot, VoidLedger.Config voters) {
        return CanonicalSealerState.load(
            ByteBuffer.wrap(snapshot), 64, Set.of(), voters,
            CanonicalSealerState.DEFAULT_INCLUSION_HORIZON_BLOCKS,
            CanonicalSealerState.DEFAULT_ORDERING_WINDOW, new LagBudgets(0L, s.recordLagBudget()));
    }

    /** One user record of the shared sender, by nonce. */
    private static RecordOutcome user(CanonicalSealerState s, int n) {
        return s.onRecord(id(n), sender(1), n, NO_DEADLINE, payload("u" + n));
    }

    /** Order user records with nonces {@code from} to {@code to - 1}. */
    private static void users(CanonicalSealerState s, int from, int to) {
        java.util.stream.IntStream.range(from, to)
            .forEach(n -> assertEquals(RecordOutcome.Kind.RELAYED, user(s, n).kind));
    }

    @Test
    void the_guard_is_off_at_budget_zero() {
        CanonicalSealerState s = state(0L);
        users(s, 0, 1);
        s.onRecordedCursor(0, 0L);
        users(s, 1, 20);
        assertFalse(s.recordLagHalted(), "budget 0 refuses nothing");
        assertEquals(0L, s.bestRecorded(), "the cursor is still kept");
    }

    @Test
    void a_record_past_the_budget_is_refused_and_moves_nothing() {
        CanonicalSealerState s = state(BUDGET);
        users(s, 0, 1);
        assertTrue(s.onRecordedCursor(0, 0L));
        users(s, 1, 3); // last index 2, best 0: lag 2 is within the budget
        assertFalse(s.recordLagHalted());
        users(s, 3, 4); // last index 3: lag 3 > 2
        assertTrue(s.recordLagHalted());

        RecordOutcome out = user(s, 4);
        assertEquals(RecordOutcome.Kind.RECORD_LAG_REJECT, out.kind);
        assertEquals(4L, s.canonicalCount(), "a refused record is not counted");
        assertEquals(4, s.dedupSize(), "a refused record does not enter the window");
        assertEquals(4L, s.expectedNonceOf(sender(1)).orElseThrow(), "the guard did not move");
    }

    @Test
    void the_guard_resumes_when_the_best_cursor_moves_up() {
        CanonicalSealerState s = state(BUDGET);
        users(s, 0, 4);
        s.onRecordedCursor(0, 0L);
        assertEquals(RecordOutcome.Kind.RECORD_LAG_REJECT, user(s, 4).kind);

        assertTrue(s.onRecordedCursor(1, 2L), "the best cursor moves up");
        assertFalse(s.recordLagHalted(), "lag 1 is within the budget");
        RecordOutcome again = user(s, 4);
        assertEquals(RecordOutcome.Kind.RELAYED, again.kind, "the resubmit is fresh");
        assertEquals(4L, again.relayed.orElseThrow().index);
    }

    @Test
    void the_best_cursor_is_the_maximum_over_the_executors() {
        CanonicalSealerState s = state(BUDGET);
        users(s, 0, 10);
        assertTrue(s.onRecordedCursor(0, 3L));
        assertTrue(s.onRecordedCursor(1, 8L));
        assertFalse(s.onRecordedCursor(2, 5L), "a lower cursor of another executor keeps the best");
        assertEquals(8L, s.bestRecorded());
        assertFalse(s.recordLagHalted(), "one recorded copy is enough: lag 1");

        assertFalse(s.onRecordedCursor(1, 4L), "a cursor never moves down");
        assertEquals(8L, s.bestRecorded());
        assertFalse(s.onRecordedCursor(0, 7L), "executor 0 moves up, the best stays");
        assertEquals(8L, s.bestRecorded());
    }

    @Test
    void the_guard_refuses_nothing_before_the_first_cursor() {
        CanonicalSealerState s = state(BUDGET);
        users(s, 0, 20);
        assertFalse(s.recordLagHalted(), "no executor sent a cursor yet");
        assertEquals(RecordedCursors.NONE, s.bestRecorded());
    }

    @Test
    void deposits_origins_votes_voids_and_boundaries_still_enter() {
        CanonicalSealerState s = state(BUDGET);
        users(s, 0, 1);
        s.onRecordedCursor(0, 0L);
        users(s, 1, 4);
        assertTrue(s.recordLagHalted());

        assertTrue(s.onRecord(id(100), payload("deposit")).isPresent(), "the zero sender is exempt");
        assertTrue(s.onOriginRecord(id(101), 7L, 1L, payload("epoch"), 1_000L).advance.isPresent(),
            "an origin record enters");
        long block = s.blockNumber();
        s.onTick(2_000L);
        assertEquals(block + 1, s.blockNumber(), "the boundary tick runs");
        assertEquals(VoidLedger.Vote.COUNTED, s.onVoidRequest(0, 3L, id(3)).vote, "a vote enters");
        s.onVoidRequest(1, 3L, id(3));
        assertTrue(s.onVoidRequest(2, 3L, id(3)).relayed.isPresent(), "the void record enters");
    }

    @Test
    void a_cursor_from_a_stranger_or_past_the_head_is_refused() {
        CanonicalSealerState s = state(BUDGET);
        users(s, 0, 3);
        assertThrows(IllegalArgumentException.class, () -> s.onRecordedCursor(3, 0L), "not a voter");
        assertThrows(IllegalArgumentException.class, () -> s.onRecordedCursor(0, 3L), "not ordered yet");
        assertThrows(IllegalArgumentException.class, () -> s.onRecordedCursor(0, -1L), "u64::MAX");
        assertEquals(RecordedCursors.NONE, s.bestRecorded(), "a refused cursor moves nothing");
    }

    @Test
    void the_cursors_survive_a_snapshot() {
        CanonicalSealerState s = state(BUDGET);
        users(s, 0, 6);
        s.onRecordedCursor(2, 1L);
        s.onRecordedCursor(0, 3L);

        CanonicalSealerState restored = reload(s, s.takeSnapshot(), VOTERS);
        assertEquals(3L, restored.bestRecorded());
        assertArrayEquals(s.takeSnapshot(), restored.takeSnapshot(), "every member writes the same bytes");
        assertFalse(restored.onRecordedCursor(0, 2L), "the restored cursor of executor 0 is 3");
        assertTrue(restored.onRecordedCursor(2, 4L), "the restored cursor of executor 2 is 1");
    }

    @Test
    void a_restore_drops_the_cursor_of_a_removed_executor() {
        CanonicalSealerState s = state(BUDGET);
        users(s, 0, 6);
        s.onRecordedCursor(0, 1L);
        s.onRecordedCursor(2, 5L);

        CanonicalSealerState a = reload(s, s.takeSnapshot(), TWO_VOTERS);
        CanonicalSealerState b = reload(s, s.takeSnapshot(), TWO_VOTERS);
        assertEquals(1L, a.bestRecorded(), "the best is over the configured executors only");
        assertTrue(a.recordLagHalted(), "lag 4 > 2");
        assertArrayEquals(a.takeSnapshot(), b.takeSnapshot());
    }

    @Test
    void a_version_10_snapshot_restores_no_cursor() {
        CanonicalSealerState s = state(BUDGET);
        users(s, 0, 6);
        // A state with no cursor writes one zero count. Cut it and set the
        // version back, which is the exact version-10 byte layout.
        byte[] v11 = s.takeSnapshot();
        byte[] v10 = Arrays.copyOf(v11, v11.length - 1);
        ByteBuffer.wrap(v10).putInt(4, 10);

        CanonicalSealerState restored = reload(s, v10, VOTERS);
        assertEquals(6L, restored.canonicalCount());
        assertEquals(RecordedCursors.NONE, restored.bestRecorded());
        assertFalse(restored.recordLagHalted(), "no cursor: the guard refuses nothing");
        assertArrayEquals(v11, restored.takeSnapshot(), "the restored state writes version 11");
    }

    @Test
    void two_members_fed_the_same_input_decide_the_same() {
        assertArrayEquals(drive(state(BUDGET)), drive(state(BUDGET)));
    }

    private static byte[] drive(CanonicalSealerState s) {
        users(s, 0, 2);
        s.onRecordedCursor(1, 0L);
        users(s, 2, 3);
        user(s, 3);
        user(s, 4);
        s.onRecordedCursor(0, 1L);
        user(s, 3);
        return s.takeSnapshot();
    }
}
