package io.kardamom.sealer;

import static io.kardamom.sealer.SealerStateFixtures.NO_DEADLINE;
import static io.kardamom.sealer.SealerStateFixtures.id;
import static io.kardamom.sealer.SealerStateFixtures.payload;
import static io.kardamom.sealer.SealerStateFixtures.sender;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** A snapshot tail must replay with the budget that made its original decisions. */
class LagBudgetReplayTest {
    private static CanonicalSealerState state(long budget) {
        CanonicalSealerState state = new CanonicalSealerState(
                64, 1, Set.of(), VoidLedger.Config.DISABLED,
                CanonicalSealerState.DEFAULT_INCLUSION_HORIZON_BLOCKS,
                CanonicalSealerState.DEFAULT_ORDERING_WINDOW, new LagBudgets(budget, 0));
        for (int i = 0; i < 5; i++) {
            state.onTick((state.blockNumber() + 1) * 1000L);
        }
        return state;
    }

    private static CanonicalSealerState restore(byte[] snapshot, long budget) {
        return CanonicalSealerState.load(
                ByteBuffer.wrap(snapshot).order(ByteOrder.BIG_ENDIAN),
                64, Set.of(), VoidLedger.Config.DISABLED,
                CanonicalSealerState.DEFAULT_INCLUSION_HORIZON_BLOCKS,
                CanonicalSealerState.DEFAULT_ORDERING_WINDOW, new LagBudgets(budget, 0));
    }

    private static CanonicalSealerState.RecordOutcome.Kind user(CanonicalSealerState state) {
        return state.onRecord(id(1), sender(1), 0, NO_DEADLINE, payload("tail")).kind;
    }

    @Test
    void the_same_budget_replays_the_same_refusal() {
        CanonicalSealerState original = state(3);
        CanonicalSealerState restored = restore(original.takeSnapshot(), 3);
        assertEquals(CanonicalSealerState.RecordOutcome.Kind.DA_LAG_REJECT, user(original));
        assertEquals(CanonicalSealerState.RecordOutcome.Kind.DA_LAG_REJECT, user(restored));
        assertEquals(original.canonicalCount(), restored.canonicalCount());
    }

    @Test
    void restoring_another_budget_admits_an_originally_refused_log_entry() {
        CanonicalSealerState original = state(3);
        CanonicalSealerState changed = restore(original.takeSnapshot(), 10_000);
        assertEquals(CanonicalSealerState.RecordOutcome.Kind.DA_LAG_REJECT, user(original));
        assertEquals(CanonicalSealerState.RecordOutcome.Kind.RELAYED, user(changed));
        assertNotEquals(original.canonicalCount(), changed.canonicalCount());
    }

    @Test
    void restoring_a_smaller_budget_refuses_an_originally_admitted_log_entry() {
        CanonicalSealerState original = state(10_000);
        CanonicalSealerState changed = restore(original.takeSnapshot(), 3);
        assertEquals(CanonicalSealerState.RecordOutcome.Kind.RELAYED, user(original));
        assertEquals(CanonicalSealerState.RecordOutcome.Kind.DA_LAG_REJECT, user(changed));
        assertNotEquals(original.canonicalCount(), changed.canonicalCount());
    }
}
