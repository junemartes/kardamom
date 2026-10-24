package io.kardamom.sealer;

import static io.kardamom.sealer.SealerStateFixtures.id;
import static io.kardamom.sealer.SealerStateFixtures.payload;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kardamom.sealer.VersionSwitch.FeatureActivation;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * The record path of decision version 4: a transaction reference is ordered
 * by the replay window of its ingress image, not by its id.
 */
class RefPathTest {

    private static final int UNIT = 1 << SessionWindows.UNIT_SHIFT;

    /** A state on version 4 from block 2, with the lag budgets {@code budgets}. */
    private static CanonicalSealerState onVersionFour(final LagBudgets budgets, final VoidLedger.Config voids) {
        final CanonicalSealerState state = new CanonicalSealerState(
            8, CanonicalSealerState.GENESIS_BLOCK_NUMBER, Set.of(), voids,
            CanonicalSealerState.DEFAULT_INCLUSION_HORIZON_BLOCKS, 0, budgets);
        state.onFeatureActivation(new FeatureActivation(1, VersionSwitch.FEATURE_SEALER_V4, 500L));
        state.onOriginRecord(id(101), 1, 1, payload("epoch"), 0L);
        state.onTick(1_000L);
        assertEquals(4, state.decisionVersion());
        return state;
    }

    private static CanonicalSealerState onVersionFour() {
        return onVersionFour(LagBudgets.DEFAULT, VoidLedger.Config.DISABLED);
    }

    @Test
    void a_reference_is_ordered_once_however_many_copies_arrive() {
        final CanonicalSealerState state = onVersionFour();
        final long first = state.onRef(0, 7, 10 * UNIT, id(1));
        assertEquals(CanonicalSealerState.REF_DUPLICATE, state.onRef(0, 7, 10 * UNIT, id(1)));
        assertEquals(first + 1, state.onRef(0, 7, 11 * UNIT, id(2)));
        assertEquals(first + 2, state.canonicalCount());
    }

    @Test
    void the_same_position_in_another_image_is_another_reference() {
        final CanonicalSealerState state = onVersionFour();
        assertTrue(state.onRef(0, 7, 10 * UNIT, id(1)) >= 0);
        assertTrue(state.onRef(1, 7, 10 * UNIT, id(2)) >= 0);
        assertTrue(state.onRef(0, 8, 10 * UNIT, id(3)) >= 0);
    }

    @Test
    void a_reference_a_window_behind_its_image_is_expired() {
        final CanonicalSealerState state = onVersionFour();
        final long far = (long) SessionWindows.DEFAULT_WIDTH * UNIT;
        assertTrue(state.onRef(0, 7, far + UNIT, id(1)) >= 0);
        assertEquals(CanonicalSealerState.REF_EXPIRED, state.onRef(0, 7, UNIT, id(2)));
    }

    /** A refused reference stays out of the window, so its copy after the halt is ordered. */
    @Test
    void the_da_lag_guard_refuses_a_reference_without_taking_it_into_the_window() {
        final CanonicalSealerState state = onVersionFour(new LagBudgets(1, 0), VoidLedger.Config.DISABLED);
        state.onTick(1_250L);
        state.onTick(1_500L);
        assertTrue(state.daLagHalted());
        assertEquals(CanonicalSealerState.REF_DA_LAG, state.onRef(0, 7, 10 * UNIT, id(1)));
        state.onPostedCursor(state.sealedHead());
        assertTrue(state.onRef(0, 7, 10 * UNIT, id(1)) >= 0);
    }

    @Test
    void a_copy_of_an_ordered_reference_stays_a_duplicate_while_a_guard_refuses() {
        final CanonicalSealerState state = onVersionFour(new LagBudgets(1, 0), VoidLedger.Config.DISABLED);
        assertTrue(state.onRef(0, 7, 10 * UNIT, id(1)) >= 0);
        state.onTick(1_250L);
        state.onTick(1_500L);
        assertEquals(CanonicalSealerState.REF_DUPLICATE, state.onRef(0, 7, 10 * UNIT, id(1)));
    }

    @Test
    void the_void_ledger_holds_a_reference_of_version_four() {
        final CanonicalSealerState state = onVersionFour(LagBudgets.DEFAULT, new VoidLedger.Config(64, 1L));
        final long index = state.onRef(0, 7, 10 * UNIT, id(1));
        final CanonicalSealerState.VoidOutcome outcome = state.onVoidRequest(0, index, id(1));
        assertEquals(VoidLedger.Vote.DECIDED, outcome.vote);
        assertTrue(outcome.relayed.isPresent());
    }

    @Test
    void a_snapshot_keeps_the_windows() {
        final CanonicalSealerState state = onVersionFour();
        state.onRef(0, 7, 10 * UNIT, id(1));
        final CanonicalSealerState restored = CanonicalSealerState.load(state.takeSnapshot(), 8);
        assertEquals(4, restored.decisionVersion());
        assertEquals(CanonicalSealerState.REF_DUPLICATE, restored.onRef(0, 7, 10 * UNIT, id(1)));
        assertTrue(restored.onRef(0, 7, 11 * UNIT, id(2)) >= 0);
    }
}
