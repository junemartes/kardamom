package io.kardamom.sealer;

import static io.kardamom.sealer.SealerStateFixtures.id;
import static io.kardamom.sealer.SealerStateFixtures.payload;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kardamom.sealer.VersionSwitch.FeatureActivation;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * The record path of decision version 5: a full reference is ordered by the
 * replay window of its ingress image, as a reference of version 4 is. It
 * carries its bytes, so the void ledger keeps nothing of it.
 */
class CarriedRefTest {

    private static final int UNIT = 1 << SessionWindows.UNIT_SHIFT;

    /** A state on version 5 from block 3, with the lag budgets {@code budgets} and the void ledger {@code voids}. */
    private static CanonicalSealerState onVersionFive(final LagBudgets budgets, final VoidLedger.Config voids) {
        final CanonicalSealerState state = new CanonicalSealerState(
            8, CanonicalSealerState.GENESIS_BLOCK_NUMBER, Set.of(), voids,
            CanonicalSealerState.DEFAULT_INCLUSION_HORIZON_BLOCKS, 0, budgets);
        state.onFeatureActivation(new FeatureActivation(1, VersionSwitch.FEATURE_SEALER_V4, 500L));
        state.onOriginRecord(id(101), 1, 1, payload("epoch"), 0L);
        state.onTick(1_000L);
        state.onFeatureActivation(new FeatureActivation(2, VersionSwitch.FEATURE_SEALER_V5, 500L));
        state.onOriginRecord(id(102), 2, 1, payload("epoch"), 0L);
        state.onTick(1_250L);
        assertEquals(5, state.decisionVersion());
        return state;
    }

    private static CanonicalSealerState onVersionFive() {
        return onVersionFive(LagBudgets.DEFAULT, VoidLedger.Config.DISABLED);
    }

    @Test
    void a_full_reference_is_ordered_once_however_many_copies_arrive() {
        final CanonicalSealerState state = onVersionFive();
        final long first = state.onCarriedRef(0, 7, 10 * UNIT);
        assertEquals(CanonicalSealerState.REF_DUPLICATE, state.onCarriedRef(0, 7, 10 * UNIT));
        assertEquals(first + 1, state.onCarriedRef(0, 7, 11 * UNIT));
        assertEquals(first + 2, state.canonicalCount());
    }

    /** A reference of version 4 and a full reference of one image share its window. */
    @Test
    void a_full_reference_and_a_reference_of_one_image_share_the_window() {
        final CanonicalSealerState state = onVersionFive();
        assertTrue(state.onRef(0, 7, 10 * UNIT, id(1)) >= 0);
        assertEquals(CanonicalSealerState.REF_DUPLICATE, state.onCarriedRef(0, 7, 10 * UNIT));
    }

    @Test
    void a_full_reference_a_window_behind_its_image_is_expired() {
        final CanonicalSealerState state = onVersionFive();
        final long far = (long) SessionWindows.DEFAULT_WIDTH * UNIT;
        assertTrue(state.onCarriedRef(0, 7, far + UNIT) >= 0);
        assertEquals(CanonicalSealerState.REF_EXPIRED, state.onCarriedRef(0, 7, UNIT));
    }

    /** A refused full reference stays out of the window, so its copy after the halt is ordered. */
    @Test
    void the_da_lag_guard_refuses_a_full_reference_without_taking_it_into_the_window() {
        final CanonicalSealerState state = onVersionFive(new LagBudgets(1, 0), VoidLedger.Config.DISABLED);
        state.onTick(1_500L);
        state.onTick(1_750L);
        assertTrue(state.daLagHalted());
        assertEquals(CanonicalSealerState.REF_DA_LAG, state.onCarriedRef(0, 7, 10 * UNIT));
        state.onPostedCursor(state.sealedHead());
        assertTrue(state.onCarriedRef(0, 7, 10 * UNIT) >= 0);
    }

    /** The void ledger holds no full reference, so no vote can remove one. */
    @Test
    void no_vote_removes_a_full_reference() {
        final CanonicalSealerState state = onVersionFive(LagBudgets.DEFAULT, new VoidLedger.Config(64, 1L));
        final long index = state.onCarriedRef(0, 7, 10 * UNIT);
        assertEquals(VoidLedger.Vote.REFUSED, state.onVoidRequest(0, index, id(1)).vote);
    }

    @Test
    void a_snapshot_keeps_the_windows_and_version_five() {
        final CanonicalSealerState state = onVersionFive();
        state.onCarriedRef(0, 7, 10 * UNIT);
        final CanonicalSealerState restored = CanonicalSealerState.load(state.takeSnapshot(), 8);
        assertEquals(5, restored.decisionVersion());
        assertEquals(CanonicalSealerState.REF_DUPLICATE, restored.onCarriedRef(0, 7, 10 * UNIT));
        assertTrue(restored.onCarriedRef(0, 7, 11 * UNIT) >= 0);
    }
}
