package io.kardamom.sealer;

import static io.kardamom.sealer.SealerStateFixtures.id;
import static io.kardamom.sealer.SealerStateFixtures.payload;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kardamom.sealer.DecisionRules.FeatureActivation;
import io.kardamom.sealer.DecisionRules.Outcome;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

/**
 * The decision rules: a state starts on version 2, takes the activation of
 * {@link DecisionRules#FEATURE_SEALER_V3} for the epoch that the record
 * fronts, and switches to version 3 at the first log entry at or after the
 * activation time. The snapshot carries the rules only when the state holds
 * an activation.
 */
class DecisionRulesTest {

    private static final long FEATURE = DecisionRules.FEATURE_SEALER_V3;

    /** A state at L1 origin {@code origin}: one epoch ordered. */
    private static CanonicalSealerState atOrigin(final long origin) {
        final CanonicalSealerState state = new CanonicalSealerState(8);
        order(state, origin);
        return state;
    }

    private static void order(final CanonicalSealerState state, final long origin) {
        state.onOriginRecord(id(100 + (int) origin), origin, 1, payload("epoch"), 0L);
    }

    private static int snapshotVersion(final byte[] snapshot) {
        return ByteBuffer.wrap(snapshot).getInt(Integer.BYTES);
    }

    private static Outcome activate(final CanonicalSealerState state, final long origin, final long atMs,
            final long nowMs) {
        return state.onFeatureActivation(new FeatureActivation(origin, FEATURE, atMs), nowMs);
    }

    @Test
    void a_state_starts_on_version_two_and_writes_snapshot_version_ten() {
        final CanonicalSealerState state = atOrigin(4);
        assertEquals(2, state.decisionVersion());
        assertEquals(10, snapshotVersion(state.takeSnapshot()));
        assertFalse(state.switchRulesAt(Long.MAX_VALUE), "no activation, no switch");
        assertEquals(new DecisionRules.View(2, 0L, 0L), state.decisionRules());
    }

    @Test
    void an_activation_at_or_before_the_record_time_switches_at_the_record() {
        final CanonicalSealerState state = atOrigin(4);
        assertEquals(Outcome.SWITCHED, activate(state, 5, 0L, 1_000L));
        assertEquals(3, state.decisionVersion());
        assertFalse(state.switchRulesAt(2_000L), "the switch happens once");
        assertEquals(new DecisionRules.View(3, 5L, 0L), state.decisionRules());
    }

    @Test
    void a_scheduled_activation_switches_at_the_first_entry_at_or_after_its_time() {
        final CanonicalSealerState state = atOrigin(4);
        assertEquals(Outcome.PENDING, activate(state, 5, 5_000L, 1_000L));
        assertEquals(2, state.decisionVersion());
        assertFalse(state.switchRulesAt(4_999L));
        assertTrue(state.switchRulesAt(5_000L));
        assertEquals(3, state.decisionVersion());
    }

    @Test
    void another_feature_changes_nothing() {
        final CanonicalSealerState state = atOrigin(4);
        assertEquals(Outcome.IGNORED,
            state.onFeatureActivation(new FeatureActivation(5, 1L, 0L), 1_000L));
        assertEquals(2, state.decisionVersion());
        assertEquals(10, snapshotVersion(state.takeSnapshot()), "the state holds no activation");
    }

    @Test
    void only_the_epoch_that_the_record_fronts_counts() {
        final CanonicalSealerState state = atOrigin(4);
        assertEquals(Outcome.IGNORED, activate(state, 3, 0L, 1_000L), "an older epoch");
        assertEquals(Outcome.IGNORED, activate(state, 6, 0L, 1_000L), "an epoch past the next one");
        assertEquals(Outcome.IGNORED, activate(state, -1L, 0L, 1_000L), "an origin above the signed range");
        assertEquals(2, state.decisionVersion());
        assertEquals(10, snapshotVersion(state.takeSnapshot()));
    }

    @Test
    void the_copy_of_the_twin_after_its_epoch_still_applies() {
        final CanonicalSealerState state = atOrigin(4);
        assertEquals(Outcome.SWITCHED, activate(state, 4, 0L, 1_000L),
            "the first copy was lost, and the epoch of the upgrade is the last one ordered");
    }

    @Test
    void a_twin_copy_changes_nothing_and_a_later_upgrade_replaces_the_time() {
        final CanonicalSealerState state = atOrigin(4);
        activate(state, 5, 5_000L, 1_000L);
        final byte[] once = state.takeSnapshot();
        assertEquals(Outcome.IGNORED, activate(state, 5, 1_000L, 1_100L));
        assertArrayEquals(once, state.takeSnapshot(), "the copy of the same epoch changes nothing");
        order(state, 5);
        assertEquals(Outcome.PENDING, activate(state, 6, 9_000L, 1_200L));
        assertFalse(state.switchRulesAt(5_000L), "the later upgrade moved the time");
        assertTrue(state.switchRulesAt(9_000L));
    }

    @Test
    void the_switch_is_one_way() {
        final CanonicalSealerState state = atOrigin(4);
        activate(state, 5, 0L, 1_000L);
        assertEquals(Outcome.IGNORED, activate(state, 5, 9_000L, 1_100L));
        assertEquals(3, state.decisionVersion());
    }

    @Test
    void an_activation_time_above_the_signed_range_is_far_in_the_future() {
        final CanonicalSealerState state = atOrigin(4);
        assertEquals(Outcome.PENDING, activate(state, 5, -1L, 1_000L));
        assertFalse(state.switchRulesAt(Long.MAX_VALUE));
    }

    @Test
    void a_version_thirteen_snapshot_restores_a_pending_activation_and_a_switch() {
        final CanonicalSealerState pending = atOrigin(4);
        activate(pending, 5, 5_000L, 1_000L);
        final byte[] pendingBytes = pending.takeSnapshot();
        assertEquals(CanonicalSealerState.RULES_SNAPSHOT_VERSION, snapshotVersion(pendingBytes));
        final CanonicalSealerState restored = CanonicalSealerState.load(pendingBytes, 8);
        assertArrayEquals(pendingBytes, restored.takeSnapshot());
        assertEquals(2, restored.decisionVersion());
        assertTrue(restored.switchRulesAt(5_000L), "the restored state switches at the same time");

        final CanonicalSealerState again = CanonicalSealerState.load(restored.takeSnapshot(), 8);
        assertEquals(3, again.decisionVersion());
    }

    @Test
    void a_version_twelve_snapshot_restores_version_three() {
        final CanonicalSealerState state = atOrigin(4);
        state.startOnNewestRules();
        final CanonicalSealerState restored =
            CanonicalSealerState.load(state.takeSnapshotWithRetainedFile(), 8);
        assertEquals(3, restored.decisionVersion());
    }

    @Test
    void a_snapshot_with_an_unknown_decision_version_is_refused() {
        final CanonicalSealerState state = atOrigin(4);
        activate(state, 5, 0L, 1_000L);
        final byte[] snapshot = state.takeSnapshot();
        snapshot[snapshot.length - DecisionRules.SNAPSHOT_LEN] = 4;
        assertThrows(IllegalArgumentException.class, () -> CanonicalSealerState.load(snapshot, 8));
    }
}
