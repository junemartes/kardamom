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
 * The decision rules: a state starts on version 2 and stores the
 * activation of {@link DecisionRules#FEATURE_SEALER_V3} for the epoch that
 * the record fronts. The rules change only at a block boundary: once the
 * first block of the epoch of the newest activation is sealed, a boundary
 * at or after its time sets version 3 for the next block, and a boundary
 * before it sets version 2. The snapshot carries the rules only when the
 * state holds an activation.
 */
class DecisionRulesTest {

    private static final long FEATURE = DecisionRules.FEATURE_SEALER_V3;

    /** A state at L1 origin {@code origin}: one epoch ordered, in an open block. */
    private static CanonicalSealerState atOrigin(final long origin) {
        final CanonicalSealerState state = new CanonicalSealerState(8);
        order(state, origin);
        return state;
    }

    /** Order the epoch {@code origin}. It closes the open block when that block holds a record. */
    private static void order(final CanonicalSealerState state, final long origin) {
        state.onOriginRecord(id(100 + (int) origin), origin, 1, payload("epoch"), 0L);
    }

    /** Close the open block at {@code ms}, and return the version of the next block. */
    private static int tick(final CanonicalSealerState state, final long ms) {
        state.onTick(ms);
        return state.decisionVersion();
    }

    private static int snapshotVersion(final byte[] snapshot) {
        return ByteBuffer.wrap(snapshot).getInt(Integer.BYTES);
    }

    private static Outcome activate(final CanonicalSealerState state, final long origin, final long atMs) {
        return state.onFeatureActivation(new FeatureActivation(origin, FEATURE, atMs));
    }

    /** Store the activation of epoch {@code origin}, order the epoch, and close its first block at {@code ms}. */
    private static int activateAndSeal(final CanonicalSealerState state, final long origin, final long atMs,
            final long ms) {
        assertEquals(Outcome.STORED, activate(state, origin, atMs));
        order(state, origin);
        return tick(state, ms);
    }

    @Test
    void a_state_starts_on_version_two_and_writes_snapshot_version_ten() {
        final CanonicalSealerState state = atOrigin(4);
        assertEquals(2, tick(state, 1_000L), "no activation, no switch");
        assertEquals(10, snapshotVersion(state.takeSnapshot()));
        assertEquals(new DecisionRules.View(2, 0L, 0L), state.decisionRules());
        assertFalse(state.reportsDecisionRules(), "a state with no activation reports no rules");
    }

    @Test
    void a_record_only_stores_its_schedule() {
        final CanonicalSealerState state = atOrigin(4);
        assertEquals(Outcome.STORED, activate(state, 5, 0L));
        assertEquals(2, state.decisionVersion());
        assertEquals(new DecisionRules.View(2, 5L, 0L), state.decisionRules());
        assertEquals(CanonicalSealerState.RULES_SNAPSHOT_VERSION, snapshotVersion(state.takeSnapshot()));
        assertTrue(state.reportsDecisionRules());
    }

    /**
     * The deposit runs in the first block of its epoch, so the rules switch
     * at the boundary that closes that block: the next block runs version 3.
     */
    @Test
    void an_activation_at_once_switches_at_the_boundary_of_the_first_block_of_its_epoch() {
        final CanonicalSealerState state = atOrigin(4);
        assertEquals(Outcome.STORED, activate(state, 5, 0L));
        assertEquals(2, tick(state, 1_000L), "the epoch of the activation is not sealed yet");
        order(state, 5);
        assertEquals(2, state.decisionVersion(), "the block of the deposit runs the rules of the block before");
        assertEquals(3, tick(state, 1_250L));
    }

    @Test
    void an_activation_with_a_time_ahead_switches_at_the_first_boundary_at_or_after_its_time() {
        final CanonicalSealerState state = atOrigin(4);
        assertEquals(2, activateAndSeal(state, 5, 5_000L, 1_000L));
        assertEquals(2, tick(state, 4_750L));
        assertEquals(3, tick(state, 5_000L), "the next block executes with 5 000");
    }

    @Test
    void a_later_activation_with_a_time_ahead_switches_the_rules_off_and_its_time_on_again() {
        final CanonicalSealerState state = atOrigin(4);
        assertEquals(3, activateAndSeal(state, 5, 0L, 1_000L), "on");
        assertEquals(Outcome.STORED, activate(state, 6, 9_000L));
        assertEquals(3, state.decisionVersion(), "the record switches nothing");
        order(state, 6);
        assertEquals(3, state.decisionVersion(), "the block of the deposit still runs version 3");
        assertEquals(2, tick(state, 2_000L), "off from the next block");
        assertEquals(new DecisionRules.View(2, 6L, 9_000L), state.decisionRules());
        assertTrue(state.reportsDecisionRules(), "a state with an activation reports version 2");
        assertEquals(2, tick(state, 8_750L));
        assertEquals(3, tick(state, 9_000L), "on again at the time");
    }

    @Test
    void a_time_of_two_to_the_sixty_four_minus_one_switches_off_for_good_until_a_later_activation() {
        final CanonicalSealerState state = atOrigin(4);
        activateAndSeal(state, 5, 0L, 1_000L);
        assertEquals(2, activateAndSeal(state, 6, -1L, 2_000L), "off for good");
        assertEquals(2, tick(state, Long.MAX_VALUE - CanonicalSealerState.TICK_INTERVAL_MS), "times compare unsigned");
        assertEquals(3, activateAndSeal(state, 7, 0L, Long.MAX_VALUE), "on again at once");
        assertEquals(new DecisionRules.View(3, 7L, 0L), state.decisionRules());
    }

    @Test
    void an_activation_with_a_time_in_the_past_keeps_the_rules_on() {
        final CanonicalSealerState state = atOrigin(4);
        activateAndSeal(state, 5, 0L, 1_000L);
        assertEquals(3, activateAndSeal(state, 6, 500L, 2_000L));
        assertEquals(new DecisionRules.View(3, 6L, 500L), state.decisionRules());
    }

    /**
     * A record of the next epoch can come while the first block of the
     * epoch before is still open. Each activation decides from the boundary
     * that seals its own epoch.
     */
    @Test
    void a_newer_activation_waits_for_its_epoch_while_the_one_before_takes_effect() {
        final CanonicalSealerState state = atOrigin(4);
        activate(state, 5, 0L);
        order(state, 5);
        assertEquals(Outcome.STORED, activate(state, 6, -1L), "the block of epoch 5 is still open");
        order(state, 6);
        assertEquals(3, state.decisionVersion(), "the boundary before epoch 6 sealed epoch 5");
        assertEquals(2, tick(state, 2_000L), "the boundary that seals epoch 6 switches off");
    }

    @Test
    void another_feature_changes_nothing() {
        final CanonicalSealerState state = atOrigin(4);
        assertEquals(Outcome.IGNORED, state.onFeatureActivation(new FeatureActivation(5, 1L, 0L)));
        assertEquals(10, snapshotVersion(state.takeSnapshot()), "the state holds no activation");
    }

    @Test
    void only_the_epoch_that_the_record_fronts_counts() {
        final CanonicalSealerState state = atOrigin(4);
        assertEquals(Outcome.IGNORED, activate(state, 3, 0L), "an older epoch");
        assertEquals(Outcome.IGNORED, activate(state, 6, 0L), "an epoch past the next one");
        assertEquals(Outcome.IGNORED, activate(state, -1L, 0L), "an origin above the signed range");
        assertEquals(10, snapshotVersion(state.takeSnapshot()));
    }

    @Test
    void a_state_with_no_epoch_takes_the_activation_of_its_first_epoch() {
        final CanonicalSealerState state = new CanonicalSealerState(8);
        assertEquals(3, activateAndSeal(state, 36, 0L, 1_000L), "the first epoch can start at any L1 block");
        assertEquals(new DecisionRules.View(3, 36L, 0L), state.decisionRules());
    }

    /** The first copy was lost, and the copy of the twin comes after the epoch: the next boundary switches. */
    @Test
    void the_copy_of_the_twin_after_its_epoch_still_applies() {
        final CanonicalSealerState state = atOrigin(4);
        assertEquals(Outcome.STORED, activate(state, 4, 0L));
        assertEquals(3, tick(state, 1_000L));
    }

    @Test
    void a_copy_of_a_record_and_a_late_copy_of_an_older_one_change_nothing() {
        final CanonicalSealerState state = atOrigin(4);
        activateAndSeal(state, 5, 0L, 1_000L);
        activateAndSeal(state, 6, 9_000L, 2_000L);
        final byte[] off = state.takeSnapshot();
        assertEquals(Outcome.IGNORED, activate(state, 6, 0L), "the copy after its epoch");
        assertEquals(Outcome.IGNORED, activate(state, 5, 0L), "a late copy of the older record");
        assertArrayEquals(off, state.takeSnapshot());
    }

    @Test
    void a_state_holds_at_most_three_activations_and_drops_the_oldest() {
        final CanonicalSealerState state = new CanonicalSealerState(8);
        activate(state, 10, -1L);
        activate(state, 11, -1L);
        activate(state, 12, -1L);
        assertEquals(Outcome.STORED, activate(state, 13, 0L));
        assertEquals(state.takeSnapshot(10).length + 2 + DecisionRules.MAX_HELD * 2 * Long.BYTES,
            state.takeSnapshot().length, "three held activations");
        final CanonicalSealerState restored = CanonicalSealerState.load(state.takeSnapshot(), 8);
        assertEquals(new DecisionRules.View(2, 13L, 0L), restored.decisionRules());
        assertEquals(3, activateAndSealOnly(restored, 13));
    }

    private static int activateAndSealOnly(final CanonicalSealerState state, final long origin) {
        order(state, origin);
        return tick(state, 1_000L);
    }

    @Test
    void a_state_on_the_rules_of_the_retained_file_never_switches() {
        final CanonicalSealerState state = atOrigin(4);
        state.startOnNewestRules();
        assertEquals(3, tick(state, 1_000L));
        assertTrue(state.reportsDecisionRules());
    }

    /** A snapshot taken while an activation waits for its epoch: the restored state switches at the same boundary. */
    @Test
    void a_version_fourteen_snapshot_restores_a_waiting_activation() {
        final CanonicalSealerState state = atOrigin(4);
        activate(state, 5, 5_000L);
        final byte[] waiting = state.takeSnapshot();
        assertEquals(CanonicalSealerState.RULES_SNAPSHOT_VERSION, snapshotVersion(waiting));
        final CanonicalSealerState restored = CanonicalSealerState.load(waiting, 8);
        assertArrayEquals(waiting, restored.takeSnapshot());
        assertEquals(2, activateAndSealOnly(restored, 5));
        assertEquals(3, tick(restored, 5_000L));
        assertEquals(3, CanonicalSealerState.load(restored.takeSnapshot(), 8).decisionVersion());
    }

    @Test
    void a_version_fourteen_snapshot_between_the_switches_restores_the_rules_off() {
        final CanonicalSealerState state = atOrigin(4);
        activateAndSeal(state, 5, 0L, 1_000L);
        activateAndSeal(state, 6, 9_000L, 2_000L);
        final byte[] off = state.takeSnapshot();
        final CanonicalSealerState restored = CanonicalSealerState.load(off, 8);
        assertEquals(new DecisionRules.View(2, 6L, 9_000L), restored.decisionRules());
        assertArrayEquals(off, restored.takeSnapshot());
        assertEquals(2, tick(restored, 8_750L));
        assertEquals(3, tick(restored, 9_000L), "the restored state switches on at the same boundary");
    }

    /** A version-13 snapshot holds one activation, which applied at its record: it is the visible one. */
    @Test
    void a_version_thirteen_snapshot_restores_its_activation() {
        final CanonicalSealerState state = atOrigin(4);
        activateAndSeal(state, 5, 9_000L, 1_000L);
        final CanonicalSealerState restored = CanonicalSealerState.load(versionThirteen(state.takeSnapshot()), 8);
        assertEquals(new DecisionRules.View(2, 5L, 9_000L), restored.decisionRules());
        assertEquals(3, tick(restored, 9_000L));
    }

    /** The bytes of version 13: version 14 with one held activation, without the count byte. */
    private static byte[] versionThirteen(final byte[] fourteen) {
        final int tail = fourteen.length - 2 - 2 * Long.BYTES;
        final ByteBuffer out = ByteBuffer.allocate(fourteen.length - 1);
        out.put(fourteen, 0, tail + 1).put(fourteen, tail + 2, 2 * Long.BYTES);
        out.putInt(Integer.BYTES, 13);
        return out.array();
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
    void a_snapshot_with_an_unknown_decision_version_or_too_many_activations_is_refused() {
        final CanonicalSealerState state = atOrigin(4);
        activate(state, 5, 0L);
        final byte[] snapshot = state.takeSnapshot();
        final int tail = snapshot.length - 2 - 2 * Long.BYTES;
        final byte[] badVersion = snapshot.clone();
        badVersion[tail] = 4;
        assertThrows(IllegalArgumentException.class, () -> CanonicalSealerState.load(badVersion, 8));
        final byte[] badCount = snapshot.clone();
        badCount[tail + 1] = DecisionRules.MAX_HELD + 1;
        assertThrows(IllegalArgumentException.class, () -> CanonicalSealerState.load(badCount, 8));
    }
}
