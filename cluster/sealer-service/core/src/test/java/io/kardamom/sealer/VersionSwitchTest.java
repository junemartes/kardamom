package io.kardamom.sealer;

import static io.kardamom.sealer.SealerStateFixtures.id;
import static io.kardamom.sealer.SealerStateFixtures.payload;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.kardamom.sealer.VersionSwitch.FeatureActivation;
import io.kardamom.sealer.VersionSwitch.Outcome;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

/**
 * The version switch: a state starts on version 3 and stores the
 * activation of {@link VersionSwitch#FEATURE_SEALER_V4} for the epoch that
 * the record fronts. The version changes only at a block boundary: once the
 * first block of the epoch of the newest activation is sealed, a boundary at
 * or after its time sets version 4 for the next block, and a boundary before
 * it sets version 3. The snapshot carries the switch only when the state
 * holds an activation.
 */
class VersionSwitchTest {

    private static final long FEATURE = VersionSwitch.FEATURE_SEALER_V4;

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
    private static int activateAndSeal(
            final CanonicalSealerState state, final long origin, final long atMs, final long ms) {
        assertEquals(Outcome.STORED, activate(state, origin, atMs));
        order(state, origin);
        return tick(state, ms);
    }

    @Test
    void a_state_starts_on_version_three_and_writes_snapshot_version_fourteen() {
        final CanonicalSealerState state = atOrigin(4);
        assertEquals(3, tick(state, 1_000L), "no activation, no switch");
        assertEquals(14, snapshotVersion(state.takeSnapshot()));
        assertEquals(new VersionSwitch.View(3, 0L, 0L), state.versionSwitch());
    }

    @Test
    void a_record_only_stores_its_schedule() {
        final CanonicalSealerState state = atOrigin(4);
        assertEquals(Outcome.STORED, activate(state, 5, 0L));
        assertEquals(3, state.decisionVersion());
        assertEquals(new VersionSwitch.View(3, 5L, 0L), state.versionSwitch());
        assertEquals(CanonicalSealerState.SWITCH_SNAPSHOT_VERSION, snapshotVersion(state.takeSnapshot()));
    }

    @Test
    void an_activation_at_once_switches_at_the_boundary_of_the_first_block_of_its_epoch() {
        final CanonicalSealerState state = atOrigin(4);
        assertEquals(Outcome.STORED, activate(state, 5, 0L));
        assertEquals(3, tick(state, 1_000L), "the epoch of the activation is not sealed yet");
        order(state, 5);
        assertEquals(3, state.decisionVersion(), "the block of the deposit runs the version of the block before");
        assertEquals(4, tick(state, 1_250L));
    }

    @Test
    void an_activation_with_a_time_ahead_switches_at_the_first_boundary_at_or_after_its_time() {
        final CanonicalSealerState state = atOrigin(4);
        assertEquals(3, activateAndSeal(state, 5, 5_000L, 1_000L));
        assertEquals(3, tick(state, 4_750L));
        assertEquals(4, tick(state, 5_000L), "the next block executes with 5 000");
    }

    @Test
    void a_later_activation_with_a_time_ahead_switches_off_and_on_again_at_its_time() {
        final CanonicalSealerState state = atOrigin(4);
        assertEquals(4, activateAndSeal(state, 5, 0L, 1_000L), "on");
        assertEquals(Outcome.STORED, activate(state, 6, 9_000L));
        order(state, 6);
        assertEquals(4, state.decisionVersion(), "the block of the deposit still runs version 4");
        assertEquals(3, tick(state, 2_000L), "off from the next block");
        assertEquals(3, tick(state, 8_750L));
        assertEquals(4, tick(state, 9_000L), "on again at the time");
    }

    @Test
    void a_time_of_two_to_the_sixty_four_minus_one_keeps_version_four_off() {
        final CanonicalSealerState state = atOrigin(4);
        activateAndSeal(state, 5, 0L, 1_000L);
        assertEquals(3, activateAndSeal(state, 6, -1L, 2_000L), "off for good");
        assertEquals(3, tick(state, Long.MAX_VALUE - CanonicalSealerState.TICK_INTERVAL_MS), "times compare unsigned");
        assertEquals(4, activateAndSeal(state, 7, 0L, Long.MAX_VALUE), "on again at once");
    }

    @Test
    void another_feature_changes_nothing() {
        final CanonicalSealerState state = atOrigin(4);
        assertEquals(Outcome.IGNORED, state.onFeatureActivation(new FeatureActivation(5, 2L, 0L)));
        assertEquals(14, snapshotVersion(state.takeSnapshot()), "the state holds no activation");
    }

    @Test
    void only_the_epoch_that_the_record_fronts_counts() {
        final CanonicalSealerState state = atOrigin(4);
        assertEquals(Outcome.MISSED, activate(state, 3, 0L), "an older epoch");
        assertEquals(Outcome.IGNORED, activate(state, 6, 0L), "an epoch past the next one");
        assertEquals(Outcome.IGNORED, activate(state, -1L, 0L), "an origin above the signed range");
    }

    /** Block 1 executes with time 0, so {@code setFeature(0)} in it stores 0: never, as on L2. */
    @Test
    void a_time_of_zero_in_block_one_means_never() {
        final CanonicalSealerState state = new CanonicalSealerState(8);
        assertEquals(3, activateAndSeal(state, 36, 0L, 1_000L));
        assertEquals(3, tick(state, Long.MAX_VALUE));
    }

    @Test
    void the_copy_of_the_twin_after_the_first_block_of_its_epoch_is_late() {
        final CanonicalSealerState state = atOrigin(4);
        tick(state, 500L);
        assertEquals(Outcome.STORED_LATE, activate(state, 4, 0L));
        assertEquals(4, tick(state, 1_000L));
    }

    @Test
    void a_state_holds_at_most_three_activations_and_refuses_a_fourth() {
        final CanonicalSealerState state = new CanonicalSealerState(8);
        activate(state, 10, -1L);
        activate(state, 11, -1L);
        activate(state, 12, 500L);
        assertEquals(Outcome.FULL, activate(state, 13, 0L));
        final CanonicalSealerState restored = CanonicalSealerState.load(state.takeSnapshot(), 8);
        assertEquals(new VersionSwitch.View(3, 12L, 500L), restored.versionSwitch());
    }

    @Test
    void a_version_fifteen_snapshot_restores_a_waiting_activation() {
        final CanonicalSealerState state = atOrigin(4);
        activate(state, 5, 5_000L);
        final byte[] waiting = state.takeSnapshot();
        assertEquals(CanonicalSealerState.SWITCH_SNAPSHOT_VERSION, snapshotVersion(waiting));
        final CanonicalSealerState restored = CanonicalSealerState.load(waiting, 8);
        assertArrayEquals(waiting, restored.takeSnapshot());
        order(restored, 5);
        assertEquals(3, tick(restored, 1_000L));
        assertEquals(4, tick(restored, 5_000L));
        assertEquals(4, CanonicalSealerState.load(restored.takeSnapshot(), 8).decisionVersion());
    }

    /**
     * A version-14 snapshot of the release before holds the activations of
     * {@code FEATURE_SEALER_V3} in its decision rules tail. They are not
     * activations of version 4: the restored state stays on version 3.
     */
    @Test
    void the_activations_in_a_version_fourteen_tail_do_not_switch_to_version_four() {
        final CanonicalSealerState state = atOrigin(4);
        final byte[] v14 = withHeldTailActivation(state.takeSnapshot(), 4L, 1L);
        final CanonicalSealerState restored = CanonicalSealerState.load(v14, 8);
        assertEquals(3, tick(restored, 1_000L));
        assertEquals(3, tick(restored, 2_000L));
        assertEquals(14, snapshotVersion(restored.takeSnapshot()));
    }

    @Test
    void a_snapshot_with_an_unknown_switch_version_is_refused() {
        final CanonicalSealerState state = atOrigin(4);
        activate(state, 5, 0L);
        final byte[] snapshot = state.takeSnapshot();
        final int at = switchOffset(snapshot, state);
        snapshot[at] = 2;
        assertThrows(IllegalArgumentException.class, () -> CanonicalSealerState.load(snapshot, 8));
    }

    /** The offset of the version byte of the switch section: after the decision rules tail of one held activation. */
    private static int switchOffset(final byte[] snapshot, final CanonicalSealerState state) {
        final SessionWindows empty = new SessionWindows(SessionWindows.DEFAULT_SLOTS, SessionWindows.DEFAULT_WIDTH);
        final int switchLen = Byte.BYTES + Long.BYTES + Byte.BYTES + 2 * Long.BYTES;
        return snapshot.length - empty.snapshotLen() - switchLen;
    }

    /**
     * A v14 snapshot whose decision rules tail holds one activation, as the
     * release before writes after {@code FEATURE_SEALER_V3}: the tail
     * {@code [3][sealed_origin][1][origin][time]} replaces the empty tail.
     */
    private static byte[] withHeldTailActivation(final byte[] v14, final long origin, final long time) {
        final int emptyTail = Byte.BYTES + Long.BYTES + Byte.BYTES;
        final ByteBuffer out = ByteBuffer.allocate(v14.length + 2 * Long.BYTES);
        out.put(v14, 0, v14.length - emptyTail);
        out.put((byte) 3).putLong(origin).put((byte) 1).putLong(origin).putLong(time);
        return out.array();
    }
}
