package io.kardamom.sealer;

import static io.kardamom.sealer.SealerStateFixtures.id;
import static io.kardamom.sealer.SealerStateFixtures.payload;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kardamom.sealer.VersionSwitch.Feature;
import io.kardamom.sealer.VersionSwitch.FeatureActivation;
import io.kardamom.sealer.VersionSwitch.Outcome;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The carry window, {@link VersionSwitch#FEATURE_CARRY_WINDOW}: a flag
 * with its own schedule, taken only while version 5 runs and never on a
 * member with the retained file. Its rules apply while the flag is on and
 * version 5 runs. The snapshot is version 18 once the state holds an
 * activation of it, and every older version restores the flag off with no
 * schedule.
 */
class CarryWindowSwitchTest {

    /** The origin of the first epoch that a test state orders after version 5. */
    private static final long NEXT_EPOCH = 6;

    /** A state on version 5: version 4 at epoch 4, the carry flag at epoch 5. */
    private static CanonicalSealerState onVersionFive() {
        final CanonicalSealerState state = new CanonicalSealerState(8);
        order(state, 3);
        assertEquals(4, activateAndSeal(state, VersionSwitch.FEATURE_SEALER_V4, 4, 0L, 1_000L));
        assertEquals(5, activateAndSeal(state, VersionSwitch.FEATURE_SEALER_V5, 5, 0L, 1_250L));
        return state;
    }

    /** {@link #onVersionFive}, then the carry window at once in front of epoch 6. */
    private static CanonicalSealerState withWindow() {
        final CanonicalSealerState state = onVersionFive();
        windowAndSeal(state, NEXT_EPOCH, 0L, 1_500L);
        assertTrue(state.carryWindowRules());
        return state;
    }

    private static void order(final CanonicalSealerState state, final long origin) {
        state.onOriginRecord(id(100 + (int) origin), origin, 1, payload("epoch"), 0L);
    }

    private static int tick(final CanonicalSealerState state, final long ms) {
        state.onTick(ms);
        return state.decisionVersion();
    }

    private static Outcome activate(
            final CanonicalSealerState state, final long feature, final long origin, final long atMs) {
        return state.onFeatureActivation(new FeatureActivation(origin, feature, atMs));
    }

    private static int activateAndSeal(final CanonicalSealerState state, final long feature, final long origin,
            final long atMs, final long ms) {
        assertEquals(Outcome.STORED, activate(state, feature, origin, atMs));
        order(state, origin);
        return tick(state, ms);
    }

    private static int windowAndSeal(final CanonicalSealerState state, final long origin, final long atMs,
            final long ms) {
        return activateAndSeal(state, VersionSwitch.FEATURE_CARRY_WINDOW, origin, atMs, ms);
    }

    private static Feature window(final CanonicalSealerState state) {
        return state.features().get(2);
    }

    private static int snapshotVersion(final byte[] snapshot) {
        return ByteBuffer.wrap(snapshot).getInt(Integer.BYTES);
    }

    @Test
    void the_carry_window_is_early_below_version_five() {
        final CanonicalSealerState state = new CanonicalSealerState(8);
        order(state, 3);
        assertEquals(Outcome.EARLY, activate(state, VersionSwitch.FEATURE_CARRY_WINDOW, 4, 0L), "version 3");
        assertEquals(4, activateAndSeal(state, VersionSwitch.FEATURE_SEALER_V4, 4, 0L, 1_000L));
        assertEquals(Outcome.EARLY, activate(state, VersionSwitch.FEATURE_CARRY_WINDOW, 5, 0L), "version 4");
        assertEquals(CanonicalSealerState.SWITCH_SNAPSHOT_VERSION, snapshotVersion(state.takeSnapshot()));
        assertEquals(Feature.NONE, window(state).state());
    }

    @Test
    void the_carry_window_turns_on_at_the_boundary_that_seals_its_epoch() {
        final CanonicalSealerState state = onVersionFive();
        assertEquals(Outcome.STORED, activate(state, VersionSwitch.FEATURE_CARRY_WINDOW, NEXT_EPOCH, 0L));
        assertEquals(Feature.HELD_OFF, window(state).state());
        assertFalse(state.carryWindowRules(), "the epoch is not sealed yet");
        order(state, NEXT_EPOCH);
        assertFalse(state.carryWindowRules(), "the block of the deposit runs the rules of the block before");
        assertEquals(5, tick(state, 1_500L));
        assertTrue(state.carryWindowRules());
        assertEquals(new Feature(VersionSwitch.FEATURE_CARRY_WINDOW, Feature.ON, NEXT_EPOCH, 1_250L), window(state));
    }

    @Test
    void the_carry_window_with_a_time_ahead_turns_off_and_on_again_at_its_time() {
        final CanonicalSealerState state = withWindow();
        windowAndSeal(state, NEXT_EPOCH + 1, 9_000L, 1_750L);
        assertFalse(state.carryWindowRules(), "off from the next block");
        assertEquals(Feature.HELD_OFF, window(state).state());
        tick(state, 8_750L);
        assertFalse(state.carryWindowRules());
        tick(state, 9_000L);
        assertTrue(state.carryWindowRules(), "on again at the time");
    }

    @Test
    void a_carry_window_at_two_to_the_sixty_four_minus_one_stays_off() {
        final CanonicalSealerState state = withWindow();
        windowAndSeal(state, NEXT_EPOCH + 1, -1L, 1_750L);
        tick(state, Long.MAX_VALUE - CanonicalSealerState.TICK_INTERVAL_MS);
        assertFalse(state.carryWindowRules(), "times compare unsigned");
    }

    /** The rules stop with version 5. The flag keeps its value, so the rules come back with version 5. */
    @Test
    void the_rules_of_the_carry_window_follow_version_five() {
        final CanonicalSealerState state = withWindow();
        assertEquals(4, activateAndSeal(state, VersionSwitch.FEATURE_SEALER_V5, NEXT_EPOCH + 1, 5_000L, 1_750L));
        assertFalse(state.carryWindowRules(), "version 4 has no carry window");
        assertEquals(Feature.ON, window(state).state(), "the flag keeps its value");
        assertEquals(5, tick(state, 5_000L));
        assertTrue(state.carryWindowRules(), "version 5 brings the rules back");
    }

    @Test
    void a_member_with_the_retained_file_never_takes_the_carry_window() {
        final CanonicalSealerState state = onVersionFive();
        state.fileStore(true);
        assertEquals(Outcome.UNSUPPORTED, activate(state, VersionSwitch.FEATURE_CARRY_WINDOW, NEXT_EPOCH, 0L));
        order(state, NEXT_EPOCH);
        tick(state, 1_500L);
        assertFalse(state.carryWindowRules());
        assertEquals(Feature.NONE, window(state).state());
        assertEquals(CanonicalSealerState.CARRY_SNAPSHOT_VERSION, state.snapshotVersion());
        final CanonicalSealerState untouched = onVersionFive();
        order(untouched, NEXT_EPOCH);
        tick(untouched, 1_500L);
        assertArrayEquals(untouched.takeSnapshotWithRetainedFile(), state.takeSnapshotWithRetainedFile(),
            "the record changes no snapshot byte");
    }

    /** Version 18 is version 16 with the flag and the schedule of the carry window after it. */
    @Test
    void a_state_with_the_carry_window_writes_version_eighteen_and_restores_it() {
        final CanonicalSealerState state = withWindow();
        assertEquals(Outcome.STORED, activate(state, VersionSwitch.FEATURE_CARRY_WINDOW, NEXT_EPOCH + 1, 7_000L));
        final byte[] v18 = state.takeSnapshot();
        assertEquals(CanonicalSealerState.WINDOW_SNAPSHOT_VERSION, snapshotVersion(v18));
        final byte[] v16 = state.takeSnapshot(CanonicalSealerState.CARRY_SNAPSHOT_VERSION);
        final byte[] section = new byte[] {1, 2, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 4, (byte) 0xE2,
            0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0x1B, 0x58};
        assertEquals(v16.length + section.length, v18.length);
        ByteBuffer.wrap(v16).putInt(Integer.BYTES, CanonicalSealerState.WINDOW_SNAPSHOT_VERSION);
        assertArrayEquals(v16, Arrays.copyOf(v18, v16.length), "version 18 starts with version 16");
        assertArrayEquals(section, Arrays.copyOfRange(v18, v16.length, v18.length));
        final CanonicalSealerState restored = CanonicalSealerState.load(v18, 8);
        assertTrue(restored.carryWindowRules());
        assertEquals(state.features(), restored.features());
        assertArrayEquals(v18, restored.takeSnapshot());
    }

    /**
     * Every version below 18 restores the carry window off with no
     * schedule. A state of a file member, version 12 or 17, then answers
     * the carry window with {@link Outcome#UNSUPPORTED}.
     */
    @Test
    void each_older_version_restores_the_carry_window_off() {
        final CanonicalSealerState state = onVersionFive();
        List.of(10, 11, 12, 14, 15, 16, 17).forEach(version -> {
            final CanonicalSealerState restored = CanonicalSealerState.load(state.takeSnapshot(version), 8);
            assertFalse(restored.carryWindowRules(), "version " + version);
            assertEquals(Feature.NONE, window(restored).state(), "version " + version);
        });
        List.of(12, 17).forEach(version -> {
            final CanonicalSealerState file = CanonicalSealerState.load(state.takeSnapshot(version), 8);
            file.fileStore(true);
            assertEquals(Outcome.UNSUPPORTED, activate(file, VersionSwitch.FEATURE_CARRY_WINDOW, 5, 0L),
                "version " + version);
        });
    }

    @Test
    void a_bad_carry_window_flag_does_not_load() {
        final byte[] v18 = withWindow().takeSnapshot();
        final int flagAt = v18.length - (Byte.BYTES + Byte.BYTES + 2 * Long.BYTES);
        v18[flagAt] = 2;
        assertThrows(IllegalArgumentException.class, () -> CanonicalSealerState.load(v18, 8));
    }
}
