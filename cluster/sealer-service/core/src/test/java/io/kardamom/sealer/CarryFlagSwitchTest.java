package io.kardamom.sealer;

import static io.kardamom.sealer.SealerStateFixtures.id;
import static io.kardamom.sealer.SealerStateFixtures.payload;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kardamom.sealer.VersionSwitch.FeatureActivation;
import io.kardamom.sealer.VersionSwitch.Outcome;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/**
 * The switch of three versions: {@link VersionSwitch#FEATURE_SEALER_V4}
 * turns version 4 on, and {@link VersionSwitch#FEATURE_SEALER_V5}, the carry
 * flag, turns version 5 on while version 4 is on. The state takes an
 * activation of the carry flag only while version 4 or 5 runs. Each feature
 * keeps its own schedule, so a time ahead or {@code 2^64 - 1} switches each
 * one off as before. The snapshot is version 16 once the state holds an
 * activation of the carry flag.
 */
class CarryFlagSwitchTest {

    /** A state on version 4: epoch 4 holds the activation of version 4 at once, and its first block is sealed. */
    private static CanonicalSealerState onVersionFour() {
        final CanonicalSealerState state = new CanonicalSealerState(8);
        order(state, 3);
        assertEquals(Outcome.STORED, activate(state, VersionSwitch.FEATURE_SEALER_V4, 4, 0L));
        order(state, 4);
        assertEquals(4, tick(state, 1_000L));
        return state;
    }

    /** {@link #onVersionFour}, then the carry flag at once in front of epoch 5: version 5 from block 4 on. */
    private static CanonicalSealerState onVersionFive() {
        final CanonicalSealerState state = onVersionFour();
        assertEquals(5, carryAndSeal(state, 5, 0L, 1_250L));
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

    /** Store an activation of {@code feature} for epoch {@code origin}, order the epoch, and seal its first block. */
    private static int activateAndSeal(final CanonicalSealerState state, final long feature, final long origin,
            final long atMs, final long ms) {
        assertEquals(Outcome.STORED, activate(state, feature, origin, atMs));
        order(state, origin);
        return tick(state, ms);
    }

    private static int carryAndSeal(final CanonicalSealerState state, final long origin, final long atMs, final long ms) {
        return activateAndSeal(state, VersionSwitch.FEATURE_SEALER_V5, origin, atMs, ms);
    }

    private static int snapshotVersion(final byte[] snapshot) {
        return ByteBuffer.wrap(snapshot).getInt(Integer.BYTES);
    }

    @Test
    void the_versions_go_from_three_to_four_to_five_at_block_boundaries() {
        final CanonicalSealerState state = onVersionFour();
        assertEquals(Outcome.STORED, activate(state, VersionSwitch.FEATURE_SEALER_V5, 5, 0L));
        assertEquals(4, tick(state, 1_100L), "the epoch of the carry flag is not sealed yet");
        order(state, 5);
        assertEquals(4, state.decisionVersion(), "the block of the deposit runs the version of the block before");
        assertEquals(5, tick(state, 1_250L));
        assertEquals(5L, state.versionSwitch().activationOrigin(), "the view shows the newest activation");
    }

    @Test
    void the_carry_flag_without_version_four_changes_nothing() {
        final CanonicalSealerState state = new CanonicalSealerState(8);
        order(state, 3);
        assertEquals(Outcome.EARLY, activate(state, VersionSwitch.FEATURE_SEALER_V5, 4, 0L));
        order(state, 4);
        assertEquals(3, tick(state, 1_000L));
        assertEquals(CanonicalSealerState.snapshotWriteVersion(), snapshotVersion(state.takeSnapshot()));
        assertEquals(new VersionSwitch.View(3, 0L, 0L), state.versionSwitch());
    }

    /** Version 4 on later does not bring back the carry flag that came too early. */
    @Test
    void an_early_carry_flag_stays_out_after_version_four_turns_on() {
        final CanonicalSealerState state = new CanonicalSealerState(8);
        order(state, 3);
        assertEquals(Outcome.EARLY, activate(state, VersionSwitch.FEATURE_SEALER_V5, 4, 0L));
        assertEquals(Outcome.STORED, activate(state, VersionSwitch.FEATURE_SEALER_V4, 4, 0L));
        order(state, 4);
        assertEquals(4, tick(state, 1_000L));
        assertEquals(4, tick(state, 1_250L));
    }

    @Test
    void the_carry_flag_with_a_time_ahead_switches_off_and_on_again_at_its_time() {
        final CanonicalSealerState state = onVersionFive();
        assertEquals(4, carryAndSeal(state, 6, 9_000L, 1_500L), "off from the next block");
        assertEquals(4, tick(state, 8_750L));
        assertEquals(5, tick(state, 9_000L), "on again at the time");
    }

    @Test
    void a_carry_flag_at_two_to_the_sixty_four_minus_one_keeps_version_five_off() {
        final CanonicalSealerState state = onVersionFive();
        assertEquals(4, carryAndSeal(state, 6, -1L, 1_500L), "off for good");
        assertEquals(4, tick(state, Long.MAX_VALUE - CanonicalSealerState.TICK_INTERVAL_MS), "times compare unsigned");
    }

    /**
     * Version 5 needs version 4. With version 4 off, the state runs version
     * 3, and it keeps the schedule of the carry flag: version 4 on again
     * brings version 5 back at once.
     */
    @Test
    void version_five_falls_back_to_three_while_version_four_is_off() {
        final CanonicalSealerState state = onVersionFive();
        assertEquals(3, activateAndSeal(state, VersionSwitch.FEATURE_SEALER_V4, 6, 5_000L, 1_500L));
        assertEquals(3, tick(state, 4_750L));
        assertEquals(5, tick(state, 5_000L), "version 4 on again, with the carry flag on");
    }

    @Test
    void a_carry_flag_while_version_four_is_off_again_is_early() {
        final CanonicalSealerState state = onVersionFive();
        assertEquals(3, activateAndSeal(state, VersionSwitch.FEATURE_SEALER_V4, 6, -1L, 1_500L));
        order(state, 7);
        assertEquals(Outcome.EARLY, activate(state, VersionSwitch.FEATURE_SEALER_V5, 7, -1L));
        assertEquals(Outcome.IGNORED, activate(state, VersionSwitch.FEATURE_SEALER_V5, 5, 0L), "a copy");
    }

    @Test
    void a_state_with_the_carry_flag_writes_snapshot_version_sixteen_and_restores_it() {
        final CanonicalSealerState state = onVersionFour();
        assertEquals(CanonicalSealerState.SWITCH_SNAPSHOT_VERSION, snapshotVersion(state.takeSnapshot()));
        assertEquals(Outcome.STORED, activate(state, VersionSwitch.FEATURE_SEALER_V5, 5, 5_000L));
        final byte[] waiting = state.takeSnapshot();
        assertEquals(CanonicalSealerState.CARRY_SNAPSHOT_VERSION, snapshotVersion(waiting));
        final CanonicalSealerState restored = CanonicalSealerState.load(waiting, 8);
        assertArrayEquals(waiting, restored.takeSnapshot());
        order(restored, 5);
        assertEquals(4, tick(restored, 1_250L));
        assertEquals(5, tick(restored, 5_000L));
        final CanonicalSealerState again = CanonicalSealerState.load(restored.takeSnapshot(), 8);
        assertEquals(5, again.decisionVersion());
        assertEquals(restored.versionSwitch(), again.versionSwitch());
    }

    /**
     * The state section of a snapshot with a retained file is version 17:
     * the bytes of version 16 with another version. A restored state keeps
     * decision version 5 and a held activation of the carry flag. A
     * version-12 section holds no decision rules, so its state runs
     * version 3.
     */
    @Test
    void a_state_with_a_retained_file_writes_snapshot_version_seventeen_and_restores_it() {
        final CanonicalSealerState state = onVersionFive();
        assertEquals(Outcome.STORED, activate(state, VersionSwitch.FEATURE_SEALER_V5, 6, 5_000L));
        final byte[] v17 = state.takeSnapshotWithRetainedFile();
        assertEquals(CanonicalSealerState.FILE_SNAPSHOT_VERSION, snapshotVersion(v17));
        final byte[] v16 = state.takeSnapshot();
        assertEquals(CanonicalSealerState.CARRY_SNAPSHOT_VERSION, snapshotVersion(v16));
        ByteBuffer.wrap(v16).putInt(Integer.BYTES, CanonicalSealerState.FILE_SNAPSHOT_VERSION);
        assertArrayEquals(v16, v17, "version 17 has the state section of version 16");
        final CanonicalSealerState restored = CanonicalSealerState.load(v17, 8);
        assertEquals(CanonicalSealerState.FILE_SNAPSHOT_VERSION, restored.restoredSnapshotVersion());
        assertEquals(5, restored.decisionVersion());
        assertEquals(state.versionSwitch(), restored.versionSwitch());
        assertArrayEquals(v17, restored.takeSnapshotWithRetainedFile());
        final CanonicalSealerState legacy = CanonicalSealerState.load(
            state.takeSnapshot(CanonicalSealerState.RETAINED_FILE_SNAPSHOT_VERSION), 8);
        assertEquals(3, legacy.decisionVersion(), "version 12 holds no decision rules");
    }

    @Test
    void only_versions_twelve_and_seventeen_hold_a_retained_file() {
        assertEquals(List.of(12, 17), IntStream
            .rangeClosed(CanonicalSealerState.SNAPSHOT_READ_MIN_VERSION, CanonicalSealerState.snapshotReadMaxVersion())
            .filter(CanonicalSealerState::holdsRetainedFile).boxed().toList());
    }

    /** A version-15 snapshot loads, and its state then takes the carry flag. */
    @Test
    void a_version_fifteen_snapshot_loads_and_then_takes_the_carry_flag() {
        final byte[] v15 = onVersionFour().takeSnapshot();
        assertEquals(CanonicalSealerState.SWITCH_SNAPSHOT_VERSION, snapshotVersion(v15));
        final CanonicalSealerState restored = CanonicalSealerState.load(v15, 8);
        assertEquals(4, restored.decisionVersion());
        assertEquals(5, carryAndSeal(restored, 5, 0L, 1_250L));
        assertEquals(CanonicalSealerState.CARRY_SNAPSHOT_VERSION, snapshotVersion(restored.takeSnapshot()));
    }

    @Test
    void a_carry_flag_of_an_older_epoch_or_a_later_one_changes_nothing() {
        final CanonicalSealerState state = onVersionFour();
        order(state, 5);
        tick(state, 1_250L);
        order(state, 6);
        assertEquals(Outcome.MISSED, activate(state, VersionSwitch.FEATURE_SEALER_V5, 5, 0L));
        assertEquals(Outcome.IGNORED, activate(state, VersionSwitch.FEATURE_SEALER_V5, 8, 0L));
        assertEquals(4, tick(state, 1_500L));
        assertEquals(CanonicalSealerState.SWITCH_SNAPSHOT_VERSION, snapshotVersion(state.takeSnapshot()));
    }

    /** Version 5 has no record-lag guard: the executors send no recorded cursor to a member on it. */
    @Test
    void the_record_lag_guard_is_off_under_version_five() {
        final CanonicalSealerState state = new CanonicalSealerState(
            8, CanonicalSealerState.GENESIS_BLOCK_NUMBER, java.util.Set.of(), new VoidLedger.Config(16, 1L),
            CanonicalSealerState.DEFAULT_INCLUSION_HORIZON_BLOCKS, 0, new LagBudgets(0, 1));
        order(state, 3);
        activate(state, VersionSwitch.FEATURE_SEALER_V4, 4, 0L);
        order(state, 4);
        assertEquals(4, tick(state, 1_000L));
        state.onRecordedCursor(0, 0L);
        assertEquals(2L, state.onRef(0, 7, 64, id(1)));
        assertTrue(state.recordLagHalted(), "version 4 keeps the guard");
        assertEquals(CanonicalSealerState.REF_RECORD_LAG, state.onRef(0, 7, 96, id(2)));
        assertEquals(5, carryAndSeal(state, 5, 0L, 1_250L));
        assertFalse(state.recordLagHalted());
        final long next = state.canonicalCount();
        assertEquals(next, state.onCarriedRef(0, 7, 96));
    }
}
