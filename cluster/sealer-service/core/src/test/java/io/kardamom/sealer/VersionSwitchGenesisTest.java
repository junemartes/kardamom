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
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * A chain that runs features from its genesis: the switch holds one
 * activation of each feature at origin 0 and time 1 ms. The feature is on
 * from the first block, a real activation replaces the genesis one, and
 * the snapshot carries the switch, so a restore keeps the rules.
 */
class VersionSwitchGenesisTest {

    /** A state at genesis with {@code features}. */
    private static CanonicalSealerState atGenesis(final GenesisFeatures features) {
        final CanonicalSealerState state = new CanonicalSealerState(8);
        state.genesisRules(features);
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

    private static int snapshotVersion(final byte[] snapshot) {
        return ByteBuffer.wrap(snapshot).getInt(Integer.BYTES);
    }

    private static CanonicalSealerState reload(final CanonicalSealerState state) {
        return CanonicalSealerState.load(state.takeSnapshot(), 8);
    }

    @Test
    void the_setting_names_only_a_prefix_of_features_three_four_and_five() {
        assertEquals(GenesisFeatures.NONE, GenesisFeatures.parse(null));
        assertEquals(GenesisFeatures.NONE, GenesisFeatures.parse(""));
        assertEquals(GenesisFeatures.SEALER_V4, GenesisFeatures.parse("3"));
        assertEquals(GenesisFeatures.SEALER_V5, GenesisFeatures.parse("3,4"));
        assertEquals(GenesisFeatures.CARRY_WINDOW, GenesisFeatures.parse(" 3,4,5 "));
        List.of("4", "3,5", "5,4,3", "3, 4", "3,4,5,6", "2", "on").forEach(bad -> {
            final IllegalArgumentException e =
                assertThrows(IllegalArgumentException.class, () -> GenesisFeatures.parse(bad));
            assertTrue(e.getMessage().contains(GenesisFeatures.SETTING), e.getMessage());
        });
    }

    @Test
    void the_first_block_runs_version_five_with_the_carry_window() {
        final CanonicalSealerState state = atGenesis(GenesisFeatures.CARRY_WINDOW);
        assertEquals(5, state.decisionVersion());
        assertTrue(state.carryWindowRules());
        assertEquals(5, tick(state, 1_000L), "the first boundary keeps the rules");
        assertTrue(state.carryWindowRules());
        order(state, 7);
        assertEquals(5, tick(state, 1_250L), "an epoch keeps the rules");
        assertEquals(List.of(new Feature(3, Feature.ON, 0L, 1L), new Feature(4, Feature.ON, 0L, 1L),
            new Feature(5, Feature.ON, 0L, 1L)), state.features());
        assertEquals(CanonicalSealerState.WINDOW_SNAPSHOT_VERSION, snapshotVersion(state.takeSnapshot()));
    }

    @Test
    void each_set_starts_its_version_and_writes_the_snapshot_that_holds_it() {
        final CanonicalSealerState none = atGenesis(GenesisFeatures.NONE);
        assertEquals(3, tick(none, 1_000L));
        assertEquals(14, snapshotVersion(none.takeSnapshot()));
        final CanonicalSealerState four = atGenesis(GenesisFeatures.SEALER_V4);
        assertEquals(4, tick(four, 1_000L));
        assertEquals(CanonicalSealerState.SWITCH_SNAPSHOT_VERSION, snapshotVersion(four.takeSnapshot()));
        final CanonicalSealerState five = atGenesis(GenesisFeatures.SEALER_V5);
        assertEquals(5, tick(five, 1_000L));
        assertFalse(five.carryWindowRules());
        assertEquals(CanonicalSealerState.CARRY_SNAPSHOT_VERSION, snapshotVersion(five.takeSnapshot()));
    }

    @Test
    void a_restore_keeps_the_genesis_rules_and_the_snapshot_bytes() {
        final CanonicalSealerState state = atGenesis(GenesisFeatures.CARRY_WINDOW);
        tick(state, 1_000L);
        final CanonicalSealerState restored = reload(state);
        assertEquals(5, restored.decisionVersion());
        assertTrue(restored.carryWindowRules());
        assertEquals(state.features(), restored.features());
        assertArrayEquals(state.takeSnapshot(), restored.takeSnapshot());
        order(state, 7);
        order(restored, 7);
        assertEquals(tick(state, 1_250L), tick(restored, 1_250L));
        assertArrayEquals(state.takeSnapshot(), restored.takeSnapshot());
    }

    /** A real activation of feature 5 with a time ahead turns the carry window off until that time. */
    @Test
    void a_later_activation_turns_the_carry_window_off_then_on() {
        final CanonicalSealerState state = atGenesis(GenesisFeatures.CARRY_WINDOW);
        order(state, 6);
        tick(state, 1_000L);
        assertEquals(Outcome.STORED, activate(state, VersionSwitch.FEATURE_CARRY_WINDOW, 7, 5_000L));
        order(state, 7);
        assertEquals(5, tick(state, 1_250L));
        assertFalse(state.carryWindowRules(), "the visible activation has a time ahead");
        assertEquals(new Feature(5, Feature.HELD_OFF, 7L, 5_000L), state.features().get(2));
        assertEquals(5, tick(state, 4_750L));
        assertFalse(state.carryWindowRules());
        assertEquals(5, tick(state, 5_000L));
        assertTrue(state.carryWindowRules(), "on again at its time");
        assertEquals(CanonicalSealerState.WINDOW_SNAPSHOT_VERSION, snapshotVersion(state.takeSnapshot()));
    }

    /** Feature 4 off stops version 5: the chain runs version 4, and the rules of the carry window stop. */
    @Test
    void feature_four_off_runs_version_four() {
        final CanonicalSealerState state = atGenesis(GenesisFeatures.CARRY_WINDOW);
        order(state, 6);
        tick(state, 1_000L);
        assertEquals(Outcome.STORED, activate(state, VersionSwitch.FEATURE_SEALER_V5, 7, -1L));
        order(state, 7);
        assertEquals(4, tick(state, 1_250L));
        assertFalse(state.carryWindowRules(), "the rules of the carry window need version 5");
        assertEquals(Feature.ON, state.features().get(2).state(), "the flag keeps its value");
        assertEquals(Outcome.STORED, activate(state, VersionSwitch.FEATURE_SEALER_V5, 8, 0L));
        order(state, 8);
        assertEquals(5, tick(state, 1_500L), "feature 4 on again at once");
        assertTrue(state.carryWindowRules());
    }

    @Test
    void a_copy_or_a_record_of_origin_zero_does_not_replace_a_genesis_activation() {
        final CanonicalSealerState state = atGenesis(GenesisFeatures.CARRY_WINDOW);
        assertEquals(Outcome.IGNORED, activate(state, VersionSwitch.FEATURE_SEALER_V4, 0, -1L));
        assertEquals(5, tick(state, 1_000L));
    }

    @Test
    void the_switch_holds_each_genesis_feature_for_all_time() {
        final VersionSwitch rules = VersionSwitch.genesis(GenesisFeatures.SEALER_V5);
        assertTrue(rules.holdsEach(GenesisFeatures.SEALER_V5));
        assertTrue(rules.holdsEach(GenesisFeatures.SEALER_V4));
        assertFalse(rules.holdsEach(GenesisFeatures.CARRY_WINDOW));
        assertTrue(VersionSwitch.start().holdsEach(GenesisFeatures.NONE));
        assertFalse(VersionSwitch.start().holdsEach(GenesisFeatures.SEALER_V4));
    }

    /** A member with the retained file holds no carry window: it drops one that it gets. */
    @Test
    void a_file_member_drops_the_carry_window() {
        final CanonicalSealerState state = atGenesis(GenesisFeatures.CARRY_WINDOW);
        state.fileStore(true);
        assertEquals(5, tick(state, 1_000L));
        assertFalse(state.carryWindowRules());
        assertEquals(Feature.NONE, state.features().get(2).state());
        assertEquals(CanonicalSealerState.CARRY_SNAPSHOT_VERSION, snapshotVersion(state.takeSnapshot()));
    }
}
