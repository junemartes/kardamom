package io.kardamom.sealer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The seed file is a contract with {@code kardamom-reconstruct}. The golden
 * bytes are the ones the Rust tests {@code the_seed_layout_matches_the_golden_bytes}
 * and {@code a_seed_with_rules_matches_the_golden_bytes} write, so a change on
 * one side fails the other side's test.
 */
class SealerSeedTest {

    /** The seed file the Rust encoder writes for its golden seed. */
    static final byte[] GOLDEN = HexFormat.of().parseHex(
        "4b534544"
            + "00000001"
            + "0000000000064aba"
            + "0000000000000007"
            + "0000000000000013"
            + "0000018bcfe568fa"
            + "000000000000002a"
            + "1111111111111111111111111111111111111111111111111111111111111111"
            + "00000002"
            + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            + "0000000000000003"
            + "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            + "0000000000000001");

    /**
     * The rules section of the seed the Rust encoder writes for its golden
     * seed with rules, {@code GOLDEN_RULES} in {@code seed_tests.rs}: a
     * carry-born chain at origin 42 that sent feature 5 with a time ahead
     * at origin 40.
     */
    static final String GOLDEN_RULES = "05"
        + "000000000000002a"
        + "01" + "0000000000000000" + "0000000000000001"
        + "01" + "0000000000000000" + "0000000000000001"
        + "00"
        + "01" + "0000000000000028" + "0000018bcfe57b88";

    /** The golden seed with version 2 and the rules section after the senders. */
    static final byte[] GOLDEN_WITH_RULES = withRules();

    private static byte[] withRules() {
        final byte[] header = GOLDEN.clone();
        ByteBuffer.wrap(header).putInt(4, SealerSeed.RULES_VERSION);
        final byte[] rules = HexFormat.of().parseHex(GOLDEN_RULES);
        final byte[] out = Arrays.copyOf(header, header.length + rules.length);
        System.arraycopy(rules, 0, out, header.length, rules.length);
        return out;
    }

    private static byte[] filled(int len, int b) {
        final byte[] out = new byte[len];
        Arrays.fill(out, (byte) b);
        return out;
    }

    @Test
    void the_golden_seed_parses_to_the_values_rust_wrote() throws Exception {
        final SealerSeed seed = SealerSeed.parse(GOLDEN);
        assertEquals(new SealerSeed.Head(412_346L, 7L, 19L, 1_700_000_000_250L, 42L), seed.head());
        assertArrayEquals(filled(32, 0x11), seed.stateRoot());
        assertEquals(2, seed.senders().size());
        assertArrayEquals(filled(20, 0xAA), seed.senders().get(0).address());
        assertEquals(3L, seed.senders().get(0).nextNonce());
        assertArrayEquals(filled(20, 0xBB), seed.senders().get(1).address());
        assertEquals(1L, seed.senders().get(1).nextNonce());
        assertArrayEquals(MessageDigest.getInstance("SHA-256").digest(GOLDEN), seed.digest());
        assertEquals(64, seed.digestHex().length());
    }

    @Test
    void a_seed_of_version_one_holds_the_start_rules() {
        final VersionSwitch rules = SealerSeed.parse(GOLDEN).rules();
        assertEquals(3, rules.version());
        assertFalse(rules.holdsActivation());
        assertFalse(rules.holdsWindowActivation());
    }

    /** The rules that Rust wrote parse to the switch that the same records give the Java switch. */
    @Test
    void the_golden_seed_with_rules_parses_to_the_switch_of_the_same_records() {
        final SealerSeed seed = SealerSeed.parse(GOLDEN_WITH_RULES);
        assertEquals(new SealerSeed.Head(412_346L, 7L, 19L, 1_700_000_000_250L, 42L), seed.head());
        assertEquals(2, seed.senders().size());
        final VersionSwitch rules = seed.rules();
        assertEquals(5, rules.version());
        assertFalse(rules.carryWindowRules());
        assertEquals(List.of(
            new VersionSwitch.Feature(3, VersionSwitch.Feature.ON, 0L, 1L),
            new VersionSwitch.Feature(4, VersionSwitch.Feature.ON, 0L, 1L),
            new VersionSwitch.Feature(5, VersionSwitch.Feature.HELD_OFF, 40L, 1_700_000_005_000L)), rules.features());

        final VersionSwitch same = VersionSwitch.genesis(GenesisFeatures.CARRY_WINDOW);
        assertEquals(VersionSwitch.Outcome.STORED, same.onActivation(new VersionSwitch.FeatureActivation(
            40L, VersionSwitch.FEATURE_CARRY_WINDOW, 1_700_000_005_000L), 39L, 1_700_000_000_000L));
        same.atBoundary(42L, 1_700_000_000_000L, 1_700_000_000_250L);
        final ByteBuffer written = ByteBuffer.allocate(
            same.snapshotLen() + same.carrySnapshotLen() + same.windowSnapshotLen());
        same.writeTo(written);
        same.writeCarryTo(written);
        same.writeWindowTo(written);
        assertEquals(GOLDEN_RULES, HexFormat.of().formatHex(written.array()));
    }

    @Test
    void a_seeded_state_takes_a_copy_of_the_rules() {
        final SealerSeed seed = SealerSeed.parse(GOLDEN_WITH_RULES);
        final CanonicalSealerState first = CanonicalSealerState.seeded(seed, 64, VoidLedger.Config.DISABLED,
            CanonicalSealerState.DEFAULT_INCLUSION_HORIZON_BLOCKS, CanonicalSealerState.DEFAULT_ORDERING_WINDOW,
            LagBudgets.DEFAULT);
        assertEquals(5, first.decisionVersion());
        first.onTick(1_700_000_006_000L);
        assertTrue(first.carryWindowRules(), "the time of the activation passed");
        assertFalse(seed.rules().carryWindowRules(), "the seed keeps its rules");
        assertEquals(CanonicalSealerState.WINDOW_SNAPSHOT_VERSION,
            ByteBuffer.wrap(first.takeSnapshot()).getInt(Integer.BYTES));
    }

    @Test
    void a_seed_with_no_sender_parses() {
        final byte[] header = Arrays.copyOf(GOLDEN, SealerSeed.HEADER_LEN);
        ByteBuffer.wrap(header).putInt(SealerSeed.HEADER_LEN - 4, 0);
        assertTrue(SealerSeed.parse(header).senders().isEmpty());
    }

    @Test
    void a_damaged_seed_is_refused() {
        assertRefused("magic", copyWithInt(0, 0x4B53_4541));
        assertRefused("version", copyWithInt(4, 3));
        assertRefused("version", copyWithInt(4, 0));
        assertRefused("rules end early", Arrays.copyOf(GOLDEN_WITH_RULES, GOLDEN_WITH_RULES.length - 1));
        assertRefused("after its rules", Arrays.copyOf(GOLDEN_WITH_RULES, GOLDEN_WITH_RULES.length + 1));
        assertRefused("rules end early", withVersion(GOLDEN, SealerSeed.RULES_VERSION));
        assertRefused("decision version", withRulesByte(0, 6));
        assertRefused("activation count", withRulesByte(9, 4));
        assertRefused("at least", Arrays.copyOf(GOLDEN, SealerSeed.HEADER_LEN - 1));
        assertRefused("2 senders", Arrays.copyOf(GOLDEN, GOLDEN.length - 1));
        assertRefused("2 senders", Arrays.copyOf(GOLDEN, GOLDEN.length + 1));
        assertRefused("3 senders", copyWithInt(SealerSeed.HEADER_LEN - 4, 3));
    }

    @Test
    void a_seed_at_genesis_or_past_a_signed_long_is_refused() {
        assertRefused("seed block", copyWithLong(16, 0L));
        assertRefused("seed block", copyWithLong(16, Long.MAX_VALUE));
        assertRefused("endTxIdx", copyWithLong(24, -1L));
        assertRefused("nextNonce", copyWithLong(SealerSeed.HEADER_LEN + 20, -1L));
    }

    private static byte[] withVersion(final byte[] seed, final int version) {
        final byte[] out = seed.clone();
        ByteBuffer.wrap(out).putInt(4, version);
        return out;
    }

    /** {@link #GOLDEN_WITH_RULES} with byte {@code offset} of its rules set to {@code value}. */
    private static byte[] withRulesByte(final int offset, final int value) {
        final byte[] out = GOLDEN_WITH_RULES.clone();
        out[GOLDEN.length + offset] = (byte) value;
        return out;
    }

    private static byte[] copyWithInt(int offset, int value) {
        final byte[] out = GOLDEN.clone();
        ByteBuffer.wrap(out).putInt(offset, value);
        return out;
    }

    private static byte[] copyWithLong(int offset, long value) {
        final byte[] out = GOLDEN.clone();
        ByteBuffer.wrap(out).putLong(offset, value);
        return out;
    }

    private static void assertRefused(String expected, byte[] file) {
        final IllegalArgumentException e =
            assertThrows(IllegalArgumentException.class, () -> SealerSeed.parse(file));
        assertTrue(e.getMessage().contains(expected), e.getMessage());
    }
}
