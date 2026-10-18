package io.kardamom.sealer;

import static io.kardamom.sealer.SealerStateFixtures.id;
import static io.kardamom.sealer.SealerStateFixtures.payload;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The sealer over the blocks of {@code crates/exec-core/testdata/feature_switch_blocks.txt}.
 * The Rust test {@code feature_switch_blocks} runs the executor rule over
 * the same blocks, and both must give the v3 column of the file. So the
 * sealer and the executor switch {@link DecisionRules#FEATURE_SEALER_V3} at
 * the same block: at once, at a time ahead, off, and on again.
 *
 * <p>The sequencer sends the feature activation in front of the epoch of
 * its deposits, and the origin record of the epoch opens the first block of
 * the epoch. A tick closes each block at its time. A block with more than
 * one deposit gets one record: the sequencer sends the last schedule of
 * the feature in the epoch, because the last {@code setFeature} wins on
 * L2. A second record of the same epoch would change nothing.</p>
 */
class FeatureSwitchBlocksTest {

    /**
     * One block of the file: its number, time, L1 origin, the times of its
     * {@code setFeature} deposits in log order ({@code "-"} for none), and v3.
     */
    private record Block(long number, long ts, long origin, String setFeature, boolean v3) {
        static Block parse(final String line) {
            final String[] fields = line.trim().split("\\s+");
            assertEquals(5, fields.length, "a block line has five fields: " + line);
            return new Block(Long.parseLong(fields[0]), Long.parseLong(fields[1]), Long.parseLong(fields[2]),
                fields[3], "1".equals(fields[4]));
        }
    }

    private final CanonicalSealerState state = new CanonicalSealerState(8);

    @Test
    void the_sealer_switches_the_feature_at_the_blocks_of_the_file() throws IOException {
        final List<Block> blocks = Files.readAllLines(Path.of(System.getProperty("kardamom.featureSwitchBlocks")))
            .stream()
            .filter(line -> !line.isBlank() && !line.startsWith("#"))
            .map(Block::parse)
            .toList();
        assertFalse(blocks.isEmpty(), "the file holds blocks");
        blocks.forEach(this::run);
    }

    /** Open the epoch of {@code block} when it is new, check its rules, and close it. */
    private void run(final Block block) {
        if (block.origin() != state.l1Origin()) {
            openEpoch(block);
        }
        assertEquals(block.v3() ? DecisionRules.NEWEST_VERSION : DecisionRules.START_VERSION,
            state.decisionVersion(), "block " + block.number());
        final Boundary boundary = state.onTick(block.ts());
        assertEquals(block.number(), boundary.blockNumber);
        assertEquals(block.ts(), boundary.l2Timestamp);
    }

    /**
     * The activation of the last deposit, if any, as the sequencer sends it,
     * then the origin record that opens the first block of the epoch.
     */
    private void openEpoch(final Block block) {
        if (!"-".equals(block.setFeature())) {
            final String[] times = block.setFeature().split(",");
            assertEquals(DecisionRules.Outcome.STORED, state.onFeatureActivation(new DecisionRules.FeatureActivation(
                block.origin(), DecisionRules.FEATURE_SEALER_V3, Long.parseUnsignedLong(times[times.length - 1]))));
        }
        final OriginOutcome outcome =
            state.onOriginRecord(id(200 + (int) block.origin()), block.origin(), 1, payload("epoch"), 0L);
        assertTrue(outcome.advance.isPresent() && outcome.advance.get().forcedBoundary().isEmpty(),
            "the origin record opens the block: the tick before closed the last one");
    }
}
