package io.kardamom.sealer;

import static io.kardamom.sealer.SealerStateFixtures.NO_DEADLINE;
import static io.kardamom.sealer.SealerStateFixtures.id;
import static io.kardamom.sealer.SealerStateFixtures.payload;
import static io.kardamom.sealer.SealerStateFixtures.sender;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/**
 * The sealer over the blocks of {@code crates/exec-core/testdata/feature_switch_blocks.txt}.
 * The Rust test {@code feature_switch_blocks} runs the executor rule over
 * the same blocks, and both must give the v3 column of the file. So the
 * sealer and the executor switch {@link DecisionRules#FEATURE_SEALER_V3} at
 * the same block: at once, at a time ahead, off, on again, and never for a
 * deposit of 0 in block 1.
 *
 * <p>The sequencer sends the feature activation in front of the epoch of
 * its deposits, and the origin record of the epoch opens the first block of
 * the epoch. A block with more than one deposit gets one record: the
 * sequencer sends the last schedule of the feature in the epoch, because
 * the last {@code setFeature} wins on L2. A second record of the same epoch
 * would change nothing.</p>
 *
 * <p>The test runs the blocks twice. A tick closes each block at its time;
 * or, in the second run, the origin record of the next epoch closes the
 * last block of an epoch with the boundary that it forces. The origin
 * record then runs under the rules that the forced boundary sets.</p>
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

        int version() {
            return v3 ? DecisionRules.NEWEST_VERSION : DecisionRules.START_VERSION;
        }
    }

    private static List<Block> blocks() throws IOException {
        final List<Block> blocks = Files.readAllLines(Path.of(System.getProperty("kardamom.featureSwitchBlocks")))
            .stream()
            .filter(line -> !line.isBlank() && !line.startsWith("#"))
            .map(Block::parse)
            .toList();
        assertFalse(blocks.isEmpty(), "the file holds blocks");
        return blocks;
    }

    @Test
    void the_sealer_switches_the_feature_at_the_blocks_of_the_file() throws IOException {
        final Chain chain = new Chain(false);
        blocks().forEach(chain::runTicked);
    }

    @Test
    void the_sealer_switches_alike_when_the_next_epoch_closes_the_block() throws IOException {
        final List<Block> blocks = blocks();
        final Chain chain = new Chain(true);
        IntStream.range(0, blocks.size())
            .forEach(i -> chain.run(blocks.get(i), i + 1 < blocks.size() ? Optional.of(blocks.get(i + 1)) : Optional.empty()));
    }

    /** One sealer state over the blocks, and how the blocks close. */
    private static final class Chain {
        private final CanonicalSealerState state = new CanonicalSealerState(64);
        /** Whether each block holds a record, so the next origin record forces its boundary. */
        private final boolean forced;
        private long nonce;
        /** The time of the block whose boundary the next origin record forces. */
        private long closingTs;

        Chain(final boolean forced) {
            this.forced = forced;
        }

        void runTicked(final Block block) {
            run(block, Optional.empty());
        }

        /** Open the epoch of {@code block} when it is new, check its rules, and close it. */
        void run(final Block block, final Optional<Block> next) {
            if (block.origin() != state.l1Origin()) {
                openEpoch(block);
            }
            assertEquals(block.version(), state.decisionVersion(), "block " + block.number());
            if (forced) {
                assertTrue(state.onRecord(id(1000 + (int) nonce), sender(1), nonce, NO_DEADLINE, payload("t"))
                    .relayed.isPresent());
                nonce++;
            }
            final boolean nextEpochCloses = forced && next.filter(n -> n.origin() != block.origin()).isPresent();
            if (nextEpochCloses) {
                closingTs = block.ts();
                return;
            }
            final Boundary boundary = state.onTick(block.ts());
            assertEquals(block.number(), boundary.blockNumber);
            assertEquals(block.ts(), boundary.l2Timestamp);
        }

        /**
         * The activation of the last deposit, if any, as the sequencer sends
         * it, then the origin record that opens the first block of the epoch.
         * In the forced run, the origin record closes the block before.
         */
        private void openEpoch(final Block block) {
            if (!"-".equals(block.setFeature())) {
                final String[] times = block.setFeature().split(",");
                assertEquals(DecisionRules.Outcome.STORED, state.onFeatureActivation(new DecisionRules.FeatureActivation(
                    block.origin(), DecisionRules.FEATURE_SEALER_V3, Long.parseUnsignedLong(times[times.length - 1]))));
            }
            final int recordVersion = state.decisionVersionOfOriginRecord(closingTs);
            final OriginOutcome outcome =
                state.onOriginRecord(id(200 + (int) block.origin()), block.origin(), 1, payload("epoch"), closingTs);
            assertTrue(outcome.advance.isPresent());
            final Optional<Boundary> boundary = outcome.advance.get().forcedBoundary();
            assertEquals(forced && block.number() > 1, boundary.isPresent(), "block " + block.number());
            boundary.ifPresent(b -> assertEquals(block.number() - 1, b.blockNumber));
            boundary.ifPresent(b -> assertEquals(closingTs, b.l2Timestamp));
            assertEquals(state.decisionVersion(), recordVersion, "the origin record runs under the rules of its block");
        }
    }
}
