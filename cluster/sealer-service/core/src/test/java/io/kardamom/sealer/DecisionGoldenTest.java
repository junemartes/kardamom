package io.kardamom.sealer;

import static io.kardamom.sealer.SealerStateFixtures.NO_DEADLINE;
import static io.kardamom.sealer.SealerStateFixtures.distinctSender;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kardamom.sealer.CanonicalSealerState.RecordOutcome.Kind;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.EnumMap;
import java.util.HexFormat;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

/**
 * The decisions of {@link CanonicalSealerState} over a fixed stream of
 * inputs, as one SHA-256 digest. Every member of the cluster must decide
 * alike, and a snapshot must restore the same state, so a change of the
 * record path must keep this digest.
 *
 * <p>A seeded random stream drives a small window with both lag guards on
 * and three void voters. Each kind of input occurs often: relays,
 * duplicates, nonce gaps, passed deadlines, the zero sender, nonce
 * releases, void votes, origin records with and without a gap, posted and
 * recorded cursors, and ticks. A feature activation in the middle of the
 * stream switches the decision rules to version 3: before it, no release
 * applies, and after it, releases apply and the snapshot is version 13. The window fills, so it also refuses records,
 * and the least recently used sender is evicted. The test checks that every
 * record outcome occurs, so the stream keeps its coverage.
 *
 * <p>The digest covers every outcome, every boundary, and the snapshot
 * bytes every {@link #SNAPSHOT_EVERY} steps and at the end. So it also
 * changes with a change of the snapshot write version, or of the order of
 * {@link Kind}. A decision version change or a snapshot version change sets
 * a new digest on purpose: run
 * {@code SEALER_GOLDEN_UPDATE=1 ./gradlew :core:test --tests '*DecisionGoldenTest' --rerun-tasks}
 * and commit the file it writes. Gradle does not track the variable, so the
 * run needs {@code --rerun-tasks}.
 */
final class DecisionGoldenTest {

    private static final long SEED = 20_261_017L;
    private static final int STEPS = 200_000;
    /** A small window, so it fills and its senders are evicted often. */
    private static final int DEDUP_CAPACITY = 512;
    /** More senders than the window tracks, so the sender eviction runs. */
    private static final int SENDERS = 1024;
    /** The void voters 0, 1 and 2; a void needs the votes of all three. */
    private static final VoidLedger.Config VOIDS = new VoidLedger.Config(256, 0b111L);
    /** Both lag guards on, with budgets that the stream crosses now and then. */
    private static final LagBudgets BUDGETS = new LagBudgets(6L, 120L);
    /** The relayed records that a release or a void vote can name. */
    private static final int RECENT = 64;
    private static final int SNAPSHOT_EVERY = 5_000;
    /** The step of the feature activation: the middle of the stream. */
    private static final int ACTIVATION_STEP = STEPS / 2;
    private static final String GOLDEN = "decision-golden.sha256";

    @Test
    void the_record_path_decides_as_the_golden_digest_says() throws IOException {
        Stream stream = new Stream();
        String digest = HexFormat.of().formatHex(stream.run());
        stream.checkCoverage();
        // The update writes the source file; the copy on the class path is
        // the old one, so the run ends here.
        if ("1".equals(System.getenv("SEALER_GOLDEN_UPDATE"))) {
            Files.writeString(Path.of("src/test/resources", GOLDEN), digest + "\n");
            return;
        }
        assertEquals(golden(), digest,
                "the sealer's decisions or its snapshot bytes changed; a decision version or a snapshot "
                        + "version change sets the digest (see the class comment)");
    }

    private static String golden() throws IOException {
        try (InputStream in = DecisionGoldenTest.class.getResourceAsStream("/" + GOLDEN)) {
            return new String(in.readAllBytes(), StandardCharsets.US_ASCII).trim();
        }
    }

    /** One relayed record: what a release and a void vote name. */
    private record Entry(byte[] id, int sender, long nonce, long index) {
    }

    /** The seeded stream, the state it drives, and the digest of what the state decides. */
    private static final class Stream {
        private final SplittableRandom random = new SplittableRandom(SEED);
        private final CanonicalSealerState state = new CanonicalSealerState(
                DEDUP_CAPACITY, 0L, Set.of(), VOIDS, CanonicalSealerState.DEFAULT_INCLUSION_HORIZON_BLOCKS, 0, BUDGETS);
        private final MessageDigest digest = sha256();
        private final ByteBuffer scratch = ByteBuffer.allocate(64);
        private final long[] nonces = new long[SENDERS];
        private final Entry[] recent = new Entry[RECENT];
        private final Map<Kind, Integer> kinds = new EnumMap<>(Kind.class);
        private int ids;
        private int relayed;
        private long clock;
        private long l1Origin;
        private int releases;
        /** The releases that applied before the activation: none may. */
        private int releasesBeforeActivation;
        private int voids;
        private int originGaps;

        /** Relay the first recent entries, then seal them, so the cursors have a head to name. */
        Stream() {
            for (int i = 0; i < RECENT; i++) {
                relayFresh();
            }
            tick();
        }

        byte[] run() {
            for (int step = 1; step <= STEPS; step++) {
                activateAt(step);
                step(random.nextInt(1000));
                if (step % SNAPSHOT_EVERY == 0) {
                    digest.update(state.takeSnapshot());
                }
            }
            digest.update(state.takeSnapshot());
            return digest.digest();
        }

        /** Every record outcome, an applied release, a decided void and an origin gap occurred. */
        void checkCoverage() {
            System.out.printf("decision golden stream: %s, applied releases %d, decided voids %d, origin gaps %d%n",
                    kinds, releases, voids, originGaps);
            for (Kind kind : Kind.values()) {
                assertTrue(kinds.getOrDefault(kind, 0) > 0, "the stream has no " + kind + " outcome: " + kinds);
            }
            assertTrue(releases > 0 && voids > 0 && originGaps > 0,
                    "releases " + releases + ", decided voids " + voids + ", origin gaps " + originGaps);
            assertEquals(0, releasesBeforeActivation, "version 2 applies no release");
            assertEquals(3, state.decisionVersion());
            assertEquals(CanonicalSealerState.RULES_SNAPSHOT_VERSION, state.snapshotVersion());
        }

        /** One step of the stream; {@code roll} in [0, 1000) picks its kind. */
        private void step(int roll) {
            if (roll < 610) {
                relayFresh();
            } else if (roll < 690) {
                offerAgain();
            } else if (roll < 760) {
                skipNonce();
            } else if (roll < 790) {
                pastDeadline();
            } else if (roll < 820) {
                zeroSender();
            } else if (roll < 850) {
                release();
            } else if (roll < 910) {
                voteVoid();
            } else if (roll < 930) {
                origin();
            } else if (roll < 950) {
                postCursor();
            } else if (roll < 958) {
                recordCursor();
            } else {
                tick();
            }
        }

        /**
         * At {@link #ACTIVATION_STEP}, the activation of version 3 for the
         * next epoch, due at once. Before it, count every applied release.
         */
        private void activateAt(int step) {
            releasesBeforeActivation = step <= ACTIVATION_STEP ? releases : releasesBeforeActivation;
            if (step != ACTIVATION_STEP) {
                return;
            }
            DecisionRules.Outcome outcome = state.onFeatureActivation(new DecisionRules.FeatureActivation(
                    l1Origin + 1, DecisionRules.FEATURE_SEALER_V3, 0L), clock);
            assertEquals(DecisionRules.Outcome.SWITCHED, outcome);
            hashLong(outcome.ordinal());
        }

        /** Offer a fresh id at a sender's next nonce; a relay moves the nonce and joins the recent entries. */
        private void relayFresh() {
            int s = random.nextInt(SENDERS);
            byte[] id = freshId();
            CanonicalSealerState.RecordOutcome outcome =
                    state.onRecord(id, distinctSender(s), nonces[s], deadline(), id);
            hashRecord(outcome);
            outcome.relayed.ifPresent(r -> {
                recent[relayed % RECENT] = new Entry(id, s, nonces[s], r.index);
                relayed++;
                nonces[s]++;
            });
        }

        private void offerAgain() {
            Entry e = recent[random.nextInt(RECENT)];
            hashRecord(state.onRecord(e.id(), distinctSender(e.sender()), e.nonce(), deadline(), e.id()));
        }

        private void skipNonce() {
            int s = random.nextInt(SENDERS);
            byte[] id = freshId();
            hashRecord(state.onRecord(id, distinctSender(s), nonces[s] + 1 + random.nextInt(3), deadline(), id));
        }

        private void pastDeadline() {
            int s = random.nextInt(SENDERS);
            byte[] id = freshId();
            hashRecord(state.onRecord(id, distinctSender(s), nonces[s], state.blockNumber() - 1, id));
        }

        private void zeroSender() {
            byte[] id = freshId();
            hashRecord(state.onRecord(id, new byte[CanonicalSealerState.SENDER_LEN], 0L, deadline(), id));
        }

        /** Release the nonce of a recent entry, as the sequencer does for a skip receipt. */
        private void release() {
            Entry e = recent[random.nextInt(RECENT)];
            boolean applied = state.onRelease(distinctSender(e.sender()), e.nonce(), e.id());
            releases += applied ? 1 : 0;
            hashLong(applied ? 1L : 0L);
        }

        private void voteVoid() {
            Entry e = recent[random.nextInt(RECENT)];
            CanonicalSealerState.VoidOutcome outcome = state.onVoidRequest(random.nextInt(3), e.index(), e.id());
            voids += outcome.vote == VoidLedger.Vote.DECIDED ? 1 : 0;
            hashLong(outcome.vote.ordinal());
            hashLong(outcome.relayed.map(r -> r.index).orElse(-1L));
        }

        /** An origin record at the next L1 origin, or one origin past it: a gap. */
        private void origin() {
            long next = l1Origin + 1 + (random.nextInt(5) == 0 ? 1 : 0);
            byte[] id = freshId();
            OriginOutcome outcome = state.onOriginRecord(id, next, 1 + random.nextInt(3), id, clock);
            originGaps += outcome.gap ? 1 : 0;
            l1Origin = outcome.advance.isPresent() ? next : l1Origin;
            hashLong(outcome.advance.map(a -> a.relayed().index).orElse(-1L));
            hashLong(outcome.gap ? outcome.expectedOrigin : -1L);
        }

        /** The batcher posts up to a head at most 3 blocks behind the sealed head. */
        private void postCursor() {
            long head = Math.max(0L, state.sealedHead() - random.nextInt(4));
            hashLong(state.onPostedCursor(head) ? 1L : 0L);
        }

        /** An executor reports its recorded index, at most 40 records behind the sealed index. */
        private void recordCursor() {
            long through = Math.max(0L, state.sealedIndex() - random.nextInt(41));
            hashLong(state.onRecordedCursor(random.nextInt(3), through) ? 1L : 0L);
        }

        private void tick() {
            clock += CanonicalSealerState.TICK_INTERVAL_MS;
            Boundary boundary = state.onTick(clock);
            hashLong(boundary.blockNumber);
            hashLong(boundary.endTxIdx);
            hashLong(boundary.l2Timestamp);
            hashLong(boundary.l1Origin);
        }

        private void hashRecord(CanonicalSealerState.RecordOutcome outcome) {
            kinds.merge(outcome.kind, 1, Integer::sum);
            hashLong(outcome.kind.ordinal());
            hashLong(outcome.relayed.map(r -> r.index).orElse(-1L));
            hashLong(outcome.expectedNonce);
            hashLong(outcome.maxInclusionBlock);
        }

        private void hashLong(long value) {
            scratch.clear().putLong(value);
            digest.update(scratch.array(), 0, Long.BYTES);
        }

        /** A deadline a few blocks ahead, or none at all. */
        private long deadline() {
            return random.nextInt(4) == 0 ? NO_DEADLINE : state.blockNumber() + 1 + random.nextInt(64);
        }

        private byte[] freshId() {
            byte[] id = new byte[CanonicalSealerState.CANONICAL_ID_LEN];
            ByteBuffer.wrap(id).putLong(SEED).putInt(24, ids);
            ids++;
            return id;
        }

        private static MessageDigest sha256() {
            try {
                return MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("the JDK has SHA-256", e);
            }
        }
    }
}
