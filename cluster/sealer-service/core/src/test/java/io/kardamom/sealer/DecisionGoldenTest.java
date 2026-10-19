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
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HexFormat;
import java.util.List;
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
 * duplicates, nonce gaps, passed deadlines, the zero sender, void votes,
 * origin records with and without a gap, posted and recorded cursors, and
 * ticks. Four feature activations switch the decision rules at block
 * boundaries: on at once, off with a time ahead that switches them on
 * again, off for good, and on at once. After the first activation the
 * snapshot is version 14. The
 * window fills, so it also refuses records,
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
    /** The relayed records that a void vote can name. */
    private static final int RECENT = 64;
    private static final int SNAPSHOT_EVERY = 5_000;
    /** The step of the activation that switches the rules on at once. */
    private static final int ON_STEP = STEPS / 4;
    /** The step of the activation with a time ahead: off at the record, on at the time. */
    private static final int OFF_STEP = STEPS / 2;
    /** The step of the activation at time {@code 2^64 - 1}: off for good. */
    private static final int OFF_FOR_GOOD_STEP = 3 * STEPS / 4;
    /** The step of the activation that switches the rules on again at once. */
    private static final int ON_AGAIN_STEP = 7 * STEPS / 8;
    /** How far ahead of its record the time of the {@link #OFF_STEP} activation is. */
    private static final long OFF_FOR_MS = 400 * CanonicalSealerState.TICK_INTERVAL_MS;
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

    /** One relayed record: what a void vote names. */
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
        /** The version after each switch of the rules, in log order. */
        private final List<Integer> switches = new ArrayList<>();
        /** The version that the stream saw last. */
        private int version = DecisionRules.START_VERSION;
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
                rulesAt(step);
                step(random.nextInt(1000));
                if (step % SNAPSHOT_EVERY == 0) {
                    digest.update(state.takeSnapshot());
                }
            }
            rulesAt(STEPS + 1);
            digest.update(state.takeSnapshot());
            return digest.digest();
        }

        /** Every record outcome, a decided void and an origin gap occurred. */
        void checkCoverage() {
            System.out.printf("decision golden stream: %s, decided voids %d, origin gaps %d%n",
                    kinds, voids, originGaps);
            for (Kind kind : Kind.values()) {
                assertTrue(kinds.getOrDefault(kind, 0) > 0, "the stream has no " + kind + " outcome: " + kinds);
            }
            assertTrue(voids > 0 && originGaps > 0, "decided voids " + voids + ", origin gaps " + originGaps);
            assertEquals(List.of(3, 2, 3, 2, 3), switches, "on, off, on at the time, off, on");
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
         * Count a switch of the rules: a boundary of the step before set a
         * new version. Then, at the steps of the activations, store the
         * activation for the next epoch.
         */
        private void rulesAt(int step) {
            if (state.decisionVersion() != version) {
                switched();
            }
            switch (step) {
                case ON_STEP, ON_AGAIN_STEP -> activate(0L);
                case OFF_STEP -> activate(clock + OFF_FOR_MS);
                case OFF_FOR_GOOD_STEP -> activate(-1L);
                default -> {
                }
            }
        }

        /** Store an activation for the next epoch at {@code activationMs}; a later boundary switches the rules. */
        private void activate(long activationMs) {
            DecisionRules.Outcome outcome = state.onFeatureActivation(new DecisionRules.FeatureActivation(
                    l1Origin + 1, DecisionRules.FEATURE_SEALER_V3, activationMs));
            assertEquals(DecisionRules.Outcome.STORED, outcome);
            hashLong(outcome.ordinal());
        }

        private void switched() {
            version = state.decisionVersion();
            switches.add(version);
            hashLong(version);
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
