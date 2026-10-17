package io.kardamom.sealer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Optional;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

/**
 * The decisions of {@link CanonicalSealerState} over a fixed record stream,
 * as one SHA-256 digest. Every member of the cluster must decide alike, and
 * a snapshot must restore the same state, so a change of the record path
 * must keep this digest, unless it is a decision version change.
 *
 * <p>A seeded random stream drives a small window, so each behaviour occurs
 * often: relays, duplicates, nonce gaps, passed deadlines, the zero sender, a
 * full window, the eviction of the least recently used sender, and ticks.
 * The digest covers each outcome, each boundary, and the snapshot bytes
 * every {@link #SNAPSHOT_EVERY} steps and at the end.
 *
 * <p>A decision version change sets a new digest on purpose: run the test
 * with the environment variable {@code SEALER_GOLDEN_UPDATE=1}, and commit the
 * file it writes.
 */
final class DecisionGoldenTest {

    private static final long SEED = 20_261_017L;
    private static final int STEPS = 200_000;
    /** A small window, so it fills and its senders are evicted often. */
    private static final int DEDUP_CAPACITY = 512;
    /** More senders than the window tracks, so the sender eviction runs. */
    private static final int SENDERS = 1024;
    private static final int SNAPSHOT_EVERY = 5_000;
    private static final String GOLDEN = "decision-golden.sha256";

    @Test
    void the_record_path_decides_as_the_golden_digest_says() throws IOException {
        String digest = HexFormat.of().formatHex(new Stream().run());
        // The update writes the source file; the copy on the class path is
        // the old one, so the run ends here.
        if ("1".equals(System.getenv("SEALER_GOLDEN_UPDATE"))) {
            Files.writeString(Path.of("src/test/resources", GOLDEN), digest + "\n");
            return;
        }
        assertEquals(golden(), digest,
                "the sealer's decisions changed; a decision version change sets the digest with "
                        + "SEALER_GOLDEN_UPDATE=1");
    }

    private static String golden() throws IOException {
        try (InputStream in = DecisionGoldenTest.class.getResourceAsStream("/" + GOLDEN)) {
            return new String(in.readAllBytes(), StandardCharsets.US_ASCII).trim();
        }
    }

    /** The seeded stream, the state it drives, and the digest of what the state decides. */
    private static final class Stream {
        private final SplittableRandom random = new SplittableRandom(SEED);
        private final CanonicalSealerState state = new CanonicalSealerState(DEDUP_CAPACITY);
        private final MessageDigest digest = sha256();
        private final ByteBuffer scratch = ByteBuffer.allocate(64);
        private final long[] nonces = new long[SENDERS];
        private final byte[][] recent = new byte[64][];
        private int ids;
        private long clock;

        byte[] run() {
            for (int step = 1; step <= STEPS; step++) {
                step(random.nextInt(100));
                snapshotEvery(step);
            }
            digest.update(state.takeSnapshot());
            return digest.digest();
        }

        /** One step of the stream; {@code roll} in [0, 100) picks its kind. */
        private void step(int roll) {
            if (roll < 55) {
                int s = random.nextInt(SENDERS);
                offer(freshId(), sender(s), nonces[s], deadline(), s);
            } else if (roll < 65) {
                offer(recent[random.nextInt(recent.length)], sender(random.nextInt(SENDERS)), 0L, deadline(), -1);
            } else if (roll < 75) {
                int s = random.nextInt(SENDERS);
                offer(freshId(), sender(s), nonces[s] + 1 + random.nextInt(3), deadline(), -1);
            } else if (roll < 80) {
                int s = random.nextInt(SENDERS);
                offer(freshId(), sender(s), nonces[s], state.blockNumber() - 1, s);
            } else if (roll < 85) {
                offer(freshId(), new byte[CanonicalSealerState.SENDER_LEN], 0L, deadline(), -1);
            } else {
                tick();
            }
        }

        /**
         * Offer one record and hash its outcome. A relayed record of a
         * tracked sender {@code s} moves that sender's next nonce.
         */
        private void offer(byte[] id, byte[] sender, long nonce, long deadline, int s) {
            if (id == null) {
                return;
            }
            CanonicalSealerState.RecordOutcome outcome = state.onRecord(id, sender, nonce, deadline, id);
            boolean relayed = outcome.kind == CanonicalSealerState.RecordOutcome.Kind.RELAYED;
            if (relayed && s >= 0) {
                nonces[s] = nonce + 1;
            }
            Optional<Relayed> record = outcome.relayed;
            scratch.clear()
                    .putInt(outcome.kind.ordinal())
                    .putLong(record.map(r -> r.index).orElse(-1L))
                    .putLong(outcome.expectedNonce)
                    .putLong(outcome.maxInclusionBlock);
            digest.update(scratch.array(), 0, scratch.position());
        }

        private void tick() {
            clock += CanonicalSealerState.TICK_INTERVAL_MS;
            Boundary boundary = state.onTick(clock);
            scratch.clear()
                    .putLong(boundary.blockNumber)
                    .putLong(boundary.endTxIdx)
                    .putLong(boundary.l2Timestamp)
                    .putLong(boundary.l1Origin);
            digest.update(scratch.array(), 0, scratch.position());
        }

        private void snapshotEvery(int step) {
            if (step % SNAPSHOT_EVERY == 0) {
                digest.update(state.takeSnapshot());
            }
        }

        /** A deadline a few blocks ahead, or none at all. */
        private long deadline() {
            return random.nextInt(4) == 0 ? Long.MAX_VALUE : state.blockNumber() + 1 + random.nextInt(8);
        }

        /** A new id; it also joins the recent ids that a duplicate offers again. */
        private byte[] freshId() {
            byte[] id = new byte[CanonicalSealerState.CANONICAL_ID_LEN];
            ByteBuffer.wrap(id).putLong(SEED).putInt(24, ids);
            recent[ids % recent.length] = id;
            ids++;
            return id;
        }

        private static byte[] sender(int s) {
            byte[] out = new byte[CanonicalSealerState.SENDER_LEN];
            out[0] = (byte) 0x7a;
            out[18] = (byte) (s >>> 8);
            out[19] = (byte) s;
            return out;
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
