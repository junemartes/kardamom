package io.kardamom.sealer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kardamom.sealer.CanonicalSealerState.RecordOutcome.Kind;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/**
 * The allocation gate of the sealer's record path: the heap bytes that
 * {@link CanonicalSealerState} allocates for each record in steady state.
 * The order is the only product of the sealer, so the target is zero.
 *
 * <p>Each case warms the state up, then measures the bytes that the test
 * thread allocates over many records. The inputs exist before the measured
 * part. Every measured record must have the outcome that the case names, so
 * a case measures the path it claims to. A case fails when it allocates more
 * than its baseline in {@code allocation-baseline.properties} allows. A
 * change that removes allocations lowers the baseline in the same pull
 * request, so the gate only moves down.
 */
final class AllocationGateTest {

    /** The production dedup capacity ({@code SealerWire.DEFAULT_DEDUP_CAPACITY}). */
    private static final int DEDUP_CAPACITY = 1 << 17;
    /** The distinct senders that a case cycles through. */
    private static final int SENDERS = 4096;
    /** The records a case runs before it measures: enough for the JIT to settle. */
    private static final int WARMUP = 400_000;
    /** The records a case measures. */
    private static final int MEASURED = 400_000;
    /** The records between two boundary ticks of the relayed case. */
    private static final int RECORDS_PER_BLOCK = 2_000;
    /**
     * The blocks a relayed id stays in the window. The window then holds
     * about 120 000 ids, below the capacity, and each tick expires as many
     * ids as a block adds: the steady state of a busy chain.
     */
    private static final long HOLD_BLOCKS = 60;
    /** A payload of the size of a small transfer envelope. */
    private static final byte[] PAYLOAD = new byte[120];
    /** The share of a baseline that a measurement may exceed it by: JIT and TLAB noise. */
    private static final double TOLERANCE = 1.05;
    /** The bytes per record that a measurement may exceed a baseline by, for a baseline near zero. */
    private static final double SLACK_BYTES = 4.0;

    private static final com.sun.management.ThreadMXBean THREADS =
            (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();

    @Test
    void a_relayed_record_allocates_at_most_its_baseline() {
        Workload work = new Workload(WARMUP + MEASURED);
        check("relayed", work.measure(new CanonicalSealerState(DEDUP_CAPACITY), Workload::relay, Kind.RELAYED));
    }

    @Test
    void a_duplicate_record_allocates_at_most_its_baseline() {
        Workload work = new Workload(DEDUP_CAPACITY);
        CanonicalSealerState state = new CanonicalSealerState(DEDUP_CAPACITY);
        work.fill(state);
        check("duplicate", work.measure(state, Workload::offerAgain, Kind.DUPLICATE));
    }

    @Test
    void a_contiguity_reject_allocates_at_most_its_baseline() {
        Workload work = new Workload(DEDUP_CAPACITY + WARMUP + MEASURED);
        CanonicalSealerState state = new CanonicalSealerState(DEDUP_CAPACITY);
        work.fill(state);
        check("contiguity_reject", work.measure(state, Workload::skipNonce, Kind.CONTIGUITY_REJECT));
    }

    private static void check(String name, double bytesPerRecord) {
        double baseline = baseline(name);
        double limit = baseline * TOLERANCE + SLACK_BYTES;
        System.out.printf("sealer allocation gate: %s %.1f bytes/record (baseline %.1f)%n",
                name, bytesPerRecord, baseline);
        assertTrue(bytesPerRecord <= limit, String.format(
                "%s allocates %.1f bytes per record, above the baseline %.1f (limit %.1f)",
                name, bytesPerRecord, baseline, limit));
    }

    private static double baseline(String name) {
        try (InputStream in = AllocationGateTest.class.getResourceAsStream("/allocation-baseline.properties")) {
            Properties baselines = new Properties();
            baselines.load(in);
            return Double.parseDouble(baselines.getProperty(name));
        } catch (IOException e) {
            throw new IllegalStateException("read allocation-baseline.properties", e);
        }
    }

    /** One record of a case, at position {@code i} of the case. Returns its outcome. */
    @FunctionalInterface
    private interface Step {
        Kind run(Workload work, CanonicalSealerState state, int i);
    }

    /**
     * The inputs of a case, built before any measurement: a distinct id
     * for each record, and the senders with their next nonces.
     */
    private static final class Workload {
        private final byte[][] ids;
        private final byte[][] senders = new byte[SENDERS][];
        private final long[] nonces = new long[SENDERS];
        /** The ids that {@link #fill} put in the window. */
        private int filled;
        private long block;
        private long clock;

        Workload(int records) {
            ids = new byte[records][];
            for (int i = 0; i < records; i++) {
                ids[i] = id(i);
            }
            for (int s = 0; s < SENDERS; s++) {
                senders[s] = sender(s);
            }
        }

        /**
         * Fill the window to one id below its capacity, with no tick, so no
         * id expires. The check for a full window runs before the nonce
         * check, so a full window would hide the contiguity guard.
         */
        void fill(CanonicalSealerState state) {
            filled = DEDUP_CAPACITY - 1;
            for (int i = 0; i < filled; i++) {
                int s = i % SENDERS;
                state.onRecord(ids[i], senders[s], nonces[s]++, Long.MAX_VALUE, PAYLOAD);
            }
        }

        /** Run the warmup records, then return the bytes per measured record. */
        double measure(CanonicalSealerState state, Step step, Kind expected) {
            for (int i = 0; i < WARMUP; i++) {
                step.run(this, state, i);
            }
            int other = 0;
            long before = THREADS.getCurrentThreadAllocatedBytes();
            for (int i = WARMUP; i < WARMUP + MEASURED; i++) {
                other += step.run(this, state, i) == expected ? 0 : 1;
            }
            long after = THREADS.getCurrentThreadAllocatedBytes();
            assertEquals(0, other, "measured records with another outcome than " + expected);
            return (double) (after - before) / MEASURED;
        }

        /** Relay a fresh record at the sender's next nonce, with a tick at each block end. */
        Kind relay(CanonicalSealerState state, int i) {
            int s = i % SENDERS;
            Kind kind = state.onRecord(ids[i], senders[s], nonces[s]++, block + HOLD_BLOCKS, PAYLOAD).kind;
            if (i % RECORDS_PER_BLOCK == RECORDS_PER_BLOCK - 1) {
                clock += CanonicalSealerState.TICK_INTERVAL_MS;
                state.onTick(clock);
                block++;
            }
            return kind;
        }

        /** Offer again a record that the window holds. */
        Kind offerAgain(CanonicalSealerState state, int i) {
            int at = i % filled;
            return state.onRecord(ids[at], senders[at % SENDERS], 0L, Long.MAX_VALUE, PAYLOAD).kind;
        }

        /** Offer a fresh record one nonce past the sender's next one. A reject stores nothing. */
        Kind skipNonce(CanonicalSealerState state, int i) {
            int s = i % SENDERS;
            return state.onRecord(ids[filled + i], senders[s], nonces[s] + 1, Long.MAX_VALUE, PAYLOAD).kind;
        }

        private static byte[] id(int i) {
            byte[] out = new byte[CanonicalSealerState.CANONICAL_ID_LEN];
            out[0] = (byte) 0x5e;
            out[28] = (byte) (i >>> 24);
            out[29] = (byte) (i >>> 16);
            out[30] = (byte) (i >>> 8);
            out[31] = (byte) i;
            return out;
        }

        private static byte[] sender(int s) {
            byte[] out = new byte[CanonicalSealerState.SENDER_LEN];
            out[0] = (byte) 0x7a;
            out[18] = (byte) (s >>> 8);
            out[19] = (byte) s;
            return out;
        }
    }
}
