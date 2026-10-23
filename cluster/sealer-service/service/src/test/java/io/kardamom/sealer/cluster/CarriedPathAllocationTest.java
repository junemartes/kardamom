package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.LagBudgets;
import io.kardamom.sealer.VersionSwitch;
import io.kardamom.sealer.VoidLedger;
import io.kardamom.sealer.cluster.ClusterStubs.StubCluster;
import io.kardamom.sealer.cluster.ClusterStubs.StubSession;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.nio.ByteOrder;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The allocation gate of the service record path of decision version 5: the
 * heap bytes that one full reference costs from the log buffer to the
 * retained ring, in steady state. Eight ingress images take turns, each one
 * a message further, and each reference comes twice, as from twin
 * sequencers. The gate measures only the delivery of each reference: the
 * ticks and the posted cursors between the blocks are not measured. A
 * second case runs with the carry window on, so each new frame evicts the
 * oldest one of the replay window at once.
 *
 * <p>The member is a follower with no client session, so no frame goes to
 * a stub session that copies it. The {@code allocationGate} task runs this
 * class with escape analysis off, so the count is exact.</p>
 */
@Tag("allocation")
final class CarriedPathAllocationTest {

    private static final int DEDUP_CAPACITY = 1 << 17;
    private static final int WARMUP = 400_000;
    private static final int MEASURED = 400_000;
    private static final int RECORDS_PER_BLOCK = 2_000;
    private static final long TICK_MS = CanonicalSealerState.TICK_INTERVAL_MS;
    /** The bytes per record that a measurement may exceed its baseline by: the rounding of the average. */
    private static final double SLACK_BYTES = 0.5;

    private static final com.sun.management.ThreadMXBean THREADS =
            (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();

    private final StubCluster cluster = new StubCluster();
    /** The session that offers every frame. The cluster does not list it, so it gets no egress. */
    private final StubSession sequencer = new StubSession(1);
    private final SealerClusteredService svc = new SealerClusteredService(
        DEDUP_CAPACITY, TICK_MS, 0, Set.of(), VoidLedger.Config.DISABLED,
        CanonicalSealerState.DEFAULT_INCLUSION_HORIZON_BLOCKS, 0, new LagBudgets(0, 0));
    /** The full reference of every record: the position and the deadline field change from record to record. */
    private final UnsafeBuffer frame = new UnsafeBuffer(FullRef.of(1, 0).frame());
    private long ticks;
    private long freshBytes;
    private long copyBytes;

    @AfterEach
    void stop() {
        svc.onTerminate(cluster);
    }

    @Test
    void a_full_reference_and_its_copy_allocate_at_most_their_baselines() {
        measure(false, "carried_fresh", "carried_copy");
    }

    @Test
    void with_the_carry_window_a_full_reference_and_its_copy_allocate_at_most_their_baselines() {
        measure(true, "carried_window_fresh", "carried_window_copy");
    }

    /** Run the references on version 5, with the carry window when {@code window}, and check both baselines. */
    private void measure(final boolean window, final String freshCase, final String copyCase) {
        cluster.role = io.aeron.cluster.service.Cluster.Role.FOLLOWER;
        svc.start(cluster, Optional.empty());
        switchToVersionFive();
        if (window) {
            switchOnTheCarryWindow();
        }
        for (int i = 0; i < WARMUP; i++) {
            offerTwice(i);
        }
        freshBytes = 0;
        copyBytes = 0;
        for (int i = WARMUP; i < WARMUP + MEASURED; i++) {
            offerTwice(i);
        }
        final double fresh = (double) freshBytes / MEASURED;
        final double copy = (double) copyBytes / MEASURED;
        check(freshCase, fresh);
        check(copyCase, copy);
    }

    /** Version 4 at the boundary of epoch 2, version 5 at the boundary of epoch 3. */
    private void switchToVersionFive() {
        deliver(IngressFrames.originRecordFrame(IngressFrames.canonicalId(1001), 1, 1, new byte[] {2}));
        tick();
        deliver(IngressFrames.featureActivationFrame(2, VersionSwitch.FEATURE_SEALER_V4, 1L));
        deliver(IngressFrames.originRecordFrame(IngressFrames.canonicalId(1002), 2, 1, new byte[] {2}));
        tick();
        deliver(IngressFrames.featureActivationFrame(3, VersionSwitch.FEATURE_SEALER_V5, 1L));
        deliver(IngressFrames.originRecordFrame(IngressFrames.canonicalId(1003), 3, 1, new byte[] {2}));
        tick();
        assertEquals(5, svc.decisionRules().version());
    }

    /** The carry window at the boundary of epoch 4. */
    private void switchOnTheCarryWindow() {
        deliver(IngressFrames.featureActivationFrame(4, VersionSwitch.FEATURE_CARRY_WINDOW, 1L));
        deliver(IngressFrames.originRecordFrame(IngressFrames.canonicalId(1004), 4, 1, new byte[] {2}));
        tick();
        assertTrue(svc.metrics().carry().carryWindow());
    }

    /** Offer reference {@code i} and its copy, and close the block at each block end. */
    private void offerTwice(final int i) {
        final long position = (long) (i >>> 3) * 128;
        frame.putLong(SealerWire.DEADLINE_OFFSET, SealerWire.POSITION_TAG | position, ByteOrder.LITTLE_ENDIAN);
        frame.putInt(SealerWire.REF_SESSION_OFFSET - Integer.BYTES, (int) position, ByteOrder.LITTLE_ENDIAN);
        frame.putInt(SealerWire.REF_SESSION_OFFSET, i & 7, ByteOrder.LITTLE_ENDIAN);
        final long before = THREADS.getCurrentThreadAllocatedBytes();
        svc.onSessionMessage(sequencer, cluster.time, frame, 0, frame.capacity(), null);
        final long between = THREADS.getCurrentThreadAllocatedBytes();
        svc.onSessionMessage(sequencer, cluster.time, frame, 0, frame.capacity(), null);
        final long after = THREADS.getCurrentThreadAllocatedBytes();
        freshBytes += between - before;
        copyBytes += after - between;
        if (i % RECORDS_PER_BLOCK == RECORDS_PER_BLOCK - 1) {
            tick();
            deliver(IngressFrames.postedCursorFrame(ticks));
        }
    }

    /** Close the open block. Each boundary of this test is a tick, so the sealed head is the tick count. */
    private void tick() {
        ticks++;
        cluster.time = ticks * TICK_MS;
        svc.onTimerEvent(SealerClusteredService.BOUNDARY_TIMER_CORRELATION_ID, cluster.time);
    }

    private void deliver(final byte[] bytes) {
        svc.onSessionMessage(sequencer, cluster.time, new UnsafeBuffer(bytes), 0, bytes.length, null);
    }

    private static void check(final String name, final double bytesPerRecord) {
        final double baseline = baseline(name);
        System.out.printf("sealer service allocation gate: %s %.1f bytes/record (baseline %.1f)%n",
            name, bytesPerRecord, baseline);
        assertTrue(bytesPerRecord <= baseline + SLACK_BYTES, String.format(
            "%s allocates %.1f bytes per record, above the baseline %.1f", name, bytesPerRecord, baseline));
    }

    private static double baseline(final String name) {
        try (InputStream in = CarriedPathAllocationTest.class.getResourceAsStream("/allocation-baseline.properties")) {
            final Properties baselines = new Properties();
            baselines.load(in);
            return Double.parseDouble(baselines.getProperty(name));
        } catch (final IOException e) {
            throw new IllegalStateException("read allocation-baseline.properties", e);
        }
    }
}
