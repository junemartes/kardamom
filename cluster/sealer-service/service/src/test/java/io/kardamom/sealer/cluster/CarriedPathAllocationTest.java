package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
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
import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The allocation gate of the service record path of decision version 5: the
 * heap bytes that one full reference costs from the log buffer to the
 * retained ring, in steady state. Eight ingress images take turns, each one
 * a message further, and each reference comes twice, as from twin
 * sequencers. The gate measures only the delivery of each reference: the
 * ticks and the posted cursors between the blocks are not measured. A
 * second case runs with the carry window on, so each new frame evicts the
 * oldest one of the replay window at once. A third case runs with a live
 * member feed on the heap, so each new frame also goes into the frame ring.
 * A fourth case measures the walk of a feed answer over the heap frames.
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
    /** The media driver and the reader of a feed case, else null. */
    private MediaDriver driver;
    private FeedReader reader;
    /** The work between two measured offers: the duty cycle of the feed and its reader, in a feed case. */
    private Runnable afterOffer = () -> { };
    /** The id of the newest feed request. */
    private long nextRequest;
    /** The bytes and the duty cycles of the walk case, inside the walk. */
    private long walkBytes;
    private int walkSteps;

    @TempDir
    Path dir;

    @AfterEach
    void stop() {
        svc.onTerminate(cluster);
        CloseHelper.closeAll(reader, driver);
    }

    @Test
    void a_full_reference_and_its_copy_allocate_at_most_their_baselines() {
        measure(false, "carried_fresh", "carried_copy");
    }

    @Test
    void with_the_carry_window_a_full_reference_and_its_copy_allocate_at_most_their_baselines() {
        measure(true, "carried_window_fresh", "carried_window_copy");
    }

    @Test
    void with_a_live_heap_feed_a_full_reference_and_its_copy_allocate_at_most_their_baselines() {
        feeding();
        startOnVersionFive(false);
        final long served = request(0, CanonicalSealerState.GENESIS_BLOCK_NUMBER);
        awaitAnswer(SealerWire.EGRESS_KIND_REPLAY_DONE, served);
        run("carried_feed_fresh", "carried_feed_copy");
        assertEquals(FeedState.LIVE, svc.metrics().feed(), "the feed stays live through the measurement");
    }

    /**
     * The walk of a feed answer over the full window. Only the duty cycles
     * that start and end inside the walk count: the start of the answer
     * and its end log a line.
     */
    @Test
    void the_walk_of_a_heap_feed_allocates_at_most_its_baseline() {
        feeding();
        startOnVersionFive(false);
        IntStream.range(0, WARMUP).forEach(this::offerTwice);
        final FeedReader.Frame floor = awaitAnswer(SealerWire.EGRESS_KIND_REPLAY_UNAVAILABLE,
            request(0, CanonicalSealerState.GENESIS_BLOCK_NUMBER));
        final long frames = svc.retained().count();
        // The first walk warms the walk up; the second one is measured.
        walk(floor);
        walkBytes = 0;
        walkSteps = 0;
        walk(floor);
        assertTrue(walkSteps > 10, "the walk takes many duty cycles: " + walkSteps);
        check("heap_feed_walk", (double) walkBytes / frames);
        check("heap_feed_walk_cycle", (double) walkBytes / walkSteps);
    }

    /** Walk the window from {@code floor} to the head, until the feed is live again. */
    private void walk(final FeedReader.Frame floor) {
        request(floor.a(), floor.b());
        while (svc.metrics().feed() != FeedState.WALKING) {
            afterOffer.run();
        }
        while (svc.metrics().feed() != FeedState.LIVE) {
            walkCycle();
        }
    }

    /** Run one duty cycle of the feed, and count its bytes when it starts and ends inside the walk. */
    private void walkCycle() {
        final FeedState before = svc.metrics().feed();
        final long start = THREADS.getCurrentThreadAllocatedBytes();
        svc.doBackgroundWork(0);
        final long end = THREADS.getCurrentThreadAllocatedBytes();
        final boolean inside = before == FeedState.WALKING && svc.metrics().feed() == FeedState.WALKING;
        walkBytes += inside ? end - start : 0;
        walkSteps += inside ? 1 : 0;
        reader.drain();
    }

    /** Run the references on version 5, with the carry window when {@code window}, and check both baselines. */
    private void measure(final boolean window, final String freshCase, final String copyCase) {
        startOnVersionFive(window);
        run(freshCase, copyCase);
    }

    /** Start the member, and switch to version 5, then the carry window when {@code window}. */
    private void startOnVersionFive(final boolean window) {
        cluster.role = io.aeron.cluster.service.Cluster.Role.FOLLOWER;
        svc.start(cluster, Optional.empty());
        switchToVersionFive();
        if (window) {
            switchOnTheCarryWindow();
        }
    }

    /** A media driver, a member feed of the service on it, and a reader of the feed. */
    private void feeding() {
        final String aeronDir = dir.resolve("aeron").toString();
        driver = MediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(aeronDir)
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true));
        svc.feeding(new FeedConfig(aeronDir, 16 << 20, 1 << 20, IdleCap.DEFAULT));
        reader = new FeedReader(aeronDir);
        afterOffer = () -> {
            svc.doBackgroundWork(0);
            reader.drain();
        };
    }

    /** Send a feed request from {@code (fromIndex, fromBlock)}, and return its id. */
    private long request(final long fromIndex, final long fromBlock) {
        nextRequest++;
        reader.request(fromIndex, fromBlock, nextRequest);
        return nextRequest;
    }

    /** Run the duty cycles of the feed until the answer {@code kind} of request {@code id} comes. */
    private FeedReader.Frame awaitAnswer(final byte kind, final long id) {
        final Predicate<FeedReader.Frame> answer = f -> !f.canonical() && f.kind() == kind && f.requestId() == id;
        reader.pollUntil(frames -> frames.stream().anyMatch(answer), () -> svc.doBackgroundWork(0));
        return reader.frames().stream().filter(answer).findFirst().orElseThrow();
    }

    /** Offer the warm-up references, then the measured ones, and check both baselines. */
    private void run(final String freshCase, final String copyCase) {
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
        afterOffer.run();
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
