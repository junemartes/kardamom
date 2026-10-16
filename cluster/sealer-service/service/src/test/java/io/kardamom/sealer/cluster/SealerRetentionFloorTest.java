package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.LagBudgets;
import io.kardamom.sealer.VoidLedger;
import io.kardamom.sealer.cluster.ClusterStubs.StubCluster;
import io.kardamom.sealer.cluster.ClusterStubs.StubSession;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.AgentTerminationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Nothing is pruned below the posted head. The retention window is a
 * minimum: a frame past it stays while its block is unposted, the floor
 * follows the batcher's cursor, and a snapshot carries the retained frames
 * so a restored member keeps the floor instead of raising it to the
 * restore point. Each rule holds for both stores of the retained frames,
 * and the retained file serves the bytes that the heap serves.
 */
class SealerRetentionFloorTest {

    /** The two stores of the retained frames. */
    enum Store {
        HEAP,
        FILE
    }

    private static final int SEGMENT_BYTES = 64 * 1024;
    /** The status frame with the retained-bytes tail. */
    private static final int STATUS_WITH_BYTES_LEN = 75;

    @TempDir
    Path dir;

    private StubCluster cluster;
    private SealerClusteredService service;
    private StubSession publisher;
    private final List<SealerClusteredService> started = new ArrayList<>();

    private void start(final Store store) {
        // A window of three frames; the guard off, so the lag never refuses.
        System.setProperty("kardamom.cluster.retention", "3");
        cluster = new StubCluster();
        service = startService(store, "a", Optional.empty());
        publisher = cluster.addSession(1);
    }

    @AfterEach
    void clearRetentionOverride() {
        System.clearProperty("kardamom.cluster.retention");
        started.forEach(s -> s.onTerminate(cluster));
    }

    /** A started service with {@code store}; a file store keeps its segments in {@code name}. */
    private SealerClusteredService startService(final Store store, final String name, final Optional<byte[]> snapshot) {
        final SealerClusteredService svc = new SealerClusteredService(
            64, 250, 0, Set.of(), VoidLedger.Config.DISABLED,
            CanonicalSealerState.DEFAULT_INCLUSION_HORIZON_BLOCKS,
            CanonicalSealerState.DEFAULT_ORDERING_WINDOW, new LagBudgets(0L, 0L));
        if (store == Store.FILE) {
            svc.carryingBytes(new RetainedFile.Config(dir.resolve(name), SEGMENT_BYTES));
        }
        svc.start(cluster, snapshot);
        started.add(svc);
        return svc;
    }

    private void deliver(
            final SealerClusteredService svc, final StubSession from, final byte[] frame) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        buf.putBytes(0, frame);
        svc.onSessionMessage(from, 0, buf, 0, frame.length, null);
    }

    /** One block on {@code svc}: records {@code from..to}, then a seal. */
    private void block(final SealerClusteredService svc, final int from, final int to) {
        final byte[] sender = new byte[CanonicalSealerState.SENDER_LEN];
        java.util.Arrays.fill(sender, (byte) 0xAA);
        for (int n = from; n < to; n++) {
            deliver(svc, publisher, IngressFrames.recordFrame(n, sender, n));
        }
        svc.onTimerEvent(SealerClusteredService.BOUNDARY_TIMER_CORRELATION_ID, 250);
    }

    /** Blocks 1..4 on the service under test, two records each: four windows of frames. */
    private void fourBlocks() {
        block(service, 0, 2);
        block(service, 2, 4);
        block(service, 4, 6);
        block(service, 6, 8);
    }

    private static long count(final StubSession s, final byte kind) {
        return s.offered.stream().filter(f -> f[0] == kind).count();
    }

    private StubSession replay(
            final SealerClusteredService svc, final int sessionId, final long fromIndex, final long fromBlock) {
        final StubSession consumer = cluster.addSession(sessionId);
        deliver(svc, consumer, IngressFrames.replayRequestFrame(fromIndex, fromBlock));
        return consumer;
    }

    @ParameterizedTest
    @EnumSource(Store.class)
    void a_replay_older_than_the_window_is_served_while_the_range_is_unposted(final Store store) {
        start(store);
        fourBlocks();
        final StubSession consumer = replay(service, 2, 0, 1);
        assertEquals(0, count(consumer, SealerWire.EGRESS_KIND_REPLAY_UNAVAILABLE));
        assertEquals(8, count(consumer, SealerWire.EGRESS_KIND_RELAYED), "every record from genesis");
        assertEquals(4, count(consumer, SealerWire.EGRESS_KIND_BOUNDARY));
        assertEquals(1, count(consumer, SealerWire.EGRESS_KIND_REPLAY_DONE));
    }

    @ParameterizedTest
    @EnumSource(Store.class)
    void the_floor_advances_with_the_posted_head(final Store store) {
        start(store);
        fourBlocks();
        // Blocks 1 and 2 are posted: their six frames may leave, down to the window.
        deliver(service, publisher, IngressFrames.postedCursorFrame(2L));
        final StubSession fromGenesis = replay(service, 2, 0, 1);
        assertEquals(1, count(fromGenesis, SealerWire.EGRESS_KIND_REPLAY_UNAVAILABLE),
            "genesis is below the posted head and past the window");
        // Block 3 starts at index 4: at the posted head, and served.
        final StubSession fromPosted = replay(service, 3, 4, 3);
        assertEquals(0, count(fromPosted, SealerWire.EGRESS_KIND_REPLAY_UNAVAILABLE));
        assertEquals(4, count(fromPosted, SealerWire.EGRESS_KIND_RELAYED), "records 4..7");
        assertEquals(2, count(fromPosted, SealerWire.EGRESS_KIND_BOUNDARY));
    }

    @ParameterizedTest
    @EnumSource(Store.class)
    void the_window_stays_a_minimum_once_everything_is_posted(final Store store) {
        start(store);
        fourBlocks();
        deliver(service, publisher, IngressFrames.postedCursorFrame(4L));
        // Everything is posted, so the window shrinks to its three newest frames:
        // record 7 and the boundaries of blocks 3 and 4 are gone from the front.
        final StubSession consumer = replay(service, 2, 6, 4);
        assertEquals(0, count(consumer, SealerWire.EGRESS_KIND_REPLAY_UNAVAILABLE),
            "the newest block is still inside the window");
        final StubSession older = replay(service, 3, 4, 3);
        assertEquals(1, count(older, SealerWire.EGRESS_KIND_REPLAY_UNAVAILABLE),
            "a posted block past the window is gone");
    }

    @ParameterizedTest
    @EnumSource(Store.class)
    void a_snapshot_restore_keeps_the_floor_at_the_posted_head(final Store store) {
        start(store);
        fourBlocks();
        deliver(service, publisher, IngressFrames.postedCursorFrame(2L));
        final byte[] snapshot = service.snapshot();
        service.onTerminate(cluster);

        final SealerClusteredService restored = startService(store, "a", Optional.of(snapshot));

        // The frames above the posted head came with the snapshot, so a
        // replay from the posted head is served, not refused at the restore point.
        final StubSession fromPosted = replay(restored, 4, 4, 3);
        assertEquals(0, count(fromPosted, SealerWire.EGRESS_KIND_REPLAY_UNAVAILABLE));
        assertEquals(4, count(fromPosted, SealerWire.EGRESS_KIND_RELAYED), "records 4..7");
        final StubSession fromGenesis = replay(restored, 5, 0, 1);
        assertEquals(1, count(fromGenesis, SealerWire.EGRESS_KIND_REPLAY_UNAVAILABLE),
            "below the posted head stays refused");
        // The restored member keeps the posted head as its floor afterwards.
        block(restored, 8, 10);
        final StubSession stillServed = replay(restored, 6, 4, 3);
        assertEquals(0, count(stillServed, SealerWire.EGRESS_KIND_REPLAY_UNAVAILABLE));
    }

    @Test
    void the_file_replays_the_bytes_that_the_heap_replays() {
        start(Store.HEAP);
        final SealerClusteredService heap = service;
        final SealerClusteredService file = startService(Store.FILE, "b", Optional.empty());
        List.of(heap, file).forEach(svc -> {
            block(svc, 0, 2);
            block(svc, 2, 4);
            block(svc, 4, 6);
        });
        final StubSession fromHeap = replay(heap, 2, 2, 2);
        final StubSession fromFile = replay(file, 3, 2, 2);
        final List<byte[]> heapFrames = withoutStatus(fromHeap);
        final List<byte[]> fileFrames = withoutStatus(fromFile);
        assertEquals(heapFrames.size(), fileFrames.size());
        IntStream.range(0, heapFrames.size()).forEach(i -> assertArrayEquals(heapFrames.get(i), fileFrames.get(i)));
    }

    private static List<byte[]> withoutStatus(final StubSession session) {
        return session.offered.stream().filter(f -> f[0] != SealerWire.EGRESS_KIND_STATUS).toList();
    }

    @Test
    void the_status_carries_the_retained_bytes_above_the_posted_head() {
        start(Store.FILE);
        fourBlocks();
        deliver(service, publisher, IngressFrames.postedCursorFrame(2L));
        final byte[] status = publisher.offered.stream()
            .filter(f -> f[0] == SealerWire.EGRESS_KIND_STATUS)
            .reduce((first, second) -> second)
            .orElseThrow();
        assertEquals(STATUS_WITH_BYTES_LEN, status.length);
        final long retainedBytes = ByteBuffer.wrap(status).order(ByteOrder.LITTLE_ENDIAN)
            .getLong(STATUS_WITH_BYTES_LEN - Long.BYTES);
        // Blocks 3 and 4 are above the posted head: four records and two boundaries.
        assertTrue(retainedBytes > 0, "retained bytes " + retainedBytes);
        deliver(service, publisher, IngressFrames.postedCursorFrame(4L));
        final byte[] after = publisher.offered.get(publisher.offered.size() - 1);
        assertEquals(0L, ByteBuffer.wrap(after).order(ByteOrder.LITTLE_ENDIAN)
            .getLong(STATUS_WITH_BYTES_LEN - Long.BYTES), "nothing is above a posted head at the sealed head");
    }

    @Test
    void a_failed_preallocation_stops_the_member() throws IOException {
        System.setProperty("kardamom.cluster.retention", "3");
        cluster = new StubCluster();
        // A directory at the name of the prepared file makes the helper fail, as a full disk does.
        Files.createDirectories(dir.resolve("c").resolve(SegmentPreallocator.PREPARED_NAME));
        service = startService(Store.FILE, "c", Optional.empty());
        publisher = cluster.addSession(1);
        final byte[] sender = new byte[CanonicalSealerState.SENDER_LEN];
        assertThrows(AgentTerminationException.class,
            () -> deliver(service, publisher, IngressFrames.recordFrame(0, sender, 0)));
    }
}
