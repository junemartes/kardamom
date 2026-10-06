package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.cluster.service.Cluster;
import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.cluster.ClusterStubs.StubCluster;
import io.kardamom.sealer.cluster.ClusterStubs.StubSession;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The egress offer path with a consumer that stops draining. An offer never
 * waits on the single service thread: a back-pressured frame goes to the
 * backlog of its session, the boundary tick returns at once, and every other
 * session gets every frame in order. A backlog that makes no progress for
 * the stall deadline closes its session, once. A session that recovers in
 * time gets its backlog in emission order.
 */
class SealerEgressCloseTest {

    private static final long STALL_NS = SessionBacklogs.STALL_DEADLINE_NS;

    private StubCluster cluster;
    private SealerClusteredService service;
    private StubSession publisher;
    private StubSession healthy;
    private StubSession wedged;

    @BeforeEach
    void start() {
        cluster = new StubCluster();
        service = new SealerClusteredService(64, 250, 0);
        service.onStart(cluster, null);
        publisher = cluster.addSession(1);
        healthy = cluster.addSession(2);
        wedged = cluster.addSession(3);
        deliver(healthy, IngressFrames.subscribeFrame());
        deliver(wedged, IngressFrames.subscribeFrame());
    }

    private void deliver(final StubSession from, final byte[] frame) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        buf.putBytes(0, frame);
        service.onSessionMessage(from, 0, buf, 0, frame.length, null);
    }

    /** One record with nonce {@code n} from a non-zero sender, so the contiguity guard applies. */
    private void record(final int n) {
        final byte[] sender = new byte[CanonicalSealerState.SENDER_LEN];
        Arrays.fill(sender, (byte) 0xAA);
        deliver(publisher, IngressFrames.recordFrame(n, sender, n));
    }

    private void tick() {
        service.onTimerEvent(SealerClusteredService.BOUNDARY_TIMER_CORRELATION_ID, 250);
    }

    private static long count(final StubSession s, final byte kind) {
        return s.offered.stream().filter(f -> f[0] == kind).count();
    }

    /** The block numbers of the boundaries a session got, in arrival order. */
    private static List<Long> boundaryBlocks(final StubSession s) {
        return s.offered.stream()
            .filter(f -> f[0] == SealerWire.EGRESS_KIND_BOUNDARY)
            .map(f -> ByteBuffer.wrap(f).order(ByteOrder.LITTLE_ENDIAN).getLong(Byte.BYTES))
            .toList();
    }

    /** Assert that a session got {@code n} boundaries for consecutive blocks. */
    private static void assertConsecutiveBoundaries(final StubSession s, final int n) {
        final List<Long> blocks = boundaryBlocks(s);
        assertEquals(n, blocks.size(), "boundaries: " + blocks);
        assertEquals(LongStream.range(blocks.get(0), blocks.get(0) + n).boxed().toList(), blocks);
    }

    /**
     * The canonical-stream frames a session got (relayed records and
     * boundaries), in arrival order. Other egress kinds are left out.
     */
    private static List<String> stream(final StubSession s) {
        return s.offered.stream()
            .filter(f -> f[0] == SealerWire.EGRESS_KIND_RELAYED || f[0] == SealerWire.EGRESS_KIND_BOUNDARY)
            .map(Arrays::toString)
            .toList();
    }

    @Test
    void aWedgedConsumerDoesNotStallTheTick() {
        wedged.backPressured = true;

        final long start = System.nanoTime();
        IntStream.range(0, 5).forEach(this::record);
        tick();
        tick();
        final long elapsedNs = System.nanoTime() - start;

        assertTrue(elapsedNs < STALL_NS / 10, "offers must not wait on a wedged session: " + elapsedNs);
        assertEquals(5, count(healthy, SealerWire.EGRESS_KIND_RELAYED));
        assertConsecutiveBoundaries(healthy, 2);
        assertEquals(0, wedged.closes, "a backlog inside the stall deadline keeps its session");
        assertTrue(stream(wedged).isEmpty());
    }

    @Test
    void aWedgedConsumerIsClosedOnceAfterTheStallDeadline() throws InterruptedException {
        wedged.backPressured = true;
        record(0);
        tick();
        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(STALL_NS) + 100);

        final long start = System.nanoTime();
        tick();
        record(1);
        tick();
        tick();
        final long elapsedNs = System.nanoTime() - start;

        assertEquals(1, wedged.closes, "a wedged session is closed exactly once");
        assertTrue(elapsedNs < STALL_NS / 10, "the close must not wait on the session: " + elapsedNs);
        assertEquals(2, count(healthy, SealerWire.EGRESS_KIND_RELAYED));
        assertConsecutiveBoundaries(healthy, 4);
        assertTrue(stream(wedged).isEmpty());
    }

    @Test
    void aConsumerThatRecoversGetsItsBacklogInOrder() {
        wedged.backPressured = true;
        IntStream.range(0, 3).forEach(this::record);
        tick();
        record(3);
        tick();

        wedged.backPressured = false;
        tick();
        record(4);

        assertEquals(0, wedged.closes);
        assertConsecutiveBoundaries(healthy, 3);
        assertEquals(stream(healthy), stream(wedged), "the backlog keeps emission order");
    }

    @Test
    void aRoleChangeDropsTheBacklog() {
        wedged.backPressured = true;
        record(0);
        service.onRoleChange(Cluster.Role.FOLLOWER);

        wedged.backPressured = false;
        tick();

        assertEquals(0, wedged.closes);
        assertEquals(0, count(wedged, SealerWire.EGRESS_KIND_RELAYED));
        assertEquals(1, count(wedged, SealerWire.EGRESS_KIND_BOUNDARY));
    }

    @Test
    void aHealthyConsumerIsNeverClosed() {
        IntStream.range(0, 3).forEach(this::record);
        tick();
        assertEquals(0, healthy.closes);
        assertEquals(0, wedged.closes);
        assertEquals(3, count(wedged, SealerWire.EGRESS_KIND_RELAYED));
    }
}
