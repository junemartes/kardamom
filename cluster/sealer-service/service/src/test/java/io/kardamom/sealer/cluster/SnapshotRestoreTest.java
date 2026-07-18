package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import io.kardamom.sealer.CanonicalSealerState;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Snapshot I/O regression tests against a REAL embedded media driver.
 *
 * <ul>
 *   <li><b>F12.1</b> — a realistic snapshot (dedup window ≫ MTU) arrives as
 *       many fragments; {@code readSnapshot} must reassemble ALL of them, not
 *       truncate at the first.</li>
 *   <li><b>F12.2</b> — an empty snapshot image must be FATAL, never a silent
 *       restart at genesis.</li>
 *   <li><b>F07.1</b> — a member restored from snapshot must answer a
 *       pre-snapshot replay request with REPLAY_UNAVAILABLE (its retention is
 *       not snapshotted), not a bogus REPLAY_DONE.</li>
 * </ul>
 *
 * <p>The channel pins {@code mtu=4096} so fragmentation is guaranteed
 * regardless of driver defaults; {@code term-length=1m} keeps the whole
 * snapshot inside the publication window so the writes need no concurrent
 * drain.</p>
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class SnapshotRestoreTest {

    private static final String CHANNEL = "aeron:ipc?term-length=1m|mtu=4096";
    private static final IdleStrategy IDLE = new YieldingIdleStrategy();

    private MediaDriver driver;
    private Aeron aeron;

    @BeforeEach
    void setUp() {
        driver = MediaDriver.launchEmbedded(new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true));
        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
    }

    @AfterEach
    void tearDown() {
        org.agrona.CloseHelper.quietCloseAll(aeron, driver);
    }

    @Test
    void snapshotLargerThanMtuRoundTrips() {
        // 5000 ids ⇒ a ~160KB snapshot: ~40 fragments at mtu=4096, and TWO
        // messages through writeSnapshot (maxMessageLength = term/8 = 128KB).
        final CanonicalSealerState original = new CanonicalSealerState(8192);
        for (int i = 0; i < 5000; i++) {
            original.onRecord(id(i), new byte[0]);
        }
        original.onTick(1000);
        original.onTick(1250);
        final byte[] snapshot = original.takeSnapshot();
        assertTrue(snapshot.length > 4096, "snapshot must exceed the MTU to exercise reassembly");

        final byte[] read = writeAndRead(snapshot, 1001);
        assertArrayEquals(snapshot, read, "reassembled snapshot must be byte-identical");

        final CanonicalSealerState restored = CanonicalSealerState.load(read, 8192);
        assertEquals(original.canonicalCount(), restored.canonicalCount());
        assertEquals(original.blockNumber(), restored.blockNumber());
        assertEquals(original.dedupSize(), restored.dedupSize());
        // Behavioural check: a snapshotted id still dedups, a fresh one relays.
        assertFalse(restored.firstSeen(id(4999)), "snapshotted id must still dedup");
        assertTrue(restored.firstSeen(id(5001)), "unseen id must be fresh");
    }

    @Test
    void emptySnapshotImageIsFatal() {
        try (Subscription sub = aeron.addSubscription(CHANNEL, 1002);
             ExclusivePublication pub = aeron.addExclusivePublication(CHANNEL, 1002)) {
            final Image image = awaitImage(sub, pub);
            pub.close(); // end-of-stream with ZERO bytes written
            final IllegalStateException e =
                    assertThrows(IllegalStateException.class, () -> SealerClusteredService.readSnapshot(image, IDLE));
            assertTrue(e.getMessage().contains("empty"), "must name the empty image: " + e.getMessage());
        }
    }

    @Test
    void restoredMemberRefusesPreSnapshotReplay() {
        // Snapshot a state that has ordered 10 records across 3 sealed blocks.
        final CanonicalSealerState original = new CanonicalSealerState(64);
        for (int i = 0; i < 10; i++) {
            original.onRecord(id(i), new byte[] {(byte) i});
        }
        original.onTick(250);
        original.onTick(500);
        original.onTick(750);
        final long count = original.canonicalCount(); // 10
        final long block = original.blockNumber(); // 4 (next to stamp)

        try (Subscription sub = aeron.addSubscription(CHANNEL, 1003);
             ExclusivePublication pub = aeron.addExclusivePublication(CHANNEL, 1003)) {
            final Image image = awaitImage(sub, pub);
            SealerClusteredService.writeSnapshot(pub, original.takeSnapshot(), IDLE);
            pub.close();

            final StubCluster cluster = new StubCluster();
            final SealerClusteredService service = new SealerClusteredService(64, 250, 0);
            service.onStart(cluster, image);

            // A cursor anywhere before the restore point must be REFUSED —
            // the retained deque is empty, so a DONE here would be a silent gap.
            final StubSession behind = cluster.addSession(7);
            service.onSessionMessage(behind, 0, replayRequest(0, 1), 0, 17, null);
            drain(service);
            assertEquals(1, behind.offered.size(), "exactly one control frame");
            assertEquals(SealerClusteredService.EGRESS_KIND_REPLAY_UNAVAILABLE, behind.offered.get(0)[0],
                    "pre-snapshot replay must be UNAVAILABLE, not DONE");
            assertEquals(count, longAt(behind.offered.get(0), 1), "floor index = restored canonicalCount");
            assertEquals(block, longAt(behind.offered.get(0), 9), "floor block = restored blockNumber");

            // A client already AT the restore point needs nothing replayed:
            // that request is honestly complete (DONE), proving the floors are
            // exact rather than merely conservative.
            final StubSession caughtUp = cluster.addSession(8);
            service.onSessionMessage(caughtUp, 0, replayRequest(count, block), 0, 17, null);
            drain(service);
            assertEquals(1, caughtUp.offered.size(), "exactly one control frame");
            assertEquals(SealerClusteredService.EGRESS_KIND_REPLAY_DONE, caughtUp.offered.get(0)[0],
                    "replay from the restore point itself must complete");
        }
    }

    // --- harness -----------------------------------------------------------

    /** Write {@code snapshot} on a fresh stream, close the pub, read it back. */
    private byte[] writeAndRead(final byte[] snapshot, final int streamId) {
        try (Subscription sub = aeron.addSubscription(CHANNEL, streamId);
             ExclusivePublication pub = aeron.addExclusivePublication(CHANNEL, streamId)) {
            final Image image = awaitImage(sub, pub);
            SealerClusteredService.writeSnapshot(pub, snapshot, IDLE);
            pub.close();
            return SealerClusteredService.readSnapshot(image, IDLE);
        }
    }

    private static Image awaitImage(final Subscription sub, final ExclusivePublication pub) {
        while (!pub.isConnected() || sub.imageCount() == 0) {
            IDLE.idle();
        }
        IDLE.reset();
        return sub.imageAtIndex(0);
    }

    private static void drain(final SealerClusteredService service) {
        // The replay drain is chunked across replay-timer events; a handful
        // suffices for these tiny replays (stub offers always succeed).
        for (int i = 0; i < 16; i++) {
            service.onTimerEvent(SealerClusteredService.REPLAY_TIMER_CORRELATION_ID, 0);
        }
    }

    private static ExpandableArrayBuffer replayRequest(final long fromIndex, final long fromBlock) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        buf.putByte(0, SealerClusteredService.KIND_REPLAY_REQUEST);
        buf.putLong(1, fromIndex, ByteOrder.LITTLE_ENDIAN);
        buf.putLong(1 + Long.BYTES, fromBlock, ByteOrder.LITTLE_ENDIAN);
        return buf;
    }

    private static long longAt(final byte[] frame, final int offset) {
        long v = 0;
        for (int i = 7; i >= 0; i--) {
            v = (v << 8) | (frame[offset + i] & 0xFFL);
        }
        return v;
    }

    private static byte[] id(final int n) {
        final byte[] out = new byte[CanonicalSealerState.CANONICAL_ID_LEN];
        out[0] = (byte) n;
        out[1] = (byte) (n >>> 8);
        out[31] = 0x5A;
        return out;
    }

    // --- minimal stubs (only what onStart/replay touches) -------------------

    private static final class StubSession implements ClientSession {
        final long id;
        final List<byte[]> offered = new ArrayList<>();
        boolean closed;

        StubSession(final long id) {
            this.id = id;
        }

        public long id() {
            return id;
        }

        public int responseStreamId() {
            return 0;
        }

        public String responseChannel() {
            return "aeron:ipc";
        }

        public byte[] encodedPrincipal() {
            return new byte[0];
        }

        public void close() {
            closed = true;
        }

        public boolean isClosing() {
            return closed;
        }

        public long offer(final DirectBuffer buffer, final int offset, final int length) {
            final byte[] copy = new byte[length];
            buffer.getBytes(offset, copy);
            offered.add(copy);
            return length;
        }

        public long offer(final io.aeron.DirectBufferVector[] vectors) {
            throw new UnsupportedOperationException();
        }

        public long tryClaim(final int length, final io.aeron.logbuffer.BufferClaim bufferClaim) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class StubCluster implements Cluster {
        final HashMap<Long, ClientSession> sessions = new HashMap<>();
        final IdleStrategy idleStrategy = new YieldingIdleStrategy();

        StubSession addSession(final long id) {
            final StubSession session = new StubSession(id);
            sessions.put(id, session);
            return session;
        }

        public int memberId() {
            return 0;
        }

        public Role role() {
            return Role.LEADER;
        }

        public long logPosition() {
            return 0;
        }

        public Aeron aeron() {
            throw new UnsupportedOperationException();
        }

        public io.aeron.cluster.service.ClusteredServiceContainer.Context context() {
            throw new UnsupportedOperationException();
        }

        public ClientSession getClientSession(final long clusterSessionId) {
            return sessions.get(clusterSessionId);
        }

        public Collection<ClientSession> clientSessions() {
            return sessions.values();
        }

        public void forEachClientSession(final Consumer<? super ClientSession> action) {
            sessions.values().forEach(action);
        }

        public boolean closeClientSession(final long clusterSessionId) {
            return sessions.remove(clusterSessionId) != null;
        }

        public long time() {
            return 0;
        }

        public TimeUnit timeUnit() {
            return TimeUnit.MILLISECONDS;
        }

        public boolean scheduleTimer(final long correlationId, final long deadline) {
            return true;
        }

        public boolean cancelTimer(final long correlationId) {
            return true;
        }

        public long offer(final DirectBuffer buffer, final int offset, final int length) {
            throw new UnsupportedOperationException();
        }

        public long offer(final io.aeron.DirectBufferVector[] vectors) {
            throw new UnsupportedOperationException();
        }

        public long tryClaim(final int length, final io.aeron.logbuffer.BufferClaim bufferClaim) {
            throw new UnsupportedOperationException();
        }

        public IdleStrategy idleStrategy() {
            return idleStrategy;
        }
    }
}
