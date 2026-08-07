package io.kardamom.sealer.cluster;

import static io.kardamom.sealer.cluster.IngressFrames.canonicalId;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.cluster.ClusterStubs.StubCluster;
import io.kardamom.sealer.cluster.ClusterStubs.StubSession;
import java.util.concurrent.TimeUnit;
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
            original.onRecord(canonicalId(i), new byte[0]);
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
        assertFalse(restored.firstSeen(canonicalId(4999)), "snapshotted id must still dedup");
        assertTrue(restored.firstSeen(canonicalId(5001)), "unseen id must be fresh");
    }

    @Test
    void emptySnapshotImageIsFatal() {
        try (Subscription sub = aeron.addSubscription(CHANNEL, 1002);
             ExclusivePublication pub = aeron.addExclusivePublication(CHANNEL, 1002)) {
            final Image image = awaitImage(sub, pub);
            pub.close(); // end-of-stream with ZERO bytes written
            final IllegalStateException e =
                    assertThrows(IllegalStateException.class, () -> SnapshotIo.readSnapshot(image, IDLE));
            assertTrue(e.getMessage().contains("empty"), "must name the empty image: " + e.getMessage());
        }
    }

    @Test
    void restoredMemberRefusesPreSnapshotReplay() {
        // Snapshot a state that has ordered 10 records across 3 sealed blocks.
        final CanonicalSealerState original = new CanonicalSealerState(64);
        for (int i = 0; i < 10; i++) {
            original.onRecord(canonicalId(i), new byte[] {(byte) i});
        }
        original.onTick(250);
        original.onTick(500);
        original.onTick(750);
        final long count = original.canonicalCount(); // 10
        final long block = original.blockNumber(); // 4 (next to stamp)

        try (Subscription sub = aeron.addSubscription(CHANNEL, 1003);
             ExclusivePublication pub = aeron.addExclusivePublication(CHANNEL, 1003)) {
            final Image image = awaitImage(sub, pub);
            SnapshotIo.writeSnapshot(pub, original.takeSnapshot(), IDLE);
            pub.close();

            final StubCluster cluster = new StubCluster();
            final SealerClusteredService service = new SealerClusteredService(64, 250, 0);
            service.onStart(cluster, image);

            // A cursor anywhere before the restore point must be REFUSED —
            // the retained deque is empty, so a DONE here would be a silent gap.
            final StubSession behind = cluster.addSession(7);
            service.onSessionMessage(behind, 0, IngressFrames.replayRequest(0, 1), 0, 17, null);
            assertEquals(1, behind.offered.size(), "exactly one control frame");
            assertEquals(SealerWire.EGRESS_KIND_REPLAY_UNAVAILABLE, behind.offered.get(0)[0],
                    "pre-snapshot replay must be UNAVAILABLE, not DONE");
            assertEquals(count, longAt(behind.offered.get(0), 1), "floor index = restored canonicalCount");
            assertEquals(block, longAt(behind.offered.get(0), 9), "floor block = restored blockNumber");

            // A client already AT the restore point needs nothing replayed:
            // that request is honestly complete (DONE), proving the floors are
            // exact rather than merely conservative.
            final StubSession caughtUp = cluster.addSession(8);
            service.onSessionMessage(caughtUp, 0, IngressFrames.replayRequest(count, block), 0, 17, null);
            assertEquals(1, caughtUp.offered.size(), "exactly one control frame");
            assertEquals(SealerWire.EGRESS_KIND_REPLAY_DONE, caughtUp.offered.get(0)[0],
                    "replay from the restore point itself must complete");
        }
    }

    // --- harness -----------------------------------------------------------

    /** Write {@code snapshot} on a fresh stream, close the pub, read it back. */
    private byte[] writeAndRead(final byte[] snapshot, final int streamId) {
        try (Subscription sub = aeron.addSubscription(CHANNEL, streamId);
             ExclusivePublication pub = aeron.addExclusivePublication(CHANNEL, streamId)) {
            final Image image = awaitImage(sub, pub);
            SnapshotIo.writeSnapshot(pub, snapshot, IDLE);
            pub.close();
            return SnapshotIo.readSnapshot(image, IDLE);
        }
    }

    private static Image awaitImage(final Subscription sub, final ExclusivePublication pub) {
        while (!pub.isConnected() || sub.imageCount() == 0) {
            IDLE.idle();
        }
        IDLE.reset();
        return sub.imageAtIndex(0);
    }

    private static long longAt(final byte[] frame, final int offset) {
        long v = 0;
        for (int i = 7; i >= 0; i--) {
            v = (v << 8) | (frame[offset + i] & 0xFFL);
        }
        return v;
    }
}
