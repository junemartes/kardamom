package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.cluster.service.Cluster;
import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.cluster.ClusterStubs.StubCluster;
import io.kardamom.sealer.cluster.ClusterStubs.StubSession;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;

/**
 * The periodic snapshot: the setting, the due ticks, and the snapshot
 * notice on the status frame of a due tick.
 */
class SnapshotScheduleTest {

    private static final int SNAPSHOT_FOLLOWS_OFFSET = SealerWire.STATUS_SNAPSHOT_FLAG_OFFSET;

    /** The flag sits at byte 67, after the record-lag tail; a reader of a longer frame finds it there. */
    @Test
    void theSnapshotFlagSitsAtByte67() {
        assertEquals(67, SealerWire.STATUS_SNAPSHOT_FLAG_OFFSET);
        assertEquals(68, SealerWire.STATUS_WITH_SNAPSHOT_FLAG_LEN);
    }

    @Test
    void theSettingGivesTheIntervalInBlocks() {
        assertEquals(Optional.of(new SnapshotSchedule(150)), SnapshotSchedule.fromSetting(300, 2000));
        assertEquals(Optional.empty(), SnapshotSchedule.fromSetting(0, 2000));
        assertThrows(IllegalStateException.class, () -> SnapshotSchedule.fromSetting(1, 2000));
    }

    @Test
    void aTickIsDueAtEachMultipleOfTheInterval() {
        final SnapshotSchedule schedule = new SnapshotSchedule(3);
        assertEquals(List.of(true, false, false, true, false, false, true),
            IntStream.rangeClosed(0, 6).mapToObj(schedule::due).toList());
    }

    /**
     * Every member sends the notice on the status of a due tick, and only
     * there: the notice is a function of the block number. A follower asks
     * for no snapshot, so the stub needs no control toggle.
     */
    @Test
    void theStatusOfADueTickCarriesTheSnapshotNotice() {
        final StubCluster cluster = new StubCluster();
        cluster.role = Cluster.Role.FOLLOWER;
        final SealerClusteredService service =
            new SealerClusteredService(64, 250, 0).snapshotEvery(new SnapshotSchedule(3));
        service.onStart(cluster, null);
        final StubSession session = cluster.addSession(1);

        final List<Boolean> notices = IntStream.range(0, 6)
            .mapToObj(i -> tickNotice(service, session))
            .toList();

        assertEquals(2, notices.stream().filter(n -> n).count(), "two of six ticks are due: " + notices);
        assertEquals(notices.subList(0, 3), notices.subList(3, 6), "the notice repeats every 3 blocks");
        assertTrue(session.offered.stream()
            .filter(f -> f[0] == SealerWire.EGRESS_KIND_STATUS)
            .allMatch(f -> f.length == SNAPSHOT_FOLLOWS_OFFSET + 1), "every status has the snapshot tail");
    }

    /**
     * A boundary that an origin record forces opens a block as a tick does.
     * So a forced boundary that opens a due block sends the notice too, and
     * no due block waits for one more interval.
     */
    @Test
    void aForcedBoundaryOfADueBlockSendsTheNotice() {
        final StubCluster cluster = new StubCluster();
        cluster.role = Cluster.Role.FOLLOWER;
        final SealerClusteredService service =
            new SealerClusteredService(64, 250, 0).snapshotEvery(new SnapshotSchedule(3));
        service.onStart(cluster, null);
        final StubSession session = cluster.addSession(1);

        // Each origin record after the first forces a boundary, with no tick.
        IntStream.range(0, 7).forEach(n -> originRecord(service, session, n));

        final long notices = session.offered.stream()
            .filter(f -> f[0] == SealerWire.EGRESS_KIND_STATUS && f[SNAPSHOT_FOLLOWS_OFFSET] == 1)
            .count();
        assertEquals(2, notices, "six forced boundaries open two due blocks");
    }

    /** Deliver the origin record of L1 block {@code 100 + n}. */
    private static void originRecord(final SealerClusteredService service, final StubSession from, final int n) {
        final byte[] id = new byte[CanonicalSealerState.CANONICAL_ID_LEN];
        id[0] = (byte) n;
        id[31] = 0x5A;
        final byte[] frame = IngressFrames.originRecordFrame(id, 100L + n, 1, new byte[] {(byte) n});
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        buf.putBytes(0, frame);
        service.onSessionMessage(from, 0, buf, 0, frame.length, null);
    }

    /** Fire one boundary tick, and return the snapshot notice of its status frame. */
    private static boolean tickNotice(final SealerClusteredService service, final StubSession session) {
        service.onTimerEvent(SealerClusteredService.BOUNDARY_TIMER_CORRELATION_ID, 250);
        final byte[] status = session.offered.stream()
            .filter(f -> f[0] == SealerWire.EGRESS_KIND_STATUS)
            .reduce((first, second) -> second)
            .orElseThrow();
        return status[SNAPSHOT_FOLLOWS_OFFSET] == 1;
    }
}
