package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.cluster.service.Cluster;
import io.kardamom.sealer.cluster.ClusterStubs.StubCluster;
import io.kardamom.sealer.cluster.ClusterStubs.StubSession;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/**
 * The periodic snapshot: the setting, the due ticks, and the snapshot
 * notice on the status frame of a due tick.
 */
class SnapshotScheduleTest {

    /** The offset of the {@code snapshot_follows} byte in a status frame. */
    private static final int SNAPSHOT_FOLLOWS_OFFSET = 67;

    @Test
    void theSettingGivesTheIntervalInBlocks() {
        assertEquals(Optional.of(new SnapshotSchedule(150)), SnapshotSchedule.fromSetting(300, 2000));
        assertEquals(Optional.empty(), SnapshotSchedule.fromSetting(0, 2000));
        assertThrows(IllegalStateException.class, () -> SnapshotSchedule.fromSetting(1, 2000));
        assertThrows(IllegalArgumentException.class, () -> new SnapshotSchedule(0));
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
