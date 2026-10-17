package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** The readiness rule and the JSON shape of one member-status sample. */
final class MemberStatusTest {
    private static final long LAG = 1024;

    private static final MemberStatus.SnapshotVersions SNAPSHOT =
        new MemberStatus.SnapshotVersions(10, 1, 11, 9);

    private static MemberStatus sample(
            final String role, final String election, final long commit, final long applied) {
        return new MemberStatus(1, role, election, commit, applied, SNAPSHOT, java.util.OptionalLong.of(0));
    }

    @Test
    void leaderWithClosedElectionWithinLagIsReady() {
        assertTrue(sample("LEADER", "CLOSED", 5000, 4000).isReady(LAG));
    }

    @Test
    void followerWithClosedElectionIsReady() {
        assertTrue(sample("FOLLOWER", "CLOSED", 5000, 5000).isReady(LAG));
    }

    @Test
    void candidateIsNotReady() {
        assertFalse(sample("CANDIDATE", "CLOSED", 5000, 5000).isReady(LAG));
    }

    @Test
    void followerInsideAnElectionIsNotReady() {
        assertFalse(sample("FOLLOWER", "FOLLOWER_CATCHUP", 5000, 5000).isReady(LAG));
    }

    @Test
    void lagOverBudgetIsNotReady() {
        assertFalse(sample("FOLLOWER", "CLOSED", 5000, 3000).isReady(LAG));
    }

    @Test
    void closedCountersAreNotReady() {
        assertFalse(sample("CLOSED", "CLOSED", 0, 0).isReady(LAG));
    }

    @Test
    void jsonCarriesEveryFieldInOrder() {
        assertEquals(
            "{\"memberId\":1,\"role\":\"LEADER\",\"election\":\"CLOSED\","
                + "\"commitPosition\":5000,\"servicePosition\":4000,"
                + "\"snapshotWrites\":10,\"snapshotReadsMin\":1,\"snapshotReadsMax\":11,"
                + "\"snapshotLatest\":9,\"diskFreeBytes\":0,"
                + "\"syncedPosition\":-1,\"durablePosition\":-1,\"durableSyncs\":0,\"durableSyncErrors\":0,"
                + "\"durableSyncLastUs\":0,\"durableSyncMaxUs\":0,\"ready\":true}",
            sample("LEADER", "CLOSED", 5000, 4000).toJson(LAG));
    }

    @Test
    void metricsCarryTheFreeDiskAndTheDurablePositionOfTheMember() {
        final String metrics = sample("LEADER", "CLOSED", 5000, 4000)
            .withDurability(new DurableSync.Durability(4096, 12, 1, 2_500_000, 40_000_000))
            .toMetrics();
        assertTrue(metrics.startsWith(
            "# HELP kardamom_sealer_disk_free_bytes The free bytes of the disk of the member state.\n"
                + "# TYPE kardamom_sealer_disk_free_bytes gauge\n"
                + "kardamom_sealer_disk_free_bytes{member_id=\"1\"} 0\n"), metrics);
        assertTrue(metrics.contains("kardamom_sealer_commit_position{member_id=\"1\"} 5000\n"), metrics);
        assertTrue(metrics.contains("kardamom_sealer_synced_position{member_id=\"1\"} 4096\n"), metrics);
        assertTrue(metrics.contains("# TYPE kardamom_sealer_durable_position gauge\n"
            + "kardamom_sealer_durable_position{member_id=\"1\"} 4096\n"), metrics);
        assertTrue(metrics.contains("kardamom_sealer_durable_syncs_total{member_id=\"1\"} 12\n"), metrics);
        assertTrue(metrics.contains("kardamom_sealer_durable_sync_errors_total{member_id=\"1\"} 1\n"), metrics);
        assertTrue(metrics.contains("kardamom_sealer_durable_sync_seconds{member_id=\"1\"} 0.0025\n"), metrics);
        assertTrue(metrics.contains("kardamom_sealer_durable_sync_max_seconds{member_id=\"1\"} 0.04\n"), metrics);
    }

    @Test
    void theDurablePositionNeverPassesTheCommitPosition() {
        final MemberStatus status = sample("FOLLOWER", "CLOSED", 5000, 4000);
        assertEquals(-1, status.durablePosition(), "no sync yet");
        assertEquals(4096, status.withDurability(synced(4096)).durablePosition(), "the sync lags the commit");
        assertEquals(5000, status.withDurability(synced(5000)).durablePosition());
        assertEquals(5000, status.withDurability(synced(9000)).durablePosition(),
            "a follower syncs appended entries that are not committed yet");
        assertTrue(status.withDurability(synced(9000)).toJson(LAG)
            .contains("\"syncedPosition\":9000,\"durablePosition\":5000,"));
    }

    private static DurableSync.Durability synced(final long position) {
        return new DurableSync.Durability(position, 1, 0, 0, 0);
    }

    @Test
    void snapshotVersionsOfThisReleaseComeFromTheState() {
        final MemberStatus.SnapshotVersions versions = MemberStatus.SnapshotVersions.of(7);
        assertEquals(io.kardamom.sealer.CanonicalSealerState.snapshotWriteVersion(), versions.writes());
        assertEquals(io.kardamom.sealer.CanonicalSealerState.SNAPSHOT_READ_MIN_VERSION, versions.readsMin());
        assertEquals(io.kardamom.sealer.CanonicalSealerState.snapshotReadMaxVersion(), versions.readsMax());
        assertEquals(7, versions.latest());
    }
}
