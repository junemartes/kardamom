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
        return new MemberStatus(1, role, election, commit, applied, SNAPSHOT);
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
                + "\"snapshotLatest\":9,\"ready\":true}",
            sample("LEADER", "CLOSED", 5000, 4000).toJson(LAG));
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
