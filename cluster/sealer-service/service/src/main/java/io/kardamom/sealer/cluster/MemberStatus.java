package io.kardamom.sealer.cluster;

import io.kardamom.sealer.CanonicalSealerState;

/**
 * One sample of a member's health: its Raft role, the election state, the
 * commit position the consensus module knows, the position the service
 * has applied, and the snapshot versions the member writes, reads and
 * holds.
 *
 * <p>The sample is a plain value. The admin endpoint renders it as JSON, and
 * the readiness rule reads it. A member is ready when it holds a settled
 * role (LEADER or FOLLOWER), its election is CLOSED, and the service has
 * applied the committed log up to the lag budget. The role and election
 * read {@code CLOSED} when the counters behind them are closed: the member
 * is on its way out and never ready.</p>
 */
final class MemberStatus {
    static final String ROLE_LEADER = "LEADER";
    static final String ROLE_FOLLOWER = "FOLLOWER";
    static final String ELECTION_CLOSED = "CLOSED";

    /**
     * The snapshot versions of a member: the version this release writes,
     * the range it reads, and the version of the newest snapshot the member
     * restored or took ({@code latest}, 0 before either). A deploy refuses
     * a release whose read maximum is below {@code latest}: that member
     * could not restore its own snapshot after the roll.
     */
    record SnapshotVersions(int writes, int readsMin, int readsMax, int latest) {
        /** This release's constants, with the member's {@code latest}. */
        static SnapshotVersions of(final int latest) {
            return new SnapshotVersions(
                CanonicalSealerState.snapshotWriteVersion(),
                CanonicalSealerState.SNAPSHOT_READ_MIN_VERSION,
                CanonicalSealerState.snapshotReadMaxVersion(),
                latest);
        }
    }

    final int memberId;
    final String role;
    final String election;
    final long commitPosition;
    final long servicePosition;
    final SnapshotVersions snapshot;

    MemberStatus(
            final int memberId,
            final String role,
            final String election,
            final long commitPosition,
            final long servicePosition,
            final SnapshotVersions snapshot) {
        this.memberId = memberId;
        this.role = role;
        this.election = election;
        this.commitPosition = commitPosition;
        this.servicePosition = servicePosition;
        this.snapshot = snapshot;
    }

    /**
     * Whether the member does its job: a settled role, no election in
     * flight, and the service within {@code lagBytes} of the commit
     * position.
     */
    boolean isReady(final long lagBytes) {
        final boolean settledRole = ROLE_LEADER.equals(role) || ROLE_FOLLOWER.equals(role);
        return settledRole
            && ELECTION_CLOSED.equals(election)
            && commitPosition - servicePosition <= lagBytes;
    }

    /** One JSON line. The values are numbers, enum names, and a boolean. */
    String toJson(final long lagBytes) {
        return "{\"memberId\":" + memberId
            + ",\"role\":\"" + role + "\""
            + ",\"election\":\"" + election + "\""
            + ",\"commitPosition\":" + commitPosition
            + ",\"servicePosition\":" + servicePosition
            + ",\"snapshotWrites\":" + snapshot.writes()
            + ",\"snapshotReadsMin\":" + snapshot.readsMin()
            + ",\"snapshotReadsMax\":" + snapshot.readsMax()
            + ",\"snapshotLatest\":" + snapshot.latest()
            + ",\"ready\":" + isReady(lagBytes)
            + "}";
    }
}
