package io.kardamom.sealer.cluster;

import io.kardamom.sealer.CanonicalSealerState;
import java.util.OptionalLong;

/**
 * One sample of a member's health: its Raft role, the election state, the
 * commit position the consensus module knows, the position the service
 * has applied, the snapshot versions the member writes, reads and
 * holds, and the free bytes of the disk that holds its state.
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
    record SnapshotVersions(int writes, int readsMin, int readsMax, int latest, long releasesSince) {
        /** This release's constants, with the member's {@code latest} and no release since it. */
        static SnapshotVersions of(final int latest) {
            return of(latest, 0L);
        }

        /**
         * This release's constants, with the member's {@code latest}, and the
         * nonce-release frames it applied since its last snapshot.
         */
        static SnapshotVersions of(final int latest, final long releasesSince) {
            return new SnapshotVersions(
                CanonicalSealerState.snapshotWriteVersion(),
                CanonicalSealerState.SNAPSHOT_READ_MIN_VERSION,
                CanonicalSealerState.snapshotReadMaxVersion(),
                latest,
                releasesSince);
        }
    }

    final int memberId;
    final String role;
    final String election;
    final long commitPosition;
    final long servicePosition;
    final SnapshotVersions snapshot;
    /**
     * The bytes that the member can still write to the disk of its retained
     * file and its log, or empty when the disk cannot be read.
     */
    final OptionalLong diskFreeBytes;

    MemberStatus(
            final int memberId,
            final String role,
            final String election,
            final long commitPosition,
            final long servicePosition,
            final SnapshotVersions snapshot,
            final OptionalLong diskFreeBytes) {
        this.memberId = memberId;
        this.role = role;
        this.election = election;
        this.commitPosition = commitPosition;
        this.servicePosition = servicePosition;
        this.snapshot = snapshot;
        this.diskFreeBytes = diskFreeBytes;
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
            + ",\"releasesSinceSnapshot\":" + snapshot.releasesSince()
            + ",\"decisionVersion\":" + CanonicalSealerState.DECISION_VERSION
            + ",\"diskFreeBytes\":" + diskFree("null")
            + ",\"ready\":" + isReady(lagBytes)
            + "}";
    }

    /** The Prometheus text of the gauges. An unknown value is {@code NaN}. */
    String toMetrics() {
        return "# HELP kardamom_sealer_disk_free_bytes The free bytes of the disk of the member state.\n"
            + "# TYPE kardamom_sealer_disk_free_bytes gauge\n"
            + "kardamom_sealer_disk_free_bytes{member_id=\"" + memberId + "\"} "
            + diskFree("NaN") + "\n";
    }

    /** The free bytes as text, or {@code unknown} when the disk cannot be read. */
    private String diskFree(final String unknown) {
        return diskFreeBytes.isPresent() ? Long.toString(diskFreeBytes.getAsLong()) : unknown;
    }
}
