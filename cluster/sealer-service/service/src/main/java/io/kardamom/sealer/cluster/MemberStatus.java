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
    private static final long NANOS_PER_MICRO = 1_000L;
    private static final long NANOS_PER_SECOND = 1_000_000_000L;

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
    /**
     * The bytes that the member can still write to the disk of its retained
     * file and its log, or empty when the disk cannot be read.
     */
    final OptionalLong diskFreeBytes;
    /** The synced log position of the member, and the rounds of its disk sync. */
    final DurableSync.Durability durability;

    MemberStatus(
            final int memberId,
            final String role,
            final String election,
            final long commitPosition,
            final long servicePosition,
            final SnapshotVersions snapshot,
            final OptionalLong diskFreeBytes) {
        this(memberId, role, election, commitPosition, servicePosition, snapshot, diskFreeBytes,
            DurableSync.Durability.NONE);
    }

    private MemberStatus(
            final int memberId,
            final String role,
            final String election,
            final long commitPosition,
            final long servicePosition,
            final SnapshotVersions snapshot,
            final OptionalLong diskFreeBytes,
            final DurableSync.Durability durability) {
        this.memberId = memberId;
        this.role = role;
        this.election = election;
        this.commitPosition = commitPosition;
        this.servicePosition = servicePosition;
        this.snapshot = snapshot;
        this.diskFreeBytes = diskFreeBytes;
        this.durability = durability;
    }

    /** This status with the synced log position of {@code durability}. */
    MemberStatus withDurability(final DurableSync.Durability durability) {
        return new MemberStatus(
            memberId, role, election, commitPosition, servicePosition, snapshot, diskFreeBytes, durability);
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

    /**
     * The log position below which every byte is committed and on the disk
     * of this member: the lower of the synced position and the commit
     * position, or -1 before the first sync. The median over the members is
     * the position that a majority holds on disk.
     */
    long durablePosition() {
        return durability.position() < 0 ? -1L : Math.min(durability.position(), commitPosition);
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
            + ",\"diskFreeBytes\":" + diskFree("null")
            + ",\"syncedPosition\":" + durability.position()
            + ",\"durablePosition\":" + durablePosition()
            + ",\"durableSyncs\":" + durability.rounds()
            + ",\"durableSyncErrors\":" + durability.errors()
            + ",\"durableSyncLastUs\":" + durability.lastRoundNs() / NANOS_PER_MICRO
            + ",\"durableSyncMaxUs\":" + durability.maxRoundNs() / NANOS_PER_MICRO
            + ",\"ready\":" + isReady(lagBytes)
            + "}";
    }

    /** The Prometheus text of the gauges. An unknown value is {@code NaN}. */
    String toMetrics() {
        return metric("kardamom_sealer_disk_free_bytes", "gauge",
                "The free bytes of the disk of the member state.", diskFree("NaN"))
            + metric("kardamom_sealer_commit_position", "gauge",
                "The commit position of the Raft log that the member knows.", Long.toString(commitPosition))
            + metric("kardamom_sealer_synced_position", "gauge",
                "The log position below which every byte of the Raft log of the member is on disk; -1 before the first sync.",
                Long.toString(durability.position()))
            + metric("kardamom_sealer_durable_position", "gauge",
                "The lower of the synced position and the commit position; -1 before the first sync.",
                Long.toString(durablePosition()))
            + metric("kardamom_sealer_durable_syncs_total", "counter",
                "The rounds of the disk sync of the Raft log.", Long.toString(durability.rounds()))
            + metric("kardamom_sealer_durable_sync_errors_total", "counter",
                "The rounds of the disk sync that failed.", Long.toString(durability.errors()))
            + metric("kardamom_sealer_durable_sync_seconds", "gauge",
                "The duration of the last round of the disk sync.", seconds(durability.lastRoundNs()))
            + metric("kardamom_sealer_durable_sync_max_seconds", "gauge",
                "The longest round of the disk sync since the start.", seconds(durability.maxRoundNs()));
    }

    /** One gauge or counter of this member in the Prometheus text format. */
    private String metric(final String name, final String type, final String help, final String value) {
        return "# HELP " + name + " " + help + "\n"
            + "# TYPE " + name + " " + type + "\n"
            + name + "{member_id=\"" + memberId + "\"} " + value + "\n";
    }

    private static String seconds(final long nanos) {
        return Double.toString(nanos / (double) NANOS_PER_SECOND);
    }

    /** The free bytes as text, or {@code unknown} when the disk cannot be read. */
    private String diskFree(final String unknown) {
        return diskFreeBytes.isPresent() ? Long.toString(diskFreeBytes.getAsLong()) : unknown;
    }
}
