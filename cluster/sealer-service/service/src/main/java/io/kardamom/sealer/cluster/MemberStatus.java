package io.kardamom.sealer.cluster;

/**
 * One sample of a member's health: its Raft role, the election state, the
 * commit position the consensus module knows, and the position the service
 * has applied.
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

    final int memberId;
    final String role;
    final String election;
    final long commitPosition;
    final long servicePosition;

    MemberStatus(
            final int memberId,
            final String role,
            final String election,
            final long commitPosition,
            final long servicePosition) {
        this.memberId = memberId;
        this.role = role;
        this.election = election;
        this.commitPosition = commitPosition;
        this.servicePosition = servicePosition;
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
            + ",\"ready\":" + isReady(lagBytes)
            + "}";
    }
}
