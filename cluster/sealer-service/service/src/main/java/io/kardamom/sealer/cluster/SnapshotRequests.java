package io.kardamom.sealer.cluster;

import io.aeron.cluster.ClusterControl;
import io.aeron.cluster.service.Cluster;
import java.util.Optional;
import org.agrona.concurrent.status.AtomicCounter;

/**
 * The snapshot requests of the service, and the stall that each snapshot
 * causes.
 *
 * <p>Only the consensus module of the leader reads its control toggle. A
 * follower never sets its toggle: the toggle stays set, and the module
 * takes it when the member leads later. The snapshot action then goes
 * through the log to every member.</p>
 *
 * <p>The consensus module fires no timer while it takes a snapshot. So the
 * first timer after a snapshot ends the stall, and the stall starts at the
 * boundary tick before the snapshot: no status frame leaves the leader
 * between the two.</p>
 */
final class SnapshotRequests {
    /** The stall start while no snapshot waits for its first timer. */
    private static final long NO_STALL = -1L;

    private final Cluster cluster;
    private final int memberId;
    /** Cluster time of the boundary tick before the newest snapshot, or {@link #NO_STALL}. */
    private long stallFromMs = NO_STALL;

    SnapshotRequests(final Cluster cluster, final int memberId) {
        this.cluster = cluster;
        this.memberId = memberId;
    }

    /**
     * Ask the consensus module of this member for a snapshot when this
     * member leads. The value tells whether the toggle took the request; a
     * toggle that holds another request refuses it. Empty on a follower.
     */
    Optional<Boolean> request() {
        if (cluster.role() != Cluster.Role.LEADER) {
            return Optional.empty();
        }
        final AtomicCounter toggle = ClusterControl.findControlToggle(
            cluster.aeron().countersReader(), cluster.context().clusterId());
        return Optional.of(toggle != null && ClusterControl.ToggleState.SNAPSHOT.toggle(toggle));
    }

    /** A snapshot starts. The stall runs from {@code lastTickMs}, the cluster time of the last boundary tick. */
    void onSnapshot(final long lastTickMs) {
        stallFromMs = lastTickMs;
    }

    /**
     * A timer fires at cluster time {@code nowMs}. The first timer after a
     * snapshot ends the stall. The leader then logs that its snapshot is
     * complete and how long the status frames stopped.
     */
    void onTimer(final long nowMs, final long blockNumber) {
        if (stallFromMs == NO_STALL) {
            return;
        }
        final long stallMs = nowMs - stallFromMs;
        stallFromMs = NO_STALL;
        if (cluster.role() == Cluster.Role.LEADER) {
            System.out.println("cluster SNAPSHOT triggered memberId=" + memberId
                + " block=" + blockNumber + " stallMs=" + stallMs);
        }
    }
}
