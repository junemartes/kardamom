package io.kardamom.sealer.cluster;

import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.logbuffer.Header;
import io.aeron.test.cluster.TestNode;
import java.util.concurrent.TimeUnit;
import org.agrona.DirectBuffer;

/**
 * A {@link TestNode.TestService} whose every {@link io.aeron.cluster.service.ClusteredService}
 * callback delegates to a real {@link SealerClusteredService}. The 1.44.0
 * {@link io.aeron.test.cluster.TestCluster} can only construct services through a
 * {@code Supplier<TestNode.TestService[]>}, so an external {@code ClusteredService}
 * is injected by composition here — the harness drives THIS object, which forwards
 * verbatim to the production service. No behaviour is added or intercepted.
 *
 * <p>Shared by {@link SealerClusterFailoverTest} and {@link SealerReplayTest}.</p>
 */
final class SealerTestService extends TestNode.TestService {
    private final SealerClusteredService delegate;

    SealerTestService(final int dedupCapacity, final long tickMs, final int memberId) {
        this.delegate = new SealerClusteredService(dedupCapacity, tickMs, memberId);
    }

    @Override
    public void onStart(final Cluster cluster, final Image snapshotImage) {
        // WARNING — snapshot/recovery limitation: on a SNAPSHOT-recovery start both
        // super.onStart(...) and delegate.onStart(...) are handed the SAME snapshot
        // Image. An Image is a consumable cursor: whoever polls it first DRAINS it,
        // so the second consumer sees an empty image and silently starts from genesis.
        // This is harmless ONLY because these tests never take a snapshot (snapshotImage
        // is always null here). DO NOT reuse this wrapper as-is for any snapshot/
        // recovery test case: in that scenario the DELEGATE — not super — must be the
        // one to consume the snapshot Image (super must be given a null/no-op image),
        // otherwise the production service will lose its recovered state.
        super.onStart(cluster, snapshotImage); // lets the harness latch its Cluster ref
        delegate.onStart(cluster, snapshotImage);
    }

    @Override
    public void onSessionOpen(final ClientSession session, final long timestamp) {
        delegate.onSessionOpen(session, timestamp);
    }

    @Override
    public void onSessionClose(
            final ClientSession session, final long timestamp, final CloseReason closeReason) {
        delegate.onSessionClose(session, timestamp, closeReason);
    }

    @Override
    public void onSessionMessage(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header) {
        delegate.onSessionMessage(session, timestamp, buffer, offset, length, header);
    }

    @Override
    public void onTimerEvent(final long correlationId, final long timestamp) {
        delegate.onTimerEvent(correlationId, timestamp);
    }

    @Override
    public void onNewLeadershipTermEvent(
            final long leadershipTermId,
            final long logPosition,
            final long timestamp,
            final long termBaseLogPosition,
            final int leaderMemberId,
            final int logSessionId,
            final TimeUnit timeUnit,
            final int appVersion) {
        // Drive the production service's deferred initial boundary-timer (re)arming.
        delegate.onNewLeadershipTermEvent(
                leadershipTermId, logPosition, timestamp, termBaseLogPosition,
                leaderMemberId, logSessionId, timeUnit, appVersion);
    }

    @Override
    public void onTakeSnapshot(final io.aeron.ExclusivePublication snapshotPublication) {
        delegate.onTakeSnapshot(snapshotPublication);
    }

    @Override
    public void onRoleChange(final Cluster.Role newRole) {
        super.onRoleChange(newRole);
        delegate.onRoleChange(newRole);
    }

    @Override
    public void onTerminate(final Cluster cluster) {
        delegate.onTerminate(cluster);
        super.onTerminate(cluster);
    }
}
