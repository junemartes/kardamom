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
 * A {@link TestNode.TestService} whose every
 * {@link io.aeron.cluster.service.ClusteredService} callback delegates to a
 * real {@link SealerClusteredService}. The 1.44.0
 * {@link io.aeron.test.cluster.TestCluster} can only construct services
 * through a {@code Supplier<TestNode.TestService[]>}. So this class injects
 * an external {@code ClusteredService} by composition: the harness drives
 * this object, which forwards every call verbatim to the production
 * service. It adds or intercepts no behavior.
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
        // Snapshot-recovery limitation:
        // - On a snapshot-recovery start, both super.onStart and delegate.onStart
        //   get the same snapshot Image.
        // - An Image is a consumable cursor: the first caller to poll it drains it.
        //   The second caller then sees an empty image and starts from genesis
        //   without warning.
        // - This is harmless here only because these tests never take a snapshot;
        //   snapshotImage is always null.
        // - Do not reuse this wrapper for a snapshot or recovery test case as-is.
        //   In that case, only the delegate may consume the snapshot Image; give
        //   super a null or no-op image instead. Otherwise the production service
        //   loses its recovered state.
        super.onStart(cluster, snapshotImage); // Lets the harness latch its Cluster reference.
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
        // Drive the production service's deferred boundary-timer arming or rearming.
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
