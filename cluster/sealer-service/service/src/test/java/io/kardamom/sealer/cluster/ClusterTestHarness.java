package io.kardamom.sealer.cluster;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import java.util.function.BooleanSupplier;

/**
 * Shared setup for the in-JVM {@link TestCluster} tests: the cluster builder
 * chain that hosts {@link SealerTestService} on every member, and the
 * generic yield-driven egress await loop.
 */
final class ClusterTestHarness {

    private ClusterTestHarness() {
    }

    /**
     * Start a static {@code memberCount}-member TestCluster. Every member
     * hosts a real {@link SealerClusteredService} through
     * {@link SealerTestService}. Register the cluster with {@code watcher}.
     */
    static TestCluster startCluster(
            final SystemTestWatcher watcher,
            final int memberCount,
            final int dedupCapacity,
            final long tickMs) {
        final TestCluster cluster = TestCluster.aCluster()
                .withStaticNodes(memberCount)
                .withServiceSupplier(memberId ->
                        new TestNode.TestService[] {
                            // Call .index(memberId): TestNode.index()/role() identity
                            // and the harness's per-node bookkeeping read
                            // services[0].index(). The default supplier sets it too.
                            (TestNode.TestService) new SealerTestService(
                                    dedupCapacity, tickMs, memberId).index(memberId)
                        })
                .start();
        watcher.cluster(cluster);
        return cluster;
    }

    /**
     * Poll egress until {@code done} holds. This loop only yields; it never
     * calls {@code Thread.sleep}. The test's
     * {@link io.aeron.test.InterruptAfter} bounds it, so a missing event
     * fails fast instead of hanging.
     */
    static void awaitCondition(final AeronCluster client, final BooleanSupplier done) {
        while (!done.getAsBoolean()) {
            client.pollEgress();
            Tests.yield();
        }
    }
}
