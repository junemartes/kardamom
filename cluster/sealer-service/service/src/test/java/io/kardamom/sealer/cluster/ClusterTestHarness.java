package io.kardamom.sealer.cluster;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import java.util.function.BooleanSupplier;

/**
 * Shared plumbing for the in-JVM {@link TestCluster} tests: the cluster
 * builder chain hosting {@link SealerTestService} on every member, and the
 * generic yield-driven egress await loop.
 */
final class ClusterTestHarness {

    private ClusterTestHarness() {
    }

    /**
     * Start a static {@code memberCount}-member TestCluster whose every member
     * hosts a real {@link SealerClusteredService} (via {@link SealerTestService})
     * and register it with {@code watcher}.
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
                            // .index(memberId) is REQUIRED: TestNode.index()/role()
                            // identity and the harness's per-node bookkeeping read
                            // services[0].index(); the default supplier sets it too.
                            (TestNode.TestService) new SealerTestService(
                                    dedupCapacity, tickMs, memberId).index(memberId)
                        })
                .start();
        watcher.cluster(cluster);
        return cluster;
    }

    /**
     * Poll egress until {@code done} holds. Purely yield-driven — no
     * {@code Thread.sleep} — and bounded by the test's
     * {@link io.aeron.test.InterruptAfter}, so a missing event fails fast
     * rather than hanging.
     */
    static void awaitCondition(final AeronCluster client, final BooleanSupplier done) {
        while (!done.getAsBoolean()) {
            client.pollEgress();
            Tests.yield();
        }
    }
}
