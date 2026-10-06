package io.kardamom.sealer.cluster;

import static io.kardamom.sealer.cluster.ClusterTestHarness.awaitCondition;
import static io.kardamom.sealer.cluster.IngressFrames.canonicalId;
import static io.kardamom.sealer.cluster.IngressFrames.offerIngress;
import static io.kardamom.sealer.cluster.IngressFrames.offerReplayRequest;
import static io.kardamom.sealer.cluster.SealerSeedServiceTest.E_H;
import static io.kardamom.sealer.cluster.SealerSeedServiceTest.H;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Three in-JVM members started from one seed. The cluster elects a leader,
 * the seed record confirms the seed, and the leader asks for a snapshot on
 * its own. The canonical stream starts at {@code E_H} in block
 * {@code H + 1}, a consumer at the seed head replays from
 * {@code (E_H, H + 1)}, and after every member restarts from the snapshot
 * the stream continues with no gap.
 */
@ExtendWith(InterruptingTestCallback.class)
class SealerSeedClusterTest {

    private static final int MEMBER_COUNT = 3;
    private static final long TICK_MS = 200L;
    private static final int K = 4;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @Test
    @InterruptAfter(value = 90, unit = TimeUnit.SECONDS)
    void a_seeded_cluster_confirms_relays_from_e_h_and_restarts_from_its_snapshot() {
        final TestCluster cluster = TestCluster.aCluster()
            .withStaticNodes(MEMBER_COUNT)
            .withServiceSupplier(memberId -> new TestNode.TestService[] {
                new SealerTestService(
                    new SealerClusteredService(8192, TICK_MS, memberId).seededFrom(SealerSeedServiceTest.seed()))
                    .index(memberId)
            })
            .start();
        systemTestWatcher.cluster(cluster);

        cluster.awaitLeader();
        final RecordingEgressListener egress = new RecordingEgressListener();
        cluster.egressListener(egress);
        final AeronCluster client = cluster.connectClient();
        // No test call takes this snapshot: the seed record's confirmation asks for it.
        cluster.awaitSnapshotCount(1);

        IntStream.range(0, K).forEach(i -> offerIngress(client, canonicalId(i)));
        awaitCondition(client, () -> egress.relayedIndexes.size() >= K && egress.boundaryCount >= 1);
        assertEquals(indexes(E_H, K), egress.relayedIndexes);
        assertTrue(egress.minBoundaryBlockNumber >= H + 1,
            "the first boundary is H + 1, got " + egress.minBoundaryBlockNumber);

        // A consumer at the seed head resumes at (E_H, H + 1).
        egress.relayedIndexes.clear();
        offerReplayRequest(client, E_H, H + 1);
        awaitCondition(client, () -> egress.replayDoneCount >= 1);
        assertEquals(0, egress.replayUnavailableCount);
        assertEquals(indexes(E_H, K), egress.relayedIndexes);

        cluster.stopAllNodes();
        IntStream.range(0, MEMBER_COUNT).forEach(i -> cluster.startStaticNode(i, false));
        cluster.awaitLeader();
        IntStream.range(0, MEMBER_COUNT).forEach(i -> assertTrue(
            ((SealerTestService) cluster.node(i).service()).restoredFromSnapshot(),
            "member " + i + " restores the seeded snapshot"));

        egress.relayedIndexes.clear();
        final AeronCluster reconnected = cluster.reconnectClient();
        IntStream.range(K, 2 * K).forEach(i -> offerIngress(reconnected, canonicalId(i)));
        awaitCondition(reconnected, () -> egress.relayedIndexes.size() >= K);
        assertEquals(indexes(E_H + K, K), egress.relayedIndexes);
    }

    private static List<Long> indexes(final long from, final int count) {
        return LongStream.range(from, from + count).boxed().toList();
    }
}
