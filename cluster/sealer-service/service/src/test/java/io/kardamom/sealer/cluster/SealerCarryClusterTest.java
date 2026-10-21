package io.kardamom.sealer.cluster;

import static io.kardamom.sealer.cluster.ClusterTestHarness.awaitCondition;
import static io.kardamom.sealer.cluster.IngressFrames.offerFrame;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.cluster.TestCluster;
import io.kardamom.sealer.VersionSwitch;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Decision version 5 on a 3-member in-JVM {@link TestCluster}: the
 * activations of version 4 and of the carry flag switch every member to
 * version 5. Then each full reference comes twice, as from twin sequencers,
 * alone and in a batch. The leader relays each one once as its canonical
 * entry, and every member holds the same state.
 */
@ExtendWith(InterruptingTestCallback.class)
class SealerCarryClusterTest {

    private static final int MEMBER_COUNT = 3;
    private static final int DEDUP_CAPACITY = 8192;
    /** No boundary tick fires inside the test: the epochs force each boundary, so the state stays still. */
    private static final long TICK_MS = 600_000L;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private static byte[] epoch(final long origin) {
        return IngressFrames.originRecordFrame(
            IngressFrames.canonicalId(1000 + (int) origin), origin, 1, new byte[] {0x02});
    }

    @Test
    @InterruptAfter(value = 120, unit = TimeUnit.SECONDS)
    void every_member_orders_each_full_reference_once_with_its_bytes() {
        try (TestCluster cluster = ClusterTestHarness.start(systemTestWatcher,
                ClusterTestHarness.builder(MEMBER_COUNT, DEDUP_CAPACITY, TICK_MS))) {
            cluster.awaitLeader();
            final RecordingEgressListener egress = new RecordingEgressListener();
            cluster.egressListener(egress);
            final AeronCluster client = cluster.connectClient();
            final List<SealerClusteredService> members = IntStream.range(0, MEMBER_COUNT)
                .mapToObj(i -> ((SealerTestService) cluster.node(i).service()).delegate())
                .toList();

            // Each epoch forces the boundary that seals the block of the epoch before it.
            offerFrame(client, IngressFrames.featureActivationFrame(1, VersionSwitch.FEATURE_SEALER_V4, 1L));
            offerFrame(client, epoch(1));
            offerFrame(client, epoch(2));
            awaitCondition(client, () -> members.stream().allMatch(m -> m.decisionRules().version() == 4));
            offerFrame(client, IngressFrames.featureActivationFrame(3, VersionSwitch.FEATURE_SEALER_V5, 1L));
            offerFrame(client, epoch(3));
            offerFrame(client, epoch(4));
            awaitCondition(client, () -> members.stream().allMatch(m -> m.decisionRules().version() == 5));

            final FullRef a = FullRef.of(21, 128);
            final FullRef b = FullRef.of(22, 160);
            final FullRef c = FullRef.of(23, 192);
            offerFrame(client, a.frame());
            offerFrame(client, a.frame());
            offerFrame(client, FullRef.batch(b.frame(), c.frame(), b.frame()));
            offerFrame(client, c.frame());
            awaitCondition(client, () -> egress.relayedPayloads.size() >= 7);
            assertMembersEqual(client, members);
            IntStream.range(0, 100).forEach(i -> client.pollEgress());

            final List<byte[]> carried = egress.relayedPayloads.subList(4, egress.relayedPayloads.size());
            assertEquals(3, carried.size(), "each full reference once");
            assertArrayEquals(a.canonicalPayload(), carried.get(0));
            assertArrayEquals(b.canonicalPayload(), carried.get(1));
            assertArrayEquals(c.canonicalPayload(), carried.get(2));
            assertEquals(List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L), egress.relayedIndexes);
        }
    }

    /** Wait until every member applied the leader's log, then compare their states. */
    private static void assertMembersEqual(final AeronCluster client, final List<SealerClusteredService> members) {
        final long applied = members.stream().mapToLong(SealerClusteredService::servicePosition).max().orElseThrow();
        awaitCondition(client, () -> members.stream().allMatch(m -> m.servicePosition() >= applied));
        final byte[] first = members.get(0).snapshot();
        members.forEach(member -> assertArrayEquals(first, member.snapshot()));
    }
}
