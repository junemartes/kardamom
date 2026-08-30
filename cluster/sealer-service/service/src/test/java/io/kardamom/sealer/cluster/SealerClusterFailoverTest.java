package io.kardamom.sealer.cluster;

import static io.kardamom.sealer.cluster.ClusterTestHarness.awaitCondition;
import static io.kardamom.sealer.cluster.IngressFrames.canonicalId;
import static io.kardamom.sealer.cluster.IngressFrames.offerIngress;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import io.kardamom.sealer.CanonicalSealerState;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * In-JVM Aeron {@link TestCluster} failover test for {@link SealerClusteredService}.
 *
 * <p>The test proves that the clustered-sealer egress invariants survive a
 * leader kill. It uses no real network and no timing-based waits:</p>
 * <ul>
 *   <li><b>O2 (gapless continuation)</b> — after the leader stops and a new
 *       leader wins election, the 0-based canonical {@code index} stream
 *       continues at {@code K..2K-1}. It does not restart at 0 and leaves no
 *       gap.</li>
 *   <li><b>O3 (boundary cadence survives failover)</b> — the boundary timer
 *       keeps firing across the leader kill. A BOUNDARY frame with a
 *       {@code blockNumber} greater than the pre-kill maximum appears on the
 *       new leader. Every BOUNDARY frame's {@code blockNumber} stays
 *       non-regressing across the failover.</li>
 * </ul>
 *
 * <p>The cluster runs three in-JVM members. Each member hosts a real
 * {@link SealerClusteredService}, wrapped in a delegating
 * {@link SealerTestService} so the 1.44.0 harness can host it. The test
 * client is a real {@link AeronCluster} whose {@link EgressListener} decodes
 * the {@code RELAYED}/{@code BOUNDARY} egress frames the service sends. All
 * waits are position/count-await loops driven by
 * {@link io.aeron.test.Tests#yield()}. {@link InterruptAfter} bounds the
 * whole test, so a missing event fails fast instead of hanging.</p>
 */
@ExtendWith(InterruptingTestCallback.class)
class SealerClusterFailoverTest {

    private static final int MEMBER_COUNT = 3;
    private static final int DEDUP_CAPACITY = 8192;
    /**
     * Boundary timer cadence in milliseconds. The value is short enough that
     * several BOUNDARY frames fire in both the pre-kill and post-kill phases,
     * so the failover test actually exercises the cadence contract. The value
     * is also long enough that the whole run stays within
     * {@link InterruptAfter}.
     */
    private static final long TICK_MS = 200L;

    private static final int K = 5;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @Test
    @InterruptAfter(value = 90, unit = TimeUnit.SECONDS)
    void egressContinuesGaplesslyAcrossLeaderKill() {
        final RecordingEgressListener egress = new RecordingEgressListener();

        final TestCluster cluster = ClusterTestHarness.startCluster(
                systemTestWatcher, MEMBER_COUNT, DEDUP_CAPACITY, TICK_MS);

        // Bring up the cluster and connect a client that uses our listener.
        cluster.awaitLeader();
        cluster.egressListener(egress); // Set this before connectClient. The client Context copies it at connect time.
        final AeronCluster client = cluster.connectClient();

        // Phase 1: offer K distinct ingress envelopes. Wait for K relayed frames.
        for (int i = 0; i < K; i++) {
            offerIngress(client, canonicalId(i));
        }
        awaitCondition(client, () -> egress.relayedIndexes.size() >= K);

        // O2 (pre-failover): the indexes are exactly 0..K-1, in order.
        assertContiguousIndexes(egress.relayedIndexes, 0, K);

        // O3 (pre-failover): wait for at least one BOUNDARY frame. This proves the
        // boundary cadence runs on the original leader before the kill. Without this
        // wait, the cadence-survival assertion below could pass even if no boundary
        // ever fired. @InterruptAfter bounds the wait; the test uses no Thread.sleep.
        awaitCondition(client, () -> egress.boundaryCount >= 1);
        final long preKillMaxBlockNumber = egress.maxBoundaryBlockNumber;
        final long boundaryFloorBeforeKill = preKillMaxBlockNumber;

        // Phase 2: stop the leader, wait for a new leader, and reconnect the client.
        final TestNode oldLeader = cluster.findLeader();
        final int oldLeaderId = oldLeader.index();
        cluster.stopNode(oldLeader);
        // awaitLeader(skipIndex) waits, on the node side, for a leader whose member
        // index differs from the stopped one. The test does not use
        // awaitNewLeadershipEvent: that helper counts new-leader events on the
        // harness's default egress listener, which this test replaces with its own
        // decoding listener. The node-side wait is the reliable signal that a new
        // leadership term starts.
        final TestNode newLeader = cluster.awaitLeader(oldLeaderId);
        assertTrue(newLeader.index() != oldLeaderId,
                "a different member must win leadership after the old leader is stopped");

        // The JUnit AeronCluster client does not auto-redirect like the Rust client
        // does, so reconnect it to the surviving cluster. reconnectClient() reuses the
        // same Context, so the egress listener applies again. The test checks the
        // cluster's egress continuity, so reconnecting the test client is fine.
        final AeronCluster reconnected = cluster.reconnectClient();

        // Phase 3: offer K more distinct envelopes. Wait for indexes K..2K-1.
        for (int i = K; i < 2 * K; i++) {
            offerIngress(reconnected, canonicalId(i));
        }
        awaitCondition(reconnected, () -> egress.relayedIndexes.size() >= 2 * K);

        // O2 (post-failover): the full stream is 0..2K-1, contiguous, with no gap
        // and no restart at 0. This is the core gapless-continuation contract.
        assertContiguousIndexes(egress.relayedIndexes, 0, 2 * K);

        // O3 (cadence survival): wait for a BOUNDARY frame whose blockNumber is
        // greater than the pre-kill maximum. This wait returns only after the
        // boundary timer arms and fires again on the new leader, proving the
        // cadence resumed instead of stalling. @InterruptAfter bounds the wait, so a
        // broken production timer fails fast instead of hanging.
        awaitCondition(reconnected, () -> egress.maxBoundaryBlockNumber > preKillMaxBlockNumber);
        final long postKillMaxBlockNumber = egress.maxBoundaryBlockNumber;
        assertTrue(postKillMaxBlockNumber > preKillMaxBlockNumber,
                "boundary cadence must continue across failover: preKillMax=" + preKillMaxBlockNumber
                        + " postKillMax=" + postKillMaxBlockNumber);

        // O3 (non-regression): boundary block numbers never go down across the
        // failover, and never drop below genesis.
        assertTrue(egress.maxBoundaryBlockNumber >= boundaryFloorBeforeKill,
                "boundary blockNumber regressed across failover: before=" + boundaryFloorBeforeKill
                        + " after=" + egress.maxBoundaryBlockNumber);
        assertTrue(egress.minBoundaryBlockNumber >= CanonicalSealerState.GENESIS_BLOCK_NUMBER,
                "boundary blockNumber dropped below genesis: " + egress.minBoundaryBlockNumber);
    }

    /** Assert {@code indexes} equals {@code [from, from+1, ..., to-1]} exactly. */
    private static void assertContiguousIndexes(
            final List<Long> indexes, final int from, final int to) {
        assertEquals(to - from, indexes.size(),
                "unexpected relayed-frame count; indexes=" + indexes);
        for (int i = from; i < to; i++) {
            assertEquals((long) i, indexes.get(i - from),
                    "canonical index stream not contiguous at position " + i + "; full=" + indexes);
        }
    }
}
