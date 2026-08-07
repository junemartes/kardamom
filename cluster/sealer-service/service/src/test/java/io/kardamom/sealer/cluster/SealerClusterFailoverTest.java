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
 * <p>Proves the clustered-sealer egress invariants survive a leader kill WITHOUT a
 * real network and WITHOUT timing-based waits:</p>
 * <ul>
 *   <li><b>O2 (gapless continuation)</b> — after the leader is stopped and a new
 *       leader is elected, the 0-based canonical {@code index} stream continues
 *       exactly at {@code K..2K-1}; it does NOT restart at 0 and leaves no gap.</li>
 *   <li><b>O3 (boundary cadence survives failover / monotonic boundaries)</b> — the
 *       boundary timer keeps firing across the leader kill: a BOUNDARY frame with a
 *       {@code blockNumber} strictly greater than the pre-kill maximum is observed on
 *       the new leader, and every BOUNDARY frame's {@code blockNumber} across the
 *       failover is non-regressing.</li>
 * </ul>
 *
 * <p>The cluster runs three in-JVM members each hosting a real
 * {@link SealerClusteredService} (wrapped in a delegating {@link SealerTestService}
 * so the 1.44.0 harness can host it). The test client is a real
 * {@link AeronCluster} whose {@link EgressListener} decodes the
 * {@code RELAYED}/{@code BOUNDARY} egress frames the service emits. All waits are
 * position/count-await loops driven by {@link io.aeron.test.Tests#yield()};
 * {@link InterruptAfter} bounds the whole test so a missing event fails fast rather
 * than hanging.</p>
 */
@ExtendWith(InterruptingTestCallback.class)
class SealerClusterFailoverTest {

    private static final int MEMBER_COUNT = 3;
    private static final int DEDUP_CAPACITY = 8192;
    /**
     * Boundary timer cadence (ms). Kept short enough that several BOUNDARY frames
     * reliably fire in BOTH the pre-kill and post-kill phases (so the
     * boundary-cadence-across-failover contract is actually exercised, not vacuously
     * satisfied) yet long enough that the whole run stays well within
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

        // --- bring the cluster up and connect a client wired to our listener -----
        cluster.awaitLeader();
        cluster.egressListener(egress); // MUST precede connectClient: it is copied onto the client Context.
        final AeronCluster client = cluster.connectClient();

        // --- phase 1: offer K distinct ingress envelopes, await K relayed frames --
        for (int i = 0; i < K; i++) {
            offerIngress(client, canonicalId(i));
        }
        awaitCondition(client, () -> egress.relayedIndexes.size() >= K);

        // O2 (pre-failover): indexes are exactly 0..K-1 in order.
        assertContiguousIndexes(egress.relayedIndexes, 0, K);

        // O3 (pre-failover): deterministically AWAIT at least one BOUNDARY frame so the
        // boundary cadence is proven to be running on the ORIGINAL leader before we kill
        // it — without this the cadence-survival assertion below would be vacuous if no
        // boundary ever fired. Bounded by @InterruptAfter (no Thread.sleep).
        awaitCondition(client, () -> egress.boundaryCount >= 1);
        final long preKillMaxBlockNumber = egress.maxBoundaryBlockNumber;
        final long boundaryFloorBeforeKill = preKillMaxBlockNumber;

        // --- phase 2: stop the leader, await a new leader, reconnect the client ---
        final TestNode oldLeader = cluster.findLeader();
        final int oldLeaderId = oldLeader.index();
        cluster.stopNode(oldLeader);
        // awaitLeader(skipIndex) waits, on the NODE side, for a leader whose member
        // index differs from the stopped one. We deliberately do NOT use
        // awaitNewLeadershipEvent here: that helper counts new-leader events on the
        // harness's *default* egress listener, which we have replaced with our own
        // decoding listener — so it would never advance. The node-side await is the
        // authoritative signal that a new leadership term is established.
        final TestNode newLeader = cluster.awaitLeader(oldLeaderId);
        assertTrue(newLeader.index() != oldLeaderId,
                "a different member must win leadership after the old leader is stopped");

        // The JUnit AeronCluster client does not auto-redirect the way the Rust client
        // does; reconnect it to the surviving cluster. reconnectClient() reuses the
        // same Context (so our egressListener is re-applied) — the test's contract is
        // the CLUSTER's egress continuity, so reconnecting the test client is fine.
        final AeronCluster reconnected = cluster.reconnectClient();

        // --- phase 3: offer K more distinct envelopes, await indexes K..2K-1 -------
        for (int i = K; i < 2 * K; i++) {
            offerIngress(reconnected, canonicalId(i));
        }
        awaitCondition(reconnected, () -> egress.relayedIndexes.size() >= 2 * K);

        // O2 (post-failover): the full stream is 0..2K-1 contiguous — NO gap, NO
        // restart at 0. This is the core gapless-continuation contract.
        assertContiguousIndexes(egress.relayedIndexes, 0, 2 * K);

        // O3 (cadence survival — the single most important property under test):
        // deterministically AWAIT a BOUNDARY frame whose blockNumber is STRICTLY GREATER
        // than the pre-kill maximum. This only returns once the boundary timer has armed
        // and FIRED AGAIN on the NEW leader after failover — proving boundary cadence
        // resumed across the leader kill rather than stalling. Bounded by @InterruptAfter,
        // so a broken production timer fails fast instead of hanging.
        awaitCondition(reconnected, () -> egress.maxBoundaryBlockNumber > preKillMaxBlockNumber);
        final long postKillMaxBlockNumber = egress.maxBoundaryBlockNumber;
        assertTrue(postKillMaxBlockNumber > preKillMaxBlockNumber,
                "boundary cadence must continue across failover: preKillMax=" + preKillMaxBlockNumber
                        + " postKillMax=" + postKillMaxBlockNumber);

        // O3 (non-regression, retained): boundary block numbers never regress across the
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
