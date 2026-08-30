package io.kardamom.sealer.cluster;

import static io.kardamom.sealer.cluster.ClusterTestHarness.awaitCondition;
import static io.kardamom.sealer.cluster.IngressFrames.canonicalId;
import static io.kardamom.sealer.cluster.IngressFrames.offerIngress;
import static io.kardamom.sealer.cluster.IngressFrames.offerReplayRequest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.cluster.TestCluster;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * In-JVM {@link TestCluster} tests for the canonical-stream REPLAY protocol
 * ({@code KIND_REPLAY_REQUEST} → retained frames → {@code REPLAY_DONE} /
 * {@code REPLAY_UNAVAILABLE}).
 *
 * <p>The product scenario: a consumer's cluster session dies, or the consumer
 * starts fresh behind the stream. It reconnects with a new session, which
 * only receives frames committed from then on. Everything before that is a
 * gap. The client sends {@code REPLAY_FROM(cursor)}, and the service serves
 * the request synchronously: it re-offers every retained frame at or after
 * the cursor to the requesting session, then sends the
 * {@code REPLAY_DONE} marker, all inline while it handles the request (see
 * {@link SealerEgress#handleReplayRequest}). This closes the gap. Frames
 * committed after the request reach the session as ordinary live broadcasts.
 * There is no mid-replay state.</p>
 */
@ExtendWith(InterruptingTestCallback.class)
class SealerReplayTest {

    private static final int MEMBER_COUNT = 3;
    private static final int DEDUP_CAPACITY = 8192;
    private static final long TICK_MS = 200L;
    private static final int K = 5;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @AfterEach
    void clearRetentionOverride() {
        System.clearProperty("kardamom.cluster.retention");
    }

    private TestCluster startCluster() {
        return ClusterTestHarness.startCluster(
                systemTestWatcher, MEMBER_COUNT, DEDUP_CAPACITY, TICK_MS);
    }

    @Test
    @InterruptAfter(value = 90, unit = TimeUnit.SECONDS)
    void reconnectedSessionCatchesUpViaReplay() {
        // Use try-with-resources: a lingering TestCluster degrades the next test's
        // cluster in the same JVM. A second cluster can hang in phase 1 when it
        // starts while the first cluster is still half-alive.
        try (TestCluster cluster = startCluster()) {
        cluster.awaitLeader();
        final RecordingEgressListener liveEgress = new RecordingEgressListener();
        cluster.egressListener(liveEgress);
        final AeronCluster client = cluster.connectClient();

        // Publish K canonical records on the first session. Wait for their relay.
        for (int i = 0; i < K; i++) {
            offerIngress(client, canonicalId(i));
        }
        // Wait for the relays and at least one boundary tick, so a boundary frame
        // is in the retention before the reconnect. Otherwise the 200ms timer
        // races a fast phase 1.
        awaitCondition(client, () ->
                liveEgress.relayedIndexes.size() >= K && liveEgress.boundaryCount > 0);

        // Reconnect: a new session that never saw records 0..K-1 live.
        // reconnectClient() reuses the original client context, so the listener
        // registered before connectClient keeps accumulating. Assert on the
        // delta past the phase-1 counts.
        final int phase1Relayed = liveEgress.relayedIndexes.size();
        final long phase1Boundaries = liveEgress.boundaryCount;
        final AeronCluster reconnected = cluster.reconnectClient();

        // Request replay from genesis. Wait for the retained range and DONE.
        offerReplayRequest(reconnected, 0L, 1L);
        awaitCondition(reconnected, () ->
                liveEgress.relayedIndexes.size() >= phase1Relayed + K
                        && liveEgress.replayDoneCount > 0);

        // The replayed records arrive in emission order: exactly 0..K-1 again.
        for (int i = 0; i < K; i++) {
            assertEquals(i, liveEgress.relayedIndexes.get(phase1Relayed + i),
                    "replayed record order at position " + i + ": "
                            + liveEgress.relayedIndexes.subList(phase1Relayed, liveEgress.relayedIndexes.size()));
        }
        // Retained boundaries are replayed too (phase 1 retained at least one),
        // and the request must never be refused.
        assertTrue(liveEgress.boundaryCount > phase1Boundaries,
                "replay must re-offer retained boundaries");
        assertEquals(0, liveEgress.replayUnavailableCount,
                "replay from genesis must be available within retention");
        }
    }

    /**
     * Regression test: a client that requests replay while live traffic keeps
     * flowing must reach the live cursor and receive REPLAY_DONE. Replay is
     * served synchronously: the service re-offers the whole retained range and
     * the DONE marker inline while it handles the replay request (see
     * {@link SealerEgress#handleReplayRequest}). An earlier timer-driven
     * chunked-drain design made live broadcasts skip mid-replay sessions; that
     * design no longer exists. A replay request also announces the session as
     * a consumer, so records committed after it arrive as ordinary live
     * broadcasts. The full canonical prefix must arrive exactly once, in
     * order.
     */
    @Test
    @InterruptAfter(value = 90, unit = TimeUnit.SECONDS)
    void replayDuringLiveTrafficReachesLiveCursorAndCompletes() {
        try (TestCluster cluster = startCluster()) {
        cluster.awaitLeader();
        final RecordingEgressListener liveEgress = new RecordingEgressListener();
        cluster.egressListener(liveEgress);
        final AeronCluster client = cluster.connectClient();

        // Phase 1: retain K records and at least one boundary.
        for (int i = 0; i < K; i++) {
            offerIngress(client, canonicalId(i));
        }
        awaitCondition(client, () ->
                liveEgress.relayedIndexes.size() >= K && liveEgress.boundaryCount > 0);

        // Phase 2: a new session that never saw records 0..K-1 live requests
        // replay from genesis, then immediately keeps publishing live traffic.
        // The service serves the replay synchronously when it processes the
        // request. The new records commit after it and reach the session as
        // ordinary live broadcasts, in canonical order.
        final int phase1Relayed = liveEgress.relayedIndexes.size();
        final AeronCluster reconnected = cluster.reconnectClient();
        offerReplayRequest(reconnected, 0L, 1L);
        for (int i = 0; i < K; i++) {
            offerIngress(reconnected, canonicalId(K + i));
        }

        // The reconnected session must converge to the live cursor (all 2K
        // records) and complete with REPLAY_DONE, never UNAVAILABLE.
        awaitCondition(reconnected, () ->
                liveEgress.relayedIndexes.size() >= phase1Relayed + 2 * K
                        && liveEgress.replayDoneCount > 0);
        assertEquals(0, liveEgress.replayUnavailableCount,
                "replay within retention must never be refused");

        // Delivery to the new session is the exact canonical prefix 0..2K-1,
        // strictly in order, with no duplicates: the synchronously replayed
        // frames (0..K-1) followed by the live-broadcast frames (K..2K-1).
        final List<Long> got =
                liveEgress.relayedIndexes.subList(phase1Relayed, liveEgress.relayedIndexes.size());
        for (int i = 0; i < 2 * K; i++) {
            assertEquals((long) i, got.get(i),
                    "canonical order/coverage at position " + i + ": " + got);
        }
        }
    }

    @Test
    @InterruptAfter(value = 90, unit = TimeUnit.SECONDS)
    void evictedRangeIsRefusedHonestly() {
        // Use a tiny retention: after K records and boundaries, genesis is evicted.
        System.setProperty("kardamom.cluster.retention", "3");
        try (TestCluster cluster = startCluster()) {
        cluster.awaitLeader();
        final RecordingEgressListener liveEgress = new RecordingEgressListener();
        cluster.egressListener(liveEgress);
        final AeronCluster client = cluster.connectClient();

        for (int i = 0; i < K; i++) {
            offerIngress(client, canonicalId(i));
        }
        awaitCondition(client, () -> liveEgress.relayedIndexes.size() >= K);

        final AeronCluster reconnected = cluster.reconnectClient();

        offerReplayRequest(reconnected, 0L, 1L);
        awaitCondition(reconnected, () -> liveEgress.replayUnavailableCount > 0);

        // The refusal names the oldest retained cursor. It must be past genesis.
        assertTrue(liveEgress.lastUnavailableOldestIndex > 0
                        || liveEgress.lastUnavailableOldestBlock > 1,
                "refusal must carry the post-eviction retention floor, got index="
                        + liveEgress.lastUnavailableOldestIndex
                        + " block=" + liveEgress.lastUnavailableOldestBlock);
        }
    }
}
