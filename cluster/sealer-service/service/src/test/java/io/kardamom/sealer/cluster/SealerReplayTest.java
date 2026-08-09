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
 * <p>The product scenario: a consumer whose cluster session died (or that
 * starts fresh behind the stream) re-connects with a NEW session, which only
 * receives frames committed from then on — everything in between is a gap. The
 * client sends {@code REPLAY_FROM(cursor)} and the service serves the request
 * SYNCHRONOUSLY: it re-offers every retained frame at/after the cursor to the
 * requesting session, then the {@code REPLAY_DONE} marker, all inline while
 * handling the request (see {@link SealerEgress#handleReplayRequest}), closing
 * the gap. Frames committed after the request reach the session as ordinary
 * live broadcasts — there is no mid-replay state.</p>
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
        // try-with-resources: a lingering TestCluster degrades the NEXT test's
        // cluster in the same JVM (observed as a phase-1 hang when a second
        // cluster starts while the first is still half-alive).
        try (TestCluster cluster = startCluster()) {
        cluster.awaitLeader();
        final RecordingEgressListener liveEgress = new RecordingEgressListener();
        cluster.egressListener(liveEgress);
        final AeronCluster client = cluster.connectClient();

        // Publish K canonical records on the FIRST session and await their relay.
        for (int i = 0; i < K; i++) {
            offerIngress(client, canonicalId(i));
        }
        // Await the relays AND at least one boundary tick, so a boundary frame
        // is actually IN the retention before the reconnect (the 200ms timer
        // races a fast phase 1 otherwise).
        awaitCondition(client, () ->
                liveEgress.relayedIndexes.size() >= K && liveEgress.boundaryCount > 0);

        // Re-connect: a NEW session that never saw records 0..K-1 live.
        // NOTE: reconnectClient() reuses the ORIGINAL client context, so the
        // one listener registered before connectClient keeps accumulating —
        // assert on the DELTA past the phase-1 counts.
        final int phase1Relayed = liveEgress.relayedIndexes.size();
        final long phase1Boundaries = liveEgress.boundaryCount;
        final AeronCluster reconnected = cluster.reconnectClient();

        // Request replay from genesis and await the retained range + DONE.
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
        // Retained boundaries are replayed too (at least one was retained in
        // phase 1), and the request must never be refused.
        assertTrue(liveEgress.boundaryCount > phase1Boundaries,
                "replay must re-offer retained boundaries");
        assertEquals(0, liveEgress.replayUnavailableCount,
                "replay from genesis must be available within retention");
        }
    }

    /**
     * CI-replay-loop regression: a client that requests replay WHILE live
     * traffic keeps flowing must reach the live cursor and receive
     * REPLAY_DONE. Replay is served SYNCHRONOUSLY: the whole retained range
     * and the DONE marker are re-offered inline while the replay request is
     * handled (the reverted F07.3 timer-driven chunked drain — which made
     * live broadcasts SKIP mid-replay sessions — no longer exists; see
     * {@link SealerEgress#handleReplayRequest}). A replay request also
     * announces the session as a consumer, so records committed after it
     * arrive as ordinary live broadcasts. The full canonical prefix must
     * arrive exactly once, in order.
     */
    @Test
    @InterruptAfter(value = 90, unit = TimeUnit.SECONDS)
    void replayDuringLiveTrafficReachesLiveCursorAndCompletes() {
        try (TestCluster cluster = startCluster()) {
        cluster.awaitLeader();
        final RecordingEgressListener liveEgress = new RecordingEgressListener();
        cluster.egressListener(liveEgress);
        final AeronCluster client = cluster.connectClient();

        // Phase 1: K records + at least one boundary retained.
        for (int i = 0; i < K; i++) {
            offerIngress(client, canonicalId(i));
        }
        awaitCondition(client, () ->
                liveEgress.relayedIndexes.size() >= K && liveEgress.boundaryCount > 0);

        // Phase 2: a NEW session that never saw records 0..K-1 live requests
        // replay from genesis and IMMEDIATELY keeps publishing live traffic.
        // The replay is served synchronously when the request is processed;
        // the new records commit after it and reach the session as ordinary
        // live broadcasts, in canonical order.
        final int phase1Relayed = liveEgress.relayedIndexes.size();
        final AeronCluster reconnected = cluster.reconnectClient();
        offerReplayRequest(reconnected, 0L, 1L);
        for (int i = 0; i < K; i++) {
            offerIngress(reconnected, canonicalId(K + i));
        }

        // The reconnected session must converge to the LIVE cursor (all 2K
        // records) and complete with REPLAY_DONE, never UNAVAILABLE.
        awaitCondition(reconnected, () ->
                liveEgress.relayedIndexes.size() >= phase1Relayed + 2 * K
                        && liveEgress.replayDoneCount > 0);
        assertEquals(0, liveEgress.replayUnavailableCount,
                "replay within retention must never be refused");

        // Delivery to the new session is the exact canonical prefix 0..2K-1,
        // strictly in order, no duplicates: the synchronously replayed frames
        // (0..K-1) followed by the live-broadcast frames (K..2K-1).
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
        // Tiny retention: after K records + boundaries, genesis is evicted.
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

        // The refusal names the oldest retained cursor — it must be past genesis.
        assertTrue(liveEgress.lastUnavailableOldestIndex > 0
                        || liveEgress.lastUnavailableOldestBlock > 1,
                "refusal must carry the post-eviction retention floor, got index="
                        + liveEgress.lastUnavailableOldestIndex
                        + " block=" + liveEgress.lastUnavailableOldestBlock);
        }
    }
}
