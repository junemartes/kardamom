package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.logbuffer.Header;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
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
 * client sends {@code REPLAY_FROM(cursor)} and the service re-offers its
 * retained frames to that session, closing the gap.</p>
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
     * REPLAY_DONE. While a session is mid-replay it is SKIPPED by live
     * broadcasts (frames arrive via the retained drain instead), so this
     * exercises the drain serving frames that are retained DURING the replay
     * — including the un-latch path when the DONE control frame is
     * back-pressured after the scan (frames retained in that window must not
     * be lost to the session). The full canonical prefix must arrive exactly
     * once, in order.
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
        // The new records are relayed while the session is mid-replay, so
        // they must reach it through the drain, in canonical order.
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
        // strictly in order, no duplicates: replayed frames (0..K-1) followed
        // by the frames relayed while mid-replay (K..2K-1), all via the drain
        // or post-DONE live broadcast.
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

    // --- harness ---------------------------------------------------------------

    private TestCluster startCluster() {
        final TestCluster cluster = TestCluster.aCluster()
                .withStaticNodes(MEMBER_COUNT)
                .withServiceSupplier(memberId ->
                        new TestNode.TestService[] {
                            (TestNode.TestService) new SealerTestService(
                                    DEDUP_CAPACITY, TICK_MS, memberId).index(memberId)
                        })
                .start();
        systemTestWatcher.cluster(cluster);
        return cluster;
    }

    /** Offer one record envelope (same shape as SealerClusterFailoverTest). */
    private void offerIngress(final AeronCluster client, final byte[] canonicalId32) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        int pos = 0;
        buf.putByte(pos, SealerClusteredService.KIND_INGRESS_RECORD);
        pos += Byte.BYTES;
        buf.putBytes(pos, canonicalId32);
        pos += canonicalId32.length;
        buf.putBytes(pos, canonicalId32); // opaque payload, relayed verbatim
        pos += canonicalId32.length;
        offerFully(client, buf, pos);
    }

    /** Offer a {@code REPLAY_FROM(from_index, from_block)} request. */
    private void offerReplayRequest(final AeronCluster client, final long fromIndex, final long fromBlock) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        int pos = 0;
        buf.putByte(pos, SealerClusteredService.KIND_REPLAY_REQUEST);
        pos += Byte.BYTES;
        buf.putLong(pos, fromIndex, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, fromBlock, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        offerFully(client, buf, pos);
    }

    private void offerFully(final AeronCluster client, final ExpandableArrayBuffer buf, final int length) {
        while (true) {
            final long result = client.offer(buf, 0, length);
            if (result > 0) {
                return;
            }
            if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("ingress offer failed terminally: " + result);
            }
            client.pollEgress();
            Tests.yield();
        }
    }

    private void awaitCondition(final AeronCluster client, final java.util.function.BooleanSupplier done) {
        while (!done.getAsBoolean()) {
            client.pollEgress();
            Tests.yield();
        }
    }

    private static byte[] canonicalId(final int n) {
        final byte[] id = new byte[32];
        id[0] = (byte) n;
        id[31] = (byte) (0x40 + n);
        return id;
    }

    // --- egress recording --------------------------------------------------------

    private static final class RecordingEgressListener implements EgressListener {
        final List<Long> relayedIndexes = new ArrayList<>();
        long boundaryCount = 0;
        long replayDoneCount = 0;
        long replayUnavailableCount = 0;
        long lastUnavailableOldestIndex = -1;
        long lastUnavailableOldestBlock = -1;

        @Override
        public void onMessage(
                final long clusterSessionId,
                final long timestamp,
                final DirectBuffer buffer,
                final int offset,
                final int length,
                final Header header) {
            if (length < Byte.BYTES) {
                return;
            }
            final byte kind = buffer.getByte(offset);
            if (kind == SealerClusteredService.EGRESS_KIND_RELAYED) {
                relayedIndexes.add(buffer.getLong(offset + Byte.BYTES, ByteOrder.LITTLE_ENDIAN));
            } else if (kind == SealerClusteredService.EGRESS_KIND_BOUNDARY) {
                boundaryCount++;
            } else if (kind == SealerClusteredService.EGRESS_KIND_REPLAY_DONE) {
                replayDoneCount++;
            } else if (kind == SealerClusteredService.EGRESS_KIND_REPLAY_UNAVAILABLE) {
                replayUnavailableCount++;
                lastUnavailableOldestIndex =
                        buffer.getLong(offset + Byte.BYTES, ByteOrder.LITTLE_ENDIAN);
                lastUnavailableOldestBlock =
                        buffer.getLong(offset + Byte.BYTES + Long.BYTES, ByteOrder.LITTLE_ENDIAN);
            }
        }
    }

    // --- service wrapper (same composition as SealerClusterFailoverTest) ----------

    private static final class SealerTestService extends TestNode.TestService {
        private final SealerClusteredService delegate;

        SealerTestService(final int dedupCapacity, final long tickMs, final int memberId) {
            this.delegate = new SealerClusteredService(dedupCapacity, tickMs, memberId);
        }

        @Override
        public void onStart(final Cluster cluster, final Image snapshotImage) {
            // See SealerClusterFailoverTest: valid only because these tests never
            // snapshot (both consumers would drain the same snapshot Image).
            super.onStart(cluster, snapshotImage);
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
            // Drive the production service's boundary-timer (re)arming.
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
}
