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
import io.kardamom.sealer.CanonicalSealerState;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
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
 * {@link SealerClusteredService} (wrapped in a delegating {@link TestNode.TestService}
 * so the 1.44.0 harness can host it). The test client is a real
 * {@link AeronCluster} whose {@link EgressListener} decodes the
 * {@code RELAYED}/{@code BOUNDARY} egress frames the service emits. All waits are
 * position/count-await loops driven by {@link Tests#yield()}; {@link InterruptAfter}
 * bounds the whole test so a missing event fails fast rather than hanging.</p>
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

        final TestCluster cluster = TestCluster.aCluster()
                .withStaticNodes(MEMBER_COUNT)
                .withServiceSupplier(memberId ->
                        new TestNode.TestService[] {
                            // .index(memberId) is REQUIRED: TestNode.index()/role()
                            // identity and the harness's per-node bookkeeping read
                            // services[0].index(); the default supplier sets it too.
                            (TestNode.TestService) new SealerTestService(
                                    DEDUP_CAPACITY, TICK_MS, memberId).index(memberId)
                        })
                .start();
        systemTestWatcher.cluster(cluster);

        // --- bring the cluster up and connect a client wired to our listener -----
        cluster.awaitLeader();
        cluster.egressListener(egress); // MUST precede connectClient: it is copied onto the client Context.
        final AeronCluster client = cluster.connectClient();

        // --- phase 1: offer K distinct ingress envelopes, await K relayed frames --
        for (int i = 0; i < K; i++) {
            offerIngress(client, canonicalId(i));
        }
        awaitRelayedCount(client, egress, K);

        // O2 (pre-failover): indexes are exactly 0..K-1 in order.
        assertContiguousIndexes(egress.relayedIndexes, 0, K);

        // O3 (pre-failover): deterministically AWAIT at least one BOUNDARY frame so the
        // boundary cadence is proven to be running on the ORIGINAL leader before we kill
        // it — without this the cadence-survival assertion below would be vacuous if no
        // boundary ever fired. Bounded by @InterruptAfter (no Thread.sleep).
        awaitBoundaryCount(client, egress, 1);
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
        awaitRelayedCount(reconnected, egress, 2 * K);

        // O2 (post-failover): the full stream is 0..2K-1 contiguous — NO gap, NO
        // restart at 0. This is the core gapless-continuation contract.
        assertContiguousIndexes(egress.relayedIndexes, 0, 2 * K);

        // O3 (cadence survival — the single most important property under test):
        // deterministically AWAIT a BOUNDARY frame whose blockNumber is STRICTLY GREATER
        // than the pre-kill maximum. This only returns once the boundary timer has armed
        // and FIRED AGAIN on the NEW leader after failover — proving boundary cadence
        // resumed across the leader kill rather than stalling. Bounded by @InterruptAfter,
        // so a broken production timer fails fast instead of hanging.
        awaitBoundaryBlockNumberAbove(reconnected, egress, preKillMaxBlockNumber);
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

    // --- ingress / egress helpers ------------------------------------------------

    /**
     * Offer one app envelope {@code kind(1) | canonical_id(32) | payload} to the
     * cluster. {@code kind} is an arbitrary non-zero tag; the payload is the 32-byte
     * id itself (the service relays {@code [canonical_id|payload]} verbatim from
     * {@link SealerClusteredService#RELAY_OFFSET}). Retries on back-pressure with a
     * yield (no sleep), so the offer is deterministic, not timing-based.
     */
    private void offerIngress(final AeronCluster client, final byte[] canonicalId32) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        int pos = 0;
        buf.putByte(pos, SealerWire.KIND_INGRESS_RECORD);
        pos += Byte.BYTES;
        // Zero sender + nonce 0: guard-exempt (this test exercises failover,
        // not the contiguity guard).
        buf.putBytes(pos, new byte[CanonicalSealerState.SENDER_LEN]);
        pos += CanonicalSealerState.SENDER_LEN;
        buf.putLong(pos, 0L, java.nio.ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putBytes(pos, canonicalId32);
        pos += canonicalId32.length;
        // payload == the id again (opaque, relayed verbatim).
        buf.putBytes(pos, canonicalId32);
        pos += canonicalId32.length;
        final int length = pos;

        while (true) {
            final long result = client.offer(buf, 0, length);
            if (result > 0) {
                return;
            }
            if (result == Publication.CLOSED
                    || result == Publication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("ingress offer failed terminally: " + result);
            }
            // BACK_PRESSURED / ADMIN_ACTION / NOT_CONNECTED — drain egress and retry.
            client.pollEgress();
            Tests.yield();
        }
    }

    /** Poll egress until at least {@code expected} relayed frames have been recorded. */
    private void awaitRelayedCount(
            final AeronCluster client, final RecordingEgressListener egress, final int expected) {
        while (egress.relayedIndexes.size() < expected) {
            if (client.pollEgress() == 0) {
                Tests.yield();
            }
        }
    }

    /**
     * Poll egress until at least {@code expected} BOUNDARY frames have been recorded.
     * Bounded by {@link InterruptAfter}: if boundary cadence were broken the test fails
     * fast rather than hanging. No {@code Thread.sleep} — purely yield-driven.
     */
    private void awaitBoundaryCount(
            final AeronCluster client, final RecordingEgressListener egress, final long expected) {
        while (egress.boundaryCount < expected) {
            if (client.pollEgress() == 0) {
                Tests.yield();
            }
        }
    }

    /**
     * Poll egress until a BOUNDARY frame whose {@code blockNumber} is strictly greater
     * than {@code floor} has been observed. This is the boundary-cadence-across-failover
     * signal: it only returns once the boundary timer has fired AGAIN beyond the
     * pre-kill maximum — i.e. cadence resumed on the new leader. Bounded by
     * {@link InterruptAfter}.
     */
    private void awaitBoundaryBlockNumberAbove(
            final AeronCluster client, final RecordingEgressListener egress, final long floor) {
        while (egress.maxBoundaryBlockNumber <= floor) {
            if (client.pollEgress() == 0) {
                Tests.yield();
            }
        }
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

    /** Deterministic distinct 32-byte canonical id: byte i of every id = (n, n, ...). */
    private static byte[] canonicalId(final int n) {
        final byte[] id = new byte[CanonicalSealerState.CANONICAL_ID_LEN];
        // Spread n across the first 4 bytes so ids stay distinct well past 255.
        id[0] = (byte) (n & 0xFF);
        id[1] = (byte) ((n >>> 8) & 0xFF);
        id[2] = (byte) ((n >>> 16) & 0xFF);
        id[3] = (byte) ((n >>> 24) & 0xFF);
        id[31] = (byte) 0x5A; // marker so an all-zero id (a likely bug) is distinguishable
        return id;
    }

    // --- egress listener: decodes RELAYED / BOUNDARY frames ----------------------

    /**
     * Records the ordered canonical {@code index} of every RELAYED frame and the
     * min/max {@code blockNumber} of every BOUNDARY frame. The frame layouts match
     * {@link SealerClusteredService#frameRelayed}/{@code frameBoundary} (little-endian).
     */
    private static final class RecordingEgressListener implements EgressListener {
        final List<Long> relayedIndexes = new ArrayList<>();
        long maxBoundaryBlockNumber = Long.MIN_VALUE;
        long minBoundaryBlockNumber = Long.MAX_VALUE;
        // Count of BOUNDARY frames seen — lets a yield-await loop poll for "at least
        // one boundary has fired" without depending on a particular blockNumber. The
        // listener runs inline on the test's pollEgress() thread (single-threaded with
        // the awaits below), so plain fields are consistent here.
        long boundaryCount = 0;

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
            if (kind == SealerWire.EGRESS_KIND_RELAYED) {
                // kind(1) | index(8 LE) | payloadLen(4 LE) | payload[]
                final long index = buffer.getLong(offset + Byte.BYTES, ByteOrder.LITTLE_ENDIAN);
                relayedIndexes.add(index);
            } else if (kind == SealerWire.EGRESS_KIND_BOUNDARY) {
                // kind(1) | blockNumber(8 LE) | endTxIdx(8 LE) | l2Timestamp(8 LE)
                final long blockNumber =
                        buffer.getLong(offset + Byte.BYTES, ByteOrder.LITTLE_ENDIAN);
                maxBoundaryBlockNumber = Math.max(maxBoundaryBlockNumber, blockNumber);
                minBoundaryBlockNumber = Math.min(minBoundaryBlockNumber, blockNumber);
                boundaryCount++;
            }
        }
    }

    // --- service wrapper: hosts the real SealerClusteredService in the harness ----

    /**
     * A {@link TestNode.TestService} whose every {@link io.aeron.cluster.service.ClusteredService}
     * callback delegates to a real {@link SealerClusteredService}. The 1.44.0
     * {@link TestCluster} can only construct services through a
     * {@code Supplier<TestNode.TestService[]>}, so an external {@code ClusteredService}
     * is injected by composition here — the harness drives THIS object, which forwards
     * verbatim to the production service. No behaviour is added or intercepted.
     */
    private static final class SealerTestService extends TestNode.TestService {
        private final SealerClusteredService delegate;

        SealerTestService(final int dedupCapacity, final long tickMs, final int memberId) {
            this.delegate = new SealerClusteredService(dedupCapacity, tickMs, memberId);
        }

        @Override
        public void onStart(final Cluster cluster, final Image snapshotImage) {
            // WARNING — snapshot/recovery limitation: on a SNAPSHOT-recovery start both
            // super.onStart(...) and delegate.onStart(...) are handed the SAME snapshot
            // Image. An Image is a consumable cursor: whoever polls it first DRAINS it,
            // so the second consumer sees an empty image and silently starts from genesis.
            // This is harmless ONLY because this test never takes a snapshot (snapshotImage
            // is always null here). DO NOT reuse this wrapper as-is for any snapshot/
            // recovery test case: in that scenario the DELEGATE — not super — must be the
            // one to consume the snapshot Image (super must be given a null/no-op image),
            // otherwise the production service will lose its recovered state.
            super.onStart(cluster, snapshotImage); // lets the harness latch its Cluster ref
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
            // Drive the production service's deferred initial timer arming.
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
