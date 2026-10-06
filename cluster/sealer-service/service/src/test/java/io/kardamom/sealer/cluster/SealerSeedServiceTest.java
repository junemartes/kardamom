package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.cluster.service.Cluster;
import io.kardamom.sealer.SealerSeed;
import io.kardamom.sealer.cluster.ClusterStubs.StubCluster;
import io.kardamom.sealer.cluster.ClusterStubs.StubSession;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.AgentTerminationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A member started from a seed: it opens block {@code H + 1} at index
 * {@code E_H}, serves a consumer that resumes at {@code (E_H, H + 1)},
 * offers the seed record on each term until the record confirms the seed,
 * and keeps the seed through a snapshot. A member whose history differs
 * from the seed record's stops.
 */
class SealerSeedServiceTest {

    static final long H = 100L;
    static final long E_H = 500L;
    private static final long HEAD_TIME = 1_700_000_000_000L;
    private static final long ORIGIN = 77L;

    /** A seed at {@code (H, E_H)} with one sender whose next nonce is 5. */
    static SealerSeed seed() {
        final byte[] digest = new byte[SealerSeed.HASH_LEN];
        Arrays.fill(digest, (byte) 0x5E);
        return new SealerSeed(
            new SealerSeed.Head(412_346L, H, E_H, HEAD_TIME, ORIGIN),
            new byte[SealerSeed.HASH_LEN],
            List.of(new SealerSeed.Sender(sender(), 5L)),
            digest);
    }

    private static byte[] sender() {
        final byte[] s = new byte[20];
        Arrays.fill(s, (byte) 0xAA);
        return s;
    }

    private StubCluster cluster;
    private SealerClusteredService service;
    private StubSession consumer;

    @BeforeEach
    void start() {
        cluster = new StubCluster();
        service = startedService(seed());
        consumer = cluster.addSession(1);
    }

    private SealerClusteredService startedService(final SealerSeed seed) {
        final SealerClusteredService s = new SealerClusteredService(64, 250, 0).seededFrom(seed);
        s.onStart(cluster, null);
        return s;
    }

    private static void deliver(
            final SealerClusteredService to, final StubSession from, final byte[] frame) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        buf.putBytes(0, frame);
        to.onSessionMessage(from, 0, buf, 0, frame.length, null);
    }

    private static void newTerm(final SealerClusteredService to) {
        to.onNewLeadershipTermEvent(1, 0, 0, 0, 0, 0, TimeUnit.MILLISECONDS, 0);
    }

    private static List<byte[]> ofKind(final StubSession s, final byte kind) {
        return s.offered.stream().filter(f -> f[0] == kind).toList();
    }

    private static ByteBuffer le(final byte[] frame) {
        return ByteBuffer.wrap(frame).order(ByteOrder.LITTLE_ENDIAN);
    }

    private static byte[] seedRecord(final byte[] digest) {
        final byte[] frame = new byte[SealerWire.SEED_EPOCH_LEN];
        frame[0] = SealerWire.KIND_SEED_EPOCH;
        System.arraycopy(digest, 0, frame, SealerWire.SEED_DIGEST_OFFSET, digest.length);
        return frame;
    }

    @Test
    void a_consumer_at_the_seed_head_resumes_at_e_h_and_h_plus_one() {
        deliver(service, consumer, IngressFrames.replayRequestFrame(E_H, H + 1));
        final List<byte[]> done = ofKind(consumer, SealerWire.EGRESS_KIND_REPLAY_DONE);
        assertEquals(1, done.size());
        assertEquals(E_H, le(done.get(0)).getLong(1));
        assertEquals(H + 1, le(done.get(0)).getLong(9));
        assertTrue(ofKind(consumer, SealerWire.EGRESS_KIND_REPLAY_UNAVAILABLE).isEmpty());

        deliver(service, consumer, IngressFrames.replayRequestFrame(E_H - 1, H));
        assertEquals(1, ofKind(consumer, SealerWire.EGRESS_KIND_REPLAY_UNAVAILABLE).size(),
            "nothing below the seed head is held");
    }

    @Test
    void the_first_boundary_is_h_plus_one_and_the_first_record_is_e_h() {
        deliver(service, consumer, IngressFrames.subscribeFrame());
        deliver(service, consumer, IngressFrames.recordFrame(1, sender(), 5L));
        service.onTimerEvent(SealerClusteredService.BOUNDARY_TIMER_CORRELATION_ID, 0);

        final List<byte[]> relayed = ofKind(consumer, SealerWire.EGRESS_KIND_RELAYED);
        assertEquals(1, relayed.size(), "the seeded sender's next nonce is accepted");
        assertEquals(E_H, le(relayed.get(0)).getLong(1));
        final ByteBuffer boundary = le(ofKind(consumer, SealerWire.EGRESS_KIND_BOUNDARY).get(0));
        assertEquals(H + 1, boundary.getLong(1));
        assertEquals(E_H + 1, boundary.getLong(9));
        assertEquals(HEAD_TIME + 1, boundary.getLong(17));
        assertEquals(ORIGIN, boundary.getLong(25));
    }

    @Test
    void each_term_offers_the_seed_record_until_one_confirms_it() {
        cluster.role = Cluster.Role.FOLLOWER;
        newTerm(service);
        assertEquals(1, cluster.logOffers.size());
        assertArrayEquals(seedRecord(seed().digest()), cluster.logOffers.get(0));
        newTerm(service);
        assertEquals(2, cluster.logOffers.size(), "an unconfirmed seed is offered again");

        deliver(service, null, cluster.logOffers.get(0));
        deliver(service, null, cluster.logOffers.get(1));
        newTerm(service);
        assertEquals(2, cluster.logOffers.size(), "a confirmed seed is not offered");
    }

    @Test
    void a_snapshot_keeps_the_seed_and_its_confirmation() {
        cluster.role = Cluster.Role.FOLLOWER;
        final SealerClusteredService pending = new SealerClusteredService(64, 250, 0);
        pending.onStart(cluster, null);
        pending.restore(service.snapshot());
        newTerm(pending);
        assertEquals(1, cluster.logOffers.size(), "a restored unconfirmed seed is offered");

        deliver(service, null, cluster.logOffers.get(0));
        final SealerClusteredService confirmed = new SealerClusteredService(64, 250, 0);
        confirmed.onStart(cluster, null);
        confirmed.restore(service.snapshot());
        newTerm(confirmed);
        assertEquals(1, cluster.logOffers.size(), "a restored confirmed seed is not offered");
        assertDoesNotThrow(() -> deliver(confirmed, null, cluster.logOffers.get(0)));

        final StubSession late = cluster.addSession(2);
        deliver(confirmed, late, IngressFrames.replayRequestFrame(E_H, H + 1));
        assertEquals(1, ofKind(late, SealerWire.EGRESS_KIND_REPLAY_DONE).size());
    }

    @Test
    void a_seed_record_with_another_digest_stops_the_member() {
        final byte[] other = seed().digest();
        other[0] ^= 1;
        assertThrows(AgentTerminationException.class, () -> deliver(service, null, seedRecord(other)));
    }

    @Test
    void a_member_started_at_genesis_stops_on_the_seed_record() {
        final SealerClusteredService genesis = new SealerClusteredService(64, 250, 0);
        genesis.onStart(cluster, null);
        final AgentTerminationException e = assertThrows(
            AgentTerminationException.class, () -> deliver(genesis, null, seedRecord(seed().digest())));
        assertTrue(e.getCause().getMessage().contains("started at genesis"), e.getCause().getMessage());
    }

    @Test
    void a_seed_record_from_a_client_is_dropped() {
        final SealerClusteredService genesis = new SealerClusteredService(64, 250, 0);
        genesis.onStart(cluster, null);
        assertDoesNotThrow(() -> deliver(genesis, consumer, seedRecord(seed().digest())));
    }

    @Test
    void a_seed_with_interop_on_is_refused() {
        final SealerClusteredService interop = new SealerClusteredService(64, 250, 0, Set.of(412_399L));
        assertThrows(IllegalArgumentException.class, () -> interop.seededFrom(seed()));
    }

    @Test
    void the_node_reads_no_seed_when_the_property_is_unset_and_refuses_a_missing_file() {
        assertEquals(Optional.empty(), ClusterNode.readSeed(null));
        assertEquals(Optional.empty(), ClusterNode.readSeed(" "));
        final IllegalStateException e = assertThrows(
            IllegalStateException.class, () -> ClusterNode.readSeed("/nonexistent/sealer.seed"));
        assertTrue(e.getMessage().contains("kardamom.cluster.seedSnapshot"), e.getMessage());
    }
}
