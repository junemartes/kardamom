package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.DecisionRules;
import io.kardamom.sealer.LagBudgets;
import io.kardamom.sealer.VoidLedger;
import io.kardamom.sealer.cluster.ClusterStubs.StubCluster;
import io.kardamom.sealer.cluster.ClusterStubs.StubSession;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.AgentTerminationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The record path with bytes, on driverless members: a member with the
 * retained file orders a full entry, retains the canonical entry, and
 * sends each egress session the relayed reference. The refusals are the
 * same as on a member without the file, and every member writes the same
 * snapshot bytes.
 */
class SealerRecordPathTest {

    private static final int CAPACITY = 16;
    /** The window of the refusal run: it fills after four entries. */
    private static final int SMALL_CAPACITY = 4;
    private static final int SEGMENT_BYTES = 64 * 1024;
    private static final byte LANE = 3;
    /** The DA-lag budget of the refusal run, in blocks. */
    private static final long DA_LAG_BUDGET = 1;
    private static final byte[] SENDER = filled(CanonicalSealerState.SENDER_LEN, 0xAA);
    private static final Set<Byte> REFUSALS = Set.of(SealerWire.EGRESS_KIND_CONTIGUITY_REJECT,
        SealerWire.EGRESS_KIND_PAST_DEADLINE, SealerWire.EGRESS_KIND_WINDOW_FULL, SealerWire.EGRESS_KIND_DA_LAG_REJECT);
    private static final VoidLedger.Config VOIDS = new VoidLedger.Config(16, 1L);

    @TempDir
    Path dir;

    private final StubCluster cluster = new StubCluster();
    private final List<SealerClusteredService> started = new ArrayList<>();

    @AfterEach
    void stop() {
        started.forEach(s -> s.onTerminate(cluster));
    }

    private static byte[] filled(final int length, final int value) {
        final byte[] out = new byte[length];
        Arrays.fill(out, (byte) value);
        return out;
    }

    /** The id of entry {@code n}: not a keccak hash, but distinct from the id of every other entry. */
    private static byte[] id(final int n) {
        return IngressFrames.canonicalId(n);
    }

    /** The envelope of entry {@code n}: opaque bytes that the sealer never parses. */
    private static byte[] envelope(final int n) {
        return filled(80 + n, 0x40 + n);
    }

    /** A full entry: kind 0, the guard header, the id, record type 5, the lane and the envelope. */
    private static byte[] offer(final int n, final long nonce, final long deadline) {
        final byte[] env = envelope(n);
        final ByteBuffer buf = ByteBuffer.allocate(SealerWire.CANONICAL_ID_OFFSET
            + CanonicalSealerState.CANONICAL_ID_LEN + 2 + env.length).order(ByteOrder.LITTLE_ENDIAN);
        buf.put(SealerWire.KIND_INGRESS_RECORD).put(SENDER).putLong(nonce).putLong(deadline)
            .putLong(0L).putLong(0L).put(id(n)).put(SealerWire.RT_TX_OFFER).put(LANE).put(env);
        return buf.array();
    }

    private static byte[] offer(final int n, final long nonce) {
        return offer(n, nonce, Long.MAX_VALUE);
    }

    /** The canonical entry that the file retains for entry {@code n} at {@code index}. */
    private static byte[] canonicalFrame(final int n, final long index) {
        final byte[] env = envelope(n);
        final int payload = CanonicalSealerState.CANONICAL_ID_LEN + 2 + env.length;
        return ByteBuffer.allocate(SealerWire.RELAYED_HEADER_LEN + payload).order(ByteOrder.LITTLE_ENDIAN)
            .put(SealerWire.EGRESS_KIND_RELAYED).putLong(index).putInt(payload)
            .put(id(n)).put(SealerWire.RT_TX).put(LANE).put(env).array();
    }

    /** The relayed reference that an egress session gets for entry {@code n} at {@code index}. */
    private static byte[] referenceFrame(final int n, final long index) {
        return ByteBuffer.allocate(SealerWire.RELAYED_REF_LEN).order(ByteOrder.LITTLE_ENDIAN)
            .put(SealerWire.EGRESS_KIND_RELAYED_REF).putLong(index).put(id(n)).put(SealerWire.RT_TX).array();
    }

    /** A member that keeps its retained frames in the file of {@code name}, or on the heap when empty. */
    private SealerClusteredService member(final Optional<String> name, final Optional<byte[]> snapshot) {
        return member(name, snapshot, CAPACITY);
    }

    private SealerClusteredService member(
            final Optional<String> name, final Optional<byte[]> snapshot, final int capacity) {
        final SealerClusteredService svc = new SealerClusteredService(
            capacity, 250, 0, Set.of(), VOIDS, CanonicalSealerState.DEFAULT_INCLUSION_HORIZON_BLOCKS,
            CanonicalSealerState.DEFAULT_ORDERING_WINDOW, new LagBudgets(DA_LAG_BUDGET, 0L));
        name.ifPresent(n -> svc.carryingBytes(new RetainedFile.Config(dir.resolve(n), SEGMENT_BYTES)));
        svc.start(cluster, snapshot);
        started.add(svc);
        return svc;
    }

    private SealerClusteredService fileMember(final String name) {
        return member(Optional.of(name), Optional.empty());
    }

    private static void deliver(final SealerClusteredService svc, final StubSession from, final byte[] frame) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        buf.putBytes(0, frame);
        svc.onSessionMessage(from, 0, buf, 0, frame.length, null);
    }

    private static void tick(final SealerClusteredService svc) {
        svc.onTimerEvent(SealerClusteredService.BOUNDARY_TIMER_CORRELATION_ID, 250);
    }

    /** The frames that {@code s} got, without the status frames, whose length depends on the store. */
    private static List<byte[]> withoutStatus(final StubSession s) {
        return s.offered.stream().filter(f -> f[0] != SealerWire.EGRESS_KIND_STATUS).toList();
    }

    private static void assertSameFrames(final List<byte[]> expected, final List<byte[]> actual) {
        assertEquals(expected.size(), actual.size());
        IntStream.range(0, expected.size()).forEach(i -> assertArrayEquals(expected.get(i), actual.get(i), "frame " + i));
    }

    @Test
    void the_second_twin_copy_drops_at_the_dedup() {
        final StubSession twinA = cluster.addSession(1);
        final StubSession twinB = cluster.addSession(2);
        final StubSession observer = cluster.addSession(3);
        final SealerClusteredService svc = fileMember("a");
        deliver(svc, observer, IngressFrames.subscribeFrame());

        deliver(svc, twinA, offer(1, 0));
        deliver(svc, twinB, offer(1, 0));

        final List<byte[]> refs = observer.offered.stream()
            .filter(f -> f[0] == SealerWire.EGRESS_KIND_RELAYED_REF).toList();
        assertSameFrames(List.of(referenceFrame(1, 0)), refs);
        assertTrue(observer.offered.stream().noneMatch(f -> f[0] == SealerWire.EGRESS_KIND_RELAYED),
            "no egress session gets the envelope");
        assertSameFrames(List.of(canonicalFrame(1, 0)), svc.retained().wanted(0, 1).toList());
        assertTrue(withoutStatus(twinB).isEmpty(), "the second copy gets no answer");
    }

    @Test
    void an_egress_replay_serves_the_reference_of_a_retained_entry() {
        final StubSession twin = cluster.addSession(1);
        final SealerClusteredService svc = fileMember("a");
        deliver(svc, twin, offer(1, 0));
        tick(svc);

        final StubSession late = cluster.addSession(4);
        deliver(svc, late, IngressFrames.replayRequestFrame(0, 1));
        final List<byte[]> replayed = withoutStatus(late);
        assertArrayEquals(referenceFrame(1, 0), replayed.get(0));
        assertEquals(SealerWire.EGRESS_KIND_BOUNDARY, replayed.get(1)[0]);
        assertEquals(SealerWire.EGRESS_KIND_REPLAY_DONE, replayed.get(2)[0]);
    }

    /** The refusal run: one entry for each refusal, on a member with the file and on one without. */
    private List<byte[]> refusals(final Optional<String> name) {
        final StubSession sequencer = cluster.addSession(name.isPresent() ? 11 : 12);
        final SealerClusteredService svc = member(name, Optional.empty(), SMALL_CAPACITY);
        deliver(svc, sequencer, offer(1, 0));
        deliver(svc, sequencer, offer(2, 2));
        deliver(svc, sequencer, offer(3, 1, 0));
        IntStream.rangeClosed(4, 6).forEach(n -> deliver(svc, sequencer, offer(n, n - 3)));
        deliver(svc, sequencer, offer(7, 4));
        tick(svc);
        tick(svc);
        deliver(svc, sequencer, offer(8, 4));
        cluster.closeClientSession(sequencer.id);
        return sequencer.offered.stream().filter(f -> REFUSALS.contains(f[0])).toList();
    }

    @Test
    void each_refusal_is_the_same_as_on_a_member_without_the_file() {
        final List<byte[]> heap = refusals(Optional.empty());
        final List<byte[]> file = refusals(Optional.of("a"));
        assertEquals(List.of(SealerWire.EGRESS_KIND_CONTIGUITY_REJECT, SealerWire.EGRESS_KIND_PAST_DEADLINE,
                SealerWire.EGRESS_KIND_WINDOW_FULL, SealerWire.EGRESS_KIND_DA_LAG_REJECT),
            file.stream().map(f -> f[0]).toList());
        assertSameFrames(heap, file);
    }

    /** The same log on a member: entries and a boundary. */
    private static void sameLog(final SealerClusteredService svc, final StubSession sequencer) {
        IntStream.range(1, 4).forEach(n -> deliver(svc, sequencer, offer(n, n - 1)));
        tick(svc);
    }

    @Test
    void every_member_writes_the_same_snapshot_bytes() {
        final StubSession sequencer = cluster.addSession(1);
        final List<byte[]> snapshots = List.of("a", "b", "c").stream().map(name -> {
            final SealerClusteredService svc = fileMember(name);
            sameLog(svc, sequencer);
            return svc.snapshot();
        }).toList();
        assertEquals(CanonicalSealerState.RETAINED_FILE_SNAPSHOT_VERSION,
            ByteBuffer.wrap(snapshots.get(0)).getInt(Integer.BYTES));
        snapshots.forEach(s -> assertArrayEquals(snapshots.get(0), s));
    }

    /**
     * The guard counts every ordered entry: after nonces 0 to 2, it refuses
     * nonce 1 and orders nonce 3. A frame of kind 10, which no release
     * defines, drops as an unknown kind and changes no guard.
     */
    @Test
    void the_guard_expects_the_nonce_after_the_last_ordered_entry() {
        final StubSession sequencer = cluster.addSession(1);
        final SealerClusteredService svc = fileMember("a");
        sameLog(svc, sequencer);
        final byte[] unknown = new byte[1 + CanonicalSealerState.SENDER_LEN + Long.BYTES
            + CanonicalSealerState.CANONICAL_ID_LEN];
        unknown[0] = 10;
        deliver(svc, sequencer, unknown);
        assertEquals(1L, svc.droppedUnknownKindCount());
        deliver(svc, sequencer, offer(9, 1));
        deliver(svc, sequencer, offer(4, 3));
        final StubSession observer = cluster.addSession(3);
        deliver(svc, observer, IngressFrames.replayRequestFrame(3, 2));
        assertSameFrames(List.of(referenceFrame(4, 3)), observer.offered.stream()
            .filter(f -> f[0] == SealerWire.EGRESS_KIND_RELAYED_REF).toList());
        assertTrue(withoutStatus(sequencer).stream().anyMatch(f -> f[0] == SealerWire.EGRESS_KIND_CONTIGUITY_REJECT),
            "the guard refuses nonce 1");
    }

    @Test
    void a_member_without_the_file_restores_a_version_ten_snapshot_and_decides_alike() {
        final StubSession sequencer = cluster.addSession(1);
        final SealerClusteredService original = member(Optional.empty(), Optional.empty());
        IntStream.range(1, 4).forEach(n -> deliver(original, sequencer, offer(n, n - 1)));
        tick(original);
        final byte[] snapshot = original.snapshot();
        assertEquals(10, ByteBuffer.wrap(snapshot).getInt(Integer.BYTES));

        final SealerClusteredService restored = member(Optional.empty(), Optional.of(snapshot));
        assertEquals(10, restored.latestSnapshotVersion());
        List.of(original, restored).forEach(svc -> {
            deliver(svc, sequencer, offer(9, 1));
            deliver(svc, sequencer, offer(4, 3));
        });
        assertArrayEquals(original.snapshot(), restored.snapshot());
    }

    @Test
    void a_member_refuses_a_snapshot_of_the_other_layout() {
        final SealerClusteredService heap = member(Optional.empty(), Optional.empty());
        final SealerClusteredService file = fileMember("a");
        assertThrows(AgentTerminationException.class, () -> member(Optional.of("b"), Optional.of(heap.snapshot())));
        assertThrows(AgentTerminationException.class, () -> member(Optional.empty(), Optional.of(file.snapshot())));
    }

    @Test
    void a_member_with_the_file_drops_the_void_request_and_the_recorded_cursor() {
        final StubSession consumer = cluster.addSession(1);
        final SealerClusteredService svc = fileMember("a");
        deliver(svc, consumer, offer(1, 0));
        deliver(svc, consumer, IngressFrames.voidRequestFrame(0, 0, id(1)));
        deliver(svc, consumer, IngressFrames.recordedCursorFrame(0, 0));
        assertEquals(2L, svc.droppedUnknownKindCount());
    }

    @Test
    void a_session_that_opens_gets_the_decision_version() {
        final StubSession sequencer = cluster.addSession(1);
        final SealerClusteredService svc = fileMember("a");
        svc.onSessionOpen(sequencer, 0);
        assertArrayEquals(new byte[] {SealerWire.EGRESS_KIND_SEALER_INFO, 3, 1, 0, 0, 0, 0, 0, 0, 0},
            sequencer.offered.get(0));
    }

    /**
     * A member without the file runs version 3 after the activation, and
     * its snapshot, version 13, restores version 3. The member is a
     * follower, so the snapshot request at the activation goes to no control
     * toggle.
     */
    @Test
    void a_member_without_the_file_restores_version_three_from_its_snapshot() {
        cluster.role = io.aeron.cluster.service.Cluster.Role.FOLLOWER;
        final StubSession sequencer = cluster.addSession(1);
        final SealerClusteredService svc = member(Optional.empty(), Optional.empty());
        // A time in the past: a 0 in block 1 would store 0, which means never.
        deliver(svc, sequencer, IngressFrames.featureActivationFrame(1, DecisionRules.FEATURE_SEALER_V3, 1));
        deliver(svc, sequencer, IngressFrames.originRecordFrame(IngressFrames.canonicalId(1001), 1, 1, new byte[] {1}));
        sameLog(svc, sequencer);
        assertEquals(3, svc.decisionRules().version());
        final byte[] snapshot = svc.snapshot();
        assertEquals(CanonicalSealerState.RULES_SNAPSHOT_VERSION, ByteBuffer.wrap(snapshot).getInt(Integer.BYTES));
        final SealerClusteredService restored = member(Optional.empty(), Optional.of(snapshot));
        assertEquals(3, restored.decisionRules().version());
    }

    @Test
    void a_member_with_the_file_takes_no_canonical_entry_from_a_session() {
        final StubSession sequencer = cluster.addSession(1);
        final SealerClusteredService svc = fileMember("a");
        final byte[] frame = offer(1, 0);
        frame[SealerWire.CANONICAL_ID_OFFSET + CanonicalSealerState.CANONICAL_ID_LEN] = SealerWire.RT_TX;
        deliver(svc, sequencer, frame);
        tick(svc);
        assertEquals(0L, svc.retained().wanted(0, 1).filter(f -> f[0] == SealerWire.EGRESS_KIND_RELAYED).count());
    }
}
