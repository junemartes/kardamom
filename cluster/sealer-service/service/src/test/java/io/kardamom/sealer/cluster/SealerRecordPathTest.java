package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kardamom.sealer.CanonicalSealerState;
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
 * same as on a member without the file, every member writes the same
 * snapshot bytes, and the nonce release applies once for each skip.
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

    /** The id of entry {@code n}: not a keccak hash, but distinct from every mark of another entry. */
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

    /** The nonce release of the skipped entry {@code n}: the chain expects {@code nonce}. */
    private static byte[] release(final int n, final long nonce) {
        return ByteBuffer.allocate(SealerWire.NONCE_RELEASE_LEN).order(ByteOrder.LITTLE_ENDIAN)
            .put(SealerWire.KIND_NONCE_RELEASE).put(SENDER).putLong(nonce).put(id(n)).array();
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

    /** The same log on a member: entries, a skip release and its twin copy, and a boundary. */
    private static void sameLog(final SealerClusteredService svc, final StubSession sequencer) {
        IntStream.range(1, 4).forEach(n -> deliver(svc, sequencer, offer(n, n - 1)));
        deliver(svc, sequencer, release(2, 1));
        deliver(svc, sequencer, release(2, 1));
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

    @Test
    void a_release_after_a_skip_lets_a_new_transaction_in_at_the_lower_nonce() {
        final StubSession sequencer = cluster.addSession(1);
        final SealerClusteredService svc = fileMember("a");
        sameLog(svc, sequencer);
        // Nonce 1 (entry 2) skipped; the guard expected 3, and now expects 1.
        deliver(svc, sequencer, offer(9, 1));
        deliver(svc, sequencer, offer(2, 1));
        final StubSession observer = cluster.addSession(3);
        deliver(svc, observer, IngressFrames.replayRequestFrame(3, 2));
        assertSameFrames(List.of(referenceFrame(9, 3)), observer.offered.stream()
            .filter(f -> f[0] == SealerWire.EGRESS_KIND_RELAYED_REF).toList());
        assertTrue(withoutStatus(sequencer).stream().noneMatch(f -> f[0] == SealerWire.EGRESS_KIND_CONTIGUITY_REJECT),
            "the new transaction gets in, and the same signed bytes drop as a duplicate");
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
            deliver(svc, sequencer, release(2, 1));
            deliver(svc, sequencer, offer(9, 1));
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
}
