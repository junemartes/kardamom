package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.LagBudgets;
import io.kardamom.sealer.VersionSwitch;
import io.kardamom.sealer.VoidLedger;
import io.kardamom.sealer.cluster.ClusterStubs.StubCluster;
import io.kardamom.sealer.cluster.ClusterStubs.StubSession;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Decision version 4 on driverless members: the activation of
 * {@code FEATURE_SEALER_V4} switches the version at the boundary that seals
 * the first block of its epoch. Under version 4, a transaction reference
 * that carries its position is ordered once by the replay window of its
 * ingress image, and a reference without a position takes the rules of
 * version 3.
 */
class SealerVersionFourTest {

    private static final int CAPACITY = 16;
    private static final byte[] SENDER = filled(CanonicalSealerState.SENDER_LEN, 0xAA);
    /** The sender of the references without a position: version 4 references move no expected nonce. */
    private static final byte[] PLAIN_SENDER = filled(CanonicalSealerState.SENDER_LEN, 0xBB);
    private static final byte RELAYED = SealerWire.EGRESS_KIND_RELAYED;
    private static final byte BOUNDARY = SealerWire.EGRESS_KIND_BOUNDARY;
    private static final byte RULES = SealerWire.EGRESS_KIND_SEALER_INFO;

    /** One log entry: an ingress frame, or a boundary tick when {@code frame} is empty. */
    private record Entry(String name, Optional<byte[]> frame, long timeMs) {
        static Entry frame(final String name, final byte[] frame, final long timeMs) {
            return new Entry(name, Optional.of(frame), timeMs);
        }

        static Entry tick(final String name, final long timeMs) {
            return new Entry(name, Optional.empty(), timeMs);
        }
    }

    /** The egress kinds that the session got for one entry, without the status frames, and the version after it. */
    private record Outcome(String name, List<Byte> kinds, int decisionVersion) {
    }

    private final List<SealerClusteredService> started = new ArrayList<>();
    private final List<StubCluster> clusters = new ArrayList<>();

    @AfterEach
    void stop() {
        IntStream.range(0, started.size()).forEach(i -> started.get(i).onTerminate(clusters.get(i)));
    }

    private static byte[] filled(final int length, final int value) {
        final byte[] out = new byte[length];
        Arrays.fill(out, (byte) value);
        return out;
    }

    private static byte[] epoch(final long origin) {
        return IngressFrames.originRecordFrame(
            IngressFrames.canonicalId(1000 + (int) origin), origin, 1, new byte[] {0x02});
    }

    /** A kind-0 transaction reference of image {@code (lane 0, session 9)} at {@code position}. */
    private static byte[] ref(final int idTag, final long nonce, final long position) {
        final ByteBuffer buf = ByteBuffer.allocate(SealerWire.TXREF_RECORD_LEN).order(ByteOrder.LITTLE_ENDIAN);
        buf.put(SealerWire.KIND_INGRESS_RECORD).put(SENDER).putLong(nonce)
            .putLong(SealerWire.POSITION_TAG | position).putLong(0L).putLong(0L)
            .put(IngressFrames.recordId(idTag)).put(SealerWire.RT_TXREF)
            .put((byte) 0).putInt(0).putInt((int) position).putInt(9);
        return buf.array();
    }

    /** {@link #ref} of another sender, with "no deadline" in place of the position. */
    private static byte[] plainRef(final int idTag, final long nonce) {
        final byte[] frame = ref(idTag, nonce, 0L);
        ByteBuffer.wrap(frame).order(ByteOrder.LITTLE_ENDIAN)
            .putLong(SealerWire.DEADLINE_OFFSET, Long.MAX_VALUE)
            .put(SealerWire.SENDER_OFFSET, PLAIN_SENDER);
        return frame;
    }

    /**
     * The stream: an activation of version 4 at once in front of epoch 2,
     * the switch at the boundary that seals the first block of the epoch,
     * then two copies of one reference, a second reference, and a reference
     * without a position.
     */
    private static List<Entry> stream() {
        return List.of(
            Entry.frame("epoch-1", epoch(1), 1_000L),
            Entry.frame("v3-ref", ref(1, 0, 64), 1_100L),
            Entry.tick("tick-1", 1_250L),
            Entry.frame("on", IngressFrames.featureActivationFrame(2, VersionSwitch.FEATURE_SEALER_V4, 0L), 1_400L),
            Entry.frame("epoch-2", epoch(2), 1_500L),
            Entry.frame("v3-in-epoch-block", ref(2, 1, 96), 1_600L),
            Entry.tick("tick-2", 2_000L),
            Entry.frame("ref-a", ref(3, 2, 128), 2_100L),
            Entry.frame("ref-a-copy", ref(3, 2, 128), 2_150L),
            Entry.frame("ref-b", ref(4, 3, 160), 2_200L),
            Entry.frame("plain", plainRef(5, 4), 2_300L),
            Entry.frame("plain-copy", plainRef(5, 4), 2_350L),
            Entry.tick("tick-3", 2_500L));
    }

    /** A heap member on its own stub cluster, from {@code snapshot} or at genesis. */
    private Member member(final Optional<byte[]> snapshot) {
        final StubCluster cluster = new StubCluster();
        cluster.role = io.aeron.cluster.service.Cluster.Role.FOLLOWER;
        final StubSession sequencer = cluster.addSession(1);
        final SealerClusteredService svc = new SealerClusteredService(
            CAPACITY, 250, 0, Set.of(), VoidLedger.Config.DISABLED,
            CanonicalSealerState.DEFAULT_INCLUSION_HORIZON_BLOCKS,
            CanonicalSealerState.DEFAULT_ORDERING_WINDOW, LagBudgets.DEFAULT);
        svc.start(cluster, snapshot);
        started.add(svc);
        clusters.add(cluster);
        return new Member(svc, cluster, sequencer);
    }

    /** A member, its cluster, and the session of its sequencer. */
    private record Member(SealerClusteredService svc, StubCluster cluster, StubSession sequencer) {
        Outcome apply(final Entry entry) {
            final int before = sequencer.offered.size();
            cluster.time = entry.timeMs();
            entry.frame().ifPresentOrElse(frame -> deliver(frame, entry.timeMs()), () -> svc.onTimerEvent(
                SealerClusteredService.BOUNDARY_TIMER_CORRELATION_ID, entry.timeMs()));
            final List<Byte> kinds = sequencer.offered.subList(before, sequencer.offered.size()).stream()
                .map(f -> f[0])
                .filter(kind -> kind != SealerWire.EGRESS_KIND_STATUS)
                .toList();
            return new Outcome(entry.name(), kinds, svc.decisionRules().version());
        }

        List<Outcome> applyAll(final List<Entry> log) {
            return log.stream().map(this::apply).toList();
        }

        private void deliver(final byte[] frame, final long timeMs) {
            final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
            buf.putBytes(0, frame);
            svc.onSessionMessage(sequencer, timeMs, buf, 0, frame.length, null);
        }
    }

    @Test
    void the_stream_switches_to_version_four_and_orders_each_reference_once() {
        assertEquals(List.of(
            new Outcome("epoch-1", List.of(RELAYED), 3),
            new Outcome("v3-ref", List.of(RELAYED), 3),
            new Outcome("tick-1", List.of(BOUNDARY), 3),
            new Outcome("on", List.of(), 3),
            new Outcome("epoch-2", List.of(RELAYED), 3),
            new Outcome("v3-in-epoch-block", List.of(RELAYED), 3),
            new Outcome("tick-2", List.of(BOUNDARY, RULES), 4),
            new Outcome("ref-a", List.of(RELAYED), 4),
            new Outcome("ref-a-copy", List.of(), 4),
            new Outcome("ref-b", List.of(RELAYED), 4),
            new Outcome("plain", List.of(RELAYED), 4),
            new Outcome("plain-copy", List.of(), 4),
            new Outcome("tick-3", List.of(BOUNDARY), 4)), member(Optional.empty()).applyAll(stream()));
    }

    @Test
    void the_switch_reports_version_four_to_every_session() {
        final Member member = member(Optional.empty());
        member.applyAll(stream());
        final List<byte[]> rules = member.sequencer().offered.stream().filter(f -> f[0] == RULES).toList();
        assertEquals(4, rules.get(rules.size() - 1)[1]);
    }

    /** The activation and the switch each ask for a snapshot, so a replay has a start point on each side. */
    @Test
    void the_activation_and_the_switch_ask_for_a_snapshot() {
        final Member member = member(Optional.empty());
        member.applyAll(stream());
        final long notices = member.sequencer().offered.stream().filter(f -> f[0] == SealerWire.EGRESS_KIND_STATUS
                && f.length > SealerWire.STATUS_SNAPSHOT_FLAG_OFFSET && f[SealerWire.STATUS_SNAPSHOT_FLAG_OFFSET] == 1)
            .count();
        assertEquals(2, notices);
    }

    private void replayFrom(final List<Entry> log, final int cut) {
        final Member original = member(Optional.empty());
        original.applyAll(log.subList(0, cut));
        final byte[] snapshot = original.svc().snapshot();
        final Member restored = member(Optional.of(snapshot));
        assertEquals(original.svc().decisionRules(), restored.svc().decisionRules());
        final List<Entry> rest = log.subList(cut, log.size());
        assertEquals(original.applyAll(rest), restored.applyAll(rest), "cut at " + cut);
        assertArrayEquals(original.svc().snapshot(), restored.svc().snapshot(), "cut at " + cut);
    }

    @Test
    void a_replay_from_a_snapshot_at_any_entry_decides_alike() {
        final List<Entry> log = stream();
        IntStream.range(1, log.size()).forEach(cut -> replayFrom(log, cut));
    }

    @Test
    void a_member_that_holds_the_activation_writes_snapshot_version_fifteen() {
        final Member member = member(Optional.empty());
        member.applyAll(stream().subList(0, 4));
        final byte[] snapshot = member.svc().snapshot();
        assertEquals(CanonicalSealerState.SWITCH_SNAPSHOT_VERSION, ByteBuffer.wrap(snapshot).getInt(Integer.BYTES));
        assertTrue(member.svc().decisionRules().activationOrigin() == 2L);
    }
}
