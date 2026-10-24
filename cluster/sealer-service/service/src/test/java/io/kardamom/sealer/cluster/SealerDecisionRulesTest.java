package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.DecisionRules;
import io.kardamom.sealer.LagBudgets;
import io.kardamom.sealer.VoidLedger;
import io.kardamom.sealer.cluster.ClusterStubs.StubCluster;
import io.kardamom.sealer.cluster.ClusterStubs.StubSession;
import java.nio.ByteBuffer;
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
 * The decision rules on driverless members: every member runs version 3
 * from its start. A feature activation record changes no state, so a
 * member decides and writes the same with or without it. A replay from a
 * snapshot at any point of the log decides alike.
 */
class SealerDecisionRulesTest {

    private static final int CAPACITY = 16;
    private static final byte[] SENDER = filled(CanonicalSealerState.SENDER_LEN, 0xAA);
    /** The feature id of the activation in the golden stream. */
    private static final long FEATURE = 2L;

    /** One log entry: an ingress frame, or a boundary tick when {@code frame} is empty. */
    private record Entry(String name, Optional<byte[]> frame, long timeMs) {
        static Entry frame(final String name, final byte[] frame, final long timeMs) {
            return new Entry(name, Optional.of(frame), timeMs);
        }

        static Entry tick(final String name, final long timeMs) {
            return new Entry(name, Optional.empty(), timeMs);
        }
    }

    /**
     * What one entry did: the egress kinds that the sequencer got, without
     * the status frames, and the decision version after the entry.
     */
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

    private static byte[] offer(final int n, final long nonce) {
        return IngressFrames.recordFrame(n, SENDER, nonce);
    }

    private static byte[] epoch(final long origin) {
        return IngressFrames.originRecordFrame(
            IngressFrames.canonicalId(1000 + (int) origin), origin, 1, new byte[] {0x01});
    }

    /** An exempt record frame that ends at the record type. */
    private static byte[] shortRecord(final int n) {
        return IngressFrames.exemptRecordFrame(IngressFrames.canonicalId(n), new byte[0]);
    }

    /**
     * The golden stream. The sender orders nonces 0 and 1, and a new
     * transaction at nonce 1 gets a contiguity reject. An activation of
     * feature 2 at once, and one that turns it off for good, come in front
     * of epochs 2 and 3. A frame that ends at the record type drops as
     * malformed in each phase, and the guard decides as before.
     */
    private static List<Entry> goldenStream(final boolean withActivations) {
        final List<Entry> log = new ArrayList<>(List.of(
            Entry.frame("epoch-1", epoch(1), 1_000L),
            Entry.frame("nonce-0", offer(1, 0), 1_100L),
            Entry.frame("nonce-1", offer(2, 1), 1_200L),
            Entry.tick("tick-1", 1_250L),
            Entry.frame("retry", offer(3, 1), 1_400L)));
        if (withActivations) {
            log.add(Entry.frame("on", IngressFrames.featureActivationFrame(2, FEATURE, 0L), 1_500L));
        }
        log.addAll(List.of(
            Entry.frame("epoch-2", epoch(2), 1_600L),
            Entry.frame("nonce-2", offer(4, 2), 1_700L),
            Entry.tick("tick-2", 2_000L),
            Entry.frame("short-on", shortRecord(70), 2_100L)));
        if (withActivations) {
            log.add(Entry.frame("off", IngressFrames.featureActivationFrame(3, FEATURE, -1L), 2_200L));
        }
        log.addAll(List.of(
            Entry.frame("epoch-3", epoch(3), 2_300L),
            Entry.frame("nonce-3", offer(5, 3), 2_400L),
            Entry.tick("tick-3", 2_500L),
            Entry.frame("short-off", shortRecord(71), 2_600L),
            Entry.frame("retry-again", offer(6, 1), 2_700L),
            Entry.tick("tick-4", 2_750L)));
        return log;
    }

    /** A heap member on its own stub cluster, from {@code snapshot} or at genesis. */
    private Member member(final Optional<byte[]> snapshot) {
        final StubCluster cluster = new StubCluster();
        // A follower asks for a snapshot through no control toggle; the
        // stub has none. The snapshot notice still goes out.
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

    private static Outcome outcome(final String name, final List<Byte> kinds) {
        return new Outcome(name, kinds, DecisionRules.VERSION);
    }

    private static final byte RELAYED = SealerWire.EGRESS_KIND_RELAYED;
    private static final byte BOUNDARY = SealerWire.EGRESS_KIND_BOUNDARY;
    private static final byte REJECT = SealerWire.EGRESS_KIND_CONTIGUITY_REJECT;
    private static final byte RULES = SealerWire.EGRESS_KIND_SEALER_INFO;

    @Test
    void the_golden_stream_decides_by_version_three() {
        final List<Outcome> outcomes = member(Optional.empty()).applyAll(goldenStream(true));
        assertEquals(List.of(
            outcome("epoch-1", List.of(RELAYED)),
            outcome("nonce-0", List.of(RELAYED)),
            outcome("nonce-1", List.of(RELAYED)),
            outcome("tick-1", List.of(BOUNDARY)),
            outcome("retry", List.of(REJECT)),
            outcome("on", List.of()),
            outcome("epoch-2", List.of(RELAYED)),
            outcome("nonce-2", List.of(RELAYED)),
            outcome("tick-2", List.of(BOUNDARY)),
            outcome("short-on", List.of()),
            outcome("off", List.of()),
            outcome("epoch-3", List.of(RELAYED)),
            outcome("nonce-3", List.of(RELAYED)),
            outcome("tick-3", List.of(BOUNDARY)),
            outcome("short-off", List.of()),
            outcome("retry-again", List.of(REJECT)),
            outcome("tick-4", List.of(BOUNDARY))), outcomes);
    }

    /** A feature activation record changes no decision, no snapshot byte, and asks for no snapshot. */
    @Test
    void a_feature_activation_changes_nothing() {
        final Member activated = member(Optional.empty());
        final Member plain = member(Optional.empty());
        final List<Outcome> with = activated.applyAll(goldenStream(true));
        assertEquals(plain.applyAll(goldenStream(false)),
            with.stream().filter(o -> !o.name().equals("on") && !o.name().equals("off")).toList());
        final byte[] snapshot = activated.svc().snapshot();
        assertEquals(CanonicalSealerState.snapshotWriteVersion(), ByteBuffer.wrap(snapshot).getInt(Integer.BYTES));
        assertArrayEquals(plain.svc().snapshot(), snapshot, "the same decisions write the same snapshot");
        assertEquals(0, notices(activated), "no record asks for a snapshot");
        assertEquals(DecisionRules.START_VIEW, activated.svc().decisionRules());
    }

    /**
     * Run {@code log} up to {@code cut}, take the snapshot, restore a new
     * member from it, and run the rest on both. The outcomes and the last
     * snapshot match.
     */
    private void replayFrom(final List<Entry> log, final int cut) {
        final Member original = member(Optional.empty());
        original.applyAll(log.subList(0, cut));
        final byte[] snapshot = original.svc().snapshot();
        assertEquals(CanonicalSealerState.snapshotWriteVersion(), ByteBuffer.wrap(snapshot).getInt(Integer.BYTES));
        final Member restored = member(Optional.of(snapshot));
        assertEquals(original.svc().decisionRules(), restored.svc().decisionRules());
        final List<Entry> rest = log.subList(cut, log.size());
        assertEquals(original.applyAll(rest), restored.applyAll(rest));
        assertArrayEquals(original.svc().snapshot(), restored.svc().snapshot());
    }

    @Test
    void a_replay_from_a_snapshot_at_any_entry_decides_alike() {
        final List<Entry> log = goldenStream(true);
        IntStream.range(1, log.size()).forEach(cut -> replayFrom(log, cut));
    }

    @Test
    void a_session_gets_the_rules_frame_when_it_opens() {
        final Member member = member(Optional.empty());
        final StubSession early = member.cluster().addSession(7);
        member.svc().onSessionOpen(early, 0);
        assertArrayEquals(new byte[] {RULES, 3, 7, 0, 0, 0, 0, 0, 0, 0}, early.offered.get(0));
    }

    private static long notices(final Member member) {
        return member.sequencer().offered.stream().filter(f -> f[0] == SealerWire.EGRESS_KIND_STATUS
                && f.length > SealerWire.STATUS_SNAPSHOT_FLAG_OFFSET && f[SealerWire.STATUS_SNAPSHOT_FLAG_OFFSET] == 1)
            .count();
    }

    @Test
    void a_new_term_offers_the_rules_frame_again() {
        final Member member = member(Optional.empty());
        newTerm(member);
        newTerm(member);
        final List<byte[]> frames = member.sequencer().offered.stream().filter(f -> f[0] == RULES).toList();
        assertEquals(2, frames.size(), "a frame that a leader change lost goes out again");
        assertTrue(frames.stream().allMatch(f -> f[1] == DecisionRules.VERSION));
    }

    private static void newTerm(final Member member) {
        member.svc().onNewLeadershipTermEvent(1, 0, 0, 0, 0, 0, java.util.concurrent.TimeUnit.MILLISECONDS, 0);
    }

    /** A frame that ends at the record type drops as malformed: every relayed frame holds its record type. */
    @Test
    void a_frame_that_ends_at_the_record_type_drops_as_malformed() {
        final byte[] shortEpoch = IngressFrames.originRecordFrame(IngressFrames.canonicalId(1001), 1, 1, new byte[0]);
        final byte[] shortRecord = shortRecord(7);
        assertEquals(SealerWire.MIN_ORIGIN_RECORD_LEN, shortEpoch.length);
        assertEquals(SealerWire.MIN_INGRESS_LEN, shortRecord.length);
        final Member member = member(Optional.empty());
        assertEquals(List.of(), member.apply(Entry.frame("short-epoch", shortEpoch, 100L)).kinds());
        assertEquals(List.of(), member.apply(Entry.frame("short-record", shortRecord, 200L)).kinds());
    }
}
