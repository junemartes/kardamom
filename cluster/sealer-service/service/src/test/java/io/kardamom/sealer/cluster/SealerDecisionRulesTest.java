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
 * The switch of the decision rules on driverless members: a member runs
 * version 2 until the feature activation of {@link DecisionRules#FEATURE_SEALER_V3}
 * is due, and version 3 after it. A replay from a snapshot at any point of
 * the log switches at the same entry and decides alike.
 */
class SealerDecisionRulesTest {

    private static final int CAPACITY = 16;
    private static final byte[] SENDER = filled(CanonicalSealerState.SENDER_LEN, 0xAA);
    /** The activation time of the golden stream. */
    private static final long ACTIVATION_MS = 10_000L;

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

    private static byte[] release(final int n, final long nonce) {
        return IngressFrames.nonceReleaseFrame(SENDER, nonce, IngressFrames.recordId(n));
    }

    private static byte[] epoch(final long origin) {
        return IngressFrames.originRecordFrame(
            IngressFrames.canonicalId(1000 + (int) origin), origin, 1, new byte[] {0x01});
    }

    private static byte[] activation(final long origin, final long activationMs) {
        return IngressFrames.featureActivationFrame(origin, DecisionRules.FEATURE_SEALER_V3, activationMs);
    }

    /**
     * The golden stream. The sender orders nonces 0 and 1, and the executor
     * skips nonce 1. Before the activation is due, the release drops as an
     * unknown kind, and a new transaction at nonce 1 gets a contiguity
     * reject. The activation of epoch 2 comes in front of the epoch, and
     * the first tick at its time switches the rules. After it, the release
     * applies, and a new transaction at nonce 1 gets in.
     */
    private static List<Entry> goldenStream(final boolean withActivation) {
        final List<Entry> log = new ArrayList<>(List.of(
            Entry.frame("epoch-1", epoch(1), 1_000L),
            Entry.frame("nonce-0", offer(1, 0), 1_100L),
            Entry.frame("nonce-1", offer(2, 1), 1_200L),
            Entry.tick("tick-1", 1_250L),
            Entry.frame("release-v2", release(2, 1), 1_300L),
            Entry.frame("retry-v2", offer(3, 1), 1_400L)));
        if (withActivation) {
            log.add(Entry.frame("activation", activation(2, ACTIVATION_MS), 1_500L));
        }
        log.addAll(List.of(
            Entry.frame("epoch-2", epoch(2), 1_600L),
            Entry.frame("release-early", release(2, 1), 5_000L),
            Entry.tick("tick-at-activation", ACTIVATION_MS),
            Entry.frame("release-v3", release(2, 1), 10_100L),
            Entry.frame("release-twin", release(2, 1), 10_150L),
            Entry.frame("retry-v3", offer(4, 1), 10_200L),
            Entry.tick("tick-2", 10_250L)));
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

    private static Outcome outcome(final String name, final List<Byte> kinds, final int version) {
        return new Outcome(name, kinds, version);
    }

    private static final byte RELAYED = SealerWire.EGRESS_KIND_RELAYED;
    private static final byte BOUNDARY = SealerWire.EGRESS_KIND_BOUNDARY;
    private static final byte REJECT = SealerWire.EGRESS_KIND_CONTIGUITY_REJECT;
    private static final byte RULES = SealerWire.EGRESS_KIND_SEALER_INFO;

    @Test
    void the_golden_stream_decides_by_version_two_before_the_activation_and_by_version_three_after_it() {
        final List<Outcome> outcomes = member(Optional.empty()).applyAll(goldenStream(true));
        assertEquals(List.of(
            outcome("epoch-1", List.of(RELAYED), 2),
            outcome("nonce-0", List.of(RELAYED), 2),
            outcome("nonce-1", List.of(RELAYED), 2),
            outcome("tick-1", List.of(BOUNDARY), 2),
            outcome("release-v2", List.of(), 2),
            outcome("retry-v2", List.of(REJECT), 2),
            outcome("activation", List.of(), 2),
            outcome("epoch-2", List.of(RELAYED), 2),
            outcome("release-early", List.of(), 2),
            outcome("tick-at-activation", List.of(RULES, BOUNDARY), 3),
            outcome("release-v3", List.of(), 3),
            outcome("release-twin", List.of(), 3),
            outcome("retry-v3", List.of(RELAYED), 3),
            outcome("tick-2", List.of(BOUNDARY), 3)), outcomes);
    }

    @Test
    void before_the_activation_a_member_decides_and_writes_as_the_previous_release() {
        final Member activated = member(Optional.empty());
        final Member plain = member(Optional.empty());
        final List<Entry> withActivation = goldenStream(true);
        final List<Entry> without = goldenStream(false);
        final int cut = withActivation.indexOf(withActivation.stream()
            .filter(e -> e.name().equals("activation")).findFirst().orElseThrow());
        assertEquals(plain.applyAll(without.subList(0, cut)), activated.applyAll(withActivation.subList(0, cut)));
        final byte[] snapshot = activated.svc().snapshot();
        assertEquals(CanonicalSealerState.snapshotWriteVersion(), ByteBuffer.wrap(snapshot).getInt(Integer.BYTES));
        assertArrayEquals(plain.svc().snapshot(), snapshot, "a kind-10 frame under version 2 changes no state");
        assertTrue(activated.sequencer().offered.stream().noneMatch(f -> f[0] == RULES),
            "no rules frame under version 2");
    }

    @Test
    void a_member_of_version_two_never_switches_without_the_activation() {
        final List<Outcome> outcomes = member(Optional.empty()).applyAll(goldenStream(false));
        assertTrue(outcomes.stream().allMatch(o -> o.decisionVersion() == 2));
        assertEquals(List.of(REJECT), outcomes.stream()
            .filter(o -> o.name().equals("retry-v3")).findFirst().orElseThrow().kinds());
    }

    /**
     * Run the golden stream up to {@code cut}, take the snapshot, restore a
     * new member from it, and run the rest on both. The outcomes and the
     * last snapshot match.
     */
    private void replayFrom(final int cut, final int snapshotVersion) {
        final List<Entry> log = goldenStream(true);
        final Member original = member(Optional.empty());
        original.applyAll(log.subList(0, cut));
        final byte[] snapshot = original.svc().snapshot();
        assertEquals(snapshotVersion, ByteBuffer.wrap(snapshot).getInt(Integer.BYTES));
        final Member restored = member(Optional.of(snapshot));
        assertEquals(original.svc().decisionRules().version(), restored.svc().decisionRules().version());
        final List<Entry> rest = log.subList(cut, log.size());
        assertEquals(original.applyAll(rest), restored.applyAll(rest));
        assertArrayEquals(original.svc().snapshot(), restored.svc().snapshot());
    }

    @Test
    void a_replay_from_a_snapshot_before_the_activation_switches_at_the_same_entry() {
        replayFrom(6, CanonicalSealerState.snapshotWriteVersion());
    }

    @Test
    void a_replay_from_a_snapshot_with_a_pending_activation_switches_at_the_same_entry() {
        replayFrom(8, CanonicalSealerState.RULES_SNAPSHOT_VERSION);
    }

    @Test
    void a_replay_from_a_snapshot_after_the_switch_keeps_version_three() {
        replayFrom(10, CanonicalSealerState.RULES_SNAPSHOT_VERSION);
    }

    @Test
    void a_session_gets_the_rules_frame_only_under_version_three() {
        final Member member = member(Optional.empty());
        final StubSession early = member.cluster().addSession(7);
        member.svc().onSessionOpen(early, 0);
        assertTrue(early.offered.isEmpty(), "version 2 sends no rules frame");
        member.apply(Entry.frame("activation", activation(1, 0), 100L));
        assertArrayEquals(new byte[] {RULES, 3, 7, 0, 0, 0, 0, 0, 0, 0},
            early.offered.stream().filter(f -> f[0] == RULES).findFirst().orElseThrow(),
            "an open session gets the rules frame at the switch");
        final StubSession late = member.cluster().addSession(8);
        member.svc().onSessionOpen(late, 200L);
        assertArrayEquals(new byte[] {RULES, 3, 8, 0, 0, 0, 0, 0, 0, 0}, late.offered.get(0));
    }

    @Test
    void an_activation_that_applies_asks_for_a_snapshot() {
        final Member member = member(Optional.empty());
        member.apply(Entry.frame("epoch-1", epoch(1), 1_000L));
        member.apply(Entry.frame("activation", activation(2, ACTIVATION_MS), 1_500L));
        assertTrue(member.sequencer().offered.stream().anyMatch(f -> f[0] == SealerWire.EGRESS_KIND_STATUS
                && f.length > SealerWire.STATUS_SNAPSHOT_FLAG_OFFSET && f[SealerWire.STATUS_SNAPSHOT_FLAG_OFFSET] == 1),
            "the status with the snapshot notice goes out at the activation record");
        assertEquals(new DecisionRules.View(2, 2L, ACTIVATION_MS), member.svc().decisionRules());
    }

    @Test
    void a_new_term_offers_the_rules_frame_again_under_version_three() {
        final Member member = member(Optional.empty());
        newTerm(member);
        assertTrue(member.sequencer().offered.stream().noneMatch(f -> f[0] == RULES), "version 2 sends none");
        member.apply(Entry.frame("activation", activation(1, 0), 100L));
        final long before = member.sequencer().offered.stream().filter(f -> f[0] == RULES).count();
        newTerm(member);
        assertEquals(before + 1, member.sequencer().offered.stream().filter(f -> f[0] == RULES).count(),
            "a frame that a leader change lost goes out again");
    }

    private static void newTerm(final Member member) {
        member.svc().onNewLeadershipTermEvent(1, 0, 0, 0, 0, 0, java.util.concurrent.TimeUnit.MILLISECONDS, 0);
    }

    /**
     * A frame that ends at the record type: version 2 relays it, as a member
     * of version 2 always did, and version 3 drops it as malformed. Both
     * rule sets decide one frame alike at one log index.
     */
    @Test
    void a_frame_that_ends_at_the_record_type_relays_under_version_two_only() {
        final byte[] shortEpoch = IngressFrames.originRecordFrame(IngressFrames.canonicalId(1001), 1, 1, new byte[0]);
        final byte[] shortRecord = IngressFrames.exemptRecordFrame(IngressFrames.canonicalId(7), new byte[0]);
        assertEquals(SealerWire.MIN_ORIGIN_RECORD_LEN, shortEpoch.length);
        assertEquals(SealerWire.MIN_INGRESS_LEN, shortRecord.length);

        final Member v2 = member(Optional.empty());
        assertEquals(List.of(RELAYED), v2.apply(Entry.frame("short-epoch", shortEpoch, 100L)).kinds());
        assertEquals(List.of(RELAYED), v2.apply(Entry.frame("short-record", shortRecord, 200L)).kinds());

        final Member v3 = member(Optional.empty());
        v3.apply(Entry.frame("activation", activation(1, 0), 50L));
        assertEquals(List.of(), v3.apply(Entry.frame("short-epoch", shortEpoch, 100L)).kinds());
        assertEquals(List.of(), v3.apply(Entry.frame("short-record", shortRecord, 200L)).kinds());
    }
}
