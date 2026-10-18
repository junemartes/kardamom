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
 * The switch of the decision rules on driverless members. A feature
 * activation of {@link DecisionRules#FEATURE_SEALER_V3} only stores its
 * schedule. The rules switch at a block boundary: once the first block of
 * the epoch of the activation is sealed, a boundary at or after its time
 * sets version 3 for the next block, and a boundary before it sets version
 * 2. So a later activation with a time ahead switches the rules off at the
 * boundary that seals its epoch, and on again at its time. A replay from a
 * snapshot at any point of the log switches at the same boundaries and
 * decides alike.
 */
class SealerDecisionRulesTest {

    private static final int CAPACITY = 16;
    private static final byte[] SENDER = filled(CanonicalSealerState.SENDER_LEN, 0xAA);
    /** The activation time of the golden stream. */
    private static final long ACTIVATION_MS = 10_000L;
    /** The time of the activation that switches the rules off: they switch on again at it. */
    private static final long ON_AGAIN_MS = 30_000L;

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

    private static byte[] activation(final long origin, final long activationMs) {
        return IngressFrames.featureActivationFrame(origin, DecisionRules.FEATURE_SEALER_V3, activationMs);
    }

    /**
     * The golden stream. The sender orders nonces 0 and 1, and a new
     * transaction at nonce 1 gets a contiguity reject. The activation of
     * epoch 2 comes in front of the epoch. Nonce 2 comes before the
     * activation is due, and the first tick at its time switches the rules.
     * After it, the guard decides as before: a new transaction at nonce 1
     * gets a contiguity reject, and nonce 3 gets in.
     */
    private static List<Entry> goldenStream(final boolean withActivation) {
        final List<Entry> log = new ArrayList<>(List.of(
            Entry.frame("epoch-1", epoch(1), 1_000L),
            Entry.frame("nonce-0", offer(1, 0), 1_100L),
            Entry.frame("nonce-1", offer(2, 1), 1_200L),
            Entry.tick("tick-1", 1_250L),
            Entry.frame("retry-v2", offer(3, 1), 1_400L)));
        if (withActivation) {
            log.add(Entry.frame("activation", activation(2, ACTIVATION_MS), 1_500L));
        }
        log.addAll(List.of(
            Entry.frame("epoch-2", epoch(2), 1_600L),
            Entry.frame("nonce-2-early", offer(4, 2), 5_000L),
            Entry.tick("tick-at-activation", ACTIVATION_MS),
            Entry.frame("retry-v3", offer(5, 1), 10_100L),
            Entry.frame("nonce-3", offer(6, 3), 10_200L),
            Entry.tick("tick-2", 10_250L)));
        return log;
    }

    /**
     * The golden stream, then an activation of epoch 3 with a time ahead.
     * The record and the block of the deposit keep version 3: a frame that
     * ends at the record type drops as malformed. The tick that seals the
     * epoch switches the rules off: the same shape relays, as under
     * version 2. The first tick at the time switches them on again. The
     * guard decides alike in both phases: nonces 4 and 5 get in.
     */
    private static List<Entry> onOffOnStream() {
        final List<Entry> log = goldenStream(true);
        log.addAll(List.of(
            Entry.frame("off", activation(3, ON_AGAIN_MS), 10_300L),
            Entry.frame("epoch-3", epoch(3), 10_400L),
            Entry.frame("short-deposit-block", shortRecord(69), 10_450L),
            Entry.frame("nonce-4", offer(7, 4), 10_500L),
            Entry.tick("tick-3", 10_600L),
            Entry.frame("short-off", shortRecord(70), 10_700L),
            Entry.tick("tick-4", 10_800L),
            Entry.tick("tick-at-on-again", ON_AGAIN_MS),
            Entry.frame("short-on-again", shortRecord(71), 30_100L),
            Entry.frame("nonce-5", offer(8, 5), 30_200L),
            Entry.tick("tick-5", 30_250L)));
        return log;
    }

    /** An exempt record frame that ends at the record type. */
    private static byte[] shortRecord(final int n) {
        return IngressFrames.exemptRecordFrame(IngressFrames.canonicalId(n), new byte[0]);
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
            outcome("retry-v2", List.of(REJECT), 2),
            outcome("activation", List.of(), 2),
            outcome("epoch-2", List.of(RELAYED), 2),
            outcome("nonce-2-early", List.of(RELAYED), 2),
            outcome("tick-at-activation", List.of(BOUNDARY, RULES), 3),
            outcome("retry-v3", List.of(REJECT), 3),
            outcome("nonce-3", List.of(RELAYED), 3),
            outcome("tick-2", List.of(BOUNDARY), 3)), outcomes);
    }

    @Test
    void a_later_activation_switches_the_rules_off_when_its_epoch_is_sealed_and_on_again_at_its_time() {
        final List<Outcome> outcomes = member(Optional.empty()).applyAll(onOffOnStream());
        assertEquals(List.of(
            outcome("off", List.of(), 3),
            outcome("epoch-3", List.of(RELAYED), 3),
            outcome("short-deposit-block", List.of(), 3),
            outcome("nonce-4", List.of(RELAYED), 3),
            outcome("tick-3", List.of(BOUNDARY, RULES), 2),
            outcome("short-off", List.of(RELAYED), 2),
            outcome("tick-4", List.of(BOUNDARY), 2),
            outcome("tick-at-on-again", List.of(BOUNDARY, RULES), 3),
            outcome("short-on-again", List.of(), 3),
            outcome("nonce-5", List.of(RELAYED), 3),
            outcome("tick-5", List.of(BOUNDARY), 3)), outcomes.subList(goldenStream(true).size(), outcomes.size()));
    }

    @Test
    void each_switch_tells_the_session_the_new_version_and_asks_for_a_snapshot() {
        final Member member = member(Optional.empty());
        member.applyAll(onOffOnStream());
        final List<Byte> reported = member.sequencer().offered.stream()
            .filter(f -> f[0] == RULES).map(f -> f[1]).toList();
        assertEquals(List.of((byte) 3, (byte) 2, (byte) 3), reported, "on, off, on");
        assertEquals(5, notices(member), "one for each stored record and one for each switch");
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
        assertArrayEquals(plain.svc().snapshot(), snapshot, "the same log writes the same snapshot");
        assertTrue(activated.sequencer().offered.stream().noneMatch(f -> f[0] == RULES),
            "no rules frame under version 2");
    }

    /** The guard decides alike on both versions: only the rules frame differs. */
    @Test
    void a_member_of_version_two_never_switches_without_the_activation() {
        final List<Outcome> outcomes = member(Optional.empty()).applyAll(goldenStream(false));
        assertTrue(outcomes.stream().allMatch(o -> o.decisionVersion() == 2));
        final List<Outcome> switched = member(Optional.empty()).applyAll(goldenStream(true));
        assertEquals(
            switched.stream().filter(o -> !o.name().equals("activation"))
                .map(o -> o.kinds().stream().filter(kind -> kind != RULES).toList()).toList(),
            outcomes.stream().map(Outcome::kinds).toList());
    }

    /**
     * Run {@code log} up to {@code cut}, take the snapshot, restore a new
     * member from it, and run the rest on both. The outcomes and the last
     * snapshot match.
     */
    private void replayFrom(final List<Entry> log, final int cut, final int snapshotVersion) {
        final Member original = member(Optional.empty());
        original.applyAll(log.subList(0, cut));
        final byte[] snapshot = original.svc().snapshot();
        assertEquals(snapshotVersion, ByteBuffer.wrap(snapshot).getInt(Integer.BYTES));
        final Member restored = member(Optional.of(snapshot));
        assertEquals(original.svc().decisionRules(), restored.svc().decisionRules());
        final List<Entry> rest = log.subList(cut, log.size());
        assertEquals(original.applyAll(rest), restored.applyAll(rest));
        assertArrayEquals(original.svc().snapshot(), restored.svc().snapshot());
    }

    @Test
    void a_replay_from_a_snapshot_before_the_activation_switches_at_the_same_entry() {
        replayFrom(goldenStream(true), 5, CanonicalSealerState.snapshotWriteVersion());
    }

    @Test
    void a_replay_from_a_snapshot_with_a_pending_activation_switches_at_the_same_entry() {
        replayFrom(goldenStream(true), 7, CanonicalSealerState.RULES_SNAPSHOT_VERSION);
    }

    @Test
    void a_replay_from_a_snapshot_after_the_switch_keeps_version_three() {
        replayFrom(goldenStream(true), 9, CanonicalSealerState.RULES_SNAPSHOT_VERSION);
    }

    @Test
    void a_replay_from_a_snapshot_after_the_switch_off_switches_on_at_the_same_entry() {
        final List<Entry> log = onOffOnStream();
        replayFrom(log, indexOf(log, "epoch-3"), CanonicalSealerState.RULES_SNAPSHOT_VERSION);
        replayFrom(log, indexOf(log, "short-off"), CanonicalSealerState.RULES_SNAPSHOT_VERSION);
        replayFrom(log, indexOf(log, "tick-at-on-again"), CanonicalSealerState.RULES_SNAPSHOT_VERSION);
    }

    @Test
    void a_replay_from_a_snapshot_after_the_switch_on_again_keeps_version_three() {
        final List<Entry> log = onOffOnStream();
        replayFrom(log, indexOf(log, "short-on-again"), CanonicalSealerState.RULES_SNAPSHOT_VERSION);
    }

    private static int indexOf(final List<Entry> log, final String name) {
        return IntStream.range(0, log.size()).filter(i -> log.get(i).name().equals(name)).findFirst().orElseThrow();
    }

    /**
     * Close the open block at {@code ms}, store the activation of epoch
     * {@code origin} at {@code time}, order the epoch, and seal its first
     * block. The first block of the epoch is then not block 1, which
     * executes with time 0.
     */
    private static void activateAndSeal(final Member member, final long origin, final long time, final long ms) {
        member.apply(Entry.tick("tick", ms));
        member.apply(Entry.frame("activation", activation(origin, time), ms + 10));
        member.apply(Entry.frame("epoch", epoch(origin), ms + 20));
        member.apply(Entry.tick("tick", ms + 500));
    }

    private static void switchOn(final Member member, final long origin, final long ms) {
        activateAndSeal(member, origin, 0, ms);
    }

    private static void switchOff(final Member member, final long origin, final long ms) {
        activateAndSeal(member, origin, -1L, ms);
    }

    @Test
    void a_session_gets_the_rules_frame_once_the_state_holds_an_activation() {
        final Member member = member(Optional.empty());
        final StubSession early = member.cluster().addSession(7);
        member.svc().onSessionOpen(early, 0);
        assertTrue(early.offered.isEmpty(), "a state with no activation sends no rules frame");
        switchOn(member, 1, 100L);
        assertArrayEquals(new byte[] {RULES, 3, 7, 0, 0, 0, 0, 0, 0, 0},
            early.offered.stream().filter(f -> f[0] == RULES).findFirst().orElseThrow(),
            "an open session gets the rules frame at the switch");
        final StubSession late = member.cluster().addSession(8);
        member.svc().onSessionOpen(late, 500L);
        assertArrayEquals(new byte[] {RULES, 3, 8, 0, 0, 0, 0, 0, 0, 0}, late.offered.get(0));
        switchOff(member, 2, 1_000L);
        final StubSession afterOff = member.cluster().addSession(9);
        member.svc().onSessionOpen(afterOff, 1_500L);
        assertArrayEquals(new byte[] {RULES, 2, 9, 0, 0, 0, 0, 0, 0, 0}, afterOff.offered.get(0),
            "after the switch off, a session that opens gets version 2");
    }

    @Test
    void a_stored_activation_and_the_switch_each_ask_for_a_snapshot() {
        final Member member = member(Optional.empty());
        member.apply(Entry.frame("epoch-1", epoch(1), 1_000L));
        member.apply(Entry.tick("tick", 1_250L));
        member.apply(Entry.frame("activation", activation(2, 0), 1_500L));
        assertEquals(1, notices(member), "a stored record asks for a snapshot");
        assertEquals(new DecisionRules.View(2, 2L, 0L), member.svc().decisionRules());
        member.apply(Entry.frame("epoch-2", epoch(2), 1_600L));
        member.apply(Entry.tick("tick", 2_000L));
        assertEquals(2, notices(member), "the boundary that switches asks for a snapshot");
        assertEquals(3, member.svc().decisionRules().version());
    }

    /**
     * An origin record opens a block. When it forces the boundary that
     * switches the rules, its shape follows the rules of the block that it
     * opens: one version holds for each block.
     */
    @Test
    void an_origin_record_that_forces_a_switch_is_checked_under_the_rules_of_its_block() {
        final Member member = member(Optional.empty());
        member.apply(Entry.frame("epoch-1", epoch(1), 1_000L));
        member.apply(Entry.tick("tick-1", 1_250L));
        member.apply(Entry.frame("activation", activation(2, 0), 1_300L));
        member.apply(Entry.frame("epoch-2", epoch(2), 1_400L));
        member.apply(Entry.frame("nonce-0", offer(1, 0), 1_500L));
        final byte[] shortNextEpoch =
            IngressFrames.originRecordFrame(IngressFrames.canonicalId(1003), 3, 1, new byte[0]);
        assertEquals(outcome("short-epoch-3", List.of(), 2),
            member.apply(Entry.frame("short-epoch-3", shortNextEpoch, 1_750L)),
            "its forced boundary would set version 3, which needs the record type");
        assertEquals(outcome("epoch-3", List.of(BOUNDARY, RELAYED, RULES), 3),
            member.apply(Entry.frame("epoch-3", epoch(3), 1_800L)));
    }

    private static long notices(final Member member) {
        return member.sequencer().offered.stream().filter(f -> f[0] == SealerWire.EGRESS_KIND_STATUS
                && f.length > SealerWire.STATUS_SNAPSHOT_FLAG_OFFSET && f[SealerWire.STATUS_SNAPSHOT_FLAG_OFFSET] == 1)
            .count();
    }

    @Test
    void a_new_term_offers_the_rules_frame_again_once_the_state_holds_an_activation() {
        final Member member = member(Optional.empty());
        newTerm(member);
        assertTrue(member.sequencer().offered.stream().noneMatch(f -> f[0] == RULES), "version 2 sends none");
        switchOn(member, 1, 100L);
        final long before = member.sequencer().offered.stream().filter(f -> f[0] == RULES).count();
        newTerm(member);
        assertEquals(before + 1, member.sequencer().offered.stream().filter(f -> f[0] == RULES).count(),
            "a frame that a leader change lost goes out again");
        switchOff(member, 2, 1_000L);
        newTerm(member);
        final List<byte[]> frames = member.sequencer().offered.stream().filter(f -> f[0] == RULES).toList();
        assertEquals(2, frames.get(frames.size() - 1)[1], "the frame of the switch off goes out again");
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
        switchOn(v3, 1, 0L);
        assertEquals(3, v3.svc().decisionRules().version());
        final byte[] shortNextEpoch =
            IngressFrames.originRecordFrame(IngressFrames.canonicalId(1002), 2, 1, new byte[0]);
        assertEquals(List.of(), v3.apply(Entry.frame("short-epoch", shortNextEpoch, 300L)).kinds());
        assertEquals(List.of(), v3.apply(Entry.frame("short-record", shortRecord, 400L)).kinds());
    }
}
