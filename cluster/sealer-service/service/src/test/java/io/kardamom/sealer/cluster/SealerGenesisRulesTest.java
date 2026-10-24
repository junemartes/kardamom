package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.GenesisFeatures;
import io.kardamom.sealer.VersionSwitch;
import io.kardamom.sealer.cluster.ClusterStubs.StubSession;
import io.kardamom.sealer.cluster.DriverlessLog.Entry;
import io.kardamom.sealer.cluster.DriverlessLog.Member;
import io.kardamom.sealer.cluster.DriverlessLog.Outcome;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Members that start at genesis with features 3, 4 and 5: version 5 and
 * the carry window from the first entry, with no upgrade transaction. A
 * later activation turns the carry window off and on again. Every member
 * sends the same frames and writes the same snapshot, version 18, at every
 * entry, and a member restored at any entry, without the setting, ends
 * with the bytes of its peer.
 */
class SealerGenesisRulesTest {

    private static final byte RELAYED = SealerWire.EGRESS_KIND_RELAYED;
    private static final byte BOUNDARY = SealerWire.EGRESS_KIND_BOUNDARY;
    private static final byte REFUSED = SealerWire.EGRESS_KIND_REF_REFUSED;

    /** The replay window of every member, in frames. */
    private static final int WINDOW = 4;
    /** The sender of every reference of record type 0 of the tests. */
    private static final byte[] SENDER = FullRef.filled(CanonicalSealerState.SENDER_LEN, 0xAA);
    /** The time of the activation that turns the carry window on again. */
    private static final long AGAIN_MS = 2_500L;

    private final DriverlessLog log = new DriverlessLog();

    @TempDir
    Path dir;

    @BeforeEach
    void window() {
        System.setProperty("kardamom.cluster.retention", Integer.toString(WINDOW));
    }

    @AfterEach
    void stop() {
        log.stop();
        System.clearProperty("kardamom.cluster.retention");
    }

    private static Entry epoch(final long origin, final long ms) {
        return Entry.frame("epoch-" + origin, IngressFrames.originRecordFrame(
            IngressFrames.canonicalId(1000 + (int) origin), origin, 1, new byte[] {0x02}), ms);
    }

    private static Entry ref(final String name, final int tag, final long ms) {
        return Entry.frame(name, IngressFrames.txRefFrame(tag, SENDER, tag, 64L + 32L * tag), ms);
    }

    private static Entry full(final String name, final int tag, final long ms) {
        return Entry.frame(name, FullRef.of(tag, 64L + 32L * tag).frame(), ms);
    }

    /**
     * Version 5 with the carry window from genesis; feature 5 at epoch 2
     * with a time ahead turns it off at the boundary of that epoch, and on
     * again at its time.
     */
    private static List<Entry> stream() {
        return List.of(
            epoch(1, 1_050L),
            full("full-1", 20, 1_100L),
            Entry.tick("tick-1", 1_250L),
            ref("ref-refused", 21, 1_300L),
            full("full-2", 22, 1_350L),
            Entry.tick("tick-2", 1_500L),
            Entry.frame("off-activation",
                IngressFrames.featureActivationFrame(2, VersionSwitch.FEATURE_CARRY_WINDOW, AGAIN_MS), 1_550L),
            epoch(2, 1_600L),
            ref("ref-in-deposit-block", 23, 1_650L),
            Entry.tick("tick-off", 1_750L),
            ref("ref-ordered", 24, 1_800L),
            Entry.tick("tick-4", 2_000L),
            Entry.tick("tick-5", 2_250L),
            Entry.tick("tick-again", AGAIN_MS),
            ref("ref-refused-again", 25, 2_550L),
            full("full-3", 26, 2_600L),
            Entry.tick("tick-end", 2_750L));
    }

    private static Outcome named(final List<Outcome> outcomes, final String name) {
        return outcomes.stream().filter(o -> o.name().equals(name)).findFirst().orElseThrow();
    }

    private static int version(final byte[] snapshot) {
        return ByteBuffer.wrap(snapshot).getInt(Integer.BYTES);
    }

    private Member genesis() {
        return log.genesis(Optional.empty(), GenesisFeatures.CARRY_WINDOW);
    }

    @Test
    void a_genesis_member_runs_version_five_with_the_carry_window_before_the_first_entry() {
        final Member member = genesis();
        assertEquals(5, member.svc().decisionRules().version());
        assertTrue(member.svc().metrics().carry().carryWindow());
        assertEquals(CanonicalSealerState.WINDOW_SNAPSHOT_VERSION, version(member.svc().snapshot()));
        assertEquals(2.0, PromText.value(member.metrics(), "kardamom_sealer_feature_state", "feature=\"5\""));
        final StubSession late = member.cluster().addSession(2);
        member.svc().onSessionOpen(late, 0);
        final byte[] info = late.offered.get(0);
        assertEquals(SealerWire.EGRESS_KIND_SEALER_INFO, info[0]);
        assertEquals(5, info[1], "the first sealer info says version 5");
    }

    @Test
    void the_carry_window_refuses_type_zero_until_a_later_activation_turns_it_off() {
        final List<Outcome> outcomes = genesis().applyAll(stream());
        assertEquals(List.of(REFUSED), named(outcomes, "ref-refused").kinds());
        assertEquals(List.of(RELAYED), named(outcomes, "full-2").kinds());
        assertEquals(List.of(REFUSED), named(outcomes, "ref-in-deposit-block").kinds());
        assertEquals(List.of(BOUNDARY), named(outcomes, "tick-off").kinds());
        assertEquals(List.of(RELAYED), named(outcomes, "ref-ordered").kinds(), "off: type 0 is ordered");
        assertEquals(List.of(REFUSED), named(outcomes, "ref-refused-again").kinds(), "on again at its time");
        assertTrue(outcomes.stream().allMatch(o -> o.decisionVersion() == 5));
    }

    /** The setting decides: a member at genesis without it orders the first type-0 reference. */
    @Test
    void a_member_without_the_setting_decides_otherwise() {
        final List<Outcome> plain = log.member(Optional.empty()).applyAll(stream());
        assertEquals(List.of(RELAYED), named(plain, "ref-refused").kinds());
        assertNotEquals(genesis().applyAll(stream()), plain);
    }

    /** Three members apply the log from genesis: after each entry, their frames and snapshots are equal. */
    @Test
    void every_member_has_the_same_bytes_after_each_entry() {
        final List<Member> members = List.of(genesis(), genesis(), genesis());
        stream().forEach(entry -> {
            final List<Outcome> outcomes = members.stream().map(m -> m.apply(entry)).toList();
            final byte[] snapshot = members.get(0).svc().snapshot();
            assertEquals(CanonicalSealerState.WINDOW_SNAPSHOT_VERSION, version(snapshot), entry.name());
            members.forEach(m -> assertArrayEquals(snapshot, m.svc().snapshot(), entry.name()));
            assertEquals(1, outcomes.stream().distinct().count(), entry.name());
        });
    }

    /**
     * A member restored from a snapshot at any entry, in block 1 or in a
     * later block, ends with the bytes of its peer and of a member that
     * replays the log from genesis. The restored member has no setting:
     * the snapshot carries the rules.
     */
    @Test
    void a_member_restored_at_any_entry_ends_with_the_bytes_of_its_peer() {
        final List<Entry> entries = stream();
        final Member replay = genesis();
        replay.applyAll(entries);
        final byte[] end = replay.svc().snapshot();
        IntStream.range(0, entries.size()).forEach(cut -> {
            final String where = "cut at " + cut;
            final Member original = genesis();
            original.applyAll(entries.subList(0, cut));
            final Member restored = log.member(Optional.of(original.svc().snapshot()));
            assertEquals(original.svc().decisionRules(), restored.svc().decisionRules(), where);
            assertEquals(original.svc().metrics().carry(), restored.svc().metrics().carry(), where);
            final List<Entry> rest = entries.subList(cut, entries.size());
            assertEquals(original.applyAll(rest), restored.applyAll(rest), where);
            assertArrayEquals(end, original.svc().snapshot(), where);
            assertArrayEquals(end, restored.svc().snapshot(), where);
        });
    }

    /** A member with the retained file runs features 3 and 4 from genesis, and its snapshot keeps version 5. */
    @Test
    void a_file_member_keeps_version_five_through_its_snapshot() {
        final Member member = log.carryingGenesis(Optional.empty(), dir.resolve("file"), GenesisFeatures.SEALER_V5);
        assertEquals(5, member.svc().decisionRules().version());
        assertFalse(member.svc().metrics().carry().carryWindow());
        member.applyAll(stream().subList(0, 3));
        final byte[] snapshot = member.svc().snapshot();
        assertEquals(CanonicalSealerState.FILE_SNAPSHOT_VERSION, version(snapshot));
        final Member restored = log.restart(member, dir.resolve("file"), snapshot);
        assertEquals(5, restored.svc().decisionRules().version());
        assertArrayEquals(snapshot, restored.svc().snapshot());
    }
}
