package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.GenesisFeatures;
import io.kardamom.sealer.SealerSeed;
import io.kardamom.sealer.VersionSwitch;
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
 * Members that start from a seed with the decision rules: they run the
 * rules of the seed from the first entry, write the snapshot that holds
 * them, and a member restored from that snapshot keeps them. A seed with
 * no rules starts version 3, as before. A member with the retained file
 * drops the carry window of a seed.
 */
class SealerSeedRulesTest {

    private static final byte REFUSED = SealerWire.EGRESS_KIND_REF_REFUSED;
    private static final byte RELAYED = SealerWire.EGRESS_KIND_RELAYED;

    /** The replay window of every member, in frames. */
    private static final int WINDOW = 4;
    /** The time of the seed head. */
    private static final long T = 1_700_000_000_000L;
    /** A sender that the seed does not hold. */
    private static final byte[] SENDER = FullRef.filled(CanonicalSealerState.SENDER_LEN, 0xBB);

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

    /** The seed of a chain that runs features 3, 4 and 5 from its genesis. */
    private static SealerSeed carryBorn() {
        return SealerSeedServiceTest.seed(VersionSwitch.genesis(GenesisFeatures.CARRY_WINDOW));
    }

    /** The epoch after the seed head, a full reference, a reference of record type 0, and two ticks. */
    private static List<Entry> stream() {
        return List.of(
            Entry.frame("epoch", IngressFrames.originRecordFrame(
                IngressFrames.canonicalId(2000), 78, 1, new byte[] {0x02}), T + 1_050L),
            Entry.frame("full", FullRef.of(30, 96L).frame(), T + 1_100L),
            Entry.tick("tick-1", T + 1_250L),
            Entry.frame("ref", IngressFrames.txRefFrame(31, SENDER, 0, 128L), T + 1_300L),
            Entry.tick("tick-2", T + 1_500L));
    }

    private static int version(final byte[] snapshot) {
        return ByteBuffer.wrap(snapshot).getInt(Integer.BYTES);
    }

    private static Outcome named(final List<Outcome> outcomes, final String name) {
        return outcomes.stream().filter(o -> o.name().equals(name)).findFirst().orElseThrow();
    }

    @Test
    void a_seed_with_the_rules_starts_them() {
        final Member member = log.seeded(Optional.empty(), carryBorn());
        assertEquals(5, member.svc().decisionRules().version());
        assertTrue(member.svc().metrics().carry().carryWindow());
        assertEquals(CanonicalSealerState.WINDOW_SNAPSHOT_VERSION, version(member.svc().snapshot()));
        final List<Outcome> outcomes = member.applyAll(stream());
        assertEquals(List.of(REFUSED), named(outcomes, "ref").kinds());
        assertTrue(outcomes.stream().allMatch(o -> o.decisionVersion() == 5));
    }

    /** Three members from one seed: after each entry, their frames and snapshots are equal. */
    @Test
    void every_seeded_member_has_the_same_bytes_after_each_entry() {
        final SealerSeed seed = carryBorn();
        final List<Member> members = List.of(
            log.seeded(Optional.empty(), seed), log.seeded(Optional.empty(), seed), log.seeded(Optional.empty(), seed));
        stream().forEach(entry -> {
            final List<Outcome> outcomes = members.stream().map(m -> m.apply(entry)).toList();
            final byte[] snapshot = members.get(0).svc().snapshot();
            members.forEach(m -> assertArrayEquals(snapshot, m.svc().snapshot(), entry.name()));
            assertEquals(1, outcomes.stream().distinct().count(), entry.name());
        });
        assertEquals(5, seed.rules().version(), "the seed keeps its rules");
    }

    /** A member restored from the snapshot of a seeded member at any entry ends with the bytes of its peer. */
    @Test
    void a_member_restored_from_a_seeded_snapshot_keeps_the_rules() {
        final List<Entry> entries = stream();
        IntStream.range(0, entries.size()).forEach(cut -> {
            final Member original = log.seeded(Optional.empty(), carryBorn());
            original.applyAll(entries.subList(0, cut));
            final Member restored = log.member(Optional.of(original.svc().snapshot()));
            assertEquals(5, restored.svc().decisionRules().version(), "cut at " + cut);
            final List<Entry> rest = entries.subList(cut, entries.size());
            assertEquals(original.applyAll(rest), restored.applyAll(rest), "cut at " + cut);
            assertArrayEquals(original.svc().snapshot(), restored.svc().snapshot(), "cut at " + cut);
        });
    }

    /** A seed with no rules, as an older writer writes it, starts version 3 with no activation. */
    @Test
    void a_seed_with_no_rules_starts_version_three() {
        final Member member = log.seeded(Optional.empty(), SealerSeedServiceTest.seed());
        assertEquals(3, member.svc().decisionRules().version());
        assertFalse(member.svc().metrics().carry().carryWindow());
        assertEquals(14, version(member.svc().snapshot()));
    }

    /** A member with the retained file holds no carry window: it runs version 5 without its rules. */
    @Test
    void a_file_member_drops_the_carry_window_of_a_seed() {
        final Member member = log.carryingSeeded(Optional.empty(), dir.resolve("file"), carryBorn());
        assertEquals(5, member.svc().decisionRules().version());
        assertFalse(member.svc().metrics().carry().carryWindow());
        assertEquals(CanonicalSealerState.FILE_SNAPSHOT_VERSION, version(member.svc().snapshot()));
        assertEquals(List.of(RELAYED), named(member.applyAll(stream()), "ref").kinds());
    }
}
