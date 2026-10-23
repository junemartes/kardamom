package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.VersionSwitch;
import io.kardamom.sealer.cluster.DriverlessLog.Entry;
import io.kardamom.sealer.cluster.DriverlessLog.Member;
import io.kardamom.sealer.cluster.DriverlessLog.Outcome;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Decision version 5 on driverless members: the carry flag switches
 * version 4 to version 5 at the boundary that seals the first block of its
 * epoch. Under version 5, a full reference (record type 7) is ordered once
 * by the replay window of its image, and goes out as its canonical entry
 * (record type 6). The copy of the twin is skipped. A reference of record
 * type 0 keeps the path of version 4, and the void requests and the
 * recorded cursors drop as unknown kinds.
 */
class SealerVersionFiveTest {

    private static final byte RELAYED = SealerWire.EGRESS_KIND_RELAYED;
    private static final byte BOUNDARY = SealerWire.EGRESS_KIND_BOUNDARY;
    private static final byte RULES = SealerWire.EGRESS_KIND_SEALER_INFO;

    private static final FullRef EARLY = FullRef.of(10, 64);
    private static final FullRef A = FullRef.of(11, 128);
    private static final FullRef C = FullRef.of(13, 192);
    private static final FullRef D = FullRef.of(14, 224);
    private static final FullRef SHORT = FullRef.of(15, 256);
    private static final FullRef UNTAGGED = FullRef.of(16, 288);

    private final DriverlessLog log = new DriverlessLog();

    @AfterEach
    void stop() {
        log.stop();
    }

    private static byte[] epoch(final long origin) {
        return IngressFrames.originRecordFrame(
            IngressFrames.canonicalId(1000 + (int) origin), origin, 1, new byte[] {0x02});
    }

    private static byte[] activation(final long origin, final long feature) {
        return IngressFrames.featureActivationFrame(origin, feature, 0L);
    }

    /** A kind-0 reference of record type 0 of image {@code (lane 0, session 9)} at {@code position}. */
    private static byte[] ref(final int idTag, final long position) {
        final ByteBuffer buf = ByteBuffer.allocate(SealerWire.TXREF_RECORD_LEN).order(ByteOrder.LITTLE_ENDIAN);
        buf.put(SealerWire.KIND_INGRESS_RECORD).put(FullRef.filled(CanonicalSealerState.SENDER_LEN, 0xAA))
            .putLong(0L).putLong(SealerWire.POSITION_TAG | position).putLong(0L).putLong(0L)
            .put(IngressFrames.recordId(idTag)).put(SealerWire.RT_TXREF)
            .put((byte) 0).putInt(0).putInt((int) position).putInt(9);
        return buf.array();
    }

    /**
     * The stream: an early carry flag that version 3 does not take, version
     * 4 at the boundary of epoch 2, a full reference that version 4 relays
     * as is, version 5 at the boundary of epoch 3, then the record path of
     * version 5.
     */
    private static List<Entry> stream() {
        return List.of(
            Entry.frame("epoch-1", epoch(1), 1_000L),
            Entry.tick("tick-1", 1_250L),
            Entry.frame("carry-early", activation(2, VersionSwitch.FEATURE_SEALER_V5), 1_300L),
            Entry.frame("v4-on", activation(2, VersionSwitch.FEATURE_SEALER_V4), 1_400L),
            Entry.frame("epoch-2", epoch(2), 1_500L),
            Entry.tick("tick-2", 2_000L),
            Entry.frame("carry-on", activation(3, VersionSwitch.FEATURE_SEALER_V5), 2_100L),
            Entry.frame("epoch-3", epoch(3), 2_200L),
            Entry.frame("full-in-v4", EARLY.frame(), 2_300L),
            Entry.tick("tick-3", 2_500L),
            Entry.frame("full-a", A.frame(), 2_600L),
            Entry.frame("full-a-copy", A.frame(), 2_650L),
            Entry.frame("ref-b", ref(12, 160), 2_700L),
            Entry.frame("ref-b-copy", ref(12, 160), 2_750L),
            Entry.frame("batch", FullRef.batch(C.frame(), C.frame(), D.frame()), 2_800L),
            Entry.frame("void", IngressFrames.voidRequestFrame(0, 4, A.id()), 2_850L),
            Entry.frame("cursor", IngressFrames.recordedCursorFrame(0, 4), 2_900L),
            Entry.frame("short", SHORT.withoutEnvelope(), 2_950L),
            Entry.frame("untagged", UNTAGGED.untagged(), 2_960L),
            Entry.tick("tick-4", 3_000L));
    }

    @Test
    void the_stream_switches_to_version_five_and_orders_each_full_reference_once() {
        assertEquals(List.of(
            new Outcome("epoch-1", List.of(RELAYED), 3),
            new Outcome("tick-1", List.of(BOUNDARY), 3),
            new Outcome("carry-early", List.of(), 3),
            new Outcome("v4-on", List.of(), 3),
            new Outcome("epoch-2", List.of(RELAYED), 3),
            new Outcome("tick-2", List.of(BOUNDARY, RULES), 4),
            new Outcome("carry-on", List.of(), 4),
            new Outcome("epoch-3", List.of(RELAYED), 4),
            new Outcome("full-in-v4", List.of(RELAYED), 4),
            new Outcome("tick-3", List.of(BOUNDARY, RULES), 5),
            new Outcome("full-a", List.of(RELAYED), 5),
            new Outcome("full-a-copy", List.of(), 5),
            new Outcome("ref-b", List.of(RELAYED), 5),
            new Outcome("ref-b-copy", List.of(), 5),
            new Outcome("batch", List.of(RELAYED, RELAYED), 5),
            new Outcome("void", List.of(), 5),
            new Outcome("cursor", List.of(), 5),
            new Outcome("short", List.of(), 5),
            new Outcome("untagged", List.of(), 5),
            new Outcome("tick-4", List.of(BOUNDARY), 5)), log.member(Optional.empty()).applyAll(stream()));
    }

    /** The relayed frame of a full reference is its canonical entry: the id, record type 6, the lane, the envelope. */
    @Test
    void a_full_reference_goes_out_as_its_canonical_entry() {
        final List<byte[]> relayed = relayedFrames(log.member(Optional.empty()));
        final long first = index(relayed.stream().filter(f -> f[SealerWire.RELAYED_HEADER_LEN] == A.id()[0])
            .findFirst().orElseThrow());
        assertArrayEquals(A.canonicalFrame(first), find(relayed, A));
        assertArrayEquals(C.canonicalFrame(first + 2), find(relayed, C));
        assertArrayEquals(D.canonicalFrame(first + 3), find(relayed, D));
    }

    /** Below version 5, a full reference relays as is, like every other payload. */
    @Test
    void version_four_relays_a_full_reference_as_is() {
        final byte[] frame = find(relayedFrames(log.member(Optional.empty())), EARLY);
        final byte[] ingress = EARLY.frame();
        assertEquals(ingress.length - SealerWire.CANONICAL_ID_OFFSET,
            ByteBuffer.wrap(frame).order(ByteOrder.LITTLE_ENDIAN).getInt(Byte.BYTES + Long.BYTES));
        assertEquals(SealerWire.RT_TXREF_OFFER, frame[SealerWire.RELAYED_HEADER_LEN + 32]);
    }

    @Test
    void the_switch_reports_version_five_to_every_session() {
        final Member member = log.member(Optional.empty());
        member.applyAll(stream());
        final List<byte[]> rules = member.sequencer().offered.stream().filter(f -> f[0] == RULES).toList();
        assertEquals(5, rules.get(rules.size() - 1)[1]);
    }

    /** A member that holds the carry flag writes snapshot version 16. */
    @Test
    void a_member_that_holds_the_carry_flag_writes_snapshot_version_sixteen() {
        final Member member = log.member(Optional.empty());
        member.applyAll(stream().subList(0, 7));
        assertEquals(CanonicalSealerState.CARRY_SNAPSHOT_VERSION,
            ByteBuffer.wrap(member.svc().snapshot()).getInt(Integer.BYTES));
    }

    /** Two members decide alike, and a member with an ordering window relays the same frames. */
    @Test
    void every_member_sends_the_same_frames() {
        final Member one = log.member(Optional.empty());
        final Member two = log.member(Optional.empty());
        final Member windowed = log.member(Optional.empty(), 4);
        one.applyAll(stream());
        two.applyAll(stream());
        windowed.applyAll(stream());
        assertFramesEqual(one.frames(), two.frames());
        assertArrayEquals(one.svc().snapshot(), two.svc().snapshot());
        assertFramesEqual(relayedFrames(one.frames()), relayedFrames(windowed.frames()));
    }

    /**
     * A carrying member writes snapshot version 17. A member that restarts
     * from it at any entry keeps the decision version, the switch and the
     * schedule of the carry flag of a peer that kept running, and decides
     * alike from there.
     */
    @Test
    void a_carrying_member_restored_at_any_entry_decides_as_its_peer(@TempDir final Path dir) {
        final List<Entry> entries = stream();
        IntStream.range(1, entries.size()).forEach(cut -> restartAt(entries, cut, dir.resolve("cut-" + cut)));
    }

    private void restartAt(final List<Entry> entries, final int cut, final Path dir) {
        final Member peer = log.carrying(dir.resolve("peer"), Optional.empty());
        final Member member = log.carrying(dir.resolve("member"), Optional.empty());
        final List<Entry> head = entries.subList(0, cut);
        assertEquals(peer.applyAll(head), member.applyAll(head));
        final byte[] snapshot = member.svc().snapshot();
        assertEquals(CanonicalSealerState.FILE_SNAPSHOT_VERSION, ByteBuffer.wrap(snapshot).getInt(Integer.BYTES));
        final Member restored = log.restart(member, dir.resolve("member"), snapshot);
        assertEquals(CanonicalSealerState.FILE_SNAPSHOT_VERSION, restored.svc().latestSnapshotVersion());
        assertEquals(peer.svc().decisionRules(), restored.svc().decisionRules(), "cut at " + cut);
        final List<Entry> rest = entries.subList(cut, entries.size());
        assertEquals(peer.applyAll(rest), restored.applyAll(rest), "cut at " + cut);
        assertEquals(5, restored.svc().decisionRules().version());
        assertArrayEquals(peer.svc().snapshot(), restored.svc().snapshot(), "cut at " + cut);
    }

    @Test
    void a_replay_from_a_snapshot_at_any_entry_decides_alike() {
        final List<Entry> entries = stream();
        IntStream.range(1, entries.size()).forEach(cut -> replayFrom(entries, cut));
    }

    private void replayFrom(final List<Entry> entries, final int cut) {
        final Member original = log.member(Optional.empty());
        original.applyAll(entries.subList(0, cut));
        final Member restored = log.member(Optional.of(original.svc().snapshot()));
        assertEquals(original.svc().decisionRules(), restored.svc().decisionRules());
        final List<Entry> rest = entries.subList(cut, entries.size());
        assertEquals(original.applyAll(rest), restored.applyAll(rest), "cut at " + cut);
        assertArrayEquals(original.svc().snapshot(), restored.svc().snapshot(), "cut at " + cut);
    }

    /** The relayed frames of the whole stream on {@code member}. */
    private static List<byte[]> relayedFrames(final Member member) {
        member.applyAll(stream());
        return member.frames().stream().filter(f -> f[0] == RELAYED).toList();
    }

    private static List<byte[]> relayedFrames(final List<byte[]> frames) {
        return frames.stream().filter(f -> f[0] == RELAYED).toList();
    }

    /** The relayed frame of {@code ref}: the first id byte names it. */
    private static byte[] find(final List<byte[]> relayed, final FullRef ref) {
        return relayed.stream().filter(f -> f[SealerWire.RELAYED_HEADER_LEN] == ref.id()[0]).findFirst().orElseThrow();
    }

    private static long index(final byte[] relayed) {
        return ByteBuffer.wrap(relayed).order(ByteOrder.LITTLE_ENDIAN).getLong(Byte.BYTES);
    }

    private static void assertFramesEqual(final List<byte[]> expected, final List<byte[]> actual) {
        assertEquals(expected.size(), actual.size());
        IntStream.range(0, expected.size()).forEach(i -> assertArrayEquals(expected.get(i), actual.get(i), "frame " + i));
    }
}
