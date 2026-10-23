package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.kardamom.sealer.LagBudgets;
import io.kardamom.sealer.VersionSwitch;
import io.kardamom.sealer.cluster.DriverlessLog.Entry;
import io.kardamom.sealer.cluster.DriverlessLog.Member;
import io.kardamom.sealer.cluster.DriverlessLog.Outcome;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The replay window under decision version 5 on driverless members. Below
 * version 5, a frame past the window stays while its block is above the
 * posted head. From the boundary that switches to version 5, a frame past
 * the window leaves at once, at any posted head, so the retained frames
 * have a fixed bound at any DA lag. The DA-lag guard still refuses, and a
 * replay below the window gets REPLAY_UNAVAILABLE. A replay of the log and
 * a restore from a snapshot at any entry evict the same frames.
 *
 * <p>The window-only rule needs a member that carries the bytes: only
 * then does the member feed run, so every reader below the window can
 * recover from the egress. A member that keeps its frames on the heap
 * keeps the posted rule under version 5 too.</p>
 */
class SealerCarryWindowTest {

    private static final byte RELAYED = SealerWire.EGRESS_KIND_RELAYED;
    /** What a member that carries the bytes sends a session in place of a relayed full entry. */
    private static final byte RELAYED_REF = SealerWire.EGRESS_KIND_RELAYED_REF;
    private static final byte BOUNDARY = SealerWire.EGRESS_KIND_BOUNDARY;

    /** The replay window of every member, in frames. */
    private static final int WINDOW = 4;
    /** The blocks of the DA lag: one full reference each, and the batcher posts none. */
    private static final int LAG_BLOCKS = 12;
    /** The records and the open block at the head: three epochs and a full reference, then one per lag block. */
    private static final long HEAD = 4 + LAG_BLOCKS;
    /** The offset of the retained frame count in the status frame. */
    private static final int RETAINED_FRAMES_OFFSET = 26;
    /** The offset of the posted head in the status frame. */
    private static final int POSTED_HEAD_OFFSET = 1;

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

    private static byte[] epoch(final long origin) {
        return IngressFrames.originRecordFrame(
            IngressFrames.canonicalId(1000 + (int) origin), origin, 1, new byte[] {0x02});
    }

    private static byte[] activation(final long origin, final long feature) {
        return IngressFrames.featureActivationFrame(origin, feature, 0L);
    }

    /**
     * Version 4 at the boundary of epoch 2, then a full reference. With
     * {@code carry}, the carry flag switches version 5 on at the boundary
     * of epoch 3 ({@code tick-3}).
     */
    private static Stream<Entry> upgrade(final boolean carry) {
        return Stream.of(
                Entry.frame("epoch-1", epoch(1), 1_000L),
                Entry.tick("tick-1", 1_250L),
                Entry.frame("v4-on", activation(2, VersionSwitch.FEATURE_SEALER_V4), 1_400L),
                Entry.frame("epoch-2", epoch(2), 1_500L),
                Entry.tick("tick-2", 2_000L),
                Entry.frame("carry-on", activation(3, VersionSwitch.FEATURE_SEALER_V5), 2_100L),
                Entry.frame("epoch-3", epoch(3), 2_200L),
                Entry.frame("full-in-v4", FullRef.of(10, 64).frame(), 2_300L),
                Entry.tick("tick-3", 2_500L))
            .filter(entry -> carry || !entry.name().equals("carry-on"));
    }

    /**
     * {@link #LAG_BLOCKS} blocks of one reference each: a full reference
     * with {@code carry}, else a reference of record type 0, which version
     * 4 orders by its replay window. No posted cursor comes.
     */
    private static Stream<Entry> lag(final boolean carry) {
        return IntStream.range(0, LAG_BLOCKS).boxed().flatMap(k -> Stream.of(
            Entry.frame("ref-" + k, lagRef(carry, 20 + k, 96 + 32L * k), 2_600L + 250L * k),
            Entry.tick("lag-tick-" + k, 2_750L + 250L * k)));
    }

    private static byte[] lagRef(final boolean carry, final int idTag, final long position) {
        return carry
            ? FullRef.of(idTag, position).frame()
            : IngressFrames.txRefFrame(idTag, FullRef.SENDER, 0L, position);
    }

    /** A replay from genesis, and a replay from the last record, inside the window. */
    private static Stream<Entry> replays() {
        return Stream.of(
            Entry.frame("replay-genesis", IngressFrames.replayRequestFrame(0, 1), 6_000L),
            Entry.frame("replay-inside", IngressFrames.replayRequestFrame(HEAD - 1, HEAD - 1), 6_100L));
    }

    private static List<Entry> stream(final boolean carry) {
        return Stream.of(upgrade(carry), lag(carry), replays()).flatMap(s -> s).toList();
    }

    private static Outcome named(final List<Outcome> outcomes, final String name) {
        return outcomes.stream().filter(o -> o.name().equals(name)).findFirst().orElseThrow();
    }

    /** The status frames that the session of {@code member} got, in order. */
    private static List<ByteBuffer> statuses(final Member member) {
        return member.sequencer().offered.stream()
            .filter(f -> f[0] == SealerWire.EGRESS_KIND_STATUS)
            .map(f -> ByteBuffer.wrap(f).order(ByteOrder.LITTLE_ENDIAN))
            .toList();
    }

    private static ByteBuffer lastStatus(final Member member) {
        final List<ByteBuffer> all = statuses(member);
        return all.get(all.size() - 1);
    }

    private static long retained(final ByteBuffer status) {
        return status.getLong(RETAINED_FRAMES_OFFSET);
    }

    private static long retained(final Member member) {
        return retained(lastStatus(member));
    }

    /** A member that carries the bytes, with its retained file under {@code name}, at genesis. */
    private Member carrying(final String name) {
        return log.carrying(Optional.empty(), dir.resolve(name), LagBudgets.DEFAULT);
    }

    @Test
    void below_version_five_the_frames_above_the_posted_head_stay() {
        final Member member = log.member(Optional.empty());
        final List<Outcome> outcomes = member.applyAll(stream(false));
        assertEquals(VersionSwitch.ON_VERSION, outcomes.get(outcomes.size() - 1).decisionVersion());
        assertEquals(0L, lastStatus(member).getLong(POSTED_HEAD_OFFSET));
        assertEquals(2 * HEAD - 1, retained(member), "every record and every boundary stays");
        final List<Byte> genesis = named(outcomes, "replay-genesis").kinds();
        assertEquals(HEAD, Collections.frequency(genesis, RELAYED), "every record from genesis");
        assertEquals(HEAD - 1, Collections.frequency(genesis, BOUNDARY));
        assertEquals(SealerWire.EGRESS_KIND_REPLAY_DONE, genesis.get(genesis.size() - 1));
    }

    @Test
    void version_five_keeps_only_the_window_at_any_posted_head() {
        final Member member = carrying("window");
        final List<Entry> upgrade = upgrade(true).toList();
        // The last entry of the upgrade is the boundary that switches to version 5.
        final int switchAt = upgrade.size() - 1;
        member.applyAll(upgrade.subList(0, switchAt));
        final int before = statuses(member).size();
        member.applyAll(Stream.concat(Stream.of(upgrade.get(switchAt)), lag(true)).toList());
        assertEquals(VersionSwitch.CARRY_VERSION, member.svc().decisionRules().version());
        assertEquals(0L, lastStatus(member).getLong(POSTED_HEAD_OFFSET), "the batcher posted nothing");
        final List<Long> after = statuses(member).stream().skip(before).map(SealerCarryWindowTest::retained)
            .toList();
        // The status of the switch tick leaves before the end of its entry,
        // under the old rule: the four records and three boundaries of
        // blocks 1 to 3 stay above the posted head.
        assertEquals(7L, after.get(0), "version 4 keeps the unposted frames");
        // The snapshot notice at the end of the entry, and the status of
        // each tick after it, show the window.
        assertEquals(Collections.nCopies(1 + LAG_BLOCKS, (long) WINDOW), after.subList(1, after.size()),
            "the switch drops the frames past the window, and each frame after it leaves at once");
    }

    @Test
    void version_five_refuses_a_replay_below_the_window_and_serves_one_inside_it() {
        final List<Outcome> outcomes = carrying("replay").applyAll(stream(true));
        assertEquals(List.of(SealerWire.EGRESS_KIND_REPLAY_UNAVAILABLE), named(outcomes, "replay-genesis").kinds());
        assertEquals(List.of(RELAYED_REF, BOUNDARY, SealerWire.EGRESS_KIND_REPLAY_DONE),
            named(outcomes, "replay-inside").kinds());
    }

    /** The guard refuses once the sealed head runs more than 6 blocks past the posted head of 0: from {@code ref-4}. */
    @Test
    void version_five_keeps_the_da_lag_guard() {
        final Member member = log.carrying(Optional.empty(), dir.resolve("guard"), new LagBudgets(6L, 0L));
        final List<Outcome> outcomes = member.applyAll(Stream.concat(upgrade(true), lag(true)).toList());
        assertEquals(List.of(RELAYED_REF), named(outcomes, "ref-3").kinds());
        assertEquals(List.of(SealerWire.EGRESS_KIND_DA_LAG_REJECT), named(outcomes, "ref-4").kinds());
        assertEquals(List.of(SealerWire.EGRESS_KIND_DA_LAG_REJECT), named(outcomes, "ref-" + (LAG_BLOCKS - 1)).kinds());
        assertEquals(WINDOW, retained(member), "the guard holds no frame");
    }

    /**
     * A member that keeps its frames on the heap runs no member feed, so no
     * reader below the window can recover from the egress. Under version 5
     * it keeps every frame above the posted head, as version 4 does.
     */
    @Test
    void version_five_on_the_heap_keeps_the_frames_above_the_posted_head() {
        final Member member = log.member(Optional.empty());
        final List<Outcome> outcomes = member.applyAll(stream(true));
        assertEquals(VersionSwitch.CARRY_VERSION, member.svc().decisionRules().version());
        assertEquals(0L, lastStatus(member).getLong(POSTED_HEAD_OFFSET), "the batcher posted nothing");
        assertEquals(2 * HEAD - 1, retained(member), "every record and every boundary stays");
        final List<Byte> genesis = named(outcomes, "replay-genesis").kinds();
        assertEquals(HEAD, Collections.frequency(genesis, RELAYED), "every record from genesis");
        assertEquals(SealerWire.EGRESS_KIND_REPLAY_DONE, genesis.get(genesis.size() - 1));
    }

    /**
     * The heap members only: a member that carries the bytes writes
     * snapshot version 12, which holds no decision rules and no version
     * switch, so a restore of it does not run version 5 yet.
     */
    @Test
    void a_replay_from_a_snapshot_at_any_entry_evicts_alike() {
        final List<Entry> entries = stream(true);
        IntStream.range(1, entries.size()).forEach(cut -> replayFrom(entries, cut));
    }

    @Test
    void below_version_five_a_replay_from_a_snapshot_at_any_entry_evicts_alike() {
        final List<Entry> entries = stream(false);
        IntStream.range(1, entries.size()).forEach(cut -> replayFrom(entries, cut));
    }

    /**
     * Run {@code entries} on one member, and from {@code cut} on a member
     * restored from the snapshot of the first at {@code cut}. Both answer
     * alike, keep the same frames and floors, and write the same snapshot.
     */
    private void replayFrom(final List<Entry> entries, final int cut) {
        final Member original = log.member(Optional.empty());
        original.applyAll(entries.subList(0, cut));
        final Member restored = log.member(Optional.of(original.svc().snapshot()));
        final List<Entry> rest = entries.subList(cut, entries.size());
        assertEquals(original.applyAll(rest), restored.applyAll(rest), "cut at " + cut);
        assertArrayEquals(lastStatus(original).array(), lastStatus(restored).array(), "cut at " + cut);
        assertArrayEquals(original.svc().snapshot(), restored.svc().snapshot(), "cut at " + cut);
    }
}
