package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.LagBudgets;
import io.kardamom.sealer.VersionSwitch;
import io.kardamom.sealer.cluster.DriverlessLog.Entry;
import io.kardamom.sealer.cluster.DriverlessLog.Member;
import io.kardamom.sealer.cluster.DriverlessLog.Outcome;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import org.agrona.concurrent.AgentTerminationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The carry window, feature 5, on driverless heap members. Under version 5
 * with the carry window on, the egress keeps only its replay window, and
 * a transaction reference of record type 0 is not ordered: its session
 * gets the refusal, egress kind 18. The flag changes at the boundary
 * inside a log entry, a timer tick or the boundary that an epoch forces.
 * The eviction rule changes at the end of that entry. A later activation
 * turns the carry window off, and another one turns it on again.
 *
 * <p>Every member sends the same frames and writes the same snapshot at
 * every cut, and a member restored at any cut ends with the bytes of its
 * peer.</p>
 */
class SealerCarryActivationTest {

    private static final byte RELAYED = SealerWire.EGRESS_KIND_RELAYED;
    private static final byte BOUNDARY = SealerWire.EGRESS_KIND_BOUNDARY;
    private static final byte REFUSED = SealerWire.EGRESS_KIND_REF_REFUSED;

    /** The replay window of every member, in frames. */
    private static final int WINDOW = 4;
    /** The sender of every reference of record type 0 of the tests. */
    private static final byte[] SENDER = FullRef.filled(CanonicalSealerState.SENDER_LEN, 0xAA);

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

    /** The boundary that turns the carry window on. */
    enum Switch {
        /** The boundary tick that seals the block of the epoch of the activation. */
        TIMER,
        /** The boundary that the next epoch forces. */
        FORCED,
        /** The boundary that a remote-origin record forces. */
        FORCED_REMOTE
    }

    /** The log of a test: frames and ticks, with the times, the ids and the positions that each entry takes. */
    private static final class Script {
        private final List<Entry> entries = new ArrayList<>();
        private long ms = 1_000L;
        private int tag = 20;
        private long refPosition = 64L;
        private long fullPosition = 64L;
        private long epoch;

        Script frame(final String name, final byte[] frame) {
            ms += 50L;
            entries.add(Entry.frame(name, frame, ms));
            return this;
        }

        Script tick(final String name) {
            ms += CanonicalSealerState.TICK_INTERVAL_MS;
            entries.add(Entry.tick(name, ms));
            return this;
        }

        /** The next epoch, in front of it an activation of {@code feature} at {@code atMs} when one is given. */
        Script epoch(final String name, final Optional<Long> feature, final long atMs) {
            epoch++;
            feature.ifPresent(f -> frame(name + "-activation", IngressFrames.featureActivationFrame(epoch, f, atMs)));
            return frame(name, IngressFrames.originRecordFrame(
                IngressFrames.canonicalId(1000 + (int) epoch), epoch, 1, new byte[] {0x02}));
        }

        Script epoch(final String name, final long feature, final long atMs) {
            return epoch(name, Optional.of(feature), atMs);
        }

        Script epoch(final String name) {
            return epoch(name, Optional.empty(), 0L);
        }

        /** The next reference of record type 0: id {@code tag}, nonce {@code tag}, the next position of its image. */
        byte[] ref() {
            refPosition += 32L;
            final int id = tag++;
            return IngressFrames.txRefFrame(id, SENDER, id, refPosition);
        }

        byte[] full() {
            fullPosition += 32L;
            return FullRef.of(tag++, fullPosition).frame();
        }

        /** {@code blocks} blocks of one full reference each, with no posted cursor. */
        Script lag(final String name, final int blocks) {
            IntStream.range(0, blocks).forEach(k -> frame(name + "-full-" + k, full()).tick(name + "-tick-" + k));
            return this;
        }

        List<Entry> entries() {
            return List.copyOf(entries);
        }
    }

    /**
     * Version 5, a lag of frames above the posted head, the carry window
     * switched on at the boundary of {@code at}, the refusal and the
     * window, the carry window off, then on again.
     */
    private static List<Entry> stream(final Switch at) {
        final Script s = new Script();
        s.epoch("epoch-1").tick("tick-1")
            .epoch("epoch-2", VersionSwitch.FEATURE_SEALER_V4, 0L).tick("tick-2")
            .epoch("epoch-3", VersionSwitch.FEATURE_SEALER_V5, 0L)
            .frame("full-in-v4", s.full()).tick("v5-tick")
            .frame("ref-before", s.ref()).frame("full-before", s.full())
            .lag("lag", 3)
            .epoch("window-epoch", VersionSwitch.FEATURE_CARRY_WINDOW, 0L)
            .frame("ref-in-deposit-block", s.ref());
        switch (at) {
            case TIMER -> s.tick("switch");
            case FORCED -> s.epoch("switch");
            case FORCED_REMOTE -> s.frame("switch", IngressFrames.remoteOriginRecordFrame(
                IngressFrames.canonicalId(2000), DriverlessLog.REMOTE_CHAIN, 1, 2, 0, 0, new byte[] {0x03}));
        }
        s.frame("ref-refused", s.ref()).frame("full-after", s.full())
            .frame("batch", FullRef.batch(s.ref(), s.full(), s.ref()))
            .lag("window-lag", 3)
            .epoch("off-epoch", VersionSwitch.FEATURE_CARRY_WINDOW, -1L).tick("off")
            .frame("ref-after-off", s.ref())
            .lag("off-lag", 3)
            .epoch("again-epoch", VersionSwitch.FEATURE_CARRY_WINDOW, 0L).tick("again")
            .frame("ref-again", s.ref())
            .tick("end");
        return s.entries();
    }

    private static Outcome named(final List<Outcome> outcomes, final String name) {
        return outcomes.stream().filter(o -> o.name().equals(name)).findFirst().orElseThrow();
    }

    private static int at(final List<Entry> entries, final String name) {
        return IntStream.range(0, entries.size()).filter(i -> entries.get(i).name().equals(name)).findFirst()
            .orElseThrow();
    }

    private static long retained(final Member member) {
        return member.svc().metrics().carry().retainedFrames();
    }

    private static boolean carryWindow(final Member member) {
        return member.svc().metrics().carry().carryWindow();
    }

    @Test
    void the_timer_boundary_turns_the_carry_window_on() {
        checkTheSwitch(Switch.TIMER, List.of(BOUNDARY));
    }

    @Test
    void the_forced_boundary_turns_the_carry_window_on() {
        checkTheSwitch(Switch.FORCED, List.of(BOUNDARY, RELAYED));
    }

    @Test
    void the_boundary_of_a_remote_origin_record_turns_the_carry_window_on() {
        checkTheSwitch(Switch.FORCED_REMOTE, List.of(BOUNDARY, RELAYED));
    }

    /** Four refusals in one second of cluster time print one line, and the next second prints again. */
    @Test
    void the_refusal_log_prints_at_most_once_a_second() {
        final List<Entry> entries = stream(Switch.TIMER);
        final Member member = log.member(Optional.empty());
        member.applyAll(entries.subList(0, at(entries, "switch") + 1));
        final Script refs = new Script();
        final long start = entries.get(at(entries, "switch")).timeMs();
        final List<Entry> burst = IntStream.range(0, 5)
            .mapToObj(i -> Entry.frame("burst-" + i, refs.ref(), start + (i < 4 ? 100L * (i + 1) : 2_000L)))
            .toList();
        final PrintStream out = System.out;
        final ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        final List<Outcome> outcomes;
        try {
            outcomes = member.applyAll(burst);
        } finally {
            System.setOut(out);
        }
        outcomes.forEach(o -> assertEquals(List.of(REFUSED), o.kinds(), o.name()));
        final List<String> lines = captured.toString(StandardCharsets.UTF_8).lines()
            .filter(line -> line.contains("cluster REF-REFUSED")).toList();
        assertEquals(2, lines.size(), String.join("\n", lines));
        assertTrue(lines.get(0).contains("total=1"), lines.get(0));
        assertTrue(lines.get(1).contains("total=5"), lines.get(1));
        assertEquals(5.0, PromText.value(member.metrics(), "kardamom_sealer_ref_refused_total", ""));
    }

    /**
     * The block of the deposit still orders a type-0 reference. The entry
     * of the switch takes its frames under the posted rule, and its end
     * drains the retained frames to the window. The next type-0 reference
     * is refused, and a full reference is ordered.
     */
    private void checkTheSwitch(final Switch at, final List<Byte> switchKinds) {
        final List<Entry> entries = stream(at);
        final Member member = log.member(Optional.empty());
        final int switchAt = at(entries, "switch");
        final List<Outcome> before = member.applyAll(entries.subList(0, switchAt));
        assertEquals(List.of(RELAYED), named(before, "ref-in-deposit-block").kinds());
        assertTrue(retained(member) > WINDOW, "the posted rule keeps the unposted frames");
        assertFalse(carryWindow(member));
        final Outcome atSwitch = member.apply(entries.get(switchAt));
        assertEquals(switchKinds, atSwitch.kinds());
        assertTrue(carryWindow(member));
        assertEquals(WINDOW, retained(member), "the end of the switch entry drains to the window");
        final List<Outcome> after = member.applyAll(entries.subList(switchAt + 1, entries.size()));
        assertEquals(List.of(REFUSED), named(after, "ref-refused").kinds());
        assertEquals(List.of(RELAYED), named(after, "full-after").kinds());
        assertEquals(List.of(REFUSED, RELAYED, REFUSED), named(after, "batch").kinds());
        assertEquals(List.of(RELAYED), named(after, "ref-after-off").kinds(), "off again: type 0 is ordered");
        assertEquals(List.of(REFUSED), named(after, "ref-again").kinds(), "on again: type 0 is refused");
    }

    /** With the carry window on, the window holds at any DA lag. Off, the posted rule keeps every new frame. */
    @Test
    void the_window_holds_while_the_carry_window_is_on() {
        final List<Entry> entries = stream(Switch.TIMER);
        final Member member = log.member(Optional.empty());
        final int switchAt = at(entries, "switch");
        final int off = at(entries, "off");
        final int again = at(entries, "again");
        member.applyAll(entries.subList(0, switchAt + 1));
        IntStream.range(switchAt + 1, off).forEach(i -> {
            member.apply(entries.get(i));
            assertEquals(WINDOW, retained(member), entries.get(i).name());
        });
        member.applyAll(entries.subList(off, again));
        assertTrue(retained(member) > WINDOW, "off: no frame leaves before its block is posted");
        member.apply(entries.get(again));
        assertEquals(WINDOW, retained(member), "on again: the window drains at the end of the entry");
    }

    @Test
    void the_refusal_carries_the_id_the_sender_the_nonce_and_version_five() {
        final List<Entry> entries = stream(Switch.TIMER);
        final Member member = log.member(Optional.empty());
        member.applyAll(entries);
        final byte[] refused = member.frames().stream().filter(f -> f[0] == REFUSED).findFirst().orElseThrow();
        final byte[] offer = entries.get(at(entries, "ref-refused")).frame().orElseThrow();
        final int tag = offer[SealerWire.CANONICAL_ID_OFFSET];
        final byte[] expected = ByteBuffer.allocate(SealerWire.REF_REFUSED_LEN).order(ByteOrder.LITTLE_ENDIAN)
            .put(REFUSED).put(IngressFrames.recordId(tag)).put(SENDER).putLong(tag)
            .put((byte) VersionSwitch.CARRY_VERSION).array();
        assertArrayEquals(expected, refused);
    }

    @Test
    void the_gauges_show_the_carry_window_and_the_counters() {
        final Member member = log.member(Optional.empty());
        final List<Entry> entries = stream(Switch.TIMER);
        member.applyAll(entries.subList(0, at(entries, "window-lag-full-0")));
        final String text = member.metrics();
        assertEquals(1.0, PromText.value(text, "kardamom_sealer_window_exact", ""));
        assertEquals(0.0, PromText.value(text, "kardamom_sealer_retained_store", ""));
        assertEquals(5.0, PromText.value(text, "kardamom_sealer_decision_version", ""));
        assertEquals(WINDOW, PromText.value(text, "kardamom_sealer_retained_frames", ""));
        assertEquals(2.0, PromText.value(text, "kardamom_sealer_feature_state", "feature=\"5\""));
        assertEquals(2.0, PromText.value(text, "kardamom_sealer_feature_state", "feature=\"4\""));
        assertEquals(3.0, PromText.value(text, "kardamom_sealer_ref_refused_total", ""), "one alone, two in the batch");
        assertEquals(1.0, PromText.value(text, "kardamom_sealer_full_ref_below_v5_total", ""), "full-in-v4");
        final MemberStatus.Carry carry = member.svc().metrics().carry();
        assertEquals("heap", carry.retainedStore());
        assertTrue(carry.carryWindow());
        assertEquals(4L, carry.activationOrigin(), "the epoch of the activation");
    }

    /** Three members apply the log: after each entry, their frames, retained frames and snapshots are equal. */
    @Test
    void every_member_has_the_same_bytes_after_each_entry() {
        List.of(Switch.values()).forEach(at -> {
            final List<Member> members = List.of(
                log.member(Optional.empty()), log.member(Optional.empty()), log.member(Optional.empty()));
            stream(at).forEach(entry -> {
                final List<Outcome> outcomes = members.stream().map(m -> m.apply(entry)).toList();
                final byte[] snapshot = members.get(0).svc().snapshot();
                members.forEach(m -> {
                    assertArrayEquals(snapshot, m.svc().snapshot(), at + " " + entry.name());
                    assertEquals(retained(members.get(0)), retained(m), at + " " + entry.name());
                });
                assertEquals(1, outcomes.stream().distinct().count(), at + " " + entry.name());
            });
        });
    }

    /**
     * A member restored from a snapshot at any entry ends with the bytes
     * of its peer: before the activation, with the activation held, at
     * the switch, after it, with the carry window off, and on again.
     */
    @Test
    void a_member_restored_at_any_entry_ends_with_the_bytes_of_its_peer() {
        List.of(Switch.values()).forEach(at -> {
            final List<Entry> entries = stream(at);
            IntStream.range(1, entries.size()).forEach(cut -> replayFrom(entries, cut, at));
        });
    }

    private void replayFrom(final List<Entry> entries, final int cut, final Switch at) {
        final String where = at + " cut at " + entries.get(cut).name();
        final Member original = log.member(Optional.empty());
        original.applyAll(entries.subList(0, cut));
        final Member restored = log.member(Optional.of(original.svc().snapshot()));
        assertEquals(original.svc().decisionRules(), restored.svc().decisionRules(), where);
        assertEquals(original.svc().metrics().carry(), restored.svc().metrics().carry(), where);
        final List<Entry> rest = entries.subList(cut, entries.size());
        assertEquals(original.applyAll(rest), restored.applyAll(rest), where);
        assertEquals(original.svc().metrics().carry(), restored.svc().metrics().carry(), where);
        assertArrayEquals(original.svc().snapshot(), restored.svc().snapshot(), where);
    }

    @Test
    void a_member_writes_version_eighteen_only_once_it_holds_the_carry_window() {
        final List<Entry> entries = stream(Switch.TIMER);
        final Member member = log.member(Optional.empty());
        final int activation = at(entries, "window-epoch-activation");
        member.applyAll(entries.subList(0, activation));
        assertEquals(CanonicalSealerState.CARRY_SNAPSHOT_VERSION, version(member.svc().snapshot()));
        member.apply(entries.get(activation));
        assertEquals(CanonicalSealerState.WINDOW_SNAPSHOT_VERSION, version(member.svc().snapshot()));
        member.applyAll(entries.subList(activation + 1, entries.size()));
        assertEquals(CanonicalSealerState.WINDOW_SNAPSHOT_VERSION, version(member.svc().snapshot()),
            "off again, the state still holds the schedule");
    }

    /** A member with the retained file answers the carry window with UNSUPPORTED, and its snapshot keeps its bytes. */
    @Test
    void a_file_member_answers_the_carry_window_unsupported() {
        final List<Entry> entries = stream(Switch.TIMER);
        final Member member = log.carrying(Optional.empty(), dir.resolve("file"), LagBudgets.DEFAULT);
        final int activation = at(entries, "window-epoch-activation");
        member.applyAll(entries.subList(0, activation));
        final byte[] before = member.svc().snapshot();
        assertEquals(List.of(), member.apply(entries.get(activation)).kinds());
        assertArrayEquals(before, member.svc().snapshot());
        assertEquals(1.0, PromText.value(member.metrics(), "kardamom_sealer_feature_unsupported_total", ""));
        assertEquals("file", member.svc().metrics().carry().retainedStore());
        assertEquals(1.0, PromText.value(member.metrics(), "kardamom_sealer_full_ref_below_v5_total", ""),
            "full-in-v4 counts on a file member too");
        assertEquals(1.0, PromText.value(member.metrics(), "kardamom_sealer_retained_store", ""));
        final List<Outcome> rest = member.applyAll(entries.subList(activation + 1, entries.size()));
        assertFalse(carryWindow(member));
        assertFalse(named(rest, "ref-refused").kinds().contains(REFUSED), "a file member refuses no reference");
        assertEquals(CanonicalSealerState.FILE_SNAPSHOT_VERSION, version(member.svc().snapshot()));
    }

    /** A file member never writes version 18, and it stops on one. */
    @Test
    void a_file_member_stops_on_version_eighteen() {
        final Member heap = log.member(Optional.empty());
        heap.applyAll(stream(Switch.TIMER));
        final byte[] v18 = heap.svc().snapshot();
        assertEquals(CanonicalSealerState.WINDOW_SNAPSHOT_VERSION, version(v18));
        assertThrows(AgentTerminationException.class,
            () -> log.carrying(Optional.of(v18), dir.resolve("file"), LagBudgets.DEFAULT));
    }

    private static int version(final byte[] snapshot) {
        return ByteBuffer.wrap(snapshot).getInt(Integer.BYTES);
    }
}
