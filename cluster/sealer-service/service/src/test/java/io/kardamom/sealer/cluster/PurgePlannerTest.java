package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Optional;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;

/**
 * The purge point rules: the margin of kept snapshots, the posted-head
 * floor, and the plan of a member that restarted. No Aeron.
 */
final class PurgePlannerTest {

    private static final PurgePlanner KEEP_THREE = new PurgePlanner(3);

    /** The snapshot at log position {@code 100 * n} closed block {@code 10 * n}. */
    private static PurgeView.Mark mark(final long n) {
        return new PurgeView.Mark(100L * n, 10L * n);
    }

    /** The recording log positions of snapshots {@code 1..count}. */
    private static List<Long> positions(final long count) {
        return LongStream.rangeClosed(1, count).map(n -> 100L * n).boxed().toList();
    }

    /** A service that knows snapshots {@code from..to} and the posted head. */
    private static PurgeView view(final long from, final long to, final long postedHead) {
        return LongStream.rangeClosed(from, to)
                .mapToObj(PurgePlannerTest::mark)
                .reduce(PurgeView.EMPTY, PurgeView::withMark, (a, b) -> b)
                .withPostedHead(postedHead);
    }

    @Test
    void noPurgeWhileTheMemberKeepsEverySnapshot() {
        assertEquals(Optional.empty(), KEEP_THREE.plan(positions(3), view(1, 3, 1_000)));
    }

    @Test
    void theMarginKeepsTheNewestSnapshots() {
        assertEquals(Optional.of(mark(2)), KEEP_THREE.plan(positions(5), view(1, 5, 1_000)));
        assertEquals(Optional.of(mark(4)), new PurgePlanner(1).plan(positions(5), view(1, 5, 1_000)));
    }

    @Test
    void theFloorKeepsTheLogOfUnpostedBlocks() {
        assertEquals(Optional.of(mark(1)), KEEP_THREE.plan(positions(5), view(1, 5, 19)));
        assertEquals(Optional.of(mark(2)), KEEP_THREE.plan(positions(5), view(1, 5, 20)));
        assertEquals(Optional.empty(), KEEP_THREE.plan(positions(5), view(1, 5, 9)));
    }

    @Test
    void aPostedHeadThatStopsHoldsThePurgePoint() {
        // The batcher stops at block 25. New snapshots come, and the purge
        // point stays at the last snapshot at or below the posted head.
        LongStream.rangeClosed(5, 40).forEach(count -> assertEquals(Optional.of(mark(2)),
                KEEP_THREE.plan(positions(count), view(1, count, 25))));
    }

    @Test
    void aRestartedMemberWaitsForNewSnapshots() {
        // The member restored snapshot 5. It knows the block of that
        // snapshot only, and the recording log holds snapshots 1..5.
        assertEquals(Optional.empty(), KEEP_THREE.plan(positions(5), view(5, 5, 1_000)));
        assertEquals(Optional.empty(), KEEP_THREE.plan(positions(7), view(5, 7, 1_000)));
        assertEquals(Optional.of(mark(5)), KEEP_THREE.plan(positions(8), view(5, 8, 1_000)));
    }

    @Test
    void aSnapshotThatTheRecordingLogLacksIsNoPurgePoint() {
        // The service took snapshot 3, but the consensus module never
        // recorded it. A restart could not start there.
        final List<Long> withoutThree = List.of(100L, 200L, 400L, 500L, 600L);
        assertEquals(Optional.of(mark(2)), KEEP_THREE.plan(withoutThree, view(1, 6, 1_000)));
    }

    @Test
    void theViewKeepsTheNewestMarks() {
        final PurgeView full = view(1, PurgeView.MAX_MARKS + 10L, 0);
        assertEquals(PurgeView.MAX_MARKS, full.marks().size());
        assertEquals(mark(11), full.marks().get(0));
    }

    @Test
    void theSettingDefaultsToThreeAndZeroTurnsThePurgeOff() {
        assertEquals(Optional.of(new PurgePlanner(3)), PurgePlanner.fromSetting(null));
        assertEquals(Optional.of(new PurgePlanner(3)), PurgePlanner.fromSetting(" "));
        assertEquals(Optional.of(new PurgePlanner(5)), PurgePlanner.fromSetting("5"));
        assertEquals(Optional.empty(), PurgePlanner.fromSetting("0"));
    }

    @Test
    void aBadSettingIsFatal() {
        assertThrows(IllegalStateException.class, () -> PurgePlanner.fromSetting("three"));
        assertThrows(IllegalStateException.class, () -> PurgePlanner.fromSetting("-1"));
        assertThrows(IllegalStateException.class,
                () -> PurgePlanner.fromSetting(Integer.toString(PurgeView.MAX_MARKS)));
    }
}
