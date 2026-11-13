package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.junit.jupiter.api.Test;

/** The waits of a log entry, from the samples of the consensus thread. */
class LogTimelineTest {

    private final AtomicLong clock = new AtomicLong();
    private final AtomicLong append = new AtomicLong(-1);
    private final AtomicLong commit = new AtomicLong(-1);
    private final LogTimeline timeline = new LogTimeline(clock::get);
    /** The waits that the timeline gave: the commit wait, then the apply wait, for each entry. */
    private final List<Long> waits = new ArrayList<>();

    LogTimelineTest() {
        timeline.follow(append::get, commit::get);
    }

    /** One duty cycle of the consensus thread at {@code now}, with the positions at these values. */
    private void cycle(final long now, final long appended, final long committed) {
        clock.set(now);
        append.set(appended);
        commit.set(committed);
        timeline.sample();
    }

    private List<Long> waitsOf(final long position, final long appliedNs) {
        waits.clear();
        timeline.waitsOf(position, appliedNs, (commitWait, applyWait) -> waits.addAll(List.of(commitWait, applyWait)));
        return waits;
    }

    @Test
    void anEntryWaitsForTheQuorumAndThenForTheServiceThread() {
        cycle(1_000, 4096, 4096);
        cycle(2_000, 4224, 4096);
        cycle(2_100, 4352, 4096);
        cycle(2_300, 4352, 4224);
        cycle(2_450, 4352, 4352);
        assertEquals(List.of(300L, 100L), waitsOf(4224, 2_400));
        assertEquals(List.of(350L, 50L), waitsOf(4352, 2_500));
    }

    @Test
    void anEntryAppliedBeforeTheSampleOfItsCommitHasNoApplyWait() {
        cycle(1_000, 4096, 4096);
        cycle(2_000, 4224, 4096);
        assertEquals(List.of(250L, 0L), waitsOf(4224, 2_250));
    }

    @Test
    void anEntryThatNoSampleHoldsYetHasWaitsOfZero() {
        cycle(1_000, 4096, 4096);
        assertEquals(List.of(0L, 0L), waitsOf(4224, 1_040));
    }

    @Test
    void anEntryWithNoSampleBeforeItsRecordGivesNoWaits() {
        assertEquals(List.of(), waitsOf(4224, 500), "a timeline with no sample");
        cycle(1_000, 8192, 8192);
        assertEquals(List.of(), waitsOf(4224, 1_100), "a replayed entry: the first sample holds it");
        LongStream.rangeClosed(1, LogTimeline.SLOTS).forEach(i -> cycle(1_000 + i, 8192 + i, 8192));
        assertEquals(List.of(), waitsOf(8193, 99_000), "an entry older than the lookback");
    }

    @Test
    void aCycleWithNoChangeKeepsNoSample() {
        cycle(1_000, 4096, 4096);
        cycle(2_000, 4224, 4096);
        cycle(9_000, 4224, 4096);
        cycle(9_500, 4224, 4224);
        assertEquals(List.of(7_500L, 100L), waitsOf(4224, 9_600));
    }

    @Test
    void theIdleStrategyOfTheConsensusThreadSamplesAtEachCall() {
        final var idle = timeline.sampling(NoOpIdleStrategy.INSTANCE);
        cycle(1_000, 4096, 4096);
        clock.set(2_000);
        append.set(4224);
        idle.idle(1);
        clock.set(2_600);
        commit.set(4224);
        idle.idle();
        assertEquals(List.of(600L, 150L), waitsOf(4224, 2_750));
        assertEquals(NoOpIdleStrategy.INSTANCE.alias(), idle.alias());
    }

    @Test
    void aTimelineThatFollowsNoPositionGivesNoWaits() {
        final LogTimeline idle = new LogTimeline(clock::get);
        idle.sample();
        idle.waitsOf(64, 100, (commitWait, applyWait) -> waits.add(commitWait));
        assertEquals(List.of(), waits);
    }
}
