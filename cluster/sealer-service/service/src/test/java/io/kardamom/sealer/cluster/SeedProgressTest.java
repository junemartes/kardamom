package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.cluster.ClusterBackup.State;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.RecordingLog;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Actual recording metadata and deterministic copy positions; no cluster or media driver starts. */
final class SeedProgressTest {
    @TempDir
    Path directory;

    @Test
    void anOlderSnapshotAndANewTermNeedTheLogToReachTheTermBase() {
        final SeedProgress progress = new SeedProgress(() -> 31_784_192L);
        final SeedWatch watch = new SeedWatch(20_000, 0);
        try (RecordingLog log = new RecordingLog(directory.toFile(), true)) {
            log.appendTerm(0, 2, 0, 0);
            log.appendSnapshot(1, 2, 0, 30_593_888, 0, ConsensusModule.Configuration.SERVICE_ID);
            log.appendTerm(0, 3, 32_317_216, 0);
            progress.onUpdatedRecordingLog(log, List.of());
            assertFalse(progress.sample().complete());
            assertEquals(SeedWatch.Outcome.PENDING, watch.observe(State.BACKING_UP, progress.sample(), 1));
            progress.onLiveLogProgress(0, 9, 32_317_215);
            assertEquals(SeedWatch.Outcome.PENDING, watch.observe(State.BACKING_UP, progress.sample(), 2));
            progress.onLiveLogProgress(0, 9, 32_317_216);
            assertEquals(SeedWatch.Outcome.SEEDED, watch.observe(State.BACKING_UP, progress.sample(), 3));
        }
        assertTrue(progress.covers(new StateDir(directory).seedFloor()));
    }

    @Test
    void aTermPublishedBeforeBackupCloseInvalidatesAnEarlierCompletion() {
        final SeedProgress progress = new SeedProgress(() -> 4_096L);
        try (RecordingLog log = new RecordingLog(directory.toFile(), true)) {
            log.appendTerm(0, 0, 0, 0);
            progress.onUpdatedRecordingLog(log, List.of());
            assertTrue(progress.sample().complete());
            log.appendTerm(0, 1, 8_192, 0);
        }
        assertFalse(progress.covers(new StateDir(directory).seedFloor()));
    }

    @Test
    void aSnapshotAboveTheTermBaseStillNeedsCopiedCoverage() {
        final SeedProgress progress = new SeedProgress(() -> 0L);
        try (RecordingLog log = new RecordingLog(directory.toFile(), true)) {
            log.appendTerm(0, 0, 0, 0);
            progress.onUpdatedRecordingLog(log, List.of());
            assertTrue(progress.sample().complete(), "no snapshot: the first term starts at zero");
            log.appendSnapshot(1, 0, 0, 4_096, 0, ConsensusModule.Configuration.SERVICE_ID);
            progress.onUpdatedRecordingLog(log, List.of());
            assertFalse(progress.sample().complete());
            progress.onLiveLogProgress(0, 9, 4_096);
            assertTrue(progress.sample().complete());
        }
    }

    @Test
    void aMissingTermOrAnotherRecordingCannotCertifyTheSeed() {
        final SeedProgress progress = new SeedProgress(() -> 4_096L);
        try (RecordingLog log = new RecordingLog(directory.toFile(), true)) {
            progress.onUpdatedRecordingLog(log, List.of());
            assertEquals(SeedWatch.Copy.MISSING, progress.sample());
            assertFalse(progress.covers(SeedProgress.Floor.of(log)));
            log.appendTerm(0, 0, 0, 0);
            progress.onUpdatedRecordingLog(log, List.of());
            assertFalse(progress.covers(java.util.Optional.of(new SeedProgress.Floor(7, 0))));
            progress.onLiveLogProgress(7, 9, 10_000);
            assertFalse(progress.sample().complete());
        }
    }

    @Test
    void aStalledCopyRetriesAndRealProgressRenewsItsBound() {
        final SeedWatch watch = new SeedWatch(100, 0);
        assertEquals(SeedWatch.Outcome.PENDING,
            watch.observe(State.BACKING_UP, new SeedWatch.Copy(10, false), 1));
        assertEquals(SeedWatch.Outcome.PENDING,
            watch.observe(State.BACKING_UP, new SeedWatch.Copy(20, false), 100));
        assertEquals(SeedWatch.Outcome.PENDING,
            watch.observe(State.BACKING_UP, new SeedWatch.Copy(20, false), 199));
        assertEquals(SeedWatch.Outcome.LOG_INCOMPLETE,
            watch.observe(State.BACKING_UP, new SeedWatch.Copy(20, false), 200));
    }
}
