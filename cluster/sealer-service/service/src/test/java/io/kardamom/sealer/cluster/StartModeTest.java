package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.cluster.ClusterBackup.State;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.RecordingLog;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The start decision of a member: the blank-directory check, the bootstrap
 * inputs, the peer seed round verdicts, and the backoff. No test starts
 * Aeron.
 */
final class StartModeTest {

    private static final long TIMEOUT_MS = 1_000;

    @TempDir
    Path dir;

    @Test
    void onlyABlankMemberWithTheBootstrapStartsAtGenesis() {
        assertEquals(StartMode.RESUME, StartMode.of(true, false));
        assertEquals(StartMode.RESUME, StartMode.of(true, true));
        assertEquals(StartMode.GENESIS, StartMode.of(false, true));
        assertEquals(StartMode.SEED_FROM_PEER, StartMode.of(false, false));
    }

    @Test
    void aMissingOrEmptyRecordingLogIsBlank() {
        final StateDir state = new StateDir(dir);
        assertFalse(state.holdsRecordingLog(), "no recording log");
        new RecordingLog(dir.toFile(), true).close();
        assertFalse(state.holdsRecordingLog(), "a recording log without entries");
        try (RecordingLog log = new RecordingLog(dir.toFile(), true)) {
            log.appendTerm(0, 0, 0, 0);
        }
        assertTrue(state.holdsRecordingLog(), "a recording log with a term");
    }

    @Test
    void theLatestSnapshotPositionComesFromTheRecordingLog() {
        final StateDir state = new StateDir(dir);
        try (RecordingLog log = new RecordingLog(dir.toFile(), true)) {
            log.appendTerm(0, 0, 0, 0);
            assertEquals(-1L, state.latestSnapshotPosition(), "no snapshot yet");
            log.appendSnapshot(1, 0, 0, 4_096, 0, ConsensusModule.Configuration.SERVICE_ID);
        }
        assertEquals(4_096L, state.latestSnapshotPosition());
    }

    @Test
    void theBootstrapIsOffUnlessAnInputSaysTrue() throws IOException {
        final Path file = dir.resolve(StartMode.BOOTSTRAP_FILE);
        assertFalse(StartMode.bootstrap(null, Optional.empty()), "no input");
        assertFalse(StartMode.bootstrap(null, Optional.of(file)), "no file");
        assertTrue(StartMode.bootstrap("true", Optional.of(file)), "the property");
        Files.writeString(file, "");
        assertFalse(StartMode.bootstrap("false", Optional.of(file)), "the empty file of a deleted variable");
        Files.writeString(file, "true\n");
        assertTrue(StartMode.bootstrap(null, Optional.of(file)), "the file of the bootstrap variable");
    }

    @Test
    void aBootstrapTypoIsFatal() {
        assertThrows(IllegalStateException.class, () -> StartMode.parseFlag("ture", "test"));
        assertThrows(IllegalStateException.class, () -> StartMode.parseFlag("1", "test"));
    }

    @Test
    void clearEmptiesTheDirectoryAndKeepsIt() throws IOException {
        Files.createDirectories(dir.resolve("a/b"));
        Files.writeString(dir.resolve("a/b/c.rec"), "x");
        Files.writeString(dir.resolve("archive.catalog"), "x");
        new StateDir(dir).clear();
        assertTrue(Files.isDirectory(dir));
        try (var left = Files.list(dir)) {
            assertEquals(0, left.count());
        }
        final Path missing = dir.resolve("missing");
        new StateDir(missing).clear();
        assertTrue(Files.isDirectory(missing), "clear creates a missing directory");
    }

    @Test
    void dropMarkFilesKeepsTheState() throws IOException {
        Files.writeString(dir.resolve("cluster-mark.dat"), "x");
        Files.writeString(dir.resolve("archive-mark.dat"), "x");
        Files.writeString(dir.resolve("recording.log"), "x");
        new StateDir(dir).dropMarkFiles();
        assertFalse(Files.exists(dir.resolve("cluster-mark.dat")));
        assertFalse(Files.exists(dir.resolve("archive-mark.dat")));
        assertTrue(Files.exists(dir.resolve("recording.log")));
    }

    @Test
    void aRoundSeedsAtBackingUp() {
        final SeedWatch watch = new SeedWatch(TIMEOUT_MS, 0);
        assertEquals(SeedWatch.Outcome.PENDING, watch.observe(State.BACKUP_QUERY, 10));
        assertEquals(SeedWatch.Outcome.PENDING, watch.observe(State.SNAPSHOT_RETRIEVE, 20));
        assertEquals(SeedWatch.Outcome.SEEDED, watch.observe(State.BACKING_UP, 30));
    }

    @Test
    void aRoundWithoutAPeerAnswerEndsAtThePeerTimeout() {
        final SeedWatch watch = new SeedWatch(TIMEOUT_MS, 0);
        assertEquals(SeedWatch.Outcome.PENDING, watch.observe(State.BACKUP_QUERY, TIMEOUT_MS - 1));
        assertEquals(SeedWatch.Outcome.NO_PEER, watch.observe(State.RESET_BACKUP, TIMEOUT_MS));
    }

    @Test
    void aPeerAnswerRestartsThePeerTimeout() {
        final SeedWatch watch = new SeedWatch(TIMEOUT_MS, 0);
        assertEquals(SeedWatch.Outcome.PENDING, watch.observe(State.LIVE_LOG_REPLAY, TIMEOUT_MS - 1));
        assertEquals(SeedWatch.Outcome.PENDING, watch.observe(State.RESET_BACKUP, 2 * TIMEOUT_MS - 2));
        assertEquals(SeedWatch.Outcome.NO_PEER, watch.observe(State.BACKUP_QUERY, 2 * TIMEOUT_MS - 1));
    }

    @Test
    void aClosedBackupEndsTheRound() {
        assertEquals(SeedWatch.Outcome.BACKUP_CLOSED, new SeedWatch(TIMEOUT_MS, 0).observe(State.CLOSED, 0));
    }

    @Test
    void theBackoffDoublesUpToItsMaximum() {
        final PeerSeed.Timing timing = new PeerSeed.Timing(TIMEOUT_MS, 1_000, 30_000);
        assertEquals(
                java.util.List.of(1_000L, 2_000L, 4_000L, 8_000L, 16_000L, 30_000L, 30_000L),
                IntStream.rangeClosed(1, 7).mapToObj(timing::backoffMs).toList());
        assertEquals(30_000L, timing.backoffMs(Integer.MAX_VALUE));
    }
}
