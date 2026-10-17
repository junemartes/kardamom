package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.cluster.RecordingLog;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.agrona.LangUtil;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** The schedule, the published position and the failure rule of the disk sync of the Raft log. */
final class DurableSyncTest {

    private static final long LOG = 5;
    private static final long SNAPSHOT = 7;

    @TempDir
    Path dir;

    private final AtomicReference<Supplier<Optional<DurableSync.View>>> view =
        new AtomicReference<>(Optional::empty);
    private final List<IOException> diskFailures = new CopyOnWriteArrayList<>();
    private final List<Throwable> deaths = new CopyOnWriteArrayList<>();
    private DurableSync sync;

    @BeforeEach
    void files() throws IOException {
        final Path archive = Files.createDirectories(dir.resolve("archive"));
        final Path cluster = Files.createDirectories(dir.resolve("cluster"));
        Files.write(archive.resolve("archive.catalog"), new byte[64]);
        Files.write(cluster.resolve(RecordingLog.RECORDING_LOG_FILE_NAME), new byte[64]);
        for (final String name : List.of("5-0.rec", "5-65536.rec", "7-0.rec")) {
            Files.write(archive.resolve(name), new byte[4096]);
        }
        sync = new DurableSync(new DurableSync.Dirs(archive, cluster), () -> view.get().get(),
            new DurableSync.Stops(0, diskFailures::add, deaths::add));
    }

    private void see(final OptionalLong position) {
        final Optional<DurableSync.View> seen = Optional.of(new DurableSync.View(LOG, position, List.of(SNAPSHOT)));
        view.set(() -> seen);
    }

    @Test
    void theSettingDefaultsToOneSecondAndZeroTurnsTheSyncOff() {
        assertEquals(Optional.of(new Interval(DurableSync.DEFAULT_MS)), DurableSync.fromSetting(null));
        assertEquals(Optional.of(new Interval(250)), DurableSync.fromSetting(" 250 "));
        assertEquals(Optional.empty(), DurableSync.fromSetting("0"));
        assertThrows(IllegalArgumentException.class, () -> DurableSync.fromSetting("-1"));
        assertThrows(NumberFormatException.class, () -> DurableSync.fromSetting("1s"));
    }

    @Test
    void aRoundPublishesThePositionThatItReadBeforeItsForces() {
        assertEquals(DurableSync.Durability.NONE, sync.durability());
        see(OptionalLong.of(70_000));
        sync.round();
        assertEquals(70_000, sync.durability().position());
        see(OptionalLong.empty());
        sync.round();
        assertEquals(70_000, sync.durability().position(), "a stopped recording keeps the last synced position");
        assertEquals(2, sync.durability().rounds());
        assertEquals(0, sync.durability().errors());
        assertEquals(List.of(), diskFailures);
    }

    @Test
    void aRecordingLogThatTheModuleIsWritingGivesNoPositionAndNoError() {
        sync.round();
        assertEquals(-1, sync.durability().position());
        assertEquals(1, sync.durability().rounds());
        assertEquals(0, sync.durability().errors());
    }

    @Test
    void aFileThatThePurgeRemovedIsNotAFailure() throws IOException {
        Files.delete(dir.resolve("archive").resolve("5-0.rec"));
        Files.delete(dir.resolve("archive").resolve("archive.catalog"));
        see(OptionalLong.of(100));
        sync.round();
        assertEquals(100, sync.durability().position());
        assertEquals(0, sync.durability().errors());
    }

    @Test
    void aFailedForceStopsTheMemberWithoutAPosition() {
        final IOException eio = new IOException("Input/output error");
        sync.forcingWith((channel, metadata) -> {
            throw eio;
        });
        see(OptionalLong.of(100));
        sync.round();
        assertEquals(List.of(eio), diskFailures);
        assertEquals(1, sync.durability().errors());
        assertEquals(-1, sync.durability().position(), "a failed round publishes no position");
    }

    /**
     * A failure that is not a force is counted, and the next round tries
     * again: an open or a listing that fails, and a read of the recording
     * log that throws, also with a checked exception that it does not
     * declare.
     */
    @Test
    void aFailureOtherThanAForceIsCountedAndTheNextRoundTriesAgain() throws IOException {
        final Path catalog = dir.resolve("archive").resolve("archive.catalog");
        Files.delete(catalog);
        Files.createDirectory(catalog);
        see(OptionalLong.of(100));
        sync.round();
        Files.delete(catalog);
        view.set(() -> {
            throw new UncheckedIOException(new IOException("Too many open files"));
        });
        sync.round();
        view.set(() -> {
            LangUtil.rethrowUnchecked(new IOException("recording log read failed"));
            return Optional.empty();
        });
        sync.round();
        assertEquals(new DurableSync.Durability(-1, 3, 3, sync.durability().lastRoundNs(),
            sync.durability().maxRoundNs()), sync.durability());
        see(OptionalLong.of(200));
        sync.round();
        assertEquals(200, sync.durability().position());
        assertEquals(List.of(), diskFailures);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aThrowableThatEscapesARoundStopsTheMember() throws InterruptedException {
        final AssertionError escaped = new AssertionError("escapes the round");
        view.set(() -> {
            throw escaped;
        });
        sync.start(new Interval(5));
        while (deaths.isEmpty()) {
            Thread.onSpinWait();
        }
        sync.close();
        assertEquals(List.of(escaped), deaths);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void theThreadRunsOneRoundEachIntervalAndEndsOnClose() throws InterruptedException {
        final long intervalMs = 20;
        see(OptionalLong.of(1));
        final long start = System.nanoTime();
        sync.start(new Interval(intervalMs));
        IntStream.rangeClosed(2, 6).forEach(position -> {
            see(OptionalLong.of(position));
            awaitPosition(position);
        });
        final long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        sync.close();
        final long rounds = sync.durability().rounds();
        assertTrue(rounds >= 5 && rounds * intervalMs <= elapsedMs + intervalMs,
            "rounds=" + rounds + " elapsedMs=" + elapsedMs);
        Thread.sleep(5 * intervalMs);
        assertEquals(rounds, sync.durability().rounds(), "no round after close");
    }

    private void awaitPosition(final long position) {
        while (sync.durability().position() < position) {
            Thread.onSpinWait();
        }
    }

    /**
     * A force through one channel writes back the dirty pages that another
     * channel of the same file wrote. Linux keeps the dirty pages in the
     * page cache of the inode, and fsync writes back that page cache, not
     * the pages of one descriptor. So the first force, through the second
     * channel, writes the data, and a later force through the writer finds
     * little to write. A per-descriptor fsync would show the reverse. The
     * test compares the medians of five trials, so one slow force on a busy
     * disk does not decide it. Medians under 50 ms tell nothing: the kernel
     * wrote the pages back first, or the file system ignores a force.
     */
    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void aForceThroughAnotherChannelCoversThePagesOfTheWriter() {
        final Path file = dir.resolve("coverage.rec");
        final List<long[]> trials = IntStream.range(0, 5).mapToObj(trial -> forceTimes(file)).toList();
        final long other = median(trials.stream().mapToLong(t -> t[0]));
        final long writer = median(trials.stream().mapToLong(t -> t[1]));
        Assumptions.assumeTrue(Math.max(other, writer) >= TimeUnit.MILLISECONDS.toNanos(50),
            "every force was fast: the disk shows no difference");
        assertTrue(writer * 2 < other, "median force of the writer " + writer + " ns, of the other channel "
            + other + " ns");
    }

    private static long median(final LongStream values) {
        final long[] sorted = values.sorted().toArray();
        return sorted[sorted.length / 2];
    }

    /** Write 64 MiB through one channel, then force through another channel and through the writer. */
    private static long[] forceTimes(final Path file) {
        try (FileChannel writer = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
             FileChannel other = FileChannel.open(file, StandardOpenOption.WRITE)) {
            final ByteBuffer block = ByteBuffer.allocateDirect(1 << 20);
            IntStream.range(0, 64).forEach(i -> write(writer, block.clear(), (long) i << 20));
            final long otherNs = timed(other);
            final long writerNs = timed(writer);
            return new long[] {otherNs, writerNs};
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void write(final FileChannel channel, final ByteBuffer block, final long at) {
        try {
            channel.write(block, at);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static long timed(final FileChannel channel) throws IOException {
        final long start = System.nanoTime();
        channel.force(false);
        return System.nanoTime() - start;
    }
}
