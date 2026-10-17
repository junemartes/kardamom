package io.kardamom.sealer.cluster;

import io.aeron.Aeron;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.RecordingLog;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.agrona.concurrent.status.CountersReader;

/**
 * Syncs the Raft log of this member to disk on a schedule, and publishes
 * the synced log position.
 *
 * <p>The archive and the consensus module run at sync level 0: a commit
 * waits for no disk sync. One daemon thread owns all the state of this
 * class. Each round reads the log recording position from the archive
 * counter, and then forces these files through its own file channels, in
 * this order:</p>
 * <ol>
 *   <li>the segment files of each snapshot recording that the recording
 *       log names and no earlier round forced;</li>
 *   <li>the archive catalog and the recording log;</li>
 *   <li>the snapshot recordings that the recording log named during
 *       step 2;</li>
 *   <li>the log segment files from the segment of the last synced
 *       position up to the newest one, and then the archive directory.</li>
 * </ol>
 *
 * <p>A force through any file descriptor writes back all dirty pages of
 * the inode, so it covers the pages that the archive wrote through its own
 * channel. The archive moves the position counter only after its write
 * returns. So when a round ends, every log byte below the position that
 * the round read is on disk, and that position is the synced position.
 * The log segments come after the catalog: a stop position in the forced
 * catalog never points past the forced log bytes.</p>
 *
 * <p>The thread never touches the service thread, the consensus module or
 * the archive conductor: it reads a counter in shared memory and opens its
 * own channels. A failed force (other than a file that the purge removed)
 * stops the member. Linux marks the pages clean after a failed write-back,
 * so a later force can report success for data that never reached the
 * disk.</p>
 */
final class DurableSync implements AutoCloseable {

    static final String SETTING = "kardamom.cluster.durableSyncMs";
    static final long DEFAULT_MS = 1_000;
    /** The exit code of a member whose disk failed a sync. */
    static final int DISK_FAILED_EXIT_CODE = 9;

    private static final String CATALOG = "archive.catalog";
    /** An archive segment file: {@code <recording id>-<segment base position>.rec}. */
    private static final Pattern SEGMENT = Pattern.compile("(\\d+)-(\\d+)\\.rec");

    /** The archive directory and the consensus module directory of the member. */
    record Dirs(Path archive, Path cluster) {
    }

    /**
     * What a round reads before it forces: the log recording, its position
     * when the recording is active, and the snapshot recordings.
     */
    record View(long logRecording, OptionalLong position, List<Long> snapshots) {
    }

    /**
     * The published result of the rounds: the synced log position, below
     * which every log byte is on the disk of this member (-1 before the
     * first round with an active log recording), the round and error
     * counts, and the duration of the last and the longest round.
     */
    record Durability(long position, long rounds, long errors, long lastRoundNs, long maxRoundNs) {
        static final Durability NONE = new Durability(-1L, 0L, 0L, 0L, 0L);
    }

    private final Dirs dirs;
    private final Supplier<Optional<View>> view;
    private final Consumer<IOException> diskFailed;
    private final Set<Long> forcedSnapshots = new HashSet<>();
    private long logRecording = RecordingPos.NULL_RECORDING_ID;
    /** The base of the log segment of the synced position. Each round forces this segment and every newer one. */
    private long forcedBase;
    /** The thread reads and writes this field. Other threads only read it. */
    private volatile Durability durability = Durability.NONE;
    private Thread thread;

    DurableSync(final Dirs dirs, final Supplier<Optional<View>> view, final Consumer<IOException> diskFailed) {
        this.dirs = dirs;
        this.view = view;
        this.diskFailed = diskFailed;
    }

    /**
     * The interval of the setting {@code raw}: unset or blank for the
     * default, 0 for no sync. A negative value or other text stops the start.
     */
    static Optional<Interval> fromSetting(final String raw) {
        final long ms = raw == null || raw.isBlank() ? DEFAULT_MS : Long.parseLong(raw.trim());
        if (ms < 0) {
            throw new IllegalArgumentException(SETTING + " must be 0 or more, not " + ms);
        }
        return ms == 0 ? Optional.empty() : Optional.of(new Interval(ms));
    }

    /**
     * The view of a running member: the recording log in {@code clusterDir}
     * names the recordings, and the archive counter in {@code counters}
     * gives the log position. A recording log that the consensus module is
     * writing gives no view.
     */
    static Supplier<Optional<View>> memberView(final StateDir clusterDir, final CountersReader counters) {
        return () -> clusterDir.recordings().map(recordings -> new View(
            recordings.log(), position(counters, recordings.log()), recordings.snapshots()));
    }

    private static OptionalLong position(final CountersReader counters, final long recordingId) {
        final int counterId = RecordingPos.findCounterIdByRecording(counters, recordingId, Aeron.NULL_VALUE);
        if (counterId == CountersReader.NULL_COUNTER_ID) {
            return OptionalLong.empty();
        }
        final long position = counters.getCounterValue(counterId);
        return RecordingPos.isActive(counters, counterId, recordingId) ? OptionalLong.of(position) : OptionalLong.empty();
    }

    /**
     * The action on a failed force: log the error and halt the process with
     * {@link #DISK_FAILED_EXIT_CODE}. The relaunch recovers the log from
     * the disk and from the leader.
     */
    static Consumer<IOException> haltMember(final int memberId) {
        return e -> {
            System.out.println("cluster DISK SYNC FAILED memberId=" + memberId
                + " — the Raft log cannot be made durable; halting: " + e);
            System.out.flush();
            Runtime.getRuntime().halt(DISK_FAILED_EXIT_CODE);
        };
    }

    /** Start the thread: one round every {@code interval}. */
    void start(final Interval interval) {
        thread = new Thread(() -> runEvery(interval), "kardamom-durable-sync");
        thread.setDaemon(true);
        thread.start();
    }

    /** The latest published result. Any thread can read it. */
    Durability durability() {
        return durability;
    }

    /** Stop the thread and wait for it. */
    @Override
    public void close() throws InterruptedException {
        thread.interrupt();
        thread.join();
    }

    private void runEvery(final Interval interval) {
        while (interval.await()) {
            round();
        }
    }

    /** Run one round, and publish its result. */
    void round() {
        final long start = System.nanoTime();
        final Optional<View> seen = view.get();
        try {
            forceRound(seen);
            publish(seen.map(View::position).orElse(OptionalLong.empty()), 0, System.nanoTime() - start);
        } catch (final ClosedByInterruptException e) {
            Thread.currentThread().interrupt();
        } catch (final IOException e) {
            publish(OptionalLong.empty(), 1, System.nanoTime() - start);
            diskFailed.accept(e);
        }
    }

    private void forceRound(final Optional<View> seen) throws IOException {
        if (seen.isPresent()) {
            forceSnapshots(seen.get());
        }
        force(dirs.archive().resolve(CATALOG));
        force(dirs.cluster().resolve(RecordingLog.RECORDING_LOG_FILE_NAME));
        final Optional<View> after = view.get();
        if (after.isPresent()) {
            forceSnapshots(after.get());
        }
        if (seen.isPresent()) {
            forceLog(seen.get());
        }
        forceDir(dirs.archive());
    }

    private void publish(final OptionalLong position, final long errors, final long roundNs) {
        final Durability last = durability;
        durability = new Durability(position.orElse(last.position()), last.rounds() + 1, last.errors() + errors,
            roundNs, Math.max(last.maxRoundNs(), roundNs));
    }

    private void forceSnapshots(final View seen) throws IOException {
        final List<Long> fresh = seen.snapshots().stream().filter(id -> !forcedSnapshots.contains(id)).toList();
        for (final Path segment : segments(Set.copyOf(fresh))) {
            force(segment);
        }
        forcedSnapshots.addAll(fresh);
    }

    private void forceLog(final View seen) throws IOException {
        if (seen.logRecording() != logRecording) {
            logRecording = seen.logRecording();
            forcedBase = 0;
        }
        final TreeMap<Long, Path> segments = logSegments();
        for (final Path segment : segments.tailMap(forcedBase, true).values()) {
            force(segment);
        }
        seen.position().ifPresent(position -> Optional.ofNullable(segments.floorKey(position))
            .ifPresent(base -> forcedBase = base));
    }

    /** The segment files of the log recording, by base position. */
    private TreeMap<Long, Path> logSegments() throws IOException {
        try (Stream<Path> paths = Files.list(dirs.archive())) {
            return paths
                .map(p -> SEGMENT.matcher(p.getFileName().toString()))
                .filter(m -> m.matches() && Long.parseLong(m.group(1)) == logRecording)
                .collect(Collectors.toMap(m -> Long.parseLong(m.group(2)), m -> dirs.archive().resolve(m.group()),
                    (a, b) -> a, TreeMap::new));
        }
    }

    /** The segment files of the recordings {@code ids}. */
    private List<Path> segments(final Set<Long> ids) throws IOException {
        if (ids.isEmpty()) {
            return List.of();
        }
        try (Stream<Path> paths = Files.list(dirs.archive())) {
            return paths
                .map(p -> SEGMENT.matcher(p.getFileName().toString()))
                .filter(Matcher::matches)
                .filter(m -> ids.contains(Long.parseLong(m.group(1))))
                .map(m -> dirs.archive().resolve(m.group()))
                .toList();
        }
    }

    /** Force the data of one file. A file that the purge removed has nothing to force. */
    private static void force(final Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            channel.force(false);
        } catch (final NoSuchFileException e) {
            // The purge removed the file between the listing and the open.
        }
    }

    /** Force a directory, so the names of new segment files survive a power loss. */
    private static void forceDir(final Path dir) throws IOException {
        try (FileChannel channel = FileChannel.open(dir, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }
}
