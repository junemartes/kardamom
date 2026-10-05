package io.kardamom.sealer.cluster;

import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.RecordingLog;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * One directory of member state: the consensus module directory or the
 * archive directory. The directory itself stays in place when its content
 * goes, because the deploy can bind-mount it into the container.
 *
 * <p>The recording log methods read {@code root} as a consensus module
 * directory.</p>
 */
record StateDir(Path root) {

    /** Each Aeron process writes its own mark file when it starts. */
    private static final String MARK_FILE_SUFFIX = "-mark.dat";

    /**
     * Whether the directory holds a recording log with at least one entry.
     * A member without one has no term and no snapshot, so it holds no
     * cluster state.
     */
    boolean holdsRecordingLog() {
        return Files.exists(root.resolve(RecordingLog.RECORDING_LOG_FILE_NAME))
                && withRecordingLog(log -> !log.entries().isEmpty());
    }

    /** The log position of the latest consensus module snapshot, or -1 when there is none. */
    long latestSnapshotPosition() {
        return withRecordingLog(log -> Optional
                .ofNullable(log.getLatestSnapshot(ConsensusModule.Configuration.SERVICE_ID))
                .map(entry -> entry.logPosition)
                .orElse(AeronArchive.NULL_POSITION));
    }

    /** Delete everything under the directory. Create the directory when it is missing. */
    void clear() {
        try {
            Files.createDirectories(root);
            try (Stream<Path> paths = Files.walk(root)) {
                paths.filter(path -> !path.equals(root))
                        .sorted(Comparator.reverseOrder())
                        .forEach(path -> unchecked(() -> Files.delete(path)));
            }
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    /** Delete the mark files that a closed process left in the directory. */
    void dropMarkFiles() {
        try (Stream<Path> paths = Files.list(root)) {
            paths.filter(path -> path.getFileName().toString().endsWith(MARK_FILE_SUFFIX))
                    .forEach(path -> unchecked(() -> Files.delete(path)));
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private <T> T withRecordingLog(final Function<RecordingLog, T> read) {
        try (RecordingLog log = new RecordingLog(root.toFile(), false)) {
            return read.apply(log);
        }
    }

    /** One file system step that can throw a checked exception. */
    @FunctionalInterface
    private interface IoStep {
        void run() throws IOException;
    }

    /** Run {@code step} and rethrow its {@link IOException} unchecked. */
    private static void unchecked(final IoStep step) {
        try {
            step.run();
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }
}
