package io.kardamom.sealer.cluster;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

/**
 * Seeds a stopped member's TestCluster directories from a closed
 * ClusterBackup node. The backup's recording log becomes the member's
 * consensus module directory, and the backup's archive becomes the member's
 * archive. The recording log keeps each entry's real log position, so the
 * member restores the snapshot and then catches up from the running leader.
 * Mark files stay behind: each process writes its own mark file when it
 * starts.
 */
final class BackupSeed {

    private static final String MARK_FILE_SUFFIX = "-mark.dat";

    private final Path backupDir;

    BackupSeed(final Path backupDir) {
        this.backupDir = backupDir;
    }

    /** Replace everything under {@code memberDir} with the backup's state. */
    void copyInto(final Path memberDir) throws IOException {
        deleteTree(memberDir);
        copyTree(backupDir.resolve("cluster-backup"), memberDir.resolve("consensus-module"));
        copyTree(backupDir.resolve("archive"), memberDir.resolve("archive"));
    }

    private static void deleteTree(final Path root) throws IOException {
        try (Stream<Path> paths = Files.walk(root)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> unchecked(() -> Files.delete(path)));
        }
    }

    private static void copyTree(final Path from, final Path to) throws IOException {
        Files.createDirectories(to);
        try (Stream<Path> paths = Files.list(from)) {
            paths.filter(path -> !path.getFileName().toString().endsWith(MARK_FILE_SUFFIX))
                    .forEach(path -> unchecked(() -> Files.copy(path, to.resolve(path.getFileName()))));
        }
    }

    /** One file system step that can throw a checked exception. */
    @FunctionalInterface
    private interface IoStep {
        void run() throws IOException;
    }

    private static void unchecked(final IoStep step) {
        try {
            step.run();
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }
}
