package io.kardamom.sealer.cluster;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

/**
 * How a member starts. {@link ClusterNode} decides it once, before the
 * launch.
 *
 * <p>A blank member holds no recording log entry. It starts at log
 * position 0 only on the bootstrap of a new cluster. Every other blank
 * member first copies the latest snapshot from a peer: a member that
 * starts at position 0 in a running cluster cannot catch up after the
 * leader purges its log, and blank members that elect each other start a
 * second history.</p>
 *
 * <p>Two inputs turn the bootstrap on, and an unset input is off:</p>
 * <ul>
 *   <li>{@code -Dkardamom.cluster.bootstrap=true}, for a launcher that
 *       always starts a new cluster, such as a local test stack.</li>
 *   <li>The file {@code bootstrap} in the Nomad task directory
 *       ({@code $NOMAD_TASK_DIR}) with the content {@code true}. The job
 *       renders this file from a Nomad variable that only the bootstrap
 *       deploy writes and then deletes. The member reads the file at each
 *       start, so a restart after the bootstrap reads it empty.</li>
 * </ul>
 */
enum StartMode {
    /** The member holds a recording log and starts from it. */
    RESUME("the member starts from its own recording log"),
    /** A blank member of a new cluster starts at log position 0. */
    GENESIS("BOOTSTRAP of a new cluster: this blank member starts at log position 0"),
    /** A blank member copies the latest snapshot from a peer, then starts from it. */
    SEED_FROM_PEER("blank member: it copies the latest snapshot from a peer before it starts");

    /** The reason that the start log line gives. */
    final String note;

    StartMode(final String note) {
        this.note = note;
    }

    static final String BOOTSTRAP_PROPERTY = "kardamom.cluster.bootstrap";
    static final String BOOTSTRAP_FILE = "bootstrap";

    /** The start mode of this process for the member state in {@code clusterDir}. */
    static StartMode decide(final StateDir clusterDir) {
        final Optional<Path> file = Optional.ofNullable(System.getenv("NOMAD_TASK_DIR"))
                .map(dir -> Path.of(dir, BOOTSTRAP_FILE));
        return of(clusterDir.holdsRecordingLog(), bootstrap(System.getProperty(BOOTSTRAP_PROPERTY), file));
    }

    static StartMode of(final boolean holdsRecordingLog, final boolean bootstrap) {
        if (holdsRecordingLog) {
            return RESUME;
        }
        return bootstrap ? GENESIS : SEED_FROM_PEER;
    }

    /** Whether the property or the bootstrap file turns the bootstrap on. */
    static boolean bootstrap(final String property, final Optional<Path> file) {
        return parseFlag(property, "-D" + BOOTSTRAP_PROPERTY)
                || file.filter(Files::exists)
                        .map(path -> parseFlag(read(path), path.toString()))
                        .orElse(false);
    }

    /**
     * Parse one bootstrap input: {@code true}, {@code false}, or blank for
     * off. Any other text is fatal, so a typo cannot pick a start mode.
     */
    static boolean parseFlag(final String raw, final String source) {
        final String value = raw == null ? "" : raw.trim();
        if (value.isEmpty() || value.equals("false")) {
            return false;
        }
        if (value.equals("true")) {
            return true;
        }
        throw new IllegalStateException(source + ": '" + value + "' is not true or false");
    }

    private static String read(final Path path) {
        try {
            return Files.readString(path);
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }
}
