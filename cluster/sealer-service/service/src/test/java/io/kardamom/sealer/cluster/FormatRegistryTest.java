package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.SealerSeed;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.IntSummaryStatistics;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.agrona.SemanticVersion;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The sealer's format constants against the format registry,
 * {@code formats.toml} at the repository root. The build passes the path
 * of the file in the {@code kardamom.formats} system property.
 */
final class FormatRegistryTest {
    private static final Pattern HEADER = Pattern.compile("^\\[(.*)\\]$");
    private static final Pattern FORMAT = Pattern.compile("^format\\.([a-z0-9-]+)$");
    private static final Pattern VERSION = Pattern.compile("^(writes|reads_min|reads_max) = (\\d+)$");

    /** The versions of each format, by format id and then by key. */
    private static Map<String, Map<String, Integer>> registry;

    @BeforeAll
    static void readRegistry() throws IOException {
        final Sections sections = new Sections();
        Files.readAllLines(Path.of(System.getProperty("kardamom.formats"))).forEach(sections::accept);
        registry = sections.formats;
    }

    @Test
    void snapshotVersionsMatchTheRegistry() {
        // A sealer that reads one snapshot version ahead of the version it
        // writes has two constants. Otherwise one constant is both.
        final Class<?> state = CanonicalSealerState.class;
        assertEquals(staticInt(state, "SNAPSHOT_WRITE_VERSION", "SNAPSHOT_VERSION"),
            version("sealer-snapshot", "writes"));
        assertEquals(staticInt(state, "SNAPSHOT_READ_VERSION", "SNAPSHOT_VERSION"),
            version("sealer-snapshot", "reads_max"));
    }

    @Test
    void ingressKindsMatchTheRegistry() {
        assertKinds("sealer-ingress-kinds", kinds("KIND_"));
    }

    @Test
    void egressKindsMatchTheRegistry() {
        assertKinds("sealer-egress-kinds", kinds("EGRESS_KIND_"));
    }

    @Test
    void seedVersionMatchesTheRegistry() {
        assertExact("sealer-seed", staticInt(SealerSeed.class, "VERSION"));
    }

    @Test
    void appVersionMajorMatchesTheRegistry() {
        assertExact("cluster-app-version", SemanticVersion.major(ClusterNode.APP_VERSION));
    }

    private static int version(final String id, final String key) {
        return registry.get(id).get(key);
    }

    private static void assertExact(final String id, final int value) {
        assertEquals(Map.of("writes", value, "reads_min", value, "reads_max", value), registry.get(id), id);
    }

    /** The sealer writes and reads every kind from the lowest to the highest. */
    private static void assertKinds(final String id, final IntSummaryStatistics kinds) {
        assertEquals(
            Map.of("writes", kinds.getMax(), "reads_min", kinds.getMin(), "reads_max", kinds.getMax()),
            registry.get(id), id);
    }

    /** The byte constants of {@link SealerWire} whose names start with {@code prefix}. */
    private static IntSummaryStatistics kinds(final String prefix) {
        return Arrays.stream(SealerWire.class.getDeclaredFields())
            .filter(field -> Modifier.isStatic(field.getModifiers()))
            .filter(field -> field.getType() == byte.class && field.getName().startsWith(prefix))
            .mapToInt(field -> read(field, () -> field.getByte(null)))
            .summaryStatistics();
    }

    /** The static int field of {@code type} with the first name in {@code names} that exists. */
    private static int staticInt(final Class<?> type, final String... names) {
        return Arrays.stream(names)
            .map(name -> field(type, name))
            .flatMap(Optional::stream)
            .findFirst()
            .map(field -> read(field, () -> field.getInt(null)))
            .orElseThrow(() -> new AssertionError("no field " + Arrays.toString(names) + " in " + type));
    }

    private static Optional<Field> field(final Class<?> type, final String name) {
        try {
            return Optional.of(type.getDeclaredField(name));
        } catch (final NoSuchFieldException absent) {
            return Optional.empty();
        }
    }

    private interface Getter {
        int get() throws IllegalAccessException;
    }

    private static int read(final Field field, final Getter getter) {
        field.setAccessible(true);
        try {
            return getter.get();
        } catch (final IllegalAccessException e) {
            throw new AssertionError(e);
        }
    }

    /** Collects the version lines of each {@code [format.<id>]} table. */
    private static final class Sections {
        private final Map<String, Map<String, Integer>> formats = new HashMap<>();
        /** The table of the lines that follow. A table that is not a format collects into a map nobody reads. */
        private Map<String, Integer> current = new HashMap<>();

        void accept(final String raw) {
            final String line = raw.strip();
            final Matcher header = HEADER.matcher(line);
            if (header.matches()) {
                final Matcher format = FORMAT.matcher(header.group(1));
                current = format.matches()
                    ? formats.computeIfAbsent(format.group(1), id -> new HashMap<>())
                    : new HashMap<>();
                return;
            }
            final Matcher version = VERSION.matcher(line);
            if (version.matches()) {
                current.put(version.group(1), Integer.parseInt(version.group(2)));
            }
        }
    }
}
