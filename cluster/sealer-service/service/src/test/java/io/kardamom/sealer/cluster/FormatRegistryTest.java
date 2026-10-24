package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.SealerSeed;
import io.kardamom.sealer.VersionSwitch;
import io.kardamom.sealer.VoidLedger;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.agrona.SemanticVersion;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The sealer's format constants and readers against the format registry,
 * {@code formats.toml} at the repository root. The build passes the path
 * of the file in the {@code kardamom.formats} system property.
 *
 * <p>The build has no TOML library, so {@link Sections} reads only the
 * version lines of each {@code [format.<id>]} table. The Rust parser in
 * {@code kardamom-formats} checks the full file.</p>
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
    void snapshotWriterMatchesTheRegistry() {
        // A sealer that reads one snapshot version ahead of the version it
        // writes has a separate write constant. Otherwise one constant is both.
        assertEquals(staticInt(CanonicalSealerState.class, "SNAPSHOT_WRITE_VERSION", "SNAPSHOT_VERSION"),
            version("sealer-snapshot", "writes"));
    }

    @Test
    void snapshotLoaderReadsExactlyTheRegistryRange() {
        final int readsMin = version("sealer-snapshot", "reads_min");
        final int readsMax = version("sealer-snapshot", "reads_max");
        final int magic = staticInt(CanonicalSealerState.class, "SNAPSHOT_MAGIC");
        final List<Integer> read = IntStream.rangeClosed(0, readsMax + 1)
            .filter(snapshotVersion -> loaderAccepts(magic, snapshotVersion))
            .boxed()
            .toList();
        assertEquals(IntStream.rangeClosed(readsMin, readsMax).boxed().toList(), read);
    }

    /**
     * The bytes of a version-18 snapshot of a heap member are frozen: the
     * state of version 16, the carry window section, then the heap section.
     * A change of these bytes takes a new snapshot version. To write the
     * file again on purpose, run
     * {@code SEALER_GOLDEN_UPDATE=1 ./gradlew :service:test --tests '*FormatRegistryTest' --rerun-tasks}.
     */
    @Test
    void aVersionEighteenSnapshotHasTheGoldenBytes() throws IOException {
        final DriverlessLog log = new DriverlessLog();
        final DriverlessLog.Member member = log.member(Optional.empty());
        member.applyAll(versionEighteenLog());
        final byte[] snapshot = member.svc().snapshot();
        log.stop();
        assertEquals(CanonicalSealerState.WINDOW_SNAPSHOT_VERSION, ByteBuffer.wrap(snapshot).getInt(Integer.BYTES));
        final String hex = HexFormat.of().formatHex(snapshot);
        if ("1".equals(System.getenv("SEALER_GOLDEN_UPDATE"))) {
            Files.writeString(Path.of("src/test/resources", V18_GOLDEN), V18_HEADER + "\n"
                + String.join("\n", hex.split("(?<=\\G.{64})")) + "\n");
            return;
        }
        final String golden = Files.readAllLines(Path.of("src/test/resources", V18_GOLDEN)).stream()
            .filter(line -> !line.startsWith("#"))
            .collect(Collectors.joining());
        assertEquals(golden, hex, "the bytes of snapshot version 18 changed; a new layout takes a new version");
    }

    private static final String V18_GOLDEN = "sealer-snapshot-v18.hex";
    private static final String V18_HEADER =
        "# v18: the snapshot of a heap member that holds the carry window (FormatRegistryTest).";

    /**
     * Version 4 at epoch 2, version 5 at epoch 3, the carry window on at
     * epoch 4, then one full reference and one boundary under its rules.
     */
    private static List<DriverlessLog.Entry> versionEighteenLog() {
        return List.of(
            epochEntry(1, 1_000L),
            DriverlessLog.Entry.tick("tick-1", 1_250L),
            activationEntry(2, VersionSwitch.FEATURE_SEALER_V4, 1_300L),
            epochEntry(2, 1_400L),
            DriverlessLog.Entry.tick("tick-2", 1_500L),
            activationEntry(3, VersionSwitch.FEATURE_SEALER_V5, 1_550L),
            epochEntry(3, 1_600L),
            DriverlessLog.Entry.tick("tick-3", 1_750L),
            activationEntry(4, VersionSwitch.FEATURE_CARRY_WINDOW, 1_800L),
            epochEntry(4, 1_850L),
            DriverlessLog.Entry.tick("tick-4", 2_000L),
            DriverlessLog.Entry.frame("full", FullRef.of(30, 64).frame(), 2_100L),
            DriverlessLog.Entry.tick("tick-5", 2_250L));
    }

    private static DriverlessLog.Entry epochEntry(final long origin, final long ms) {
        return DriverlessLog.Entry.frame("epoch-" + origin, IngressFrames.originRecordFrame(
            IngressFrames.canonicalId(1000 + (int) origin), origin, 1, new byte[] {0x02}), ms);
    }

    private static DriverlessLog.Entry activationEntry(final long origin, final long feature, final long ms) {
        return DriverlessLog.Entry.frame("activation-" + feature,
            IngressFrames.featureActivationFrame(origin, feature, 0L), ms);
    }

    @Test
    void voidRecordTypeMatchesTheRegistry() {
        assertEquals(VoidLedger.RT_VOID, version("sealer-record-types", "writes"));
    }

    @Test
    void recordTypeReadersReachTheRegistryMaximum() {
        // The sealer names only the record types it writes or reads.
        assertEquals(version("sealer-record-types", "reads_max"),
            kinds("RT_", name -> true).getMax());
    }

    @Test
    void ingressKindsMatchTheRegistry() {
        // The sealer reads the recorded cursor and the feature activation,
        // but no service sends them.
        assertKinds("sealer-ingress-kinds", "KIND_",
            Set.of("KIND_RECORDED_CURSOR", "KIND_FEATURE_ACTIVATION"));
    }

    @Test
    void egressKindsMatchTheRegistry() {
        // The record-lag reject waits for recordLagBudget. The relayed
        // reference waits for carryBytes, and the feed gap for the member
        // feed.
        assertKinds("sealer-egress-kinds", "EGRESS_KIND_", Set.of(
            "EGRESS_KIND_RECORD_LAG_REJECT", "EGRESS_KIND_RELAYED_REF", "EGRESS_KIND_FEED_GAP"));
    }

    @Test
    void feedControlIsFrozenAtVersionOne() {
        assertExact("sealer-feed-control", 1);
        assertEquals(Byte.BYTES + 3 * Long.BYTES, SealerWire.FEED_REQUEST_LEN);
    }

    @Test
    void seedVersionMatchesTheRegistry() {
        assertExact("sealer-seed", staticInt(SealerSeed.class, "VERSION"));
    }

    @Test
    void appVersionMajorMatchesTheRegistry() {
        assertExact("cluster-app-version", SemanticVersion.major(ClusterNode.APP_VERSION));
    }

    /**
     * False when the loader refuses the snapshot version itself. The rest of
     * the snapshot is zeros, so a later field can still fail to parse.
     */
    private static boolean loaderAccepts(final int magic, final int snapshotVersion) {
        final byte[] snapshot = ByteBuffer.allocate(4096).putInt(magic).putInt(snapshotVersion).array();
        try {
            CanonicalSealerState.load(snapshot, 16);
            return true;
        } catch (final RuntimeException refused) {
            return !String.valueOf(refused.getMessage()).startsWith("unsupported snapshot version");
        }
    }

    private static int version(final String id, final String key) {
        return registry.get(id).get(key);
    }

    private static void assertExact(final String id, final int value) {
        assertEquals(Map.of("writes", value, "reads_min", value, "reads_max", value), registry.get(id), id);
    }

    /**
     * The sealer reads every kind from the lowest to the highest. The
     * default writers send every kind except the {@code unsent} ones.
     */
    private static void assertKinds(final String id, final String prefix, final Set<String> unsent) {
        final IntSummaryStatistics read = kinds(prefix, name -> true);
        final IntSummaryStatistics written = kinds(prefix, name -> !unsent.contains(name));
        assertEquals(
            Map.of("writes", written.getMax(), "reads_min", read.getMin(), "reads_max", read.getMax()),
            registry.get(id), id);
    }

    /**
     * The byte constants of {@link SealerWire} whose names start with
     * {@code prefix} and pass {@code keep}.
     */
    private static IntSummaryStatistics kinds(final String prefix, final Predicate<String> keep) {
        return Arrays.stream(SealerWire.class.getDeclaredFields())
            .filter(field -> Modifier.isStatic(field.getModifiers()))
            .filter(field -> field.getType() == byte.class && field.getName().startsWith(prefix))
            .filter(field -> keep.test(field.getName()))
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
