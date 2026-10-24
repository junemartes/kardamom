package io.kardamom.sealer.bench;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

/**
 * The settings of one bench run, read once from {@code bench.*} system
 * properties. Every count is checked here, so the rest of the bench uses
 * the values as they are.
 */
record BenchConfig(
        int members,
        long ratePerTwin,
        int batch,
        int twins,
        long warmupSeconds,
        long seconds,
        int senders,
        Path workDir,
        Path jar,
        List<String> memberJvm,
        List<String> memberProps,
        boolean v4Ids) {

    /** Read the settings from the system properties. */
    static BenchConfig fromProperties() {
        final BenchConfig config = new BenchConfig(
                Integer.getInteger("bench.members", 3),
                Long.getLong("bench.rate", 100_000L),
                Integer.getInteger("bench.batch", 64),
                Integer.getInteger("bench.twins", 2),
                Long.getLong("bench.warmupSeconds", 3L),
                Long.getLong("bench.seconds", 10L),
                Integer.getInteger("bench.senders", 4096),
                Path.of(System.getProperty("bench.dir", "build/sealer-bench")).toAbsolutePath(),
                Path.of(required("bench.jar")),
                words(setting("bench.memberJvm", "BENCH_MEMBER_JVM", "-Xmx384m -XX:+UseSerialGC")),
                words(setting("bench.memberProps", "BENCH_MEMBER_PROPS", "")),
                Boolean.getBoolean("bench.v4Ids"));
        config.check();
        return config;
    }

    /** Whether the twins offer at the fixed rate; otherwise they offer as fast as the cluster takes. */
    boolean openLoop() {
        return ratePerTwin > 0;
    }

    /** The records each twin offers, warm-up included. */
    long totalRecords() {
        final long perSecond = openLoop() ? ratePerTwin : 2_000_000L;
        return Math.multiplyExact(perSecond, warmupSeconds + seconds) / batch * batch;
    }

    /** The time between two batches of one twin, in nanoseconds; 0 in a closed loop. */
    long batchIntervalNs() {
        return openLoop() ? 1_000_000_000L * batch / ratePerTwin : 0L;
    }

    private void check() {
        require(members == 1 || members == 3, "bench.members must be 1 or 3");
        require(ratePerTwin >= 0, "bench.rate must not be negative");
        require(batch > 0 && batch <= 1024, "bench.batch must be from 1 to 1024");
        require(twins > 0 && twins <= 4, "bench.twins must be from 1 to 4");
        require(seconds > 0 && warmupSeconds >= 0, "bench.seconds must be positive");
        require(senders > 0, "bench.senders must be positive");
    }

    private static void require(final boolean ok, final String message) {
        if (!ok) {
            throw new IllegalArgumentException(message);
        }
    }

    private static String required(final String name) {
        final String value = System.getProperty(name);
        require(value != null && !value.isBlank(), name + " must be set");
        return value;
    }

    /** A system property, else an environment variable, else {@code fallback}. */
    private static String setting(final String property, final String env, final String fallback) {
        return System.getProperty(property, System.getenv().getOrDefault(env, fallback));
    }

    private static List<String> words(final String raw) {
        return Arrays.stream(raw.trim().split("\\s+")).filter(w -> !w.isEmpty()).toList();
    }
}
