package io.kardamom.sealer.bench;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.DatagramSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Sealer members as local JVM processes, started from the sealer jar with
 * the settings of the deploy. Each member has its own directories and five
 * free UDP ports on 127.0.0.1.
 */
final class LocalCluster implements AutoCloseable {

    private static final int PORTS_PER_MEMBER = 5;
    private static final long START_TIMEOUT_MS = 60_000L;
    private static final Pattern PAUSE_MS = Pattern.compile("Pause.* ([0-9.]+)ms$");

    private final BenchConfig config;
    private final int[][] ports;
    private final List<Process> processes = new ArrayList<>();

    LocalCluster(final BenchConfig config) {
        this.config = config;
        this.ports = IntStream.range(0, config.members())
                .mapToObj(m -> IntStream.range(0, PORTS_PER_MEMBER).map(p -> freeUdpPort()).toArray())
                .toArray(int[][]::new);
    }

    /** Start every member and wait until each one logs that it is up. */
    void start() {
        IntStream.range(0, config.members()).forEach(m -> processes.add(spawn(m)));
        IntStream.range(0, config.members()).forEach(this::awaitUp);
    }

    /** The cluster client's ingress endpoints: {@code id=host:port} for each member. */
    String ingressEndpoints() {
        return IntStream.range(0, config.members())
                .mapToObj(m -> m + "=127.0.0.1:" + ports[m][0])
                .collect(Collectors.joining(","));
    }

    /** The GC pause count, sum and maximum of each member, from its GC log. */
    List<String> gcSummaries() {
        return IntStream.range(0, config.members()).mapToObj(this::gcSummary).toList();
    }

    @Override
    public void close() {
        processes.forEach(Process::destroy);
        processes.forEach(LocalCluster::awaitExit);
    }

    private String members() {
        return IntStream.range(0, config.members())
                .mapToObj(m -> m + "," + IntStream.of(ports[m])
                        .mapToObj(p -> "127.0.0.1:" + p)
                        .collect(Collectors.joining(",")))
                .collect(Collectors.joining("|"));
    }

    private Process spawn(final int member) {
        final Path root = config.workDir().resolve("sealer-" + member);
        final List<String> cmd = new ArrayList<>(List.of("java"));
        cmd.addAll(config.memberJvm());
        cmd.addAll(List.of(
                "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
                "--add-opens", "java.base/java.util.zip=ALL-UNNAMED",
                "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
                "-Xlog:gc:file=" + gcLog(member),
                "-Dkardamom.cluster.memberId=" + member,
                "-Dkardamom.cluster.members=" + members(),
                "-Daeron.dir=" + root.resolve("aeron"),
                "-Dkardamom.cluster.dir=" + root.resolve("cluster"),
                "-Dkardamom.archive.dir=" + root.resolve("archive"),
                "-Dkardamom.cluster.bootstrap=true",
                "-Dkardamom.cluster.ingressStreamId=101",
                "-Dkardamom.cluster.daLagBudgetBlocks=0"));
        cmd.addAll(config.memberProps());
        cmd.addAll(List.of("-cp", config.jar().toString(), "io.kardamom.sealer.cluster.ClusterNode"));
        try {
            Files.createDirectories(root);
            return new ProcessBuilder(cmd)
                    .redirectErrorStream(true)
                    .redirectOutput(memberLog(member).toFile())
                    .start();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void awaitUp(final int member) {
        final long deadline = System.currentTimeMillis() + START_TIMEOUT_MS;
        while (!logContains(member, "cluster node up")) {
            waitAlive(member, deadline);
        }
    }

    /** Wait a moment for a member that is still alive and within its start time. */
    private void waitAlive(final int member, final long deadline) {
        if (!processes.get(member).isAlive() || System.currentTimeMillis() > deadline) {
            throw new IllegalStateException("sealer member " + member + " did not start; see " + memberLog(member));
        }
        sleepQuietly(100);
    }

    private boolean logContains(final int member, final String text) {
        try {
            return Files.exists(memberLog(member)) && Files.readString(memberLog(member)).contains(text);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String gcSummary(final int member) {
        final double[] pauses = readLines(gcLog(member)).stream()
                .map(PAUSE_MS::matcher)
                .filter(Matcher::find)
                .mapToDouble(m -> Double.parseDouble(m.group(1)))
                .toArray();
        final double sum = java.util.Arrays.stream(pauses).sum();
        final double max = java.util.Arrays.stream(pauses).max().orElse(0);
        return String.format("member-%d gc pauses=%d total=%.1fms max=%.1fms", member, pauses.length, sum, max);
    }

    private static List<String> readLines(final Path path) {
        try {
            return Files.exists(path) ? Files.readAllLines(path) : List.of();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path memberLog(final int member) {
        return config.workDir().resolve("sealer-" + member + ".log");
    }

    private Path gcLog(final int member) {
        return config.workDir().resolve("sealer-" + member + "-gc.log");
    }

    private static int freeUdpPort() {
        try (DatagramSocket socket = new DatagramSocket(0)) {
            return socket.getLocalPort();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void awaitExit(final Process process) {
        try {
            if (!process.waitFor(10, TimeUnit.SECONDS)) {
                process.destroyForcibly();
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static void sleepQuietly(final long ms) {
        try {
            Thread.sleep(ms);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
