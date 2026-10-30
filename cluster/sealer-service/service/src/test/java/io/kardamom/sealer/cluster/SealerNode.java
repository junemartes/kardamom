package io.kardamom.sealer.cluster;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.agrona.CloseHelper;

/**
 * One sealer node of a {@link LocalSealers} group: a member process started
 * from the production main class, its directories, its address on the
 * loopback network, and the shared media driver of the node, which carries
 * the member feed. A start appends a new log file, so the evidence of each
 * start stays apart, like the logs of a Nomad allocation.
 */
final class SealerNode implements AutoCloseable {

    /** The cluster endpoints of a member: ingress, consensus, log, catch-up, archive. */
    static final int ENDPOINTS = 5;
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build();
    private static final Pattern FIELD = Pattern.compile("\"(\\w+)\":\"?([^,\"}]*)\"?");

    /** The admin status of a member, as {@code /status} answers it. */
    record View(int memberId, String role, String election, long commitPosition, long servicePosition, long term) {
        boolean isLeader() {
            return "LEADER".equals(role) && "CLOSED".equals(election);
        }

        boolean isFollower() {
            return "FOLLOWER".equals(role) && "CLOSED".equals(election);
        }

        static View parse(final String json) {
            final Matcher m = FIELD.matcher(json);
            final java.util.Map<String, String> f = new java.util.HashMap<>();
            while (m.find()) {
                f.put(m.group(1), m.group(2));
            }
            return new View(Integer.parseInt(f.get("memberId")), f.get("role"), f.get("election"),
                Long.parseLong(f.get("commitPosition")), Long.parseLong(f.get("servicePosition")),
                Long.parseLong(f.get("leadershipTermId")));
        }
    }

    final int id;
    final String name;
    private String ip;
    private final int[] ports;
    private final int blockFirst;
    private final int blockLast;
    private final Path root;
    private final Path shm;
    private final LocalSealers group;
    private final List<Path> logs = new ArrayList<>();
    private MediaDriver feedDriver;
    private Process process;

    SealerNode(final LocalSealers group, final int id, final String ip, final int[] ports, final int[] block,
               final Path root, final Path shm) {
        this.group = group;
        this.id = id;
        this.name = "sealer-" + id + ".kardamom.test";
        this.ip = ip;
        this.ports = ports;
        this.blockFirst = block[0];
        this.blockLast = block[1];
        this.root = root;
        this.shm = shm;
    }

    String ip() {
        return ip;
    }

    int port(final int endpoint) {
        return ports[endpoint];
    }

    /** The members-string entry of this node: its id and its five endpoints by name. */
    String memberEntry() {
        return id + "," + IntStream.of(ports).mapToObj(p -> name + ":" + p).collect(Collectors.joining(","));
    }

    /** The owner entry of the link faults: the address and the wildcard port block of this node. */
    String ownerEntry() {
        return id + "=" + ip + "/" + blockFirst + "-" + blockLast;
    }

    /** The member state lives on tmpfs: a process kill keeps it, as the page cache keeps a disk write. */
    Path clusterDir() {
        return shm.resolve("cluster");
    }

    Path archiveDir() {
        return shm.resolve("archive");
    }

    String feedDir() {
        return shm.resolve("feed").toString();
    }

    /** Start the member process. {@code extra} adds system properties of this start only. */
    void start(final List<String> extra) {
        ensureFeedDriver();
        final Path log = root.resolve("start-" + logs.size() + ".log");
        logs.add(log);
        final int half = (blockLast - blockFirst + 1) / 2;
        final List<String> cmd = new ArrayList<>(List.of(
            ProcessHandle.current().info().command().orElse("java"),
            "-Xmx384m", "-XX:+EnableDynamicAgentLoading",
            "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
            "--add-opens", "java.base/java.util.zip=ALL-UNNAMED",
            "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
            "-Djdk.net.hosts.file=" + group.hostsFile(),
            // A node replacement takes minutes in the deploy, so the JVM's
            // 30 s DNS cache of the old address has expired before the new
            // member starts. A 1 s cache lets the test start it at once.
            "-Dsun.net.inetaddr.ttl=1",
            "-Daeron.mtu.length=1344",
            "-Daeron.dir=" + shm.resolve("aeron"),
            "-Daeron.sender.wildcard.port.range=" + blockFirst + " " + (blockFirst + half - 1),
            "-Daeron.receiver.wildcard.port.range=" + (blockFirst + half) + " " + blockLast,
            "-Daeron.ReceiveChannelEndpoint.supplier=" + FaultyReceiveEndpoint.Supplier.class.getName(),
            "-Daeron.SendChannelEndpoint.supplier=" + FaultySendEndpoint.Supplier.class.getName(),
            "-Dkardamom.test.links.plan=" + group.planFile(),
            "-Dkardamom.test.links.self=" + id,
            "-Dkardamom.test.parentPid=" + ProcessHandle.current().pid(),
            "-Dkardamom.cluster.clockClass=" + OffsetClusterClock.class.getName(),
            "-Dkardamom.test.clockOffsetFile=" + group.clockFile(),
            "-Dkardamom.test.links.owners=" + group.owners(),
            "-Dkardamom.cluster.memberId=" + id,
            "-Dkardamom.cluster.nodeIp=" + ip,
            "-Dkardamom.cluster.members=" + group.members(),
            "-Dkardamom.cluster.dir=" + clusterDir(),
            "-Dkardamom.archive.dir=" + archiveDir(),
            "-Dkardamom.cluster.adminPort=" + group.adminPort(),
            "-Dkardamom.cluster.adminHost=" + ip,
            "-Dkardamom.cluster.feedAeronDir=" + feedDir(),
            "-Dkardamom.cluster.feedRingBytes=" + FeedConfig.MIN_RING_BYTES));
        cmd.addAll(group.settings().properties());
        cmd.addAll(extra);
        cmd.addAll(List.of("-cp", System.getProperty("kardamom.testClasspath"), ClusterNode.class.getName()));
        try {
            Files.createDirectories(root);
            process = new ProcessBuilder(cmd).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    boolean isAlive() {
        return process != null && process.isAlive();
    }

    long pid() {
        return process.pid();
    }

    /** SIGKILL the member process and wait for its exit: a crash with no shutdown step. */
    void kill() {
        if (process != null) {
            process.destroyForcibly();
            awaitExit();
        }
    }

    /** Ask the member process to stop with SIGTERM, as a job stop does. {@link #awaitStopped} waits for it. */
    void stop() {
        if (process != null && process.isAlive()) {
            process.destroy();
        }
    }

    void awaitStopped() {
        if (process != null) {
            awaitExit();
        }
    }

    /**
     * Copy the cluster and archive directories of the stopped {@code donor}
     * into this stopped node, without the process mark files, and check the
     * hash of every copied file. The copy is one immutable recovery image.
     */
    void copyFrom(final SealerNode donor) {
        wipe();
        copyTree(donor.clusterDir(), clusterDir());
        copyTree(donor.archiveDir(), archiveDir());
    }

    private static void copyTree(final Path from, final Path to) {
        try (Stream<Path> paths = Files.walk(from)) {
            paths.filter(p -> !p.getFileName().toString().matches("(cluster|archive)-mark.*"))
                .forEach(p -> copyChecked(p, to.resolve(from.relativize(p))));
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void copyChecked(final Path source, final Path target) {
        try {
            if (Files.isDirectory(source)) {
                Files.createDirectories(target);
                return;
            }
            Files.copy(source, target);
            if (!java.util.Arrays.equals(sha256(source), sha256(target))) {
                throw new AssertionError("the offline copy of " + source + " has another hash");
            }
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static byte[] sha256(final Path file) throws IOException {
        try {
            return java.security.MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file));
        } catch (final java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Send {@code signal} to the member process, for example {@code STOP} or {@code CONT}. */
    void signal(final String signal) {
        try {
            new ProcessBuilder("kill", "-" + signal, Long.toString(process.pid())).start().waitFor();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    /**
     * Wait for the process to exit. An interrupt, such as the test deadline,
     * does not stop the wait: a member left running would hold its ports and
     * its tmpfs. The interrupt stays set for the caller.
     */
    private void awaitExit() {
        final boolean interrupted = Thread.interrupted();
        try {
            if (!process.waitFor(20, TimeUnit.SECONDS)) {
                throw new AssertionError("member " + id + " did not exit");
            }
        } catch (final InterruptedException e) {
            process.destroyForcibly();
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** Kill the process without a wait: the last cleanup of a test JVM that exits. */
    void killNow() {
        if (process != null) {
            process.destroyForcibly();
        }
    }

    /** Delete the cluster and archive directories: the member keeps no Raft log, snapshot or seed. */
    void wipe() {
        deleteTree(clusterDir());
        deleteTree(archiveDir());
    }

    /**
     * Replace the node: the member moves to {@code newIp} with empty volumes
     * and a new shared driver. The test then moves its name in the hosts file.
     */
    void replace(final String newIp) {
        wipe();
        CloseHelper.quietClose(feedDriver);
        feedDriver = null;
        ip = newIp;
    }

    /** The status of the member, or empty when its admin endpoint does not answer. */
    Optional<View> view() {
        try {
            final HttpResponse<String> answer = HTTP.send(HttpRequest.newBuilder(
                    URI.create("http://" + ip + ":" + group.adminPort() + "/status"))
                .timeout(Duration.ofSeconds(1)).build(), HttpResponse.BodyHandlers.ofString());
            return answer.statusCode() == 200 ? Optional.of(View.parse(answer.body())) : Optional.empty();
        } catch (final IOException e) {
            return Optional.empty();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    /** The count of starts so far. */
    int starts() {
        return logs.size();
    }

    /** The log of start {@code start}, and of every later start. */
    String logSince(final int start) {
        return logs.subList(Math.min(start, logs.size()), logs.size()).stream()
            .map(SealerNode::read).collect(Collectors.joining());
    }

    String log() {
        return logSince(0);
    }

    private static String read(final Path path) {
        try {
            return Files.exists(path) ? Files.readString(path) : "";
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void ensureFeedDriver() {
        if (feedDriver == null) {
            feedDriver = MediaDriver.launch(new MediaDriver.Context()
                .aeronDirectoryName(feedDir())
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true));
        }
    }

    static void deleteTree(final Path dir) {
        if (!Files.exists(dir)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(dir)) {
            paths.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        if (process != null && process.isAlive()) {
            process.destroyForcibly();
            awaitExit();
        }
        CloseHelper.quietClose(feedDriver);
    }
}
