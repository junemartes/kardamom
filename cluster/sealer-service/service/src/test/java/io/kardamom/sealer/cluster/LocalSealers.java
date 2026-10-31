package io.kardamom.sealer.cluster;

import io.aeron.driver.DefaultNameResolver;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Real sealer members as separate JVM processes on one host, started from
 * the production main class with the settings of the deploy. Each node has
 * its own loopback address and directories, and the members name each
 * other by host names from a hosts file, as the deploy names them through
 * Consul. Each member's media driver loads the {@link LinkFaults}, so a test
 * can cut, lose or delay the frames of one member to its peers.
 *
 * <p>Every group takes its own addresses and a free block of ports, so
 * groups of tests in parallel processes do not meet.</p>
 */
final class LocalSealers implements AutoCloseable {

    /** The name prefix of the tmpfs directory of a group: the prefix, the pid of the test JVM, a dash. */
    private static final String PREFIX = "kardamom-sealers-";
    /** The free tmpfs that a group of three members needs: its Aeron logs, archives and feeds. */
    private static final long TMPFS_ROOM = 2L << 30;
    /** The ports of one group: cluster endpoints, the admin port and the wildcard blocks. */
    private static final int PORT_SPAN = 500;
    private static final int BLOCK = 40;
    private static final Pattern TERM = Pattern.compile(
        "cluster TERM memberId=(\\d+) leadershipTermId=(\\d+) leaderMemberId=(\\d+) logPosition=(\\d+)");

    /** The replicated settings that every member of the group starts with. */
    record Settings(long tickMs, long snapshotIntervalS, int logPurgeKeepSnapshots) {
        /** The deployed tick of 2 s: the inclusion horizon of 64 blocks then holds an id for 128 s. */
        static final long TICK_MS = 2000;
        static final Settings DEFAULT = new Settings(TICK_MS, 0, 0);
        /** A snapshot on each block, so each test reaches a snapshot within seconds. */
        static final Settings SNAPSHOTS = new Settings(TICK_MS, TICK_MS / 1000, 0);

        List<String> properties() {
            return List.of(
                "-Dkardamom.cluster.ingressStreamId=101",
                "-Dkardamom.cluster.tickMs=" + tickMs,
                "-Dkardamom.cluster.snapshotIntervalS=" + snapshotIntervalS,
                "-Dkardamom.cluster.logPurgeKeepSnapshots=" + logPurgeKeepSnapshots);
        }
    }

    private final Settings settings;
    private final Path dir;
    private final Path shm;
    private final int base;
    private final String net;
    private final List<SealerNode> nodes = new ArrayList<>();
    private Duration clockOffset = Duration.ZERO;
    /** Kills the members and frees the tmpfs when the test JVM exits before {@link #close}. */
    private final Thread cleanup = new Thread(this::release, "kardamom-sealers-cleanup");

    LocalSealers(final Path dir, final int members, final Settings settings) {
        Stage.begin();
        System.out.println("sealer group directory: " + dir);
        this.settings = settings;
        this.dir = dir;
        final Path shared = Path.of("/dev/shm");
        if (Files.isDirectory(shared)) {
            sweepStale(shared);
        }
        // Member state lives on tmpfs where it has room: a process kill keeps
        // the page cache, so the fault stays the same, and the fsync latency
        // of a busy disk stays out of the recovery bounds.
        final Path tmpfs = hasRoom(shared) ? shared : dir;
        this.shm = tmpfs.resolve(PREFIX + ProcessHandle.current().pid() + "-" + System.nanoTime());
        this.base = freePortSpan();
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        this.net = "127." + random.nextInt(1, 255) + "." + random.nextInt(0, 255) + ".";
        IntStream.range(0, members).forEach(m -> nodes.add(new SealerNode(this, m, address(m + 2),
            IntStream.range(0, SealerNode.ENDPOINTS).map(p -> base + 10 * m + p).toArray(),
            new int[] {base + 100 + BLOCK * m, base + 100 + BLOCK * m + BLOCK - 1},
            dir.resolve("sealer-" + m), shm.resolve("sealer-" + m))));
        Runtime.getRuntime().addShutdownHook(cleanup);
        writeHosts();
        links(Map.of());
        advanceClock(Duration.ZERO);
    }

    /** A loopback address of this group. */
    String address(final int host) {
        return net + host;
    }

    Settings settings() {
        return settings;
    }

    SealerNode node(final int id) {
        return nodes.get(id);
    }

    List<SealerNode> nodes() {
        return List.copyOf(nodes);
    }

    int adminPort() {
        return base + 90;
    }

    Path hostsFile() {
        return dir.resolve("hosts");
    }

    Path clockFile() {
        return dir.resolve("clock-offset-ms");
    }

    /**
     * Move the cluster clock of every member forward by {@code step}: the
     * Raft timeouts that run see {@code step} pass at once.
     */
    void advanceClock(final Duration step) {
        clockOffset = clockOffset.plus(step);
        write(clockFile(), Long.toString(clockOffset.toMillis()));
    }

    Path planFile() {
        return dir.resolve("links");
    }

    String members() {
        return nodes.stream().map(SealerNode::memberEntry).collect(Collectors.joining("|"));
    }

    String owners() {
        return nodes.stream().map(SealerNode::ownerEntry).collect(Collectors.joining(";"));
    }

    /**
     * A media driver for a client of the group. It resolves the member names
     * through the hosts file of the group, as a client of the deploy resolves
     * them through Consul: a new-leader event names the leader's ingress by
     * its host name.
     */
    MediaDriver clientDriver(final Path aeronDir) {
        return MediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(aeronDir.toString())
            .threadingMode(ThreadingMode.SHARED)
            .nameResolver(this::resolve)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true));
    }

    /** The address of a member name in the hosts file, or the default lookup for any other name. */
    private InetAddress resolve(final String name, final String uriParamName, final boolean isReResolution) {
        try {
            return Files.readAllLines(hostsFile()).stream().map(l -> l.split(" "))
                .filter(f -> f.length == 2 && f[1].equals(name)).findFirst()
                .map(f -> addressOf(f[0]))
                .orElseGet(() -> DefaultNameResolver.INSTANCE.resolve(name, uriParamName, isReResolution));
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static InetAddress addressOf(final String ip) {
        try {
            return InetAddress.getByName(ip);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** The ingress endpoints of the cluster client, by address. */
    String ingressEndpoints() {
        return nodes.stream().map(n -> n.id + "=" + n.ip() + ":" + n.port(0)).collect(Collectors.joining(","));
    }

    /** Start every member: a new cluster takes the bootstrap flag, and {@code extra} goes to each start. */
    void startAll(final List<String> extra) {
        nodes.forEach(n -> n.start(extra));
    }

    /** Stop every member as a job stop does: all at once, then wait for each exit. */
    void stopAll() {
        nodes.forEach(SealerNode::stop);
        nodes.forEach(SealerNode::awaitStopped);
    }

    /**
     * Require that no running member commits for {@code window} while the
     * client offers record {@code n}: a group without a quorum stalls and
     * shows no false progress.
     */
    void assertStalled(final CanonicalClient client, final int n, final Duration window) {
        final List<Long> before = nodes.stream().filter(SealerNode::isAlive)
            .map(node -> node.view().map(SealerNode.View::commitPosition).orElse(-1L)).toList();
        if (client.relayedWithin(n, window)) {
            throw new AssertionError("record " + n + " committed without a quorum");
        }
        final List<Long> after = nodes.stream().filter(SealerNode::isAlive)
            .map(node -> node.view().map(SealerNode.View::commitPosition).orElse(-1L)).toList();
        if (!before.equals(after)) {
            throw new AssertionError("a member without a quorum moved its commit position: " + before + " -> " + after);
        }
    }

    /** Write the hosts file that every member reads its peers' addresses from. */
    void writeHosts() {
        write(hostsFile(), nodes.stream().map(n -> n.ip() + " " + n.name).collect(Collectors.joining("\n", "", "\n")));
    }

    /** Set the link faults of the group: for each faulted source member, its plan line. */
    void links(final Map<Integer, String> faults) {
        write(planFile(), faults.entrySet().stream()
            .map(e -> e.getValue().replace("{src}", Integer.toString(e.getKey())))
            .collect(Collectors.joining("\n", "", "\n")));
    }

    /** The frame counts at member {@code receiver} for the frames from member {@code source}. */
    LinkCounts counts(final int receiver, final int source) {
        final Path file = dir.resolve("links.counts-" + receiver);
        try {
            return Files.exists(file)
                ? Files.readAllLines(file).stream().map(l -> l.split(" "))
                    .filter(f -> Integer.parseInt(f[0]) == source).findFirst()
                    .map(f -> new LinkCounts(Long.parseLong(f[1]), Long.parseLong(f[2]),
                        Long.parseLong(f[3]), Long.parseLong(f[4])))
                    .orElseThrow()
                : new LinkCounts(0, 0, 0, 0);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** The frames of one link at its receiver: passed, dropped, held and released. */
    record LinkCounts(long passed, long dropped, long held, long released) {
    }

    /** The views of the members that answer. */
    List<SealerNode.View> views() {
        return nodes.stream().map(SealerNode::view).flatMap(Optional::stream).toList();
    }

    /** The one leader with a closed election, or empty while there is none or more than one. */
    Optional<SealerNode.View> leader() {
        final List<SealerNode.View> leaders = views().stream().filter(SealerNode.View::isLeader).toList();
        return leaders.size() == 1 ? Optional.of(leaders.get(0)) : Optional.empty();
    }

    SealerNode.View awaitLeader(final Duration bound) {
        return Await.until(bound, "one leader with a closed election", this::leader);
    }

    /** Wait until a leader other than {@code old} leads a term above {@code term}. */
    SealerNode.View awaitNewLeader(final int old, final long term, final Duration bound) {
        return Await.until(bound, "a new leader in a term above " + term, () -> leader()
            .filter(v -> v.memberId() != old && v.term() > term));
    }

    /** Wait until every member in {@code ids} answers as a leader or a follower of term {@code >= term}. */
    void awaitJoined(final List<Integer> ids, final Duration bound) {
        Await.holds(bound, "members " + ids + " joined with closed elections", () -> ids.stream()
            .allMatch(id -> node(id).view().filter(v -> v.isLeader() || v.isFollower()).isPresent()));
    }

    /**
     * The leader of each term, from the log-driven TERM lines of every member,
     * also over restarts. Fail when two members name different leaders, or
     * different start positions, for one term.
     */
    Map<Long, Integer> assertOneLeaderPerTerm() {
        return assertOneLeaderPerTerm(nodes.stream().map(n -> 0).toList());
    }

    /**
     * {@link #assertOneLeaderPerTerm()} for one history: the logs of each
     * member from its start in {@code starts}, for a cluster that a wipe
     * began again.
     */
    Map<Long, Integer> assertOneLeaderPerTerm(final List<Integer> starts) {
        nodes.forEach(n -> {
            if (!TERM.matcher(n.logSince(starts.get(n.id))).find()) {
                throw new AssertionError("member " + n.id + " logged no leadership term");
            }
        });
        final Map<Long, List<long[]>> byTerm = nodes.stream()
            .flatMap(n -> TERM.matcher(n.logSince(starts.get(n.id))).results())
            .map(r -> new long[] {Long.parseLong(r.group(2)), Long.parseLong(r.group(3)), Long.parseLong(r.group(4))})
            .collect(Collectors.groupingBy(t -> t[0]));
        byTerm.forEach((term, lines) -> {
            final long leaders = lines.stream().mapToLong(l -> l[1]).distinct().count();
            final long positions = lines.stream().mapToLong(l -> l[2]).distinct().count();
            if (leaders != 1 || positions != 1) {
                throw new AssertionError("term " + term + " has " + leaders + " leaders and " + positions
                    + " start positions across the members");
            }
        });
        return byTerm.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
            e -> (int) e.getValue().get(0)[1]));
    }

    /** The count of lines of member {@code id} since start {@code start} that contain {@code text}. */
    long count(final int id, final int start, final String text) {
        return node(id).logSince(start).lines().filter(l -> l.contains(text)).count();
    }

    /** The block of the last {@code snapshot TAKEN} line of member {@code id} since start {@code start}. */
    long lastSnapshotBlock(final int id, final int start) {
        return lastSnapshotBlock(node(id).logSince(start), id);
    }

    /** The block of the last {@code snapshot TAKEN} line of member {@code id} in {@code log}, or -1. */
    static long lastSnapshotBlock(final String log, final int id) {
        final Matcher m = Pattern.compile("snapshot TAKEN memberId=" + id + " block=(\\d+)").matcher(log);
        long block = -1;
        while (m.find()) {
            block = Long.parseLong(m.group(1));
        }
        return block;
    }

    @Override
    public void close() {
        nodes.forEach(SealerNode::killNow);
        nodes.forEach(SealerNode::close);
        SealerNode.deleteTree(shm);
        Runtime.getRuntime().removeShutdownHook(cleanup);
    }

    private void release() {
        nodes.forEach(SealerNode::killNow);
        SealerNode.deleteTree(shm);
    }

    private static void write(final Path file, final String text) {
        try {
            Files.createDirectories(file.getParent());
            final Path temp = file.resolveSibling(file.getFileName() + ".tmp");
            Files.writeString(temp, text);
            Files.move(temp, file, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Whether {@code tmpfs} exists with {@link #TMPFS_ROOM} free. */
    private static boolean hasRoom(final Path tmpfs) {
        try {
            return Files.isDirectory(tmpfs) && Files.getFileStore(tmpfs).getUsableSpace() >= TMPFS_ROOM;
        } catch (final IOException e) {
            return false;
        }
    }

    /**
     * Delete the tmpfs directories of groups whose test JVM is dead: a JVM
     * that the runner killed at its deadline closed no group. Its members
     * stop on their own when they see their parent die.
     */
    private static void sweepStale(final Path tmpfs) {
        try (java.util.stream.Stream<Path> dirs = Files.list(tmpfs)) {
            dirs.filter(d -> d.getFileName().toString().startsWith(PREFIX))
                .filter(d -> ProcessHandle.of(Long.parseLong(d.getFileName().toString()
                    .substring(PREFIX.length()).split("-")[0])).map(h -> !h.isAlive()).orElse(true))
                .forEach(SealerNode::deleteTree);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** A block of {@link #PORT_SPAN} ports below the ephemeral range that no socket holds now. */
    private static int freePortSpan() {
        return IntStream.generate(() -> 20_000 + PORT_SPAN * ThreadLocalRandom.current().nextInt(0, 24))
            .limit(64).filter(LocalSealers::spanIsFree).findFirst()
            .orElseThrow(() -> new IllegalStateException("no free port block"));
    }

    private static boolean spanIsFree(final int first) {
        return IntStream.range(first, first + PORT_SPAN).allMatch(LocalSealers::portIsFree);
    }

    private static boolean portIsFree(final int port) {
        try (DatagramSocket udp = new DatagramSocket(new InetSocketAddress(InetAddress.getByName("0.0.0.0"), port));
             ServerSocket tcp = new ServerSocket(port)) {
            return udp.isBound() && tcp.isBound();
        } catch (final IOException e) {
            return false;
        }
    }
}
