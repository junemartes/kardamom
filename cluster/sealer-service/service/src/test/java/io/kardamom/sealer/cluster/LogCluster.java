package io.kardamom.sealer.cluster;

import static io.kardamom.sealer.cluster.IngressFrames.canonicalId;
import static io.kardamom.sealer.cluster.IngressFrames.offerIngress;
import static io.kardamom.sealer.cluster.IngressFrames.offerPostedCursor;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import io.aeron.test.driver.RedirectingNameResolver;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * One 3-member in-JVM {@link TestCluster} for one log test: the members
 * host the real sealer service, the leader, and a connected client. The
 * log has a small term and segment, so a few hundred KB of log span
 * several archive segments. With carried bytes, each member keeps its
 * retained frames in a file and serves the file on an admin endpoint, and
 * a peer seed copies the file.
 */
final class LogCluster implements AutoCloseable {

    static final int MEMBER_COUNT = 3;
    static final int SEGMENT_LENGTH = 64 * 1024;
    static final long LOG_BYTES_BEFORE_SNAPSHOT = 4L * SEGMENT_LENGTH;
    static final long LOG_BYTES_TAIL = 8L * 1024;

    private static final int DEDUP_CAPACITY = 8192;
    /** A small retained-file segment, so the frames of a short log span several segments. */
    static final int RETAINED_SEGMENT_BYTES = 64 * 1024;
    private static final long ADMIN_LAG_BYTES = 0;
    private static final long TICK_MS = 200L;
    private static final String LOG_CHANNEL = "aeron:udp?term-length=64k|alias=raft";

    /** The prefix of the node directories. TestCluster puts member {@code i} in {@code node-i}. */
    private static final String NODE_DIR = "node";

    /** The TestCluster member host names, which every member's media driver maps to localhost. */
    private static final String NODE_NAMES =
            "node0,localhost,localhost|node1,localhost,localhost|node2,localhost,localhost";

    final TestCluster cluster;
    final TestNode leader;
    final MemberProbe leaderProbe;
    private final Path baseDir;
    /** The admin endpoints that serve the retained files, one for each member; empty without carried bytes. */
    private final List<AdminServer> admins;
    private AeronCluster client;
    private int nextRecordId = 0;

    LogCluster(final SystemTestWatcher watcher, final Path baseDir) {
        this(watcher, baseDir, false);
    }

    /** A cluster whose members keep the retained frames in a file when {@code carryBytes} is true. */
    LogCluster(final SystemTestWatcher watcher, final Path baseDir, final boolean carryBytes) {
        this.baseDir = baseDir;
        final TestCluster.Builder builder = ClusterTestHarness.builder(MEMBER_COUNT, DEDUP_CAPACITY, TICK_MS)
                .withLogChannel(LOG_CHANNEL)
                .withSegmentFileLength(SEGMENT_LENGTH)
                .withClusterBaseDir(baseDir.resolve(NODE_DIR).toString());
        if (carryBytes) {
            builder.withServiceSupplier(memberId -> new TestNode.TestService[] {
                new SealerTestService(new SealerClusteredService(DEDUP_CAPACITY, TICK_MS, memberId)
                        .carryingBytes(new RetainedFile.Config(retainedDir(memberId), RETAINED_SEGMENT_BYTES)))
                        .index(memberId)
            });
        }
        this.cluster = ClusterTestHarness.start(watcher, builder);
        this.admins = carryBytes
                ? IntStream.range(0, MEMBER_COUNT).mapToObj(this::startAdmin).toList()
                : List.of();
        this.leader = cluster.awaitLeader();
        this.leaderProbe = new MemberProbe(leader);
        this.client = cluster.connectClient();
    }

    /** The retained-file directory of a member: in its consensus module directory, as the node puts it. */
    Path retainedDir(final int memberId) {
        return nodeDir(memberId).resolve("consensus-module").resolve(RetainedFile.DIR_NAME);
    }

    private AdminServer startAdmin(final int memberId) {
        final MemberStatus status = new MemberStatus(
                memberId, MemberStatus.ROLE_FOLLOWER, MemberStatus.ELECTION_CLOSED, 0, 0,
                MemberStatus.SnapshotVersions.of(0), 0);
        try {
            return AdminServer.start(0, () -> status, ADMIN_LAG_BYTES, Optional.of(retainedDir(memberId)));
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** A member that started from a peer seed, and the snapshot position of the seed. */
    record Seeded(TestNode member, long snapshotPosition) {
    }

    /**
     * Offer records until the leader's commit position grows by
     * {@code bytes}. Each batch of offers uses a new client session: the
     * test sends no keep-alive, and its waits between batches can pass the
     * 10 s session timeout of TestCluster.
     */
    void appendLog(final long bytes) {
        client = cluster.reconnectClient();
        final long target = leader.commitPosition() + bytes;
        while (leader.commitPosition() < target) {
            offerIngress(client, canonicalId(nextRecordId++));
        }
    }

    /** The leader and the running followers. */
    List<TestNode> running() {
        return Stream.concat(Stream.of(leader), cluster.followers().stream()).toList();
    }

    /** Take one snapshot and wait until every running member completes it. */
    void snapshotRunning() {
        snapshot(running());
    }

    /** Take one snapshot and wait until each of {@code members} completes it. */
    void snapshot(final List<TestNode> members) {
        final long[] before = members.stream().mapToLong(cluster::getSnapshotCount).toArray();
        cluster.takeSnapshot(leader);
        IntStream.range(0, before.length)
                .forEach(i -> cluster.awaitSnapshotCount(members.get(i), before[i] + 1));
    }

    /**
     * Post the block of the leader's newest snapshot as the posted head, and
     * wait until every running member applies it. The sealer refuses a head
     * past its sealed head, so the offer repeats until a boundary tick seals
     * that block. Return the head.
     */
    long postNewestSnapshotBlock() {
        final List<PurgeView.Mark> marks = service(leader).purgeView().marks();
        final long head = marks.get(marks.size() - 1).blockNumber();
        client = cluster.reconnectClient();
        while (service(leader).purgeView().postedHead() < head) {
            offerPostedCursor(client, head);
            Tests.sleep(TICK_MS);
        }
        running().forEach(member -> awaitPostedHead(member, head));
        return head;
    }

    /** The snapshot marks and the posted head that the service of {@code member} publishes. */
    static PurgeView purgeView(final TestNode member) {
        return service(member).purgeView();
    }

    private static void awaitPostedHead(final TestNode member, final long head) {
        while (service(member).purgeView().postedHead() < head) {
            Tests.yield();
        }
    }

    /** The production purger of a running member. */
    LogPurger purger(final TestNode member, final PurgePlanner planner) {
        final int memberId = member.index();
        final MemberContexts contexts = new MemberContexts(
                member.mediaDriver().aeronDirectoryName(),
                member.consensusModule().context().clusterDir().getPath(),
                nodeDir(memberId).resolve("archive").toString(),
                ClusterNode.memberEndpoints(TestCluster.clusterMembers(0, MEMBER_COUNT), memberId));
        return new LogPurger(new LogPurger.Member(memberId, contexts, service(member)), planner);
    }

    /** Stop one follower and return its member id. */
    int stopFollower() {
        final TestNode follower = cluster.followers().get(0);
        cluster.stopNode(follower);
        return follower.index();
    }

    /** The consensus module directory of a member. */
    StateDir clusterState(final int memberId) {
        return new StateDir(nodeDir(memberId).resolve("consensus-module"));
    }

    /**
     * Clear a stopped member, seed it from a peer, and start it. Wait until
     * its election closes, and take one snapshot on it and the leader.
     */
    Seeded seedAndStart(final int memberId) {
        new StateDir(nodeDir(memberId)).clear();
        final long snapshotPosition = peerSeed(memberId).run();
        appendLog(LOG_BYTES_TAIL);
        final TestNode member = cluster.startStaticNode(memberId, false);
        TestCluster.awaitElectionClosed(member);
        snapshot(List.of(leader, member));
        return new Seeded(member, snapshotPosition);
    }

    /** Assert that the newest sealer snapshot of {@code member} equals the leader's. */
    void assertSameStateAsLeader(final TestNode member) {
        assertArrayEquals(leaderProbe.latestServiceSnapshot(), new MemberProbe(member).latestServiceSnapshot(),
                "the sealer state of member " + member.index() + " must equal the leader's");
    }

    @Override
    public void close() {
        admins.forEach(AdminServer::close);
        cluster.close();
    }

    private Path nodeDir(final int memberId) {
        return baseDir.resolve(NODE_DIR + "-" + memberId);
    }

    /** The production peer seed of a stopped member, with the member's TestCluster directories and endpoints. */
    private PeerSeed peerSeed(final int memberId) {
        final String members = TestCluster.clusterMembers(0, MEMBER_COUNT);
        final MemberContexts contexts = new MemberContexts(
                baseDir.resolve("seed-aeron").toString(),
                nodeDir(memberId).resolve("consensus-module").toString(),
                nodeDir(memberId).resolve("archive").toString(),
                ClusterNode.memberEndpoints(members, memberId),
                new RedirectingNameResolver(NODE_NAMES));
        return new PeerSeed(memberId, ClusterNode.peerConsensusEndpoints(members, memberId), contexts,
                PeerSeed.Timing.DEFAULT, retainedCopy(memberId));
    }

    /** The copy of the retained file from the admin endpoints of the other members, with carried bytes. */
    private Optional<RetainedCopy> retainedCopy(final int memberId) {
        final List<URI> peers = IntStream.range(0, admins.size())
                .filter(id -> id != memberId)
                .mapToObj(id -> URI.create("http://127.0.0.1:" + admins.get(id).port()))
                .toList();
        return admins.isEmpty() ? Optional.empty() : Optional.of(new RetainedCopy(peers, retainedDir(memberId), memberId));
    }

    private static SealerClusteredService service(final TestNode member) {
        return ((SealerTestService) member.service()).delegate();
    }
}
