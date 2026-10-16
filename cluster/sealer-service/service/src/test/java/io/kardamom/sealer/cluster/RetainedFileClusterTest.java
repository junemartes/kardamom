package io.kardamom.sealer.cluster;

import static io.kardamom.sealer.cluster.ClusterTestHarness.awaitCondition;
import static io.kardamom.sealer.cluster.IngressFrames.canonicalId;
import static io.kardamom.sealer.cluster.IngressFrames.offerRecord;
import static io.kardamom.sealer.cluster.RetainedFiles.assertEqualFiles;
import static io.kardamom.sealer.cluster.RetainedFiles.awaitHeads;
import static io.kardamom.sealer.cluster.RetainedFiles.nextPublishedHead;
import static io.kardamom.sealer.cluster.RetainedFiles.segments;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/**
 * Three in-JVM members keep their retained frames in a file. Every member
 * holds the same file bytes. A follower that restarts from a snapshot
 * truncates its file at the snapshot head, and the log replay rebuilds the
 * same bytes as the leader's, even after the bytes above the snapshot head
 * were overwritten on disk.
 */
@ExtendWith(InterruptingTestCallback.class)
class RetainedFileClusterTest {

    private static final int MEMBER_COUNT = 3;
    private static final int DEDUP_CAPACITY = 8192;
    private static final long TICK_MS = 200L;
    private static final int SEGMENT_BYTES = 64 * 1024;
    private static final int PAYLOAD_BYTES = 4 * 1024;
    private static final int RECORDS = 60;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @TempDir
    Path dir;

    private TestCluster cluster;
    private final RecordingEgressListener egress = new RecordingEgressListener();

    private Path retainedDir(final int memberId) {
        return dir.resolve("retained-" + memberId);
    }

    private void startCluster() {
        cluster = ClusterTestHarness.start(systemTestWatcher, TestCluster.aCluster()
            .withStaticNodes(MEMBER_COUNT)
            .withServiceSupplier(memberId -> new TestNode.TestService[] {
                new SealerTestService(new SealerClusteredService(DEDUP_CAPACITY, TICK_MS, memberId)
                    .carryingBytes(new RetainedFile.Config(retainedDir(memberId), SEGMENT_BYTES)))
                    .index(memberId)
            }));
        cluster.awaitLeader();
        cluster.egressListener(egress);
        cluster.connectClient();
    }

    /** Offer records {@code from..to - 1}, each with a payload of {@link #PAYLOAD_BYTES}, and await their relay. */
    private void offerRecords(final int from, final int to) {
        final AeronCluster client = cluster.reconnectClient();
        IntStream.range(from, to).forEach(i -> {
            final byte[] payload = new byte[PAYLOAD_BYTES];
            Arrays.fill(payload, (byte) i);
            offerRecord(client, canonicalId(i), payload);
        });
        awaitCondition(client, () -> egress.relayedIndexes.size() >= to);
    }

    private List<TestNode> members() {
        return IntStream.range(0, MEMBER_COUNT).mapToObj(cluster::node).toList();
    }

    @Test
    @InterruptAfter(value = 120, unit = TimeUnit.SECONDS)
    void every_member_holds_the_same_file_bytes() {
        startCluster();
        offerRecords(0, RECORDS);
        final long head = nextPublishedHead(cluster.awaitLeader());
        awaitHeads(members(), head);

        assertTrue(segments(retainedDir(0)).size() > 2, "the frames span several segments");
        assertEqualFiles(members(), this::retainedDir, head);
        final Map<Integer, List<Long>> starts = members().stream().collect(Collectors.toMap(
            TestNode::index, m -> List.copyOf(segments(retainedDir(m.index())).headMap(head, false).keySet())));
        assertEquals(1, starts.values().stream().distinct().count(), "the same segments " + starts);
    }

    @Test
    @InterruptAfter(value = 120, unit = TimeUnit.SECONDS)
    void a_restore_truncates_the_file_and_the_log_replay_rebuilds_it() throws IOException {
        startCluster();
        offerRecords(0, RECORDS / 2);
        final TestNode follower = cluster.followers().get(0);
        final int followerId = follower.index();
        // Two ticks seal the records, so the snapshot holds boundaries in its index.
        nextPublishedHead(follower);
        nextPublishedHead(follower);
        cluster.takeSnapshot(cluster.awaitLeader());
        cluster.awaitSnapshotCount(1);
        // A tick after the snapshot publishes a head at or above the snapshot head.
        final long aboveSnapshot = nextPublishedHead(follower);

        offerRecords(RECORDS / 2, RECORDS);
        awaitHeads(List.of(follower), nextPublishedHead(cluster.awaitLeader()));
        cluster.stopNode(follower);

        // Overwrite every byte above the snapshot head, and add a segment far above it.
        final TreeMap<Long, Path> files = segments(retainedDir(followerId));
        files.forEach((start, path) -> garble(path, Math.max(0, aboveSnapshot - start)));
        final Path stray = retainedDir(followerId).resolve(RetainedFile.segmentName(1L << 40));
        Files.write(stray, new byte[SEGMENT_BYTES]);

        final TestNode restarted = cluster.startStaticNode(followerId, false);
        TestCluster.awaitElectionClosed(restarted);
        assertTrue(((SealerTestService) restarted.service()).restoredFromSnapshot(), "the follower restores");
        final long head = nextPublishedHead(cluster.awaitLeader());
        final List<TestNode> running = List.of(cluster.awaitLeader(), restarted);
        awaitHeads(running, head);

        assertFalse(Files.exists(stray), "the restore deletes the segments above the snapshot head");
        assertEqualFiles(running, this::retainedDir, head);
    }

    /** Overwrite the bytes of {@code path} from {@code from} to its end. */
    private static void garble(final Path path, final long from) {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            final long size = channel.size();
            if (from < size) {
                final byte[] junk = new byte[Math.toIntExact(size - from)];
                Arrays.fill(junk, (byte) 0x5A);
                channel.write(ByteBuffer.wrap(junk), from);
            }
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
