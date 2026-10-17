package io.kardamom.sealer.cluster;

import static io.kardamom.sealer.cluster.ClusterTestHarness.awaitCondition;
import static io.kardamom.sealer.cluster.IngressFrames.canonicalId;
import static io.kardamom.sealer.cluster.IngressFrames.offerFrame;
import static io.kardamom.sealer.cluster.IngressFrames.offerRecord;
import static io.kardamom.sealer.cluster.IngressFrames.offerReplayRequest;
import static io.kardamom.sealer.cluster.IngressFrames.originRecordFrame;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.CommonContext;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import io.kardamom.sealer.CanonicalSealerState;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.agrona.CloseHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/**
 * Three in-JVM members, each with a member feed on its own media driver,
 * and a test reader on each feed. The feed of every member carries the
 * same retained frames as the leader's, byte for byte. A reader that does
 * not poll fills the ring of its member: the member keeps its commit pace,
 * writes {@code FEED_GAP}, and a replay from the gap reaches the head.
 */
@ExtendWith(InterruptingTestCallback.class)
class SealerFeedClusterTest {

    private static final int MEMBER_COUNT = 3;
    private static final int DEDUP_CAPACITY = 8192;
    private static final long TICK_MS = 200L;
    private static final int SEGMENT_BYTES = 64 * 1024;
    private static final int PAYLOAD_BYTES = 1024;
    private static final long GENESIS = CanonicalSealerState.GENESIS_BLOCK_NUMBER;
    /** A ring and a term that a few hundred records of {@link #PAYLOAD_BYTES} fill. */
    private static final FeedSize SMALL = new FeedSize(64 * 1024, 64 * 1024);
    private static final FeedSize ROOMY = new FeedSize(1 << 20, 1 << 20);
    /** A ring that takes a boundary or an epoch frame, and never a frame of {@link #PAYLOAD_BYTES} or more. */
    private static final FeedSize TINY = new FeedSize(2 * 1024, 64 * 1024);

    /** The ring and the term of a test feed. */
    private record FeedSize(int ringBytes, int termBytes) {
    }

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @TempDir
    Path dir;

    private TestCluster cluster;
    private final RecordingEgressListener egress = new RecordingEgressListener();
    private final List<FeedReader> readers = new ArrayList<>();

    @AfterEach
    void closeReaders() {
        CloseHelper.closeAll(readers);
    }

    private static String driverDir(final int memberId) {
        return CommonContext.getAeronDirectoryName() + "-" + memberId + "-driver";
    }

    /** Start the cluster, with the feed of member {@code i} of {@code size.apply(i)}. */
    private void startCluster(final IntFunction<FeedSize> size) {
        cluster = ClusterTestHarness.start(systemTestWatcher, ClusterTestHarness.builder(MEMBER_COUNT,
            memberId -> new SealerTestService(new SealerClusteredService(DEDUP_CAPACITY, TICK_MS, memberId)
                .carryingBytes(new RetainedFile.Config(dir.resolve("retained-" + memberId), SEGMENT_BYTES),
                    Optional.of(new FeedConfig(driverDir(memberId),
                        size.apply(memberId).ringBytes(), size.apply(memberId).termBytes()))))));
        cluster.awaitLeader();
        cluster.egressListener(egress);
        cluster.connectClient();
    }

    /** A reader on the feed of {@code member} that asked for the stream from genesis and got its answer. */
    private FeedReader readFromGenesis(final TestNode member) {
        final FeedReader reader = new FeedReader(driverDir(member.index()));
        readers.add(reader);
        reader.request(0, GENESIS, 1);
        reader.pollUntil(frames -> has(frames, SealerWire.EGRESS_KIND_REPLAY_DONE, 1));
        return reader;
    }

    /** Offer records {@code from..to - 1} of {@link #PAYLOAD_BYTES} and await their relay on the egress. */
    private void offerRecords(final int from, final int to) {
        offerRecords(from, to, PAYLOAD_BYTES);
    }

    /** Offer records {@code from..to - 1} of {@code payloadBytes} and await their relay on the egress. */
    private void offerRecords(final int from, final int to, final int payloadBytes) {
        final AeronCluster client = cluster.reconnectClient();
        IntStream.range(from, to).forEach(i -> {
            final byte[] payload = new byte[payloadBytes];
            Arrays.fill(payload, (byte) i);
            // Record type 0: a member with the retained file takes no
            // canonical entry (type 6) from a session.
            payload[0] = 0;
            offerRecord(client, canonicalId(i), payload);
        });
        awaitCondition(client, () -> egress.relayedIndexes.size() >= to);
    }

    private static boolean has(final List<FeedReader.Frame> frames, final byte kind, final long requestId) {
        return frames.stream().anyMatch(f -> f.kind() == kind && f.requestId() == requestId);
    }

    /** Whether the frames hold a boundary that seals every record below {@code count}. */
    private static Predicate<List<FeedReader.Frame>> sealed(final long count) {
        return frames -> frames.stream()
            .anyMatch(f -> f.kind() == SealerWire.EGRESS_KIND_BOUNDARY && f.b() >= count);
    }

    /** The retained frames up to and with the first boundary that seals every record below {@code count}. */
    private static List<byte[]> canonicalThrough(final List<FeedReader.Frame> frames, final long count) {
        final List<FeedReader.Frame> canonical = frames.stream().filter(FeedReader.Frame::canonical).toList();
        final int end = IntStream.range(0, canonical.size())
            .filter(i -> canonical.get(i).kind() == SealerWire.EGRESS_KIND_BOUNDARY && canonical.get(i).b() >= count)
            .findFirst()
            .orElseThrow();
        return canonical.subList(0, end + 1).stream().map(FeedReader.Frame::bytes).toList();
    }

    private static void assertSameFrames(final List<byte[]> expected, final List<byte[]> actual, final String what) {
        assertEquals(expected.size(), actual.size(), what + ": the frame count");
        IntStream.range(0, expected.size()).forEach(i ->
            assertArrayEquals(expected.get(i), actual.get(i), what + ": frame " + i));
    }

    @Test
    @InterruptAfter(value = 120, unit = TimeUnit.SECONDS)
    void every_member_feeds_the_leaders_frames_byte_for_byte() {
        startCluster(memberId -> ROOMY);
        final TestNode leader = cluster.awaitLeader();
        final Map<Integer, FeedReader> feeds = IntStream.range(0, MEMBER_COUNT).boxed()
            .collect(Collectors.toMap(i -> i, i -> readFromGenesis(cluster.node(i))));
        final int records = 40;
        offerRecords(0, records);
        feeds.values().forEach(reader -> reader.pollUntil(sealed(records)));

        final List<byte[]> leaderFrames = canonicalThrough(feeds.get(leader.index()).frames(), records);
        assertEquals(records, leaderFrames.stream().filter(f -> f[0] == SealerWire.EGRESS_KIND_RELAYED).count());
        feeds.forEach((member, reader) ->
            assertSameFrames(leaderFrames, canonicalThrough(reader.frames(), records), "member " + member));
        feeds.values().forEach(SealerFeedClusterTest::assertDoneFollowsTheReplay);
    }

    /**
     * The one {@code REPLAY_DONE} names the cursor after the frames before
     * it: the count of the relayed records and the block after the last
     * boundary.
     */
    private static void assertDoneFollowsTheReplay(final FeedReader reader) {
        final List<FeedReader.Frame> frames = reader.frames();
        final int done = IntStream.range(0, frames.size())
            .filter(i -> frames.get(i).kind() == SealerWire.EGRESS_KIND_REPLAY_DONE).findFirst().orElseThrow();
        final List<FeedReader.Frame> replayed = frames.subList(0, done);
        assertTrue(replayed.stream().allMatch(FeedReader.Frame::canonical), "only retained frames before the answer");
        final long blocks = replayed.stream().filter(f -> f.kind() == SealerWire.EGRESS_KIND_BOUNDARY).count();
        final long records = replayed.size() - blocks;
        assertEquals(records, frames.get(done).a(), "up_to_index");
        assertEquals(GENESIS + blocks, frames.get(done).b(), "up_to_block");
        assertEquals(1, frames.stream().filter(f -> f.kind() == SealerWire.EGRESS_KIND_REPLAY_DONE).count());
    }

    @Test
    @InterruptAfter(value = 120, unit = TimeUnit.SECONDS)
    void a_stalled_subscriber_does_not_slow_the_commit() {
        startCluster(memberId -> SMALL);
        final TestNode leader = cluster.awaitLeader();
        final int batch = 200;
        final long freeNs = timed(() -> offerRecords(0, batch));
        // The reader of the leader's feed gets the replay, and then stops
        // polling: the term and the ring of the live feed fill.
        final FeedReader stalled = readFromGenesis(leader);
        final long stalledNs = timed(() -> offerRecords(batch, 2 * batch));

        assertTrue(stalledNs <= Math.max(TimeUnit.SECONDS.toNanos(5), 3 * freeNs),
            "the commit with a stalled feed takes " + stalledNs + " ns, without one " + freeNs + " ns");
        final long head = ((SealerTestService) leader.service()).delegate().servicePosition();
        cluster.followers().forEach(follower -> awaitPosition(follower, head));
        stalled.pollUntil(frames -> has(frames, SealerWire.EGRESS_KIND_FEED_GAP, 1));
    }

    private static long timed(final Runnable step) {
        final long start = System.nanoTime();
        step.run();
        return System.nanoTime() - start;
    }

    private void awaitPosition(final TestNode member, final long position) {
        final SealerClusteredService service = ((SealerTestService) member.service()).delegate();
        awaitCondition(cluster.reconnectClient(), () -> service.servicePosition() >= position);
    }

    @Test
    @InterruptAfter(value = 120, unit = TimeUnit.SECONDS)
    void a_full_ring_writes_feed_gap_and_the_replay_reaches_the_head() {
        // Member 0 has a small feed. Member 1 has room for every frame of
        // the test, so its reader needs no poll during the load.
        startCluster(memberId -> memberId == 0 ? SMALL : ROOMY);
        final FeedReader full = readFromGenesis(cluster.node(1));
        final FeedReader gapped = readFromGenesis(cluster.node(0));
        final int batch = 200;
        offerRecords(0, batch);
        gapped.pollUntil(frames -> has(frames, SealerWire.EGRESS_KIND_FEED_GAP, 1));

        final List<FeedReader.Frame> before = gapped.frames();
        final FeedReader.Frame gap = before.get(before.size() - 1);
        assertEquals(SealerWire.EGRESS_KIND_FEED_GAP, gap.kind(), "the gap is the last frame of the answer");
        assertEquals(SealerWire.FEED_ANSWER_LEN, gap.bytes().length);
        gapped.request(gap.a(), gap.b(), 2);
        gapped.pollUntil(frames -> has(frames, SealerWire.EGRESS_KIND_REPLAY_DONE, 2));
        offerRecords(batch, batch + 5);
        gapped.pollUntil(sealed(batch + 5));
        full.pollUntil(sealed(batch + 5));

        final List<FeedReader.Frame> after = gapped.frames().subList(before.size(), gapped.frames().size());
        final List<FeedReader.Frame> joined = new ArrayList<>(before);
        joined.addAll(after);
        assertSameFrames(canonicalThrough(full.frames(), batch + 5), canonicalThrough(joined, batch + 5),
            "the frames before the gap and the replay from it");
    }

    /**
     * An epoch record with deposits takes several index slots. The feed
     * carries it and the boundary after it, then gaps on a frame that never
     * fits. The gap cursor is the cursor after that boundary: its end index,
     * not one above the index of the epoch. A replay from it is served.
     */
    @Test
    @InterruptAfter(value = 120, unit = TimeUnit.SECONDS)
    void the_gap_after_an_epoch_with_deposits_names_a_cursor_that_replays() {
        startCluster(memberId -> memberId == 0 ? TINY : ROOMY);
        final FeedReader reader = readFromGenesis(cluster.node(0));
        offerRecords(0, 2, 16);
        final int slots = 4;
        final AeronCluster client = cluster.reconnectClient();
        offerFrame(client, originRecordFrame(canonicalId(1000), 100, slots, new byte[] {2}));
        awaitCondition(client, () -> egress.relayedIndexes.size() >= 3);
        final long epochIndex = egress.relayedIndexes.get(2);
        final long afterEpoch = epochIndex + slots;
        // The next tick seals the block of the epoch.
        reader.pollUntil(frames -> frames.stream()
            .anyMatch(f -> f.kind() == SealerWire.EGRESS_KIND_BOUNDARY && f.b() == afterEpoch));

        offerRecords(3, 4);
        reader.pollUntil(frames -> has(frames, SealerWire.EGRESS_KIND_FEED_GAP, 1));
        final List<FeedReader.Frame> frames = reader.frames();
        final FeedReader.Frame gap = frames.get(frames.size() - 1);
        final FeedReader.Frame lastBoundary = frames.stream()
            .filter(f -> f.kind() == SealerWire.EGRESS_KIND_BOUNDARY)
            .reduce((a, b) -> b)
            .orElseThrow();
        assertEquals(SealerWire.EGRESS_KIND_BOUNDARY, frames.get(frames.size() - 2).kind(),
            "a boundary is the last frame before the gap");
        assertEquals(afterEpoch, gap.a(), "next_index is the end index of the boundary");
        assertEquals(lastBoundary.a() + 1, gap.b(), "next_block");

        final long served = egress.replayDoneCount;
        final AeronCluster current = cluster.client();
        offerReplayRequest(current, gap.a(), gap.b());
        awaitCondition(current, () -> egress.replayDoneCount > served || egress.replayUnavailableCount > 0);
        assertEquals(0, egress.replayUnavailableCount, "the cursor of the gap names one point of the stream");
    }
}
