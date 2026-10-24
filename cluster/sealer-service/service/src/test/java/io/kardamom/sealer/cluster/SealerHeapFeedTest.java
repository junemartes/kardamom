package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.cluster.DriverlessLog.Entry;
import io.kardamom.sealer.cluster.DriverlessLog.Member;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The member feed of a driverless member that keeps its retained frames on
 * the heap, on a real media driver. The test runs the duty cycles of the
 * service, so each walk step is a known point of the log.
 *
 * <ul>
 *   <li>A request below the floor gets {@code REPLAY_UNAVAILABLE} with the
 *       floors. A request from the floors gets every retained frame, then
 *       {@code REPLAY_DONE}, then the live frames.</li>
 *   <li>A walk while the window evicts ends with the frames, or gaps when
 *       the front passes the walk. After a gap, a request from a cursor
 *       inside the window serves.</li>
 *   <li>A member restored from a snapshot gives the same answer.</li>
 *   <li>A member with no feed dir runs no feed.</li>
 * </ul>
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class SealerHeapFeedTest {

    /** The replay window, in frames. */
    private static final int WINDOW = 512;
    /** The records of one block. */
    private static final int PER_BLOCK = 64;
    /** The envelope of each record: about 230 frames fill the walk budget of one duty cycle. */
    private static final int ENVELOPE_BYTES = 1_000;
    /** The blocks that fill the window more than once. */
    private static final int FILL_BLOCKS = 10;
    private static final long GENESIS = CanonicalSealerState.GENESIS_BLOCK_NUMBER;

    @TempDir
    Path dir;

    private final DriverlessLog log = new DriverlessLog();
    private final List<FeedReader> readers = new ArrayList<>();
    private MediaDriver driver;
    private FeedConfig feed;
    /** The records that the log offered so far, and the boundary ticks. */
    private int records;
    private long ticks;
    /** The next request id. */
    private long requestId;

    @BeforeEach
    void start() {
        System.setProperty("kardamom.cluster.retention", Integer.toString(WINDOW));
        final String aeronDir = dir.resolve("aeron").toString();
        driver = MediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(aeronDir)
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true));
        feed = new FeedConfig(aeronDir, 4 << 20, 1 << 20, IdleCap.DEFAULT);
    }

    @AfterEach
    void stop() {
        CloseHelper.closeAll(readers);
        log.stop();
        CloseHelper.close(driver);
        System.clearProperty("kardamom.cluster.retention");
    }

    private FeedReader reader() {
        final FeedReader reader = new FeedReader(feed.aeronDir());
        readers.add(reader);
        return reader;
    }

    /**
     * Apply {@code blocks} blocks of {@link #PER_BLOCK} full references, each
     * block sealed by a tick and posted at once, so the window evicts.
     */
    private void blocks(final Member member, final int blocks) {
        IntStream.range(0, blocks).forEach(b -> block(member));
    }

    private void block(final Member member) {
        IntStream.range(0, PER_BLOCK).forEach(i -> {
            records++;
            final byte[] frame = new FullRef(records, 9, 64L * records, FullRef.filled(ENVELOPE_BYTES, records)).frame();
            // The sender of every record is one account: its nonces follow each other from 0.
            final UnsafeBuffer buffer = new UnsafeBuffer(frame);
            buffer.putLong(SealerWire.NONCE_OFFSET, records - 1L, ByteOrder.LITTLE_ENDIAN);
            buffer.putBytes(SealerWire.CANONICAL_ID_OFFSET, IngressFrames.canonicalId(records));
            member.apply(Entry.frame("ref-" + records, frame, time()));
        });
        ticks++;
        member.apply(Entry.tick("tick-" + ticks, time()));
        member.apply(Entry.frame("posted-" + ticks, IngressFrames.postedCursorFrame(ticks), time()));
    }

    /** A log time that each block moves past the tick interval. */
    private long time() {
        return 1_000L + 250L * ticks;
    }

    /** Send a request from {@code (fromIndex, fromBlock)}, and return its id. */
    private long request(final FeedReader reader, final long fromIndex, final long fromBlock) {
        requestId++;
        reader.request(fromIndex, fromBlock, requestId);
        return requestId;
    }

    /** Run the duty cycles of {@code member} and poll, until the answer {@code kind} of {@code id} comes. */
    private static FeedReader.Frame await(final Member member, final FeedReader reader, final byte kind, final long id) {
        reader.pollUntil(frames -> answer(frames, kind, id).isPresent(), () -> member.svc().doBackgroundWork(0));
        return answer(reader.frames(), kind, id).orElseThrow();
    }

    private static Optional<FeedReader.Frame> answer(final List<FeedReader.Frame> frames, final byte kind, final long id) {
        return frames.stream().filter(f -> !f.canonical() && f.kind() == kind && f.requestId() == id).findFirst();
    }

    /** Run the duty cycles of {@code member} until its feed shows {@code state}. */
    private static void awaitState(final Member member, final FeedState state) {
        while (member.svc().metrics().feed() != state) {
            member.svc().doBackgroundWork(0);
        }
    }

    /** The floors that a request from genesis gets with {@code REPLAY_UNAVAILABLE}. */
    private long[] floors(final Member member, final FeedReader reader) {
        final FeedReader.Frame refusal = await(member, reader, SealerWire.EGRESS_KIND_REPLAY_UNAVAILABLE,
            request(reader, 0, GENESIS));
        return new long[] {refusal.a(), refusal.b()};
    }

    /** The retained frames that the session of {@code member} got, from {@code (fromIndex, fromBlock)}. */
    private static List<byte[]> streamFrom(final Member member, final long fromIndex, final long fromBlock) {
        return member.frames().stream()
            .filter(f -> f[0] == SealerWire.EGRESS_KIND_RELAYED || f[0] == SealerWire.EGRESS_KIND_BOUNDARY)
            .filter(f -> FrameKey.wanted(new UnsafeBuffer(f), fromIndex, fromBlock))
            .toList();
    }

    /** The retained frames that the reader got after the first {@code from} frames, up to the next answer frame. */
    private static List<byte[]> answerFrames(final FeedReader reader, final int from) {
        final List<FeedReader.Frame> after = reader.frames().subList(from, reader.frames().size());
        return after.stream().takeWhile(FeedReader.Frame::canonical).map(FeedReader.Frame::bytes).toList();
    }

    private static void assertSameFrames(final List<byte[]> expected, final List<byte[]> actual, final String what) {
        assertEquals(expected.size(), actual.size(), what + ": the frame count");
        IntStream.range(0, expected.size()).forEach(i ->
            assertArrayEquals(expected.get(i), actual.get(i), what + ": frame " + i));
    }

    @Test
    void a_heap_feed_serves_the_window_from_the_floor_and_then_the_live_frames() {
        final Member member = log.feeding(Optional.empty(), feed);
        assertEquals(FeedState.IDLE, member.svc().metrics().feed());
        blocks(member, FILL_BLOCKS);
        assertEquals(WINDOW, member.svc().retained().count(), "the window evicts down to its cap");
        final FeedReader reader = reader();
        final long[] floor = floors(member, reader);
        assertTrue(floor[0] > 0, "the window evicted records: the floor is above genesis");

        final int mark = reader.frames().size();
        final long id = request(reader, floor[0], floor[1]);
        final FeedReader.Frame done = await(member, reader, SealerWire.EGRESS_KIND_REPLAY_DONE, id);
        final List<byte[]> window = streamFrom(member, floor[0], floor[1]);
        assertSameFrames(member.svc().retained().wanted(floor[0], floor[1]).toList(), window, "the window");
        assertSameFrames(window, answerFrames(reader, mark), "the answer from the floor");
        assertEquals(records, done.a(), "REPLAY_DONE names the head index");
        assertEquals(FeedState.LIVE, member.svc().metrics().feed());
        assertEquals("live", member.svc().metrics().carry().feed());
        assertEquals(3.0, PromText.value(member.metrics(), "kardamom_sealer_feed_state", ""));

        block(member);
        reader.pollUntil(frames -> frames.size() >= mark + window.size() + 1 + PER_BLOCK + 1);
        final List<byte[]> live = reader.frames().subList(mark + window.size() + 1, reader.frames().size()).stream()
            .map(FeedReader.Frame::bytes).toList();
        final List<byte[]> all = streamFrom(member, floor[0], floor[1]);
        assertSameFrames(all.subList(all.size() - PER_BLOCK - 1, all.size()), live, "the live frames");
    }

    @Test
    void a_walk_that_the_front_does_not_pass_ends_with_every_frame() {
        final Member member = log.feeding(Optional.empty(), feed);
        blocks(member, FILL_BLOCKS);
        final FeedReader reader = reader();
        final long[] floor = floors(member, reader);
        final int mark = reader.frames().size();
        final long id = request(reader, floor[0], floor[1]);
        awaitState(member, FeedState.WALKING);
        // One block evicts its own frames from the front: the walk already passed them.
        block(member);
        await(member, reader, SealerWire.EGRESS_KIND_REPLAY_DONE, id);
        assertSameFrames(streamFrom(member, floor[0], floor[1]), answerFrames(reader, mark),
            "the walk carries the frames of the request and the frames of the walk");
    }

    @Test
    void a_walk_that_the_front_passes_gaps_and_a_request_inside_the_window_serves() {
        final Member member = log.feeding(Optional.empty(), feed);
        blocks(member, FILL_BLOCKS);
        final FeedReader reader = reader();
        final long[] floor = floors(member, reader);
        final int mark = reader.frames().size();
        final long id = request(reader, floor[0], floor[1]);
        awaitState(member, FeedState.WALKING);
        final List<byte[]> before = streamFrom(member, floor[0], floor[1]);
        // The next blocks evict every frame of the window: the front passes the walk.
        blocks(member, FILL_BLOCKS);
        final FeedReader.Frame gap = await(member, reader, SealerWire.EGRESS_KIND_FEED_GAP, id);
        assertEquals(FeedState.GAPPED, member.svc().metrics().feed());
        assertEquals(4.0, PromText.value(member.metrics(), "kardamom_sealer_feed_state", ""));

        final List<byte[]> carried = answerFrames(reader, mark);
        assertTrue(!carried.isEmpty() && carried.size() < before.size(), "the walk stopped inside the window");
        assertSameFrames(before.subList(0, carried.size()), carried, "the frames before the gap");
        final FeedCursor next = new FeedCursor();
        next.reset(floor[0], floor[1]);
        carried.forEach(f -> next.pass(new UnsafeBuffer(f)));
        assertEquals(next.index(), gap.a(), "the gap names the cursor after the carried frames");
        assertEquals(next.block(), gap.b());

        // The frames after the gap cursor left the window, so the floor refuses it.
        await(member, reader, SealerWire.EGRESS_KIND_REPLAY_UNAVAILABLE, request(reader, gap.a(), gap.b()));
        final long[] moved = floors(member, reader);
        final int again = reader.frames().size();
        await(member, reader, SealerWire.EGRESS_KIND_REPLAY_DONE, request(reader, moved[0], moved[1]));
        assertSameFrames(streamFrom(member, moved[0], moved[1]), answerFrames(reader, again),
            "the request inside the window");
        assertEquals(FeedState.LIVE, member.svc().metrics().feed());
    }

    @Test
    void a_member_restored_from_a_snapshot_gives_the_same_answer() {
        final Member original = log.feeding(Optional.empty(), feed);
        blocks(original, FILL_BLOCKS);
        final FeedReader reader = reader();
        final long[] floor = floors(original, reader);
        final int first = reader.frames().size();
        await(original, reader, SealerWire.EGRESS_KIND_REPLAY_DONE, request(reader, floor[0], floor[1]));
        final List<FeedReader.Frame> answer = reader.frames().subList(first, reader.frames().size());
        final byte[] snapshot = original.svc().snapshot();
        log.end(original);

        final Member restored = log.feeding(Optional.of(snapshot), feed);
        assertEquals(floor[0], floors(restored, reader)[0], "the restored floor index");
        final int second = reader.frames().size();
        await(restored, reader, SealerWire.EGRESS_KIND_REPLAY_DONE, request(reader, floor[0], floor[1]));
        final List<FeedReader.Frame> again = reader.frames().subList(second, reader.frames().size());
        assertSameFrames(answer.stream().map(FeedReader.Frame::bytes).toList().subList(0, answer.size() - 1),
            again.stream().map(FeedReader.Frame::bytes).toList().subList(0, again.size() - 1),
            "the retained frames of the answer");
        assertEquals(answer.get(answer.size() - 1).a(), again.get(again.size() - 1).a(), "the head index");
        assertEquals(answer.get(answer.size() - 1).b(), again.get(again.size() - 1).b(), "the head block");
    }

    @Test
    void a_member_with_no_feed_dir_runs_no_feed() {
        assertTrue(FeedConfig.parse(null, null, IdleCap.DEFAULT).isEmpty());
        final Member member = log.member(Optional.empty());
        blocks(member, 1);
        assertEquals(0, member.svc().doBackgroundWork(0));
        assertEquals(FeedState.OFF, member.svc().metrics().feed());
        assertEquals("off", member.svc().metrics().carry().feed());
        assertEquals(0.0, PromText.value(member.metrics(), "kardamom_sealer_feed_state", ""));
    }
}
