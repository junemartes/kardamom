package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.Relayed;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.AgentTerminationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The retained file on its own: it serves the frames that the heap store
 * serves, a restore truncates it at the snapshot head, a missing segment or
 * a failed preallocation stops the member, the purge keeps the segments of
 * the last snapshot, and the heap does not grow with the retained bytes.
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class RetainedFileTest {

    private static final int SEGMENT_BYTES = 64 * 1024;
    private static final int PAYLOAD_BYTES = 4 * 1024;
    private static final int BLOCKS = 12;
    private static final int RECORDS_PER_BLOCK = 4;
    /** The offset of the head position in a section: two floors, the count and the front. */
    private static final int SECTION_HEAD_OFFSET = 4 * Long.BYTES;
    /** The floors at the restore point; a retained-file section brings its own. */
    private static final RetainedFrames.Floors RESTORE_POINT = new RetainedFrames.Floors(99, 99);
    private static final RetainedToken TOKEN = RetainedToken.of("t").orElseThrow();

    @TempDir
    Path dir;

    private final ExpandableArrayBuffer staging = new ExpandableArrayBuffer();
    private final EgressFrames frames = new EgressFrames(staging);
    private final List<RetainedFrames> opened = new ArrayList<>();

    @AfterEach
    void closeStores() {
        opened.forEach(RetainedFrames::close);
    }

    private RetainedFile open(final Path in, final int segmentBytes) {
        final RetainedFile file = new RetainedFile(new RetainedFile.Config(in, segmentBytes), 0);
        opened.add(file);
        return file;
    }

    private RetainedFile openFresh() {
        final RetainedFile file = open(dir, SEGMENT_BYTES);
        file.clear();
        return file;
    }

    private void record(final RetainedFrames store, final long index, final int payloadBytes) {
        final byte[] payload = new byte[payloadBytes];
        Arrays.fill(payload, (byte) index);
        store.append(staging, frames.relayed(new Relayed(index, payload)));
    }

    private void boundary(final RetainedFrames store, final long block, final long endTxIdx) {
        staging.putByte(0, SealerWire.EGRESS_KIND_BOUNDARY);
        staging.putLong(FrameKey.KEY_OFFSET, block, ByteOrder.LITTLE_ENDIAN);
        staging.putLong(FrameKey.BOUNDARY_END_OFFSET, endTxIdx, ByteOrder.LITTLE_ENDIAN);
        staging.putLong(FrameKey.BOUNDARY_END_OFFSET + Long.BYTES, 0L, ByteOrder.LITTLE_ENDIAN);
        staging.putLong(FrameKey.BOUNDARY_END_OFFSET + 2 * Long.BYTES, 0L, ByteOrder.LITTLE_ENDIAN);
        store.append(staging, FrameKey.BOUNDARY_END_OFFSET + 3 * Long.BYTES);
    }

    /** Blocks {@code from..to - 1}, each with its records, then its boundary. */
    private void blocks(final RetainedFrames store, final int from, final int to) {
        IntStream.range(from, to).forEach(block -> {
            final long first = (long) (block - 1) * RECORDS_PER_BLOCK;
            IntStream.range(0, RECORDS_PER_BLOCK).forEach(i -> record(store, first + i, PAYLOAD_BYTES));
            boundary(store, block, first + RECORDS_PER_BLOCK);
        });
    }

    private static List<byte[]> replay(final RetainedFrames store, final long fromIndex, final long fromBlock) {
        return store.wanted(fromIndex, fromBlock).toList();
    }

    private static void assertSameFrames(final List<byte[]> expected, final List<byte[]> actual) {
        assertEquals(expected.size(), actual.size());
        IntStream.range(0, expected.size()).forEach(i -> assertArrayEquals(expected.get(i), actual.get(i), "frame " + i));
    }

    private List<Path> segmentFiles() throws IOException {
        try (Stream<Path> paths = Files.list(dir)) {
            return paths.filter(p -> SegmentDir.SEGMENT_NAME.matcher(p.getFileName().toString()).matches())
                .sorted().toList();
        }
    }

    private static long head(final byte[] section) {
        return ByteBuffer.wrap(section).getLong(SECTION_HEAD_OFFSET);
    }

    private static RetainedFrames.Floors restore(final RetainedFile file, final byte[] section) {
        return file.restore(ByteBuffer.wrap(section), RESTORE_POINT);
    }

    @Test
    void the_file_serves_the_frames_that_the_heap_serves() throws IOException {
        final HeapFrames heap = new HeapFrames();
        final RetainedFile file = openFresh();
        blocks(heap, 1, BLOCKS + 1);
        blocks(file, 1, BLOCKS + 1);
        assertTrue(segmentFiles().size() > 2, "the frames span several segments: " + segmentFiles());

        assertEquals(heap.count(), file.count());
        assertEquals(heap.bytes(), file.bytes(), "both stores count the frame bytes");
        assertSameFrames(replay(heap, 0, 1), replay(file, 0, 1));
        assertSameFrames(replay(heap, 18, 5), replay(file, 18, 5));
        IntStream.rangeClosed(0, BLOCKS + 1).forEach(block ->
            assertEquals(heap.boundaryEnd(block), file.boundaryEnd(block), "block " + block));

        IntStream.range(0, 2 * RECORDS_PER_BLOCK + 3).forEach(i -> {
            assertEquals(heap.frontIsBoundary(), file.frontIsBoundary());
            assertEquals(heap.frontKey(), file.frontKey());
            heap.evictFront();
            file.evictFront();
        });
        assertEquals(heap.count(), file.count());
        assertEquals(heap.bytes(), file.bytes());
        assertSameFrames(replay(heap, 10, 3), replay(file, 10, 3));
        assertEquals(-1L, file.boundaryEnd(2), "an evicted boundary leaves the index");
    }

    @Test
    void a_restore_truncates_the_file_at_the_snapshot_head() throws IOException {
        final RetainedFile before = openFresh();
        blocks(before, 1, 6);
        final byte[] section = before.writeSnapshot(0, 1);
        final List<byte[]> atSnapshot = replay(before, 0, 1);
        final int segmentsAtSnapshot = segmentFiles().size();
        blocks(before, 6, BLOCKS + 1);
        assertTrue(segmentFiles().size() > segmentsAtSnapshot, "the appends after the snapshot open a segment");
        before.close();

        final RetainedFile restored = open(dir, SEGMENT_BYTES);
        assertEquals(new RetainedFrames.Floors(0, 1), restore(restored, section));
        final long snapshotHead = head(section);
        final Path last = segmentFiles().get(segmentFiles().size() - 1);
        final long lastStart = Long.parseLong(SegmentDir.SEGMENT_NAME.matcher(last.getFileName().toString())
            .results().findFirst().orElseThrow().group(1));
        assertTrue(lastStart < snapshotHead, "no segment starts at or above the head");
        final byte[] tail = Files.readAllBytes(last);
        final int headOffset = Math.toIntExact(snapshotHead - lastStart);
        assertTrue(IntStream.range(headOffset, tail.length).allMatch(i -> tail[i] == 0), "the file ends at the head");
        assertSameFrames(atSnapshot, replay(restored, 0, 1));

        // The log replay appends the same frames again, at the same positions.
        blocks(restored, 6, BLOCKS + 1);
        final RetainedFile fresh = open(dir.resolve("fresh"), SEGMENT_BYTES);
        fresh.clear();
        blocks(fresh, 1, BLOCKS + 1);
        assertSameFrames(replay(fresh, 0, 1), replay(restored, 0, 1));
        assertEquals(head(fresh.writeSnapshot(0, 1)), head(restored.writeSnapshot(0, 1)));
    }

    @Test
    void a_restore_stops_the_member_when_the_file_misses_a_segment() throws IOException {
        final RetainedFile before = openFresh();
        blocks(before, 1, BLOCKS + 1);
        final byte[] section = before.writeSnapshot(0, 1);
        before.close();
        Files.delete(segmentFiles().get(1));

        final RetainedFile restored = open(dir, SEGMENT_BYTES);
        assertThrows(AgentTerminationException.class, () -> restore(restored, section));
    }

    @Test
    void a_failed_preallocation_stops_the_member() throws IOException {
        final RetainedFile file = open(dir, SEGMENT_BYTES);
        // A directory at the name of the prepared file makes the helper fail, as a full disk does.
        Files.createDirectory(dir.resolve(SegmentPreallocator.PREPARED_NAME));
        file.clear();
        assertThrows(AgentTerminationException.class, () -> record(file, 0, PAYLOAD_BYTES));
    }

    @Test
    void the_boundary_tick_stops_the_member_after_a_failed_preallocation() throws IOException {
        final RetainedFile file = open(dir, SEGMENT_BYTES);
        Files.createDirectory(dir.resolve(SegmentPreallocator.PREPARED_NAME));
        file.clear();
        assertThrows(AgentTerminationException.class, () -> {
            while (true) {
                file.checkHealth();
                Thread.onSpinWait();
            }
        });
    }

    @Test
    void the_purge_keeps_the_segments_of_the_last_snapshot() throws IOException {
        final RetainedFile file = openFresh();
        blocks(file, 1, BLOCKS + 1);
        final List<Path> all = segmentFiles();
        // Evict the first half of the frames: the front moves into a later segment.
        IntStream.range(0, Math.toIntExact(file.count() / 2)).forEach(i -> file.evictFront());
        final List<byte[]> served = replay(file, file.frontKey(), BLOCKS / 2 + 1);

        file.writeSnapshot(0, 1);
        assertEquals(all, segmentFiles(), "the first snapshot keeps the segments below its own front");
        file.writeSnapshot(0, 1);
        final List<Path> kept = segmentFiles();
        assertTrue(kept.size() < all.size(), "the next snapshot deletes them: " + kept);
        assertEquals(all.get(all.size() - 1), kept.get(kept.size() - 1));
        assertSameFrames(served, replay(file, file.frontKey(), BLOCKS / 2 + 1));
    }

    @Test
    void the_heap_does_not_grow_with_the_retained_bytes() {
        final int frameBytes = 64 * 1024;
        final int frameCount = 1024;
        final long retainedBytes = (long) frameBytes * frameCount;

        final RetainedFile file = open(dir, 8 << 20);
        file.clear();
        final long fileGrowth = heapGrowth(file, frameBytes, frameCount);
        final long heapGrowth = heapGrowth(new HeapFrames(), frameBytes, frameCount);

        assertTrue(heapGrowth > retainedBytes / 2, "the measure sees the heap store: " + heapGrowth);
        assertTrue(fileGrowth < retainedBytes / 8,
            "the file store keeps " + retainedBytes + " bytes off the heap, and the heap grew " + fileGrowth);
    }

    /** The bytes that the heap holds after {@code count} frames of {@code bytes} go into {@code store}. */
    private long heapGrowth(final RetainedFrames store, final int bytes, final int count) {
        final long before = usedHeap();
        IntStream.range(0, count).forEach(i -> record(store, i, bytes - SealerWire.RELAYED_HEADER_LEN));
        final long growth = usedHeap() - before;
        assertEquals(count, store.count());
        return growth;
    }

    private static long usedHeap() {
        IntStream.range(0, 3).forEach(i -> System.gc());
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
    }

    @Test
    void the_settings_parse_once_at_the_start() {
        final Path clusterDir = Path.of("/opt/kardamom/cluster");
        assertTrue(RetainedFile.Config.parse(null, null, clusterDir).isEmpty());
        assertTrue(RetainedFile.Config.parse(" ", "1", clusterDir).isEmpty());
        assertTrue(RetainedFile.Config.parse("false", null, clusterDir).isEmpty());
        assertEquals(new RetainedFile.Config(clusterDir.resolve("retained"), RetainedFile.Config.DEFAULT_SEGMENT_BYTES),
            RetainedFile.Config.parse("true", "", clusterDir).orElseThrow());
        assertEquals(RetainedFile.Config.MIN_SEGMENT_BYTES,
            RetainedFile.Config.parse("true", Integer.toString(RetainedFile.Config.MIN_SEGMENT_BYTES), clusterDir)
                .orElseThrow().segmentBytes());
        assertThrows(IllegalArgumentException.class, () -> RetainedFile.Config.parse("yes", null, clusterDir));
        assertThrows(IllegalArgumentException.class, () -> RetainedFile.Config.parse(
            "true", Integer.toString(RetainedFile.Config.MIN_SEGMENT_BYTES - 1), clusterDir));
        assertThrows(IllegalArgumentException.class, () -> RetainedFile.Config.parse("true", "64m", clusterDir));
    }

    @Test
    void a_restore_stops_the_member_when_the_file_ends_below_the_head() throws IOException {
        final RetainedFile before = openFresh();
        blocks(before, 1, BLOCKS + 1);
        final byte[] section = before.writeSnapshot(0, 1);
        before.close();
        // A copy from a peer behind the snapshot holds the zeros of the preallocation below the head.
        final Path last = segmentFiles().get(segmentFiles().size() - 1);
        final long lastStart = Long.parseLong(SegmentDir.SEGMENT_NAME.matcher(last.getFileName().toString())
            .results().findFirst().orElseThrow().group(1));
        try (FileChannel channel = FileChannel.open(last, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.allocate(PAYLOAD_BYTES), head(section) - lastStart - PAYLOAD_BYTES);
        }

        final RetainedFile restored = open(dir, SEGMENT_BYTES);
        assertThrows(AgentTerminationException.class, () -> restore(restored, section));
    }

    /** A sealer snapshot payload of {@code version}: the magic and the version, then the retained section. */
    private static byte[] snapshotOf(final int version, final byte[] section) {
        return ByteBuffer.allocate(2 * Integer.BYTES + section.length)
            .putInt(0x4B53_4541).putInt(version).put(section).array();
    }

    @Test
    void a_seed_copy_passes_only_when_it_holds_the_snapshot() throws IOException {
        final Path source = dir.resolve("source");
        final RetainedFile peer = open(source, SEGMENT_BYTES);
        peer.clear();
        blocks(peer, 1, BLOCKS + 1);
        final byte[] section = peer.writeSnapshot(0, 1);
        final byte[] snapshot = snapshotOf(CanonicalSealerState.FILE_SNAPSHOT_VERSION, section);
        final long head = peer.publishedHead();
        final RetainedCopy copy = new RetainedCopy(List.of(), new SegmentDir(dir.resolve("into")), TOKEN, 1);

        assertTrue(copyFrom(source, head, copy, snapshot), "a peer at the snapshot head serves a whole copy");
        assertTrue(copyFrom(source, head, copy,
            snapshotOf(CanonicalSealerState.RETAINED_FILE_SNAPSHOT_VERSION, section)),
            "a version-12 snapshot also holds a retained file");
        assertFalse(copyFrom(source, head - 1, copy, snapshot), "a peer behind the snapshot fails the walk");
        assertFalse(copyFrom(source, head, copy, snapshotOf(10, new byte[Integer.BYTES])),
            "a snapshot without a retained file fails");
    }

    /** Serve the file in {@code source} below {@code head}, and run {@code copy} against it. */
    private static boolean copyFrom(final Path source, final long head, final RetainedCopy copy, final byte[] snapshot)
            throws IOException {
        final RetainedEndpoint endpoint = RetainedEndpoint.start(new SegmentDir(source), () -> head, TOKEN);
        final MemberStatus status = new MemberStatus(0, MemberStatus.ROLE_LEADER, MemberStatus.ELECTION_CLOSED,
            0, 0, MemberStatus.SnapshotVersions.of(0), io.kardamom.sealer.DecisionRules.START_VIEW, OptionalLong.empty());
        try (AdminServer admin = AdminServer.start(0, () -> status, 0, Optional.of(endpoint))) {
            final URI peer = URI.create("http://127.0.0.1:" + admin.port());
            return new RetainedCopy(List.of(peer), copy.into(), TOKEN, 1).run(snapshot);
        }
    }

    @Test
    void the_carried_bytes_need_the_admin_port_the_token_and_the_snapshots() {
        final Path clusterDir = Path.of("/opt/kardamom/cluster");
        assertTrue(new CarryBytes.Inputs(null, null, null, clusterDir, 0, 0).parse().isEmpty());
        final RetainedToken token = new CarryBytes.Inputs("true", null, " t ", clusterDir, 40205, 300).parse()
            .orElseThrow().token();
        assertTrue(token.authorizes("Bearer t"), "the token is trimmed");
        assertEquals("RetainedToken[hidden]", token.toString(), "a log line never prints the token");
        assertFalse(token.authorizes("Bearer u"));
        assertFalse(token.authorizes(null));
        assertThrows(IllegalArgumentException.class,
            () -> new CarryBytes.Inputs("true", null, "t", clusterDir, 0, 300).parse());
        assertThrows(IllegalArgumentException.class,
            () -> new CarryBytes.Inputs("true", null, " ", clusterDir, 40205, 300).parse());
        assertThrows(IllegalArgumentException.class,
            () -> new CarryBytes.Inputs("true", null, "t", clusterDir, 40205, 0).parse());
    }

    @Test
    void a_restore_stops_the_member_when_a_record_index_does_not_follow() throws IOException {
        final RetainedFile before = openFresh();
        blocks(before, 1, BLOCKS + 1);
        final byte[] section = before.writeSnapshot(0, 1);
        before.close();
        // The first record has index 0. A damaged index 5 would move the floor of this member only.
        try (FileChannel channel = FileChannel.open(segmentFiles().get(0), StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(0, 5L),
                RetainedFile.LENGTH_PREFIX + FrameKey.KEY_OFFSET);
        }

        final RetainedFile restored = open(dir, SEGMENT_BYTES);
        assertThrows(AgentTerminationException.class, () -> restore(restored, section));
    }

    @Test
    void a_section_must_end_with_its_own_length() {
        final RetainedFile file = openFresh();
        blocks(file, 1, 3);
        final byte[] section = file.writeSnapshot(0, 1);
        assertEquals(file.count(), RetainedSection.read(ByteBuffer.wrap(section)).count());

        final byte[] longer = Arrays.copyOf(section, section.length + 1);
        assertThrows(IllegalStateException.class, () -> RetainedSection.read(ByteBuffer.wrap(longer)));
        final byte[] badLength = section.clone();
        ByteBuffer.wrap(badLength).putInt(badLength.length - Integer.BYTES, 1);
        assertThrows(IllegalStateException.class, () -> RetainedSection.read(ByteBuffer.wrap(badLength)));
        assertThrows(AgentTerminationException.class, () -> restore(open(dir.resolve("other"), SEGMENT_BYTES), longer));
    }
}
