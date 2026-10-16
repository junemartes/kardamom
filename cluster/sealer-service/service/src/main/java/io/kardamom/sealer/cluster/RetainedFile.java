package io.kardamom.sealer.cluster;

import io.kardamom.sealer.ClusterStatus;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * The retained frames in an append-only file on the disk of the member.
 * The heap keeps only an index: the end index and the file position of each
 * retained boundary.
 *
 * <p>The file is a sequence of segment files in one directory. A segment is
 * named by the position of its first byte: {@code retained-<start>.seg},
 * with 20 digits. A position counts the bytes of all frames from the start
 * of the file, so the positions do not depend on the segment size. Each
 * frame is a record {@code [len:u32 LE][frame]}, and a record never crosses
 * a segment. A segment ends where the next segment starts. The last segment
 * ends at the head position.</p>
 *
 * <p>The service thread appends each frame through the mapped buffer of the
 * last segment, with no system call. When a frame does not fit, it takes the
 * next segment from {@link SegmentPreallocator} and asks for one more. When
 * the helper has not finished that segment, the service thread waits for
 * it. The helper thread owns only the preparation. The service thread owns the
 * appends, the index, the purge and the restore.</p>
 *
 * <p>Every byte of the file is a function of the log. A file I/O result
 * never changes the replicated state or the egress: an I/O error stops the
 * member with {@code sealer RETAINED-FILE FATAL}.</p>
 *
 * <p>The snapshot section holds the floors, the frame count, the front and
 * head positions, and the index. It does not hold the frames. Before it
 * writes the section, the member forces the mapped segments to disk up to
 * the head position. After a crash, the restore truncates the file at that
 * position, and the log replay appends the rest.</p>
 */
final class RetainedFile implements RetainedFrames {

    /** The directory of the retained file in the consensus module directory. */
    static final String DIR_NAME = "retained";
    /** The first int of the snapshot section. A heap section starts with its frame count, which is never negative. */
    static final int SECTION_MARKER = -1;
    /** The length prefix of each record in a segment. */
    static final int LENGTH_PREFIX = Integer.BYTES;
    /** The name of a segment file. Group 1 is the start position. */
    static final Pattern SEGMENT_NAME = Pattern.compile("retained-(\\d{20})\\.seg");

    /**
     * The retained-file settings, parsed once at the start.
     *
     * @param dir          the directory of the segment files
     * @param segmentBytes the size of a new segment; at least one record of the largest frame
     */
    record Config(Path dir, int segmentBytes) {
        /** The switch: {@code true} keeps the retained frames in the file. */
        static final String SETTING = "kardamom.cluster.carryBytes";
        /** The size of a new segment, in bytes. */
        static final String SEGMENT_SETTING = "kardamom.cluster.retainedSegmentBytes";
        static final int DEFAULT_SEGMENT_BYTES = 64 << 20;
        /** One record of the largest egress frame fits: a frame is shorter than one session message. */
        static final int MIN_SEGMENT_BYTES = SealerWire.MAX_SESSION_MESSAGE_LEN;

        /**
         * The settings from their property values, or empty when the switch
         * is off. An unset or empty switch is off.
         *
         * @throws IllegalArgumentException when a value does not parse, or the segment is too small
         */
        static Optional<Config> parse(final String carryBytes, final String segmentBytes, final Path clusterDir) {
            final boolean on = switch (Optional.ofNullable(carryBytes).map(String::trim).orElse("")) {
                case "", "false" -> false;
                case "true" -> true;
                default -> throw new IllegalArgumentException(SETTING + " must be true or false, got " + carryBytes);
            };
            return on ? Optional.of(of(clusterDir.resolve(DIR_NAME), segmentBytes)) : Optional.empty();
        }

        private static Config of(final Path dir, final String segmentBytes) {
            final int bytes = Optional.ofNullable(segmentBytes).filter(s -> !s.isBlank())
                .map(s -> Integer.parseInt(s.trim()))
                .orElse(DEFAULT_SEGMENT_BYTES);
            if (bytes < MIN_SEGMENT_BYTES) {
                throw new IllegalArgumentException(
                    SEGMENT_SETTING + " must be at least " + MIN_SEGMENT_BYTES + ", got " + bytes);
            }
            return new Config(dir, bytes);
        }
    }

    /** One mapped segment file. The capacity is the size of the file. */
    private record Segment(long start, Path path, MappedByteBuffer buffer) {
        long end() {
            return Math.addExact(start, buffer.capacity());
        }

        int offset(final long pos) {
            return Math.toIntExact(pos - start);
        }

        boolean fits(final long pos, final int recordBytes) {
            return start <= pos && Math.addExact(pos, recordBytes) <= end();
        }
    }

    /** One retained boundary: its end index, and the position after its record. */
    private record Mark(long endTxIdx, long endPos) {
    }

    private final Config config;
    private final int memberId;
    private final SegmentPreallocator preallocator;
    /** The live segments by start position. The last one takes the appends. */
    private final TreeMap<Long, Segment> segments = new TreeMap<>();
    /** The retained boundaries by block number. */
    private final TreeMap<Long, Mark> boundaries = new TreeMap<>();
    /** Whether a request to the helper is open. At most one is. */
    private boolean spareAsked;
    /** The position of the oldest retained frame. */
    private long frontPos;
    /** The position after the newest frame. */
    private long headPos;
    private long count;
    /** The mapped bytes below this position are on disk. */
    private long forcedPos;
    /** The purge deletes each segment that ends at or below this position: the front of the last snapshot. */
    private long purgeBelow;
    /** The head position at the last boundary tick, for readers on other threads. */
    private volatile long publishedHead;

    RetainedFile(final Config config, final int memberId) {
        this.config = config;
        this.memberId = memberId;
        io("create " + config.dir(), () -> Files.createDirectories(config.dir()));
        this.preallocator = new SegmentPreallocator(config.dir(), config.segmentBytes(), memberId);
    }

    @Override
    public byte[] append(final DirectBuffer staged, final int length) {
        final int recordBytes = LENGTH_PREFIX + length;
        final Segment segment = Optional.ofNullable(segments.lastEntry())
            .map(Map.Entry::getValue)
            .filter(s -> s.fits(headPos, recordBytes))
            .orElseGet(this::roll);
        final int offset = segment.offset(headPos);
        segment.buffer().putInt(offset, length);
        staged.getBytes(0, segment.buffer(), offset + LENGTH_PREFIX, length);
        headPos = Math.addExact(headPos, recordBytes);
        count = Math.addExact(count, 1);
        markBoundary(staged);
        final byte[] frame = new byte[length];
        staged.getBytes(0, frame);
        return frame;
    }

    /** Index the frame in {@code staged} when it is a boundary. */
    private void markBoundary(final DirectBuffer staged) {
        final FrameKey key = FrameKey.of(staged);
        if (key.boundary()) {
            boundaries.put(key.key(), new Mark(
                staged.getLong(FrameKey.BOUNDARY_END_OFFSET, ByteOrder.LITTLE_ENDIAN), headPos));
        }
    }

    /** Take the prepared segment at the head position, and ask for the next one. */
    private Segment roll() {
        final SegmentPreallocator.Ready ready = takePrepared();
        final Path path = config.dir().resolve(segmentName(headPos));
        io("name the segment " + path, () -> Files.move(ready.path(), path, StandardCopyOption.ATOMIC_MOVE));
        final Segment segment = new Segment(headPos, path, ready.buffer());
        segments.put(headPos, segment);
        askSpare();
        return segment;
    }

    private SegmentPreallocator.Ready takePrepared() {
        final SegmentPreallocator.Prepared prepared;
        try {
            prepared = preallocator.take();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw fatal("wait for the next segment", e);
        }
        spareAsked = false;
        if (prepared instanceof SegmentPreallocator.Failed failed) {
            throw fatal("preallocate the next segment", failed.cause());
        }
        return (SegmentPreallocator.Ready) prepared;
    }

    private void askSpare() {
        if (!spareAsked) {
            preallocator.ask();
            spareAsked = true;
        }
    }

    @Override
    public long count() {
        return count;
    }

    @Override
    public FrameKey front() {
        return keyAt(frontPos);
    }

    @Override
    public void evictFront() {
        final FrameKey key = front();
        if (key.boundary()) {
            boundaries.remove(key.key());
        }
        frontPos = next(frontPos);
        count = Math.subtractExact(count, 1);
    }

    @Override
    public long boundaryEnd(final long block) {
        return Optional.ofNullable(boundaries.get(block)).map(Mark::endTxIdx).orElse(-1L);
    }

    /**
     * The scan starts after the boundary of the block before
     * {@code fromBlock} when that boundary is retained, else at the front.
     * A frame before that start is a boundary below {@code fromBlock}, or a
     * record of an earlier block, whose index is below {@code fromIndex}.
     */
    @Override
    public Stream<byte[]> wanted(final long fromIndex, final long fromBlock) {
        final long start = Optional.ofNullable(boundaries.get(fromBlock - 1)).map(Mark::endPos).orElse(frontPos);
        return Stream.iterate(start, pos -> pos < headPos, this::next)
            .filter(pos -> keyAt(pos).wanted(fromIndex, fromBlock))
            .map(this::frameAt);
    }

    private Segment segmentAt(final long pos) {
        return segments.floorEntry(pos).getValue();
    }

    private FrameKey keyAt(final long pos) {
        final Segment segment = segmentAt(pos);
        return FrameKey.of(new UnsafeBuffer(segment.buffer(), segment.offset(pos) + LENGTH_PREFIX,
            Byte.BYTES + Long.BYTES));
    }

    private long next(final long pos) {
        final Segment segment = segmentAt(pos);
        return Math.addExact(pos, LENGTH_PREFIX + segment.buffer().getInt(segment.offset(pos)));
    }

    private byte[] frameAt(final long pos) {
        final Segment segment = segmentAt(pos);
        final int offset = segment.offset(pos);
        final byte[] frame = new byte[segment.buffer().getInt(offset)];
        segment.buffer().get(offset + LENGTH_PREFIX, frame);
        return frame;
    }

    /** The bytes of the retained file above the posted head. */
    long bytesAbove(final long postedHead) {
        final long from = Optional.ofNullable(boundaries.floorEntry(postedHead))
            .map(e -> e.getValue().endPos())
            .orElse(frontPos);
        return Math.subtractExact(headPos, from);
    }

    @Override
    public int status(final EgressFrames codec, final ClusterStatus status, final long postedHead) {
        return codec.status(status, bytesAbove(postedHead));
    }

    /**
     * Force the segments to disk up to the head, purge the segments below
     * the front of the last snapshot, and write the section:
     * {@code marker(4) | floor_index(8) | floor_block(8) | count(8) |
     * front(8) | head(8) | n(4) | n * (block(8) | end_tx_idx(8) | end_pos(8))},
     * big-endian like the state.
     *
     * <p>The purge keeps the segments of the last snapshot. So a peer seed
     * that copies the latest snapshot finds every segment that it needs,
     * unless the peer takes two more snapshots during the copy.</p>
     */
    @Override
    public byte[] writeSnapshot(final long floorIndex, final long floorBlock) {
        force();
        purge();
        final int markBytes = Math.multiplyExact(3 * Long.BYTES, boundaries.size());
        final ByteBuffer buf = ByteBuffer
            .allocate(Math.addExact(2 * Integer.BYTES + 5 * Long.BYTES, markBytes))
            .order(ByteOrder.BIG_ENDIAN);
        buf.putInt(SECTION_MARKER).putLong(floorIndex).putLong(floorBlock)
            .putLong(count).putLong(frontPos).putLong(headPos).putInt(boundaries.size());
        boundaries.forEach((block, mark) -> buf.putLong(block).putLong(mark.endTxIdx()).putLong(mark.endPos()));
        return buf.array();
    }

    private void force() {
        final long from = Optional.ofNullable(segments.floorKey(forcedPos)).orElse(forcedPos);
        segments.subMap(from, true, headPos, false).values().stream()
            .filter(s -> Math.min(headPos, s.end()) > Math.max(forcedPos, s.start()))
            .forEach(s -> io("force " + s.path(), () -> forceRange(s)));
        forcedPos = headPos;
    }

    private Void forceRange(final Segment segment) {
        final int from = segment.offset(Math.max(forcedPos, segment.start()));
        segment.buffer().force(from, segment.offset(Math.min(headPos, segment.end())) - from);
        return null;
    }

    private void purge() {
        Optional.ofNullable(segments.floorKey(purgeBelow)).ifPresent(keep -> drop(segments.headMap(keep, false)));
        purgeBelow = frontPos;
    }

    /** Delete the segments of {@code dead}, unmap them, and remove them from the map. */
    private void drop(final NavigableMap<Long, Segment> dead) {
        dead.values().forEach(s -> delete(s.path()));
        forget(dead);
    }

    /**
     * Unmap the segments of {@code old} and remove them from the map. A
     * deleted file frees its disk blocks only when no mapping holds it.
     */
    private static void forget(final NavigableMap<Long, Segment> old) {
        old.values().forEach(s -> IoUtil.unmap(s.buffer()));
        old.clear();
    }

    /**
     * Restore the section after its marker, and make the file match it:
     * delete the segments at or above the head, zero the last segment from
     * the head on, and delete the segments below the front. The kept
     * segments must hold every byte from the front to the head. A file that
     * does not stops the member: it would serve a partial retention.
     */
    @Override
    public Floors restore(final ByteBuffer section) {
        final Floors floors = new Floors(section.getLong(), section.getLong());
        count = section.getLong();
        frontPos = section.getLong();
        headPos = section.getLong();
        final int marks = section.getInt();
        boundaries.clear();
        IntStream.range(0, marks).forEach(i -> boundaries.put(section.getLong(),
            new Mark(section.getLong(), section.getLong())));
        forget(segments);
        final TreeMap<Long, Path> files = listSegments();
        final long first = Optional.ofNullable(files.floorKey(frontPos)).orElse(frontPos);
        files.tailMap(headPos, true).values().forEach(this::delete);
        files.headMap(first, false).values().forEach(this::delete);
        files.subMap(first, true, headPos, false).forEach(this::map);
        if (!covers()) {
            throw fatal("restore", new IllegalStateException("the retained file in " + config.dir()
                + " does not hold the positions [" + frontPos + ", " + headPos + ") of the snapshot"));
        }
        truncateAtHead();
        forcedPos = frontPos;
        purgeBelow = frontPos;
        publishedHead = headPos;
        askSpare();
        return floors;
    }

    /** Whether the mapped segments hold every byte from the front to the head, with no gap. */
    private boolean covers() {
        final List<Segment> live = List.copyOf(segments.values());
        return frontPos == headPos || !live.isEmpty()
            && live.get(0).start() <= frontPos
            && IntStream.range(1, live.size()).allMatch(i -> live.get(i - 1).end() >= live.get(i).start())
            && live.get(live.size() - 1).end() >= headPos;
    }

    /** Zero the bytes of the last segment from the head on: the file ends at the head. */
    private void truncateAtHead() {
        Optional.ofNullable(segments.lastEntry()).map(Map.Entry::getValue)
            .filter(s -> s.start() <= headPos && headPos < s.end())
            .ifPresent(s -> new UnsafeBuffer(s.buffer())
                .setMemory(s.offset(headPos), s.buffer().capacity() - s.offset(headPos), (byte) 0));
    }

    @Override
    public void clear() {
        drop(segments);
        listSegments().values().forEach(this::delete);
        boundaries.clear();
        count = 0;
        frontPos = 0;
        headPos = 0;
        forcedPos = 0;
        purgeBelow = 0;
        publishedHead = 0;
        askSpare();
    }

    /** The segment files in the directory, by start position. */
    private TreeMap<Long, Path> listSegments() {
        return io("list " + config.dir(), () -> {
            try (Stream<Path> paths = Files.list(config.dir())) {
                return paths
                    .map(p -> SEGMENT_NAME.matcher(p.getFileName().toString()))
                    .filter(Matcher::matches)
                    .collect(Collectors.toMap(m -> Long.parseLong(m.group(1)),
                        m -> config.dir().resolve(m.group()), (a, b) -> a, TreeMap::new));
            }
        });
    }

    private void delete(final Path path) {
        io("delete " + path, () -> Files.deleteIfExists(path));
    }

    private void map(final long start, final Path path) {
        final MappedByteBuffer buffer = io("map " + path, () -> {
            try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                return channel.map(FileChannel.MapMode.READ_WRITE, 0, channel.size());
            }
        });
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        segments.put(start, new Segment(start, path, buffer));
    }

    /** Stop the member when the helper failed to prepare a segment, and publish the head position. */
    @Override
    public void checkHealth() {
        preallocator.failure().ifPresent(cause -> {
            throw fatal("preallocate the next segment", cause);
        });
        publishedHead = headPos;
    }

    /** The head position at the last boundary tick. Every byte below it is in the file. */
    long publishedHead() {
        return publishedHead;
    }

    /** Stop the helper. The mappings stay until the collector frees them, so no late read can fault. */
    @Override
    public void close() {
        preallocator.close();
    }

    /** The name of the segment that starts at {@code start}. */
    static String segmentName(final long start) {
        return String.format("retained-%020d.seg", start);
    }

    /** Log the fatal error and return the exception that stops the service agent. */
    private AgentTerminationException fatal(final String what, final Exception cause) {
        System.out.println("sealer RETAINED-FILE FATAL memberId=" + memberId + " " + what + ": " + cause);
        System.out.flush();
        return new AgentTerminationException(cause);
    }

    /** One file system step that can throw a checked exception. */
    @FunctionalInterface
    private interface IoStep<T> {
        T run() throws IOException;
    }

    /** Run {@code step}. An I/O error stops the member. */
    private <T> T io(final String what, final IoStep<T> step) {
        try {
            return step.run();
        } catch (final IOException | UncheckedIOException e) {
            throw fatal(what, e);
        }
    }
}
