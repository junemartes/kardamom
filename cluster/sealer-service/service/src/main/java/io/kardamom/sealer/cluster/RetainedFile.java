package io.kardamom.sealer.cluster;

import io.kardamom.sealer.ClusterStatus;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * The retained frames in an append-only file on the disk of the member.
 * The heap keeps only an index: the end index and the file position of each
 * retained boundary.
 *
 * <p>The file is a sequence of segment files in one directory
 * ({@link SegmentDir}). A position counts the bytes of all records from the
 * start of the file, so the positions do not depend on the segment size.
 * Each frame is a record {@code [len:u32 LE][frame]}, and a record never
 * crosses a segment. The records of a segment end where the next segment
 * starts. A segment file can be longer than its records: the rest of it
 * holds the zeros of the preallocation.</p>
 *
 * <p>The service thread appends each frame through the mapped buffer of the
 * last segment, with no system call. When a frame does not fit, it takes the
 * next segment from {@link SegmentPreallocator} and asks for one more. When
 * the helper has not finished that segment, the service thread waits for
 * it. The helper thread owns only the preparation. The service thread owns
 * the appends, the index, the purge and the restore.</p>
 *
 * <p>Every byte of the file is a function of the log. A file I/O result
 * never changes the replicated state or the egress: an I/O error stops the
 * member with {@code sealer RETAINED-FILE FATAL}.</p>
 *
 * <p>The eviction and the replay read the records of the file. The reads
 * act alike on every member because every record from the front to the
 * head is a whole frame that this member wrote, or that a restore checked
 * with {@link RetainedWalk}: a restore that finds any other byte stops the
 * member.</p>
 *
 * <p>The snapshot section ({@link RetainedSection}) holds the floors, the
 * frame count, the front and head positions, and the index. It does not
 * hold the frames. Before it writes the section, the member forces the
 * mapped segments to disk up to the head position. After a crash, the
 * restore truncates the file at that position, and the log replay appends
 * the rest.</p>
 */
final class RetainedFile implements RetainedFrames {

    /** The directory of the retained file in the consensus module directory. */
    static final String DIR_NAME = "retained";
    /** The length prefix of each record in a segment. */
    static final int LENGTH_PREFIX = Integer.BYTES;

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

        /** Whether a record of {@code recordBytes} at {@code pos}, at or after the start, fits. */
        boolean fits(final long pos, final int recordBytes) {
            return Math.addExact(pos, recordBytes) <= end();
        }
    }

    private final Config config;
    private final SegmentDir dir;
    private final int memberId;
    private final SegmentPreallocator preallocator;
    /** The live segments by start position. The last one takes the appends. */
    private final TreeMap<Long, Segment> segments = new TreeMap<>();
    /** The retained boundaries by block number. */
    private final TreeMap<Long, RetainedSection.Mark> boundaries = new TreeMap<>();
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
    /**
     * The head position at the last boundary tick, snapshot or restore. The
     * admin thread serves the file only below it: every byte below it is a
     * whole record.
     */
    private volatile long publishedHead;

    RetainedFile(final Config config, final int memberId) {
        this.config = config;
        this.dir = new SegmentDir(config.dir());
        this.memberId = memberId;
        this.preallocator = new SegmentPreallocator(config.dir(), config.segmentBytes(), memberId);
        io("create " + config.dir(), () -> Files.createDirectories(config.dir()));
    }

    @Override
    public void append(final DirectBuffer staged, final int length) {
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
    }

    /** Index the frame in {@code staged} when it is a boundary. */
    private void markBoundary(final DirectBuffer staged) {
        final FrameKey key = FrameKey.of(staged);
        if (key.boundary()) {
            boundaries.put(key.key(), new RetainedSection.Mark(
                staged.getLong(FrameKey.BOUNDARY_END_OFFSET, ByteOrder.LITTLE_ENDIAN), headPos));
        }
    }

    /**
     * Take the prepared segment at the head position, name it, sync the
     * directory, and ask for the next one. The sync keeps the name of a
     * segment that a later snapshot counts on.
     */
    private Segment roll() {
        final SegmentPreallocator.Ready ready = takePrepared();
        final Path path = dir.segment(headPos);
        io("name the segment " + path, () -> Files.move(ready.path(), path, StandardCopyOption.ATOMIC_MOVE));
        io("sync " + config.dir(), () -> {
            dir.sync();
            return null;
        });
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
    public long bytes() {
        return Math.subtractExact(headPos, frontPos);
    }

    @Override
    public boolean carriesBytes() {
        return true;
    }

    @Override
    public boolean frontIsBoundary() {
        return keyAt(frontPos).boundary();
    }

    @Override
    public long frontKey() {
        return keyAt(frontPos).key();
    }

    @Override
    public void evictFront() {
        final FrameKey key = keyAt(frontPos);
        if (key.boundary()) {
            boundaries.remove(key.key());
        }
        frontPos = walkNext(frontPos);
        count = Math.subtractExact(count, 1);
    }

    @Override
    public long boundaryEnd(final long block) {
        return Optional.ofNullable(boundaries.get(block)).map(RetainedSection.Mark::endTxIdx).orElse(-1L);
    }

    /**
     * The scan starts after the boundary of the block before
     * {@code fromBlock} when that boundary is retained, else at the front.
     * A frame before that start is a boundary below {@code fromBlock}, or a
     * record of an earlier block, whose index is below {@code fromIndex}.
     */
    @Override
    public Stream<byte[]> wanted(final long fromIndex, final long fromBlock) {
        return Stream.iterate(walkStart(fromBlock), pos -> pos < headPos, this::walkNext)
            .filter(pos -> FrameKey.wanted(keyView(pos), fromIndex, fromBlock))
            .map(this::frameAt);
    }

    /** The walk key is the file position of the record. */
    @Override
    public long walkStart(final long fromBlock) {
        return Optional.ofNullable(boundaries.get(fromBlock - 1))
            .map(RetainedSection.Mark::endPos)
            .orElse(frontPos);
    }

    /** The position of the oldest retained frame. Every record from it to {@link #walkHead()} is readable. */
    @Override
    public long walkFront() {
        return frontPos;
    }

    /** The position after the newest frame. */
    @Override
    public long walkHead() {
        return headPos;
    }

    /** The file keeps the bytes of a record until the next purge, so the view stays valid until then. */
    @Override
    public void view(final long pos, final UnsafeBuffer view) {
        final Segment segment = segmentAt(pos);
        final int offset = segment.offset(pos);
        view.wrap(segment.buffer(), offset + LENGTH_PREFIX, segment.buffer().getInt(offset));
    }

    private Segment segmentAt(final long pos) {
        return segments.floorEntry(pos).getValue();
    }

    private FrameKey keyAt(final long pos) {
        return FrameKey.of(keyView(pos));
    }

    /** The kind and the key of the frame of the record at {@code pos}. */
    private UnsafeBuffer keyView(final long pos) {
        final Segment segment = segmentAt(pos);
        return new UnsafeBuffer(segment.buffer(), segment.offset(pos) + LENGTH_PREFIX, Byte.BYTES + Long.BYTES);
    }

    /** The position of the record after the record at {@code pos}. */
    @Override
    public long walkNext(final long pos) {
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
    public int status(
            final EgressFrames codec, final ClusterStatus status, final boolean snapshotFollows, final long postedHead) {
        return codec.status(status, snapshotFollows, bytesAbove(postedHead));
    }

    /**
     * Force the segments to disk up to the head, purge the segments below
     * the front of the last snapshot, and write the section.
     *
     * <p>The purge keeps the segments of the last snapshot. So a peer seed
     * that copies the latest snapshot finds every segment that it needs,
     * unless the peer takes two more snapshots during the copy. A segment
     * leaves the disk one to two snapshot intervals after the front passes
     * it.</p>
     */
    @Override
    public byte[] writeSnapshot(final long floorIndex, final long floorBlock) {
        force();
        purge();
        publishedHead = headPos;
        return new RetainedSection(new Floors(floorIndex, floorBlock), count, frontPos, headPos, boundaries).write();
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
    private void forget(final NavigableMap<Long, Segment> old) {
        old.values().forEach(s -> IoUtil.unmap(s.buffer()));
        old.clear();
    }

    /**
     * Restore a section, and make the file match it: delete the segments at
     * or above the head and below the front, check the records from the
     * front to the head with {@link RetainedWalk}, and zero the last segment
     * from the head on. A file that fails the walk stops the member: it
     * would serve a partial retention, or evict on other bytes than its
     * peers.
     */
    @Override
    public Floors restore(final ByteBuffer buf, final Floors restorePoint) {
        final RetainedSection section = readSection(buf);
        count = section.count();
        frontPos = section.front();
        headPos = section.head();
        boundaries.clear();
        boundaries.putAll(section.marks());
        forget(segments);
        final TreeMap<Long, Path> files = io("list " + config.dir(), dir::list);
        final long first = Optional.ofNullable(files.floorKey(frontPos)).orElse(frontPos);
        files.tailMap(headPos, true).values().forEach(this::delete);
        files.headMap(first, false).values().forEach(this::delete);
        files.subMap(first, true, headPos, false).keySet().forEach(this::map);
        new RetainedWalk(buffers(), section).problem().ifPresent(problem -> {
            throw fatal("restore", new IllegalStateException(
                "the retained file in " + config.dir() + " does not match the snapshot: " + problem));
        });
        truncateAtHead();
        forcedPos = headPos;
        purgeBelow = frontPos;
        publishedHead = headPos;
        askSpare();
        return section.floors();
    }

    /** The section in {@code buf}. A section that does not parse stops the member. */
    private RetainedSection readSection(final ByteBuffer buf) {
        try {
            return RetainedSection.read(buf);
        } catch (final IllegalStateException | BufferUnderflowException e) {
            throw fatal("read the snapshot section", e);
        }
    }

    private TreeMap<Long, ByteBuffer> buffers() {
        return segments.values().stream().collect(Collectors.toMap(
            Segment::start, Segment::buffer, (a, b) -> a, TreeMap::new));
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
        io("list " + config.dir(), dir::list).values().forEach(this::delete);
        boundaries.clear();
        count = 0;
        frontPos = 0;
        headPos = 0;
        forcedPos = 0;
        purgeBelow = 0;
        publishedHead = 0;
        askSpare();
    }

    private void delete(final Path path) {
        io("delete " + path, () -> Files.deleteIfExists(path));
    }

    private void map(final long start) {
        final MappedByteBuffer buffer = io("map " + dir.segment(start),
            () -> dir.map(start, FileChannel.MapMode.READ_WRITE));
        segments.put(start, new Segment(start, dir.segment(start), buffer));
    }

    /** Stop the member when the helper failed to prepare a segment, and publish the head position. */
    @Override
    public void checkHealth() {
        preallocator.failure().ifPresent(cause -> {
            throw fatal("preallocate the next segment", cause);
        });
        publishedHead = headPos;
    }

    @Override
    public long publishedHead() {
        return publishedHead;
    }

    @Override
    public byte[] forSession(final byte[] frame) {
        return EgressFrames.forSession(frame);
    }

    @Override
    public int forSession(final MutableDirectBuffer staged, final int length) {
        return EgressFrames.forSession(staged, length);
    }

    /** Stop the helper and wait for it. The mappings stay until the collector frees them, so no late read can fault. */
    @Override
    public void close() {
        preallocator.close();
    }

    /**
     * Ask the helper to stop, log the fatal error, and return the exception
     * that stops the service agent. It does not wait for the helper: a
     * helper inside a sync of a failed disk could hold up the stop.
     */
    private AgentTerminationException fatal(final String what, final Exception cause) {
        preallocator.stop();
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
