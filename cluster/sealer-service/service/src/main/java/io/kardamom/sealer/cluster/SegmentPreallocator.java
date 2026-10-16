package io.kardamom.sealer.cluster;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.agrona.IoUtil;

/**
 * The helper thread of the retained file. It creates the next segment file,
 * writes zeros into all of it, syncs it, and maps it. So the disk holds
 * every block of the segment before the service thread writes through the
 * mapping. A write through a mapping to a block that the disk cannot give
 * raises SIGBUS, not an exception. The zero fill turns a full disk into an
 * {@link IOException} on this thread.
 *
 * <p>Ownership: this thread owns the one file it prepares until it puts the
 * file into {@link #ready}. The service thread owns each segment that it
 * takes. Two queues are the only shared state. The service thread asks for
 * one segment through {@link #demand} and gets it, or the failure, through
 * {@link #ready}. At most one request is open, so one name serves every
 * prepared file.</p>
 */
final class SegmentPreallocator {

    /** The name of the prepared file, until the service thread names it by its start position. */
    static final String PREPARED_NAME = "segment.next";

    /** One prepared segment, or the failure of its preparation. */
    sealed interface Prepared permits Ready, Failed {
    }

    /** A prepared segment: its file, mapped read-write, little-endian. */
    record Ready(Path path, MappedByteBuffer buffer) implements Prepared {
    }

    /** A preparation that failed: the member cannot extend its retained file. */
    record Failed(IOException cause) implements Prepared {
    }

    /** The longest wait of {@link #close()} for the helper thread. */
    private static final long CLOSE_WAIT_MS = 5_000;
    /** The size of one write of the zero fill. */
    private static final int ZERO_CHUNK = 1 << 20;

    private final Path path;
    private final int segmentBytes;
    private final BlockingQueue<Boolean> demand = new ArrayBlockingQueue<>(1);
    private final BlockingQueue<Prepared> ready = new ArrayBlockingQueue<>(1);
    private final Thread thread;

    SegmentPreallocator(final Path dir, final int segmentBytes, final int memberId) {
        this.path = dir.resolve(PREPARED_NAME);
        this.segmentBytes = segmentBytes;
        this.thread = new Thread(this::run, "kardamom-retained-prealloc-" + memberId);
        thread.setDaemon(true);
        thread.start();
    }

    /** Ask for the next segment. The service thread calls it once after each take, and once at the start. */
    void ask() {
        demand.add(Boolean.TRUE);
    }

    /** The next prepared segment. Wait while the helper still writes it. */
    Prepared take() throws InterruptedException {
        return ready.take();
    }

    /** The failure that waits in the queue, if the last preparation failed. */
    Optional<IOException> failure() {
        return Optional.ofNullable(ready.peek())
            .filter(Failed.class::isInstance)
            .map(p -> ((Failed) p).cause());
    }

    /**
     * Ask the helper thread to stop, and do not wait. A helper inside a sync
     * of a failed disk does not wake on the interrupt, so a fatal stop must
     * not wait for it. The thread is a daemon, so it never keeps the JVM
     * alive.
     */
    void stop() {
        thread.interrupt();
    }

    /**
     * Stop the helper thread, wait for it up to {@link #CLOSE_WAIT_MS}, and
     * unmap a segment that waits in the queue. The file of that segment
     * stays on disk, and the next helper writes over it.
     */
    void close() {
        stop();
        try {
            thread.join(CLOSE_WAIT_MS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        Optional.ofNullable(ready.poll())
            .filter(Ready.class::isInstance)
            .ifPresent(p -> IoUtil.unmap(((Ready) p).buffer()));
    }

    private void run() {
        boolean running = true;
        while (running) {
            running = prepareOnDemand();
        }
    }

    /** Wait for one request and answer it. Return false when the thread must stop. */
    private boolean prepareOnDemand() {
        try {
            demand.take();
        } catch (final InterruptedException e) {
            return false;
        }
        // The service thread takes each answer before it asks again, so the queue has room.
        ready.add(prepare());
        return true;
    }

    private Prepared prepare() {
        try (FileChannel channel = FileChannel.open(path,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            fillWithZeros(channel);
            channel.force(true);
            final MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, segmentBytes);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            return new Ready(path, buffer);
        } catch (final IOException e) {
            return new Failed(e);
        }
    }

    private void fillWithZeros(final FileChannel channel) throws IOException {
        final ByteBuffer zeros = ByteBuffer.allocateDirect(ZERO_CHUNK);
        long written = 0;
        while (written < segmentBytes) {
            written = writeZeros(channel, zeros, written);
        }
    }

    /** Write one chunk of zeros at {@code at}, and return the position after the write. */
    private long writeZeros(final FileChannel channel, final ByteBuffer zeros, final long at) throws IOException {
        final int chunk = Math.toIntExact(Math.min(ZERO_CHUNK, segmentBytes - at));
        return Math.addExact(at, channel.write(zeros.clear().limit(chunk), at));
    }
}
