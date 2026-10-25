package io.kardamom.sealer.cluster;

import io.kardamom.sealer.ClusterStatus;
import java.nio.ByteBuffer;
import java.util.stream.Stream;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * The egress frames that a member retains for replay, in emission order.
 * {@link SealerEgress} owns the floors and the eviction rule. This store
 * owns the bytes.
 *
 * <p>The set is closed: {@link HeapFrames} keeps each frame on the heap,
 * and {@link RetainedFile} keeps the frames in an append-only file and
 * only an index on the heap. {@code kardamom.cluster.carryBytes} selects
 * one. Every call runs on the clustered-service thread, and every result
 * is a pure function of the log.</p>
 *
 * <p>The member feed walks the frames by a walk key of type {@code long}.
 * The key of a frame does not change while the frame stays: an append, an
 * eviction and a growth of a ring keep it. The key after a frame is above
 * the key of the frame. The keys are local to the member: they are not in
 * a snapshot, and no other member reads them.</p>
 */
sealed interface RetainedFrames permits HeapFrames, RetainedFile {

    /** The floors of a restored retention: the oldest record index and boundary block. */
    record Floors(long index, long block) {
    }

    /** Retain the first {@code length} bytes of {@code staged}, a relayed frame or a boundary frame. */
    void append(DirectBuffer staged, int length);

    /** The number of retained frames. */
    long count();

    /** The bytes of the retained frames. */
    long bytes();

    /**
     * Whether this store keeps the retained file. Then the egress keeps only
     * its replay window under decision version 5, also without the carry
     * window.
     */
    boolean carriesBytes();

    /** Whether the oldest retained frame is a boundary. Call it only when {@link #count()} is above 0. */
    boolean frontIsBoundary();

    /** The replay key of the oldest retained frame. Call it only when {@link #count()} is above 0. */
    long frontKey();

    /** Drop the oldest retained frame. Call it only when {@link #count()} is above 0. */
    void evictFront();

    /**
     * The end index of the retained boundary of {@code block}, or -1 when
     * this store does not hold it: block 0 has no boundary, an old one aged
     * out, and the open block has none yet.
     */
    long boundaryEnd(long block);

    /**
     * The retained frames that a replay from {@code (fromIndex, fromBlock)}
     * includes, in emission order. The caller checked that the cursor lies
     * inside its block and at or above the floors.
     */
    Stream<byte[]> wanted(long fromIndex, long fromBlock);

    /**
     * The walk key where a feed answer from {@code fromBlock} starts: the
     * key after the retained boundary of the block before it, else
     * {@link #walkFront()}. A frame before that key is a boundary below
     * {@code fromBlock}, or a record of an earlier block.
     */
    long walkStart(long fromBlock);

    /** The walk key of the oldest retained frame. Every key from it to {@link #walkHead()} names a retained frame. */
    long walkFront();

    /** The walk key after the newest retained frame. */
    long walkHead();

    /**
     * Point {@code view} at the bytes of the frame at {@code key}, without a
     * copy. The key is at or above {@link #walkFront()} and below
     * {@link #walkHead()}. The view stays valid until the next append or
     * eviction.
     */
    void view(long key, UnsafeBuffer view);

    /** The walk key of the frame after the frame at {@code key}. */
    long walkNext(long key);

    /** The retention section of a snapshot. Every member writes the same bytes at the same log position. */
    byte[] writeSnapshot(long floorIndex, long floorBlock);

    /**
     * Restore the retention section of a snapshot that this store wrote, at
     * the position of {@code section}. Return the floors. The floors start
     * at {@code restorePoint}, the next record and block to emit.
     */
    Floors restore(ByteBuffer section, Floors restorePoint);

    /** Drop every retained frame: the start of an empty retention. */
    void clear();

    /**
     * Frame the status into {@code frames}, with the snapshot flag, and
     * return the length: 68 bytes on the heap, 76 bytes with the retained
     * bytes above {@code postedHead} for the file.
     */
    int status(EgressFrames frames, ClusterStatus status, boolean snapshotFollows, long postedHead);

    /**
     * Stop the member when the store cannot go on, and publish the progress
     * of the store for readers on other threads. Call it at each boundary tick.
     */
    void checkHealth();

    /**
     * The position below which every byte of the retained file is a whole
     * record, for the admin thread that serves the file. 0 for the heap.
     */
    long publishedHead();

    /**
     * A retained frame as an egress session gets it. The heap store gives
     * the frame as is. The file store gives a canonical entry as its
     * relayed reference: no egress session gets the envelope.
     */
    byte[] forSession(byte[] frame);

    /**
     * {@link #forSession(byte[])} in place: rewrite the first {@code length}
     * bytes of {@code staged} as the session gets them, and return the new
     * length.
     */
    int forSession(MutableDirectBuffer staged, int length);

    /** Release the files and the threads of the store. */
    void close();
}
