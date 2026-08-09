package io.kardamom.sealer.cluster;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.ImageFragmentAssembler;
import io.aeron.Publication;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Cluster snapshot stream I/O for {@link SealerClusteredService}: reads and
 * writes the {@link io.kardamom.sealer.CanonicalSealerState} snapshot byte
 * stream over an Aeron snapshot image/publication. Pure static helpers with
 * zero instance state, so the snapshot-fidelity tests drive them directly
 * against a real embedded media driver (see {@code SnapshotRestoreTest}).
 */
final class SnapshotIo {

    /**
     * Read the WHOLE snapshot byte stream off the snapshot image. Snapshots
     * larger than the MTU arrive as MANY fragments (and, above
     * {@code maxMessageLength}, as many messages — see {@link #writeSnapshot}),
     * so fragments are reassembled with an {@link ImageFragmentAssembler} and
     * messages concatenated until end-of-stream. A snapshot image that closes
     * early or carries no bytes is FATAL — never fabricate genesis state.
     */
    static byte[] readSnapshot(final Image snapshotImage, final IdleStrategy idleStrategy) {
        final ExpandableArrayBuffer assembled = new ExpandableArrayBuffer();
        final int[] size = {0};
        final ImageFragmentAssembler assembler = new ImageFragmentAssembler(
                (buffer, offset, length, header) -> {
                    assembled.putBytes(size[0], buffer, offset, length);
                    size[0] += length;
                });
        while (!snapshotImage.isEndOfStream()) {
            final int fragments = snapshotImage.poll(assembler, 16);
            if (fragments == 0) {
                if (snapshotImage.isClosed()) {
                    throw new IllegalStateException(
                        "snapshot image closed before end-of-stream (read " + size[0] + " bytes)");
                }
                idleStrategy.idle();
            } else {
                idleStrategy.reset();
            }
        }
        if (size[0] == 0) {
            throw new IllegalStateException("snapshot image was empty");
        }
        final byte[] snapshot = new byte[size[0]];
        assembled.getBytes(0, snapshot);
        return snapshot;
    }

    /**
     * Offer the full snapshot, chunked at the publication's
     * {@code maxMessageLength} so ANY dedup-window size round-trips. A
     * terminal offer result is FATAL: exiting silently would record an
     * empty/truncated snapshot, and the member restoring from it would
     * diverge (or refuse to start) with no recorded error.
     */
    static void writeSnapshot(
            final ExclusivePublication snapshotPublication,
            final byte[] snapshot,
            final IdleStrategy idleStrategy) {
        final UnsafeBuffer buf = new UnsafeBuffer(snapshot);
        final int maxChunk = snapshotPublication.maxMessageLength();
        int offset = 0;
        while (offset < snapshot.length) {
            final int chunk = Math.min(maxChunk, snapshot.length - offset);
            long result;
            while ((result = snapshotPublication.offer(buf, offset, chunk)) < 0) {
                if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
                    throw new IllegalStateException("snapshot offer failed terminally (" + result
                        + ") at offset " + offset + "/" + snapshot.length);
                }
                idleStrategy.idle();
            }
            offset += chunk;
        }
    }

    private SnapshotIo() {
    }
}
