package io.kardamom.sealer.cluster;

import io.aeron.cluster.client.EgressListener;
import io.aeron.logbuffer.Header;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.agrona.DirectBuffer;

/**
 * Decoding {@link EgressListener} shared by the in-JVM TestCluster tests
 * ({@link SealerClusterFailoverTest}, {@link SealerReplayTest}). It records
 * everything those tests assert on:
 * <ul>
 *   <li>the ordered canonical {@code index} of every RELAYED frame;</li>
 *   <li>the count and min/max {@code blockNumber} of every BOUNDARY frame;</li>
 *   <li>REPLAY_DONE / REPLAY_UNAVAILABLE control-frame counts, and the
 *       retention floor carried by the last UNAVAILABLE refusal.</li>
 * </ul>
 * The frame layouts match {@link SealerEgress}'s framing (little-endian).
 *
 * <p>The listener runs inline on the test's {@code pollEgress()} thread. This
 * thread also runs the await loops, so plain fields stay consistent.</p>
 */
final class RecordingEgressListener implements EgressListener {
    final List<Long> relayedIndexes = new ArrayList<>();
    /** The relayed {@code [canonical_id|payload]} of every RELAYED frame, in arrival order. */
    final List<byte[]> relayedPayloads = new ArrayList<>();
    long maxBoundaryBlockNumber = Long.MIN_VALUE;
    long minBoundaryBlockNumber = Long.MAX_VALUE;
    // Count of BOUNDARY frames seen. A yield-await loop can poll for "at least
    // one boundary fired" without checking a specific blockNumber.
    long boundaryCount = 0;
    long replayDoneCount = 0;
    long replayUnavailableCount = 0;
    long lastUnavailableOldestIndex = -1;
    long lastUnavailableOldestBlock = -1;
    /** The decision version of every SEALER_INFO frame, in arrival order. */
    final List<Integer> sealerInfoVersions = new ArrayList<>();
    long refRefusedCount = 0;

    @Override
    public void onMessage(
            final long clusterSessionId,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header) {
        if (length < Byte.BYTES) {
            return;
        }
        final byte kind = buffer.getByte(offset);
        if (kind == SealerWire.EGRESS_KIND_RELAYED) {
            // kind(1) | index(8 LE) | payloadLen(4 LE) | payload[]
            relayedIndexes.add(buffer.getLong(offset + Byte.BYTES, ByteOrder.LITTLE_ENDIAN));
            final int payloadOffset = offset + Byte.BYTES + Long.BYTES + Integer.BYTES;
            final byte[] payload =
                    new byte[buffer.getInt(offset + Byte.BYTES + Long.BYTES, ByteOrder.LITTLE_ENDIAN)];
            buffer.getBytes(payloadOffset, payload);
            relayedPayloads.add(payload);
        } else if (kind == SealerWire.EGRESS_KIND_BOUNDARY) {
            // kind(1) | blockNumber(8 LE) | endTxIdx(8 LE) | l2Timestamp(8 LE)
            final long blockNumber =
                    buffer.getLong(offset + Byte.BYTES, ByteOrder.LITTLE_ENDIAN);
            maxBoundaryBlockNumber = Math.max(maxBoundaryBlockNumber, blockNumber);
            minBoundaryBlockNumber = Math.min(minBoundaryBlockNumber, blockNumber);
            boundaryCount++;
        } else if (kind == SealerWire.EGRESS_KIND_SEALER_INFO) {
            // kind(1) | version(1) | session(8 LE)
            sealerInfoVersions.add((int) buffer.getByte(offset + Byte.BYTES));
        } else if (kind == SealerWire.EGRESS_KIND_REF_REFUSED) {
            refRefusedCount++;
        } else if (kind == SealerWire.EGRESS_KIND_REPLAY_DONE) {
            replayDoneCount++;
        } else if (kind == SealerWire.EGRESS_KIND_REPLAY_UNAVAILABLE) {
            // kind(1) | oldest_index(8 LE) | oldest_block(8 LE)
            replayUnavailableCount++;
            lastUnavailableOldestIndex =
                    buffer.getLong(offset + Byte.BYTES, ByteOrder.LITTLE_ENDIAN);
            lastUnavailableOldestBlock =
                    buffer.getLong(offset + Byte.BYTES + Long.BYTES, ByteOrder.LITTLE_ENDIAN);
        }
    }
}
