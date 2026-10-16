package io.kardamom.sealer.cluster;

import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.ClusterStatus;
import io.kardamom.sealer.Relayed;
import java.nio.ByteOrder;
import org.agrona.MutableDirectBuffer;

/**
 * The egress frame codec: each method writes one frame at the start of the
 * staging buffer and returns its length. All integers are little-endian.
 * The Rust reader is {@code EgressItem::decode} in
 * {@code crates/cluster-adapter/src/wire/egress.rs}.
 *
 * <p>This class is single-threaded, like {@link SealerEgress}, which owns
 * the staging buffer.</p>
 */
final class EgressFrames {
    private final MutableDirectBuffer buf;

    EgressFrames(final MutableDirectBuffer buf) {
        this.buf = buf;
    }

    /**
     * A relayed record:
     * {@code [kind:1][index:u64][payload_len:u32][payload]}. The payload is
     * copied through as is.
     */
    int relayed(final Relayed relayed) {
        int pos = 0;
        buf.putByte(pos, SealerWire.EGRESS_KIND_RELAYED);
        pos += Byte.BYTES;
        buf.putLong(pos, relayed.index, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putInt(pos, relayed.payload.length, ByteOrder.LITTLE_ENDIAN);
        pos += Integer.BYTES;
        buf.putBytes(pos, relayed.payload);
        pos += relayed.payload.length;
        return pos;
    }

    /**
     * A relayed reference:
     * {@code [kind:15][index:u64][canonical_id:32][record_type:u8]}.
     */
    int relayedRef(final long index, final byte[] canonicalId, final byte recordType) {
        int pos = 0;
        buf.putByte(pos, SealerWire.EGRESS_KIND_RELAYED_REF);
        pos += Byte.BYTES;
        buf.putLong(pos, index, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putBytes(pos, canonicalId, 0, CanonicalSealerState.CANONICAL_ID_LEN);
        pos += CanonicalSealerState.CANONICAL_ID_LEN;
        buf.putByte(pos, recordType);
        pos += Byte.BYTES;
        return pos;
    }

    /** A control message {@code [kind][a:u64][b:u64]}: the replay answers, the origin gap, the feed gap. */
    int control(final byte kind, final long a, final long b) {
        int pos = 0;
        buf.putByte(pos, kind);
        pos += Byte.BYTES;
        buf.putLong(pos, a, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, b, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        return pos;
    }

    /** A feed gap: {@code [kind:16][next_index:u64][next_block:u64]}. */
    int feedGap(final long nextIndex, final long nextBlock) {
        return control(SealerWire.EGRESS_KIND_FEED_GAP, nextIndex, nextBlock);
    }

    /**
     * The chain's status with the record-lag tail: {@code kind(1) |
     * posted_head(8) | sealed_head(8) | budget(8) | halted(1) | retained(8)
     * | floor_index(8) | floor_block(8) | best_recorded(8) |
     * record_lag_budget(8) | record_lag_halted(1)}.
     */
    int status(final ClusterStatus status) {
        int pos = 0;
        buf.putByte(pos, SealerWire.EGRESS_KIND_STATUS);
        pos += Byte.BYTES;
        buf.putLong(pos, status.postedHead(), ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, status.sealedHead(), ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, status.budgetBlocks(), ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putByte(pos, status.halted() ? (byte) 1 : (byte) 0);
        pos += Byte.BYTES;
        buf.putLong(pos, status.retainedFrames(), ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, status.floorIndex(), ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, status.floorBlock(), ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, status.bestRecorded(), ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, status.recordLagBudget(), ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putByte(pos, status.recordLagHalted() ? (byte) 1 : (byte) 0);
        pos += Byte.BYTES;
        return pos;
    }

    /** {@link #status(ClusterStatus)}, then the byte tail {@code [retained_bytes:u64]}. */
    int status(final ClusterStatus status, final long retainedBytes) {
        final int pos = status(status);
        buf.putLong(pos, retainedBytes, ByteOrder.LITTLE_ENDIAN);
        return pos + Long.BYTES;
    }

    /** A copy of the first {@code length} bytes of the staging buffer. */
    byte[] copy(final int length) {
        final byte[] frame = new byte[length];
        buf.getBytes(0, frame);
        return frame;
    }
}
