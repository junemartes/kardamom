package io.kardamom.sealer.cluster;

import io.aeron.Publication;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.test.Tests;
import io.kardamom.sealer.CanonicalSealerState;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.agrona.ExpandableArrayBuffer;

/**
 * Ingress-envelope frame builders and offer-with-retry helpers shared by the
 * service tests. Frame layouts come from {@link SealerWire}. This class only
 * assembles bytes; it never parses them.
 */
final class IngressFrames {

    private IngressFrames() {
    }

    /** Deterministic distinct 32-byte canonical id: {@code n} little-endian in bytes 0..3. */
    static byte[] canonicalId(final int n) {
        final byte[] id = new byte[CanonicalSealerState.CANONICAL_ID_LEN];
        // Spread n across the first 4 bytes so ids stay distinct well past 255.
        id[0] = (byte) (n & 0xFF);
        id[1] = (byte) ((n >>> 8) & 0xFF);
        id[2] = (byte) ((n >>> 16) & 0xFF);
        id[3] = (byte) ((n >>> 24) & 0xFF);
        id[31] = (byte) 0x5A; // Marker so an all-zero id, a likely bug, is easy to spot.
        return id;
    }

    /**
     * Offer one app envelope {@code kind(1) | sender(20) | nonce(8) |
     * canonical_id(32) | payload} to the cluster. The sender is zero and the
     * nonce is 0, so the record is exempt from the contiguity guard. The
     * TestCluster tests exercise failover and replay, not the guard. The
     * payload is the 32-byte id itself; the service relays
     * {@code [canonical_id|payload]} verbatim from
     * {@link SealerWire#RELAY_OFFSET}. On back-pressure, the method retries
     * with a yield, not a sleep, so the offer stays deterministic.
     */
    static void offerIngress(final AeronCluster client, final byte[] canonicalId32) {
        offerRecord(client, canonicalId32, canonicalId32);
    }

    /** {@link #offerIngress} with an opaque {@code payload} of any length. */
    static void offerRecord(final AeronCluster client, final byte[] canonicalId32, final byte[] payload) {
        offerFrame(client, exemptRecordFrame(canonicalId32, payload));
    }

    /** Offer a complete ingress {@code frame} until the cluster takes it. */
    static void offerFrame(final AeronCluster client, final byte[] frame) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer(frame.length);
        buf.putBytes(0, frame);
        offerFully(client, buf, frame.length);
    }

    /**
     * A kind-0 frame that the guard exempts: a zero sender, nonce 0, no
     * deadline and no tip, then the canonical id and the opaque payload.
     */
    static byte[] exemptRecordFrame(final byte[] canonicalId32, final byte[] payload) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        int pos = 0;
        buf.putByte(pos, SealerWire.KIND_INGRESS_RECORD);
        pos += Byte.BYTES;
        buf.putBytes(pos, new byte[CanonicalSealerState.SENDER_LEN]);
        pos += CanonicalSealerState.SENDER_LEN;
        buf.putLong(pos, 0L, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, Long.MAX_VALUE, ByteOrder.LITTLE_ENDIAN); // no deadline
        pos += Long.BYTES;
        buf.putBytes(pos, new byte[SealerWire.TIP_LEN]); // no tip
        pos += SealerWire.TIP_LEN;
        buf.putBytes(pos, canonicalId32);
        pos += canonicalId32.length;
        buf.putBytes(pos, payload); // opaque payload, relayed verbatim
        pos += payload.length;
        final byte[] out = new byte[pos];
        buf.getBytes(0, out);
        return out;
    }

    /** Offer a {@code REPLAY_FROM(from_index, from_block)} request to the cluster. */
    static void offerReplayRequest(
            final AeronCluster client, final long fromIndex, final long fromBlock) {
        offerFully(client, replayRequest(fromIndex, fromBlock), SealerWire.MIN_REPLAY_REQUEST_LEN);
    }

    /**
     * Offer until accepted, treating CLOSED and MAX_POSITION_EXCEEDED as
     * terminal. On BACK_PRESSURED, ADMIN_ACTION, or NOT_CONNECTED, drain
     * egress and retry with a yield, so the offer stays deterministic.
     */
    static void offerFully(
            final AeronCluster client, final ExpandableArrayBuffer buf, final int length) {
        while (true) {
            final long result = client.offer(buf, 0, length);
            if (result > 0) {
                return;
            }
            if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("ingress offer failed terminally: " + result);
            }
            client.pollEgress();
            Tests.yield();
        }
    }

    /**
     * A {@code KIND_REPLAY_REQUEST} frame {@code [kind:1][from_index:u64 LE]
     * [from_block:u64 LE]} ({@link SealerWire#MIN_REPLAY_REQUEST_LEN} bytes).
     */
    static ExpandableArrayBuffer replayRequest(final long fromIndex, final long fromBlock) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        buf.putByte(0, SealerWire.KIND_REPLAY_REQUEST);
        buf.putLong(1, fromIndex, ByteOrder.LITTLE_ENDIAN);
        buf.putLong(1 + Long.BYTES, fromBlock, ByteOrder.LITTLE_ENDIAN);
        return buf;
    }

    /** A {@code KIND_REPLAY_REQUEST} frame: {@code kind(1) | fromIndex(8 LE) | fromBlock(8 LE)}. */
    static byte[] replayRequestFrame(final long fromIndex, final long fromBlock) {
        return java.nio.ByteBuffer.allocate(1 + 2 * Long.BYTES)
            .order(java.nio.ByteOrder.LITTLE_ENDIAN)
            .put(SealerWire.KIND_REPLAY_REQUEST)
            .putLong(fromIndex)
            .putLong(fromBlock)
            .array();
    }

    /** The canonical id that {@link #recordFrame} gives to {@code idTag}. */
    static byte[] recordId(final int idTag) {
        final byte[] id = new byte[CanonicalSealerState.CANONICAL_ID_LEN];
        id[0] = (byte) idTag;
        id[31] = 0x5A;
        return id;
    }

    /** A {@code KIND_VOID_REQUEST} frame: {@code [kind:6][voter_id:u8][index:u64 LE][tx_hash:32]}. */
    static byte[] voidRequestFrame(final int voterId, final long index, final byte[] txHash32) {
        final byte[] out = new byte[SealerWire.MIN_VOID_REQUEST_LEN];
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer(out.length);
        buf.putByte(SealerWire.KIND_OFFSET, SealerWire.KIND_VOID_REQUEST);
        buf.putByte(SealerWire.VOID_VOTER_OFFSET, (byte) voterId);
        buf.putLong(SealerWire.VOID_INDEX_OFFSET, index, ByteOrder.LITTLE_ENDIAN);
        buf.putBytes(SealerWire.VOID_HASH_OFFSET, txHash32);
        buf.getBytes(0, out);
        return out;
    }

    /** A one-byte {@code KIND_SUBSCRIBE} consumer announcement. */
    static byte[] subscribeFrame() {
        return new byte[] {SealerWire.KIND_SUBSCRIBE};
    }

    /** A {@code KIND_POSTED_CURSOR} frame: {@code [kind:7][posted_head:u64 LE]}. */
    static byte[] postedCursorFrame(final long postedHead) {
        return java.nio.ByteBuffer.allocate(SealerWire.MIN_POSTED_CURSOR_LEN)
            .order(java.nio.ByteOrder.LITTLE_ENDIAN)
            .put(SealerWire.KIND_POSTED_CURSOR)
            .putLong(postedHead)
            .array();
    }

    /** A {@code KIND_RECORDED_CURSOR} frame: {@code [kind:9][executor_id:u8][recorded_through:u64 LE]}. */
    static byte[] recordedCursorFrame(final int executorId, final long recordedThrough) {
        return java.nio.ByteBuffer.allocate(SealerWire.RECORDED_CURSOR_LEN)
            .order(java.nio.ByteOrder.LITTLE_ENDIAN)
            .put(SealerWire.KIND_RECORDED_CURSOR)
            .put((byte) executorId)
            .putLong(recordedThrough)
            .array();
    }

    /**
     * A {@code KIND_FEATURE_ACTIVATION} frame:
     * {@code [kind:11][l1_origin:u64 LE][feature_id:u64 LE][activation_ms:u64 LE]}.
     */
    static byte[] featureActivationFrame(final long l1Origin, final long featureId, final long activationMs) {
        return java.nio.ByteBuffer.allocate(SealerWire.FEATURE_ACTIVATION_LEN)
            .order(java.nio.ByteOrder.LITTLE_ENDIAN)
            .put(SealerWire.KIND_FEATURE_ACTIVATION)
            .putLong(l1Origin)
            .putLong(featureId)
            .putLong(activationMs)
            .array();
    }

    /** Offer the batcher's posted cursor to the cluster. */
    static void offerPostedCursor(final AeronCluster client, final long postedHead) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        buf.putBytes(0, postedCursorFrame(postedHead));
        offerFully(client, buf, SealerWire.MIN_POSTED_CURSOR_LEN);
    }

    /**
     * A kind-0 transaction reference (record type 0) of {@code sender20}
     * at {@code nonce}, of image {@code (lane 0, session 9)} at
     * {@code position}: the position goes in the deadline field with its
     * tag.
     */
    static byte[] txRefFrame(final int idTag, final byte[] sender20, final long nonce, final long position) {
        final ByteBuffer buf = ByteBuffer.allocate(SealerWire.TXREF_RECORD_LEN).order(ByteOrder.LITTLE_ENDIAN);
        buf.put(SealerWire.KIND_INGRESS_RECORD).put(sender20).putLong(nonce)
            .putLong(SealerWire.POSITION_TAG | position).putLong(0L).putLong(0L)
            .put(recordId(idTag)).put(SealerWire.RT_TXREF)
            .put((byte) 0).putInt(0).putInt((int) position).putInt(9);
        return buf.array();
    }

    /**
     * A complete single-record ingress frame that a batch can embed: a guard
     * header ({@code sender20}, {@code nonce} and a deadline no block
     * passes), a canonical id tagged with {@code idTag}, and a 1-byte
     * payload.
     */
    static byte[] recordFrame(final int idTag, final byte[] sender20, final long nonce) {
        return recordFrame(idTag, sender20, nonce, Long.MAX_VALUE);
    }

    /** {@link #recordFrame(int, byte[], long)} with an explicit deadline and no tip. */
    static byte[] recordFrame(
            final int idTag, final byte[] sender20, final long nonce, final long deadline) {
        return recordFrame(idTag, sender20, nonce, deadline, 0L);
    }

    /** {@link #recordFrame(int, byte[], long, long)} with a tip (the low 64 bits of it). */
    static byte[] recordFrame(
            final int idTag, final byte[] sender20, final long nonce, final long deadline,
            final long tip) {
        final byte[] out = new byte[SealerWire.CANONICAL_ID_OFFSET
                + CanonicalSealerState.CANONICAL_ID_LEN + 1];
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer(out.length);
        buf.putByte(SealerWire.KIND_OFFSET, SealerWire.KIND_INGRESS_RECORD);
        buf.putBytes(SealerWire.SENDER_OFFSET, sender20);
        buf.putLong(SealerWire.NONCE_OFFSET, nonce, ByteOrder.LITTLE_ENDIAN);
        buf.putLong(SealerWire.DEADLINE_OFFSET, deadline, ByteOrder.LITTLE_ENDIAN);
        buf.putLong(SealerWire.TIP_OFFSET, tip, ByteOrder.LITTLE_ENDIAN);
        buf.putLong(SealerWire.TIP_OFFSET + Long.BYTES, 0L, ByteOrder.LITTLE_ENDIAN);
        buf.putBytes(SealerWire.CANONICAL_ID_OFFSET, recordId(idTag));
        buf.putByte(out.length - 1, (byte) idTag);
        buf.getBytes(0, out);
        return out;
    }

    /**
     * A complete {@code KIND_REMOTE_ORIGIN_RECORD} frame
     * {@code [kind:5][canonical_id:32][origin_chain_id:u64 LE][anchor_number:u64 LE]
     * [slot_count:u32 LE][first_seq:u64 LE][last_seq:u64 LE][payload…]}. Built from the {@link SealerWire}
     * constants; {@link RemoteOriginFrameTest} pins those constants to the
     * literal offsets the Rust encoder writes.
     */
    static byte[] remoteOriginRecordFrame(
            final byte[] id32,
            final long originChainId,
            final long anchorNumber,
            final int slots,
            final long firstSeq,
            final long lastSeq,
            final byte[] payload) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        buf.putByte(SealerWire.KIND_OFFSET, SealerWire.KIND_REMOTE_ORIGIN_RECORD);
        buf.putBytes(SealerWire.REMOTE_ID_OFFSET, id32);
        buf.putLong(SealerWire.REMOTE_CHAIN_ID_OFFSET, originChainId, ByteOrder.LITTLE_ENDIAN);
        buf.putLong(SealerWire.REMOTE_ANCHOR_OFFSET, anchorNumber, ByteOrder.LITTLE_ENDIAN);
        buf.putInt(SealerWire.REMOTE_SLOT_COUNT_OFFSET, slots, ByteOrder.LITTLE_ENDIAN);
        buf.putLong(SealerWire.REMOTE_FIRST_SEQ_OFFSET, firstSeq, ByteOrder.LITTLE_ENDIAN);
        buf.putLong(SealerWire.REMOTE_LAST_SEQ_OFFSET, lastSeq, ByteOrder.LITTLE_ENDIAN);
        buf.putBytes(SealerWire.MIN_REMOTE_ORIGIN_RECORD_LEN, payload);
        final byte[] out = new byte[SealerWire.MIN_REMOTE_ORIGIN_RECORD_LEN + payload.length];
        buf.getBytes(0, out);
        return out;
    }

    /**
     * A complete {@code KIND_ORIGIN_RECORD} frame
     * {@code [kind:4][canonical_id:32][l1_origin:u64 LE][slot_count:u32 LE][payload…]}.
     */
    static byte[] originRecordFrame(
            final byte[] id32, final long origin, final int slots, final byte[] payload) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        int pos = 0;
        buf.putByte(pos, SealerWire.KIND_ORIGIN_RECORD);
        pos += Byte.BYTES;
        buf.putBytes(pos, id32);
        pos += id32.length;
        buf.putLong(pos, origin, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putInt(pos, slots, ByteOrder.LITTLE_ENDIAN);
        pos += Integer.BYTES;
        buf.putBytes(pos, payload);
        pos += payload.length;
        final byte[] out = new byte[pos];
        buf.getBytes(0, out);
        return out;
    }
}
