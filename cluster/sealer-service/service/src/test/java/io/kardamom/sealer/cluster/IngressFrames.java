package io.kardamom.sealer.cluster;

import io.aeron.Publication;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.test.Tests;
import io.kardamom.sealer.CanonicalSealerState;
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
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        int pos = 0;
        buf.putByte(pos, SealerWire.KIND_INGRESS_RECORD);
        pos += Byte.BYTES;
        buf.putBytes(pos, new byte[CanonicalSealerState.SENDER_LEN]);
        pos += CanonicalSealerState.SENDER_LEN;
        buf.putLong(pos, 0L, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putBytes(pos, canonicalId32);
        pos += canonicalId32.length;
        buf.putBytes(pos, canonicalId32); // opaque payload, relayed verbatim
        pos += canonicalId32.length;
        offerFully(client, buf, pos);
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

    /** A one-byte {@code KIND_SUBSCRIBE} consumer announcement. */
    static byte[] subscribeFrame() {
        return new byte[] {SealerWire.KIND_SUBSCRIBE};
    }

    /**
     * A complete single-record ingress frame that a batch can embed: a guard
     * header ({@code sender20} and {@code nonce}), a canonical id tagged with
     * {@code idTag}, and a 1-byte payload.
     */
    static byte[] recordFrame(final int idTag, final byte[] sender20, final long nonce) {
        final byte[] out = new byte[SealerWire.CANONICAL_ID_OFFSET
                + CanonicalSealerState.CANONICAL_ID_LEN + 1];
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer(out.length);
        buf.putByte(SealerWire.KIND_OFFSET, SealerWire.KIND_INGRESS_RECORD);
        buf.putBytes(SealerWire.SENDER_OFFSET, sender20);
        buf.putLong(SealerWire.NONCE_OFFSET, nonce, ByteOrder.LITTLE_ENDIAN);
        final byte[] id = new byte[CanonicalSealerState.CANONICAL_ID_LEN];
        id[0] = (byte) idTag;
        id[31] = 0x5A;
        buf.putBytes(SealerWire.CANONICAL_ID_OFFSET, id);
        buf.putByte(out.length - 1, (byte) idTag);
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
