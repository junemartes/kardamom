package io.kardamom.sealer.bench;

import java.nio.ByteOrder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * The deterministic record sequence of one run. Record {@code i} is a kind-0
 * frame for a transaction reference:
 * <ul>
 *   <li>The sender is {@code i % senders}, and its nonce is
 *       {@code i / senders}. The senders take turns, so each sender's nonces
 *       arrive in order, as the contiguity guard of decision version 3
 *       expects.</li>
 *   <li>The canonical id is 32 pseudo-random bytes, like a transaction
 *       hash.</li>
 *   <li>The reference points at position {@code i * 128} of one ingress
 *       image with a 64 MiB term. The consumer reads {@code i} back from
 *       these fields.</li>
 * </ul>
 * Every twin writes the same bytes for the same {@code i}, as racing
 * sequencers do.
 */
final class RecordStream {

    static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;

    /** The bytes of one kind-0 record frame. */
    static final int FRAME_LEN = 99;

    /** The offset of the term id in a relayed payload: after the id, the record type and the shard. */
    static final int RELAYED_TERM_ID = 32 + 1 + 1;

    private static final int SENDER_LEN = 20;
    private static final int TIP_LEN = 16;
    private static final int POSITION_STEP = 128;
    private static final int TERM_SHIFT = 26;
    private static final int TERM_MASK = (1 << TERM_SHIFT) - 1;
    private static final int SESSION_ID = 7;
    private static final long POSITION_FLAG = 1L << 62;

    private final int senders;
    private final long seed;
    private final boolean v4Ids;

    RecordStream(final int senders, final long seed, final boolean v4Ids) {
        this.senders = senders;
        this.seed = seed;
        this.v4Ids = v4Ids;
    }

    /** Write record {@code i} at {@code offset} and return its length. */
    int write(final MutableDirectBuffer buf, final int offset, final long i) {
        final long position = i * POSITION_STEP;
        int pos = offset;
        buf.putByte(pos, (byte) 0);
        pos += 1;
        pos = writeSender(buf, pos, i % senders);
        buf.putLong(pos, i / senders, LE);
        pos += Long.BYTES;
        buf.putLong(pos, v4Ids ? POSITION_FLAG | position : Long.MAX_VALUE, LE);
        pos += Long.BYTES;
        buf.setMemory(pos, TIP_LEN, (byte) 0);
        pos += TIP_LEN;
        pos = writeId(buf, pos, i);
        buf.putByte(pos, (byte) 0);
        pos += 1;
        buf.putByte(pos, (byte) 0);
        pos += 1;
        buf.putInt(pos, (int) (position >>> TERM_SHIFT), LE);
        pos += Integer.BYTES;
        buf.putInt(pos, (int) (position & TERM_MASK), LE);
        pos += Integer.BYTES;
        buf.putInt(pos, SESSION_ID, LE);
        pos += Integer.BYTES;
        return pos - offset;
    }

    /** The record index that a relayed payload at {@code offset} carries. */
    static long indexOf(final DirectBuffer payload, final int offset) {
        final int fields = offset + RELAYED_TERM_ID;
        final long termId = payload.getInt(fields, LE) & 0xFFFF_FFFFL;
        final long termOffset = payload.getInt(fields + Integer.BYTES, LE) & 0xFFFF_FFFFL;
        return ((termId << TERM_SHIFT) | termOffset) / POSITION_STEP;
    }

    private int writeSender(final MutableDirectBuffer buf, final int pos, final long sender) {
        buf.setMemory(pos, SENDER_LEN, (byte) 0);
        buf.putByte(pos, (byte) 1);
        buf.putLong(pos + Long.BYTES, sender, LE);
        return pos + SENDER_LEN;
    }

    private int writeId(final MutableDirectBuffer buf, final int pos, final long i) {
        final long base = mix(seed ^ i);
        buf.putLong(pos, base, LE);
        buf.putLong(pos + 8, mix(base + 1), LE);
        buf.putLong(pos + 16, mix(base + 2), LE);
        buf.putLong(pos + 24, mix(base + 3), LE);
        return pos + 32;
    }

    /** SplitMix64: a cheap mix with no correlation between nearby inputs. */
    private static long mix(final long x) {
        long z = x + 0x9E37_79B9_7F4A_7C15L;
        z = (z ^ (z >>> 30)) * 0xBF58_476D_1CE4_E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D0_49BB_1331_11EBL;
        return z ^ (z >>> 31);
    }
}
