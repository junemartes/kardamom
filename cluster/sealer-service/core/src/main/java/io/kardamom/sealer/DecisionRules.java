package io.kardamom.sealer;

import java.nio.ByteBuffer;

/**
 * The rules that decide the replicated state: decision version 3 on every
 * member, from its start. Version 3 relays a frame of kinds 0, 3, 4 and 5
 * only when it carries the record-type byte, and the member sends the rules
 * frame (egress kind 17) to each session.
 *
 * <p>The snapshot of version 14 carries the rules after the seed digest. A
 * member of an older release reads that tail and switches its rules by the
 * feature activations that it holds. This class keeps the held activations
 * as it loads them, and writes them back with version 3 and the L1 origin of
 * the last sealed block. While {@code FEATURE_SEALER_V3} stays on, a member
 * of the older release then holds the same tail, decides every entry as this
 * member does, and writes the same snapshot bytes. The held activations
 * change no decision of this member.</p>
 */
public final class DecisionRules {
    /** The version of the rules of every member. */
    public static final int VERSION = 3;

    /** The version of the rules of a member that ran with the feature off. A snapshot can still hold it. */
    private static final int OFF_VERSION = 2;

    /** The rules of a state that holds no activation, as a value. */
    public static final View START_VIEW = new View(VERSION, 0L, 0L);

    /** The most activations that the snapshot tail holds. */
    static final int MAX_HELD = 3;

    /** The L1 origins of the held activations, oldest first. */
    private final long[] origins = new long[MAX_HELD];

    /** The times in ms of the held activations, as the snapshot carries them. */
    private final long[] times = new long[MAX_HELD];

    /** How many activations the tail holds. */
    private int held;

    /** The L1 origin of the last block that a boundary sealed. */
    private long sealedOrigin;

    /** The rules of a state that starts at genesis or from a seed: no held activation. */
    static DecisionRules start() {
        return new DecisionRules();
    }

    /** The rules as a value, for the admin endpoint: the version and the newest held activation. */
    public View view() {
        return held == 0 ? START_VIEW : new View(VERSION, origins[held - 1], times[held - 1]);
    }

    /** The boundary of a block with L1 origin {@code l1Origin} seals that block. */
    void atBoundary(final long l1Origin) {
        sealedOrigin = l1Origin;
    }

    /** The length of the snapshot tail that {@link #writeTo} writes. */
    int snapshotLen() {
        return Byte.BYTES + Long.BYTES + Byte.BYTES + held * (Long.BYTES + Long.BYTES);
    }

    /**
     * Write the snapshot tail of version 14:
     * {@code [version:u8][sealed_origin:u64][held:u8]}, then
     * {@code [origin:u64][time_ms:u64]} for each held activation, oldest
     * first.
     */
    void writeTo(final ByteBuffer buf) {
        buf.put((byte) VERSION);
        buf.putLong(sealedOrigin);
        buf.put((byte) held);
        for (int i = 0; i < held; i++) {
            buf.putLong(origins[i]).putLong(times[i]);
        }
    }

    /**
     * Read the snapshot tail of version 14 that {@link #writeTo} wrote. A
     * tail of version 2 loads as version 3: the two versions decide alike
     * every frame that a client of the cluster writes.
     */
    static DecisionRules readFrom(final ByteBuffer buf) {
        checkVersion(buf.get());
        final DecisionRules rules = new DecisionRules();
        rules.sealedOrigin = buf.getLong();
        final int count = buf.get();
        if (count < 0 || count > MAX_HELD) {
            throw new IllegalArgumentException("bad snapshot activation count: " + count);
        }
        for (int i = 0; i < count; i++) {
            rules.hold(buf.getLong(), buf.getLong());
        }
        return rules;
    }

    /**
     * Read the snapshot tail of version 13:
     * {@code [version:u8][activation_origin:u64][activation_ms:u64]}. A
     * version-13 state holds at most one activation, and the activation is
     * the visible one. Its time of 0 reads as 1 ms, the time that a member
     * of an older release gives it.
     */
    static DecisionRules readVersionThirteen(final ByteBuffer buf) {
        checkVersion(buf.get());
        final DecisionRules rules = new DecisionRules();
        final long origin = buf.getLong();
        final long time = buf.getLong();
        if (origin != 0L) {
            rules.hold(origin, time == 0L ? 1L : time);
            rules.sealedOrigin = origin;
        }
        return rules;
    }

    private static void checkVersion(final byte version) {
        if (version != OFF_VERSION && version != VERSION) {
            throw new IllegalArgumentException("bad snapshot decision version: " + version);
        }
    }

    /** Hold one more activation, after the ones held. */
    private void hold(final long origin, final long time) {
        origins[held] = origin;
        times[held] = time;
        held++;
    }

    /**
     * The rules as a value: the version, and the origin and the time of the
     * newest held activation, both 0 when the state holds none.
     */
    public record View(int version, long activationOrigin, long activationMs) {
    }
}
