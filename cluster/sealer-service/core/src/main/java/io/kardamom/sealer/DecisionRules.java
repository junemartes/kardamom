package io.kardamom.sealer;

import java.nio.ByteBuffer;

/**
 * The version of the rules that decide the replicated state, and the
 * switch between version 2 and version 3. Two members on two versions can
 * decide one log entry differently, and the state forks. So the version is
 * replicated state: it changes only at a block boundary, at the same log
 * index on every member, and the snapshot carries it. All the entries of
 * one block run under one version.
 *
 * <p>Version 2 is the rules of a state that holds no activation. Version 3
 * relays a frame of kinds 0, 3, 4 and 5 only when it carries the
 * record-type byte. A state that holds an activation writes snapshot
 * version 14, and sends the rules frame to each session.</p>
 *
 * <p>An upgrade transaction on L1 schedules {@link #FEATURE_SEALER_V3} at
 * an activation time. Its {@code setFeature} deposit runs in the first
 * block of its epoch, and the sequencer sends the schedule as a feature
 * activation record in front of that epoch. The record only stores the
 * schedule. The rules follow the executor, which reads the flag once per
 * block: the flags of block N+1 are the chain state after block N, at the
 * time of block N (the time that block N+1 executes with).</p>
 *
 * <p>So at the boundary of block N, the newest activation whose epoch is at
 * or below the L1 origin of block N is visible. {@code setFeature} stores a
 * time of 0 as the time that the block of its deposit executes with, so
 * the activation takes that time when it becomes visible: the time of the
 * boundary before the block of the deposit. That time is 0 only for block
 * 1, and a stored 0 means never. The rules of block N+1 are version 3 when
 * the visible activation has a time that is not 0 and is at or below the
 * time of block N, and version 2 otherwise. So a deposit in block N
 * switches the rules at block N+1 at the earliest, a time ahead switches
 * them off and on again at its time, and a time of {@code 2^64 - 1} keeps
 * them off.</p>
 *
 * <p>An activation applies only when its L1 origin names the epoch that the
 * record fronts: the next origin of the state, or the origin of the state
 * when the epoch got in first. A state with no epoch yet (origin 0) takes
 * any origin, because its first epoch can start at any L1 block. The
 * activation must also be above the origin of the newest activation that
 * the state holds. So a copy of the record from a twin sequencer, or a
 * record that a sequencer sends again with its epoch, changes nothing. The
 * origins and the times are u64 values from the wire, so every comparison
 * is unsigned.</p>
 *
 * <p>A record that comes too late is the record of a lost first copy. Its
 * epoch is the epoch of the state, but the first block of the epoch is
 * sealed: the state stores it, and the rules switch at the next boundary,
 * after the executor ({@link Outcome#STORED_LATE}). Or its epoch is below
 * the epoch of the state: the state does not store it, and the rules do not
 * follow it ({@link Outcome#MISSED}). The service logs and counts both.</p>
 *
 * <p>The state holds the visible activation and the ones whose epoch is not
 * sealed yet. Each epoch closes the first block of the epoch before it, so
 * at most the epoch of the state and the next one wait: {@link #MAX_HELD}
 * activations in all. A record that comes when the state holds that many
 * changes nothing ({@link Outcome#FULL}).</p>
 *
 * <p>A member that keeps its retained frames in a file runs version 3 with
 * no activation, and takes no activation record. Its rules never switch.</p>
 */
public final class DecisionRules {
    /** The rules of a state that holds no activation. Every member starts on it. */
    public static final int START_VERSION = 2;

    /** The rules of an active feature: a relayed frame needs its record type. */
    public static final int NEWEST_VERSION = 3;

    /**
     * The feature id of version 3 in {@code KardamomChainState}. Matches
     * Rust {@code kardamom_types::upgrades::FEATURE_SEALER_V3}.
     */
    public static final long FEATURE_SEALER_V3 = 2L;

    /** The rules of a state that holds no activation, as a value. */
    public static final View START_VIEW = new View(START_VERSION, 0L, 0L);

    /** The most activations that a state holds: the visible one and two that wait. */
    static final int MAX_HELD = 3;

    /** The bytes of one held activation in the snapshot: its origin and its time. */
    private static final int HELD_LEN = Long.BYTES + Long.BYTES;

    /** The L1 origins of the held activations, oldest first, each above the one before. */
    private final long[] origins = new long[MAX_HELD];

    /**
     * The times in ms of the held activations, compared unsigned: the wire
     * carries a u64. The time of a visible activation is resolved: a 0 from
     * the wire is the time that the block of its deposit executes with.
     */
    private final long[] times = new long[MAX_HELD];

    /** How many activations the state holds. */
    private int held;

    /**
     * The L1 origin of the last block that a boundary sealed. An activation
     * at or below it is visible, and its time is resolved.
     */
    private long sealedOrigin;

    private int version;

    private DecisionRules(final int version) {
        this.version = version;
    }

    /** The rules of a state that holds no activation. */
    static DecisionRules start() {
        return new DecisionRules(START_VERSION);
    }

    /**
     * Version 3 with no activation: the rules of a member that keeps its
     * retained frames in a file. That setting comes with version 3, and every
     * member must match it.
     */
    static DecisionRules newest() {
        return new DecisionRules(NEWEST_VERSION);
    }

    /** The version of the rules. */
    int version() {
        return version;
    }

    /** Whether the state holds an activation: the snapshot must then carry the rules. */
    boolean holdsActivation() {
        return held > 0;
    }

    /** The rules as a value, for the admin endpoint: the version and the newest activation. */
    public View view() {
        return held == 0 ? new View(version, 0L, 0L) : new View(version, origins[held - 1], times[held - 1]);
    }

    /**
     * Whether the member tells each session the version of its rules: under
     * version 3, and after an activation, also under version 2. A state
     * with no activation sends no rules frame.
     */
    boolean reports() {
        return version == NEWEST_VERSION || holdsActivation();
    }

    /**
     * Take a feature activation record, with {@code stateOrigin} the L1
     * origin of the state and {@code openBlockMs} the time that the open
     * block executes with. The record only stores the schedule: the rules
     * change at a block boundary ({@link #atBoundary}).
     *
     * @return what the record did
     */
    Outcome onActivation(final FeatureActivation activation, final long stateOrigin, final long openBlockMs) {
        final long origin = activation.l1Origin();
        final long newest = held == 0 ? 0L : origins[held - 1];
        if (activation.featureId() != FEATURE_SEALER_V3 || Long.compareUnsigned(origin, newest) <= 0) {
            return Outcome.IGNORED;
        }
        if (stateOrigin != 0L && Long.compareUnsigned(origin, stateOrigin) < 0) {
            return Outcome.MISSED;
        }
        if (stateOrigin != 0L && origin != stateOrigin && origin != stateOrigin + 1) {
            return Outcome.IGNORED;
        }
        if (held == MAX_HELD) {
            return Outcome.FULL;
        }
        final boolean late = Long.compareUnsigned(origin, sealedOrigin) <= 0;
        final long time = activation.activationMs();
        hold(origin, late && time == 0L ? openBlockMs : time);
        return late ? Outcome.STORED_LATE : Outcome.STORED;
    }

    /**
     * The version that {@link #atBoundary} would set for the same boundary.
     * It changes nothing, so a record that forces a boundary can be checked
     * under the rules of the block that it opens.
     */
    int versionAtBoundary(final long l1Origin, final long sealedBlockMs, final long l2Timestamp) {
        final int visible = visibleAt(l1Origin);
        if (visible == 0) {
            return version;
        }
        final long time = resolvedTime(visible - 1, sealedBlockMs);
        return time != 0L && Long.compareUnsigned(time, l2Timestamp) <= 0 ? NEWEST_VERSION : START_VERSION;
    }

    /**
     * Set the rules of the next block at the boundary of a block with L1
     * origin {@code l1Origin} and time {@code l2Timestamp}: the time that
     * the next block executes with. {@code sealedBlockMs} is the time that
     * the sealed block executed with: the time of a {@code setFeature} of 0
     * in it. The newest activation whose epoch is at or below
     * {@code l1Origin} decides, and the older ones go. A state with no
     * visible activation keeps its version.
     *
     * @return whether the rules switched at this boundary
     */
    boolean atBoundary(final long l1Origin, final long sealedBlockMs, final long l2Timestamp) {
        final int due = versionAtBoundary(l1Origin, sealedBlockMs, l2Timestamp);
        final int visible = visibleAt(l1Origin);
        if (visible > 0) {
            times[visible - 1] = resolvedTime(visible - 1, sealedBlockMs);
            dropOldest(visible - 1);
        }
        sealedOrigin = l1Origin;
        final boolean switched = due != version;
        version = due;
        return switched;
    }

    /** How many held activations, from the oldest, have an epoch at or below {@code l1Origin}. */
    private int visibleAt(final long l1Origin) {
        int visible = 0;
        while (visible < held && Long.compareUnsigned(origins[visible], l1Origin) <= 0) {
            visible++;
        }
        return visible;
    }

    /**
     * The time of held activation {@code index}: a 0 of an activation that
     * becomes visible now is {@code sealedBlockMs}, the time that the block of
     * its deposit executed with.
     */
    private long resolvedTime(final int index, final long sealedBlockMs) {
        final boolean becomesVisible = Long.compareUnsigned(origins[index], sealedOrigin) > 0;
        return becomesVisible && times[index] == 0L ? sealedBlockMs : times[index];
    }

    /** Drop the {@code count} oldest held activations. */
    private void dropOldest(final int count) {
        System.arraycopy(origins, count, origins, 0, held - count);
        System.arraycopy(times, count, times, 0, held - count);
        held -= count;
    }

    /** The length of the snapshot tail that {@link #writeTo} writes. */
    int snapshotLen() {
        return Byte.BYTES + Long.BYTES + Byte.BYTES + held * HELD_LEN;
    }

    /**
     * Write the snapshot tail of version 14:
     * {@code [version:u8][sealed_origin:u64][held:u8]}, then
     * {@code [origin:u64][time_ms:u64]} for each held activation, oldest
     * first.
     */
    void writeTo(final ByteBuffer buf) {
        buf.put((byte) version);
        buf.putLong(sealedOrigin);
        buf.put((byte) held);
        for (int i = 0; i < held; i++) {
            buf.putLong(origins[i]).putLong(times[i]);
        }
    }

    /** Read the snapshot tail of version 14 that {@link #writeTo} wrote. */
    static DecisionRules readFrom(final ByteBuffer buf) {
        final DecisionRules rules = new DecisionRules(versionOf(buf.get()));
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
     * due from the record on: it is the visible one. Its time of 0 is due at
     * once, so it reads as 1 ms, which is due at every boundary.
     */
    static DecisionRules readVersionThirteen(final ByteBuffer buf) {
        final DecisionRules rules = new DecisionRules(versionOf(buf.get()));
        final long origin = buf.getLong();
        final long time = buf.getLong();
        if (origin != 0L) {
            rules.hold(origin, time == 0L ? 1L : time);
            rules.sealedOrigin = origin;
        }
        return rules;
    }

    private static int versionOf(final byte version) {
        if (version != START_VERSION && version != NEWEST_VERSION) {
            throw new IllegalArgumentException("bad snapshot decision version: " + version);
        }
        return version;
    }

    /** Hold one more activation, after the ones held. */
    private void hold(final long origin, final long time) {
        origins[held] = origin;
        times[held] = time;
        held++;
    }

    /** What a feature activation record did. */
    public enum Outcome {
        /** The record changed nothing: another feature, a copy, or a later epoch. */
        IGNORED,
        /** The state holds the schedule; the rules change at a block boundary. */
        STORED,
        /**
         * The state holds the schedule, but the first block of its epoch is
         * sealed: the rules switch at the next boundary, after the executor.
         */
        STORED_LATE,
        /** The epoch of the record is below the epoch of the state: the rules do not follow it. */
        MISSED,
        /** The state holds {@link #MAX_HELD} activations: the record changed nothing. */
        FULL
    }

    /**
     * A feature activation record: the upgrade transaction of the epoch at
     * {@code l1Origin} schedules {@code featureId} at {@code activationMs}.
     */
    public record FeatureActivation(long l1Origin, long featureId, long activationMs) {
    }

    /**
     * The rules as a value: the version, and the origin and the time of the
     * newest activation that the state holds, both 0 when it holds none. A
     * check outside the cluster compares the origin with the
     * {@code setFeature} deposit of that epoch on L1.
     */
    public record View(int version, long activationOrigin, long activationMs) {
    }
}
