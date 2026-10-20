package io.kardamom.sealer;

import java.nio.ByteBuffer;

/**
 * The switch between decision version 3 and decision version 4. Two members
 * on two versions can decide one log entry differently, and the state
 * forks. So the version is replicated state: it changes only at a block
 * boundary, at the same log index on every member, and the snapshot carries
 * it. All the entries of one block run under one version.
 *
 * <p>Version 3 orders a transaction reference by its canonical id, with the
 * id window, the deadline and the contiguity guard. Version 4 orders it by
 * the replay window of its ingress image: see {@link SessionWindows}.</p>
 *
 * <p>An upgrade transaction on L1 schedules {@link #FEATURE_SEALER_V4} at an
 * activation time. Its {@code setFeature} deposit runs in the first block
 * of its epoch, and the sequencer sends the schedule as a feature
 * activation record in front of that epoch. The record only stores the
 * schedule. The switch follows the executor, which reads the flag once per
 * block: the flags of block N+1 are the chain state after block N, at the
 * time of block N.</p>
 *
 * <p>So at the boundary of block N, the newest activation whose epoch is at
 * or below the L1 origin of block N is visible. {@code setFeature} stores a
 * time of 0 as the time that the block of its deposit executes with, so the
 * activation takes that time when it becomes visible: the time of the
 * boundary before the block of the deposit. Block N+1 runs version 4 when
 * the visible activation has a time that is not 0 and is at or below the
 * time of block N, and version 3 otherwise. A time ahead switches version 4
 * off and on again at its time, and a time of {@code 2^64 - 1} keeps it
 * off.</p>
 *
 * <p>An activation applies only when its L1 origin names the epoch that the
 * record fronts: the next origin of the state, or the origin of the state
 * when the epoch got in first. A state with no epoch yet (origin 0) takes
 * any origin. The activation must also be above the origin of the newest
 * activation that the state holds. So a copy of the record from a twin
 * sequencer, or a record that a sequencer sends again with its epoch,
 * changes nothing. The origins and the times are u64 values from the wire,
 * so every comparison is unsigned.</p>
 *
 * <p>A record that comes too late is the record of a lost first copy. Its
 * epoch is the epoch of the state, but the first block of the epoch is
 * sealed: the state stores it, and the switch happens at the next boundary,
 * after the executor ({@link Outcome#STORED_LATE}). Or its epoch is below
 * the epoch of the state: the state does not store it
 * ({@link Outcome#MISSED}). The service logs and counts both.</p>
 *
 * <p>The state holds the visible activation and the ones whose epoch is not
 * sealed yet: at most {@link #MAX_HELD} in all. A record that comes when the
 * state holds that many changes nothing ({@link Outcome#FULL}).</p>
 */
public final class VersionSwitch {

    /** The version of a state that holds no visible activation. */
    public static final int OFF_VERSION = 3;

    /** The version of an active feature: the replay windows order the references. */
    public static final int ON_VERSION = 4;

    /**
     * The feature id of version 4 in {@code KardamomChainState}. Matches
     * Rust {@code kardamom_types::upgrades::FEATURE_SEALER_V4}.
     */
    public static final long FEATURE_SEALER_V4 = 3L;

    /** The most activations that a state holds: the visible one and two that wait. */
    static final int MAX_HELD = 3;

    private static final int HELD_LEN = Long.BYTES + Long.BYTES;

    /** The L1 origins of the held activations, oldest first, each above the one before. */
    private final long[] origins = new long[MAX_HELD];

    /** The times in ms of the held activations, compared unsigned. A visible activation's time is resolved. */
    private final long[] times = new long[MAX_HELD];

    private int held;

    /** The L1 origin of the last block that a boundary sealed. An activation at or below it is visible. */
    private long sealedOrigin;

    private int version;

    private VersionSwitch(final int version) {
        this.version = version;
    }

    /** The switch of a state that starts at genesis or from a seed. */
    static VersionSwitch start() {
        return new VersionSwitch(OFF_VERSION);
    }

    /** The version of the rules of the open block. */
    public int version() {
        return version;
    }

    /** Whether the state holds an activation: the snapshot must then carry the switch. */
    public boolean holdsActivation() {
        return held > 0;
    }

    /** The newest held activation, as a value, for the admin endpoint. */
    public View view() {
        return held == 0 ? new View(version, 0L, 0L) : new View(version, origins[held - 1], times[held - 1]);
    }

    /**
     * Take a feature activation record, with {@code stateOrigin} the L1
     * origin of the state and {@code openBlockMs} the time that the open
     * block executes with. The record only stores the schedule.
     */
    Outcome onActivation(final FeatureActivation activation, final long stateOrigin, final long openBlockMs) {
        final long origin = activation.l1Origin();
        final long newest = held == 0 ? 0L : origins[held - 1];
        if (activation.featureId() != FEATURE_SEALER_V4 || Long.compareUnsigned(origin, newest) <= 0) {
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
        return time != 0L && Long.compareUnsigned(time, l2Timestamp) <= 0 ? ON_VERSION : OFF_VERSION;
    }

    /**
     * Set the version of the next block at the boundary of a block with L1
     * origin {@code l1Origin}. {@code l2Timestamp} is the time that the next
     * block executes with, and {@code sealedBlockMs} the time that the
     * sealed block executed with. The newest visible activation decides,
     * and the older ones go.
     *
     * @return whether the version switched at this boundary
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

    /** The time of held activation {@code index}: a 0 of one that becomes visible now is {@code sealedBlockMs}. */
    private long resolvedTime(final int index, final long sealedBlockMs) {
        final boolean becomesVisible = Long.compareUnsigned(origins[index], sealedOrigin) > 0;
        return becomesVisible && times[index] == 0L ? sealedBlockMs : times[index];
    }

    private void dropOldest(final int count) {
        System.arraycopy(origins, count, origins, 0, held - count);
        System.arraycopy(times, count, times, 0, held - count);
        held -= count;
    }

    private void hold(final long origin, final long time) {
        origins[held] = origin;
        times[held] = time;
        held++;
    }

    /** The length of the snapshot section that {@link #writeTo} writes. */
    int snapshotLen() {
        return Byte.BYTES + Long.BYTES + Byte.BYTES + held * HELD_LEN;
    }

    /**
     * Write {@code [version:u8][sealed_origin:u64][held:u8]}, then
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

    /** Read the section that {@link #writeTo} wrote. */
    static VersionSwitch readFrom(final ByteBuffer buf) {
        final VersionSwitch rules = new VersionSwitch(versionOf(buf.get()));
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

    private static int versionOf(final byte version) {
        if (version != OFF_VERSION && version != ON_VERSION) {
            throw new IllegalArgumentException("bad snapshot decision version: " + version);
        }
        return version;
    }

    /** What a feature activation record did. */
    public enum Outcome {
        /** The record changed nothing: another feature, a copy, or a later epoch. */
        IGNORED,
        /** The state holds the schedule; the version changes at a block boundary. */
        STORED,
        /** The state holds the schedule, but the first block of its epoch is sealed. */
        STORED_LATE,
        /** The epoch of the record is below the epoch of the state: the switch does not follow it. */
        MISSED,
        /** The state holds {@link #MAX_HELD} activations: the record changed nothing. */
        FULL
    }

    /** A feature activation record: the upgrade transaction of the epoch at {@code l1Origin} schedules {@code featureId}. */
    public record FeatureActivation(long l1Origin, long featureId, long activationMs) {
    }

    /** The version, and the origin and the time of the newest held activation, both 0 when none. */
    public record View(int version, long activationOrigin, long activationMs) {
    }
}
