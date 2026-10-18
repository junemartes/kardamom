package io.kardamom.sealer;

import java.nio.ByteBuffer;

/**
 * The version of the rules that decide the replicated state, and the
 * switch from version 2 to version 3. Two members on two versions can
 * decide one log entry differently, and the state forks. So the version is
 * replicated state: it changes only at a record in the log, at the same log
 * index on every member, and the snapshot carries it.
 *
 * <p>Version 2 is the rules of a state that holds no activation. Version 3
 * adds the nonce release ({@link CanonicalSealerState#onRelease}) and the
 * rules frame to each session. An upgrade transaction on L1 schedules
 * {@link #FEATURE_SEALER_V3} at an activation time. The sequencer sends the
 * schedule as a feature activation record in front of the epoch that holds
 * the upgrade. The rules switch to version 3 at the first log entry whose
 * cluster time is at or above the activation time. An activation time at or
 * below the time of the activation record switches the rules at that
 * record. The switch is one way.</p>
 *
 * <p>An activation applies only when its L1 origin names the epoch that the
 * record fronts: the next origin of the state, or the origin of the state
 * when the epoch got in first. A state with no epoch yet (origin 0) takes
 * any origin, because its first epoch can start at any L1 block. The
 * activation must also be above the origin of the pending activation. So a copy of the record from a twin sequencer, or a
 * record that a sequencer sends again with its epoch, changes nothing, and a
 * record for an older or a later epoch changes nothing. The copy of the twin
 * still applies after the epoch, so one lost record does not lose the
 * activation. A later upgrade before the switch replaces the activation
 * time, as {@code KardamomChainState.setFeature} does. The origins and the
 * time are u64 values from the wire, so every comparison is unsigned.</p>
 */
public final class DecisionRules {
    /** The rules of a state that holds no activation. Every member starts on it. */
    public static final int START_VERSION = 2;

    /** The newest rules: the nonce release applies. */
    public static final int NEWEST_VERSION = 3;

    /**
     * The feature id of version 3 in {@code KardamomChainState}. Matches
     * Rust {@code kardamom_types::upgrades::FEATURE_SEALER_V3}.
     */
    public static final long FEATURE_SEALER_V3 = 2L;

    /** The rules of a state that holds no activation, as a value. */
    public static final View START_VIEW = new View(START_VERSION, 0L, 0L);

    /** The snapshot tail: {@code [version:u8][activation_origin:u64][activation_ms:u64]}. */
    static final int SNAPSHOT_LEN = Byte.BYTES + Long.BYTES + Long.BYTES;

    /** The origin of the pending activation, or 0 when the state holds none. */
    private long activationOrigin;

    /** The activation time in ms, compared unsigned: the wire carries a u64. */
    private long activationMs;

    private int version;

    private DecisionRules(final int version, final long activationOrigin, final long activationMs) {
        this.version = version;
        this.activationOrigin = activationOrigin;
        this.activationMs = activationMs;
    }

    /** The rules of a state that holds no activation. */
    static DecisionRules start() {
        return new DecisionRules(START_VERSION, 0L, 0L);
    }

    /**
     * Version 3 with no activation: the rules of a member that keeps its
     * retained frames in a file. That setting comes with version 3, and every
     * member must match it.
     */
    static DecisionRules newest() {
        return new DecisionRules(NEWEST_VERSION, 0L, 0L);
    }

    /** The version of the rules. */
    int version() {
        return version;
    }

    /** Whether the state holds an activation: the snapshot must then carry the rules. */
    boolean holdsActivation() {
        return activationOrigin != 0L;
    }

    /** The rules as a value, for the admin endpoint. */
    public View view() {
        return new View(version, activationOrigin, activationMs);
    }

    /**
     * Take a feature activation record at cluster time {@code nowMs}, with
     * {@code stateOrigin} the L1 origin of the state.
     *
     * @return what the record did
     */
    Outcome onActivation(final FeatureActivation activation, final long stateOrigin, final long nowMs) {
        final long origin = activation.l1Origin();
        final boolean frontsTheEpoch = stateOrigin == 0L || origin == stateOrigin || origin == stateOrigin + 1;
        final boolean applies = activation.featureId() == FEATURE_SEALER_V3
            && version == START_VERSION
            && frontsTheEpoch
            && Long.compareUnsigned(origin, activationOrigin) > 0;
        if (!applies) {
            return Outcome.IGNORED;
        }
        activationOrigin = origin;
        activationMs = activation.activationMs();
        return switchAt(nowMs) ? Outcome.SWITCHED : Outcome.PENDING;
    }

    /**
     * Switch to version 3 when an activation is pending and {@code nowMs},
     * the cluster time of the log entry, is at or above its time.
     *
     * @return whether the rules switched at this entry
     */
    boolean switchAt(final long nowMs) {
        final boolean due = version == START_VERSION
            && holdsActivation()
            && Long.compareUnsigned(nowMs, activationMs) >= 0;
        if (due) {
            version = NEWEST_VERSION;
        }
        return due;
    }

    /** Write the snapshot tail. */
    void writeTo(final ByteBuffer buf) {
        buf.put((byte) version);
        buf.putLong(activationOrigin);
        buf.putLong(activationMs);
    }

    /** Read the snapshot tail that {@link #writeTo} wrote. */
    static DecisionRules readFrom(final ByteBuffer buf) {
        final int version = buf.get();
        if (version != START_VERSION && version != NEWEST_VERSION) {
            throw new IllegalArgumentException("bad snapshot decision version: " + version);
        }
        return new DecisionRules(version, buf.getLong(), buf.getLong());
    }

    /** What a feature activation record did. */
    public enum Outcome {
        /** The record changed nothing. */
        IGNORED,
        /** The state holds the activation, and its time is not due yet. */
        PENDING,
        /** The rules switched to version 3 at this record. */
        SWITCHED
    }

    /**
     * A feature activation record: the upgrade transaction of the epoch at
     * {@code l1Origin} schedules {@code featureId} at {@code activationMs}.
     */
    public record FeatureActivation(long l1Origin, long featureId, long activationMs) {
    }

    /**
     * The rules as a value: the version, and the origin and the time of the
     * activation, both 0 when the state holds none. A check outside the
     * cluster compares the origin with the {@code setFeature} deposit of
     * that epoch on L1.
     */
    public record View(int version, long activationOrigin, long activationMs) {
    }
}
