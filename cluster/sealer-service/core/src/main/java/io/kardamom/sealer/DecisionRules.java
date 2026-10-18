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
 * <p>An activation applies only when its L1 origin is at or above the
 * origin of the state, and above the origin of the pending activation. So
 * a copy of the record from a twin sequencer, or a record that a sequencer
 * sends again with its epoch, changes nothing, and the record of an older
 * epoch changes nothing. The copy of the twin still applies after the
 * epoch, so one lost record does not lose the activation. A later upgrade
 * before the switch replaces the activation time, as
 * {@code KardamomChainState.setFeature} does.</p>
 */
public final class DecisionRules {
    /** The rules of a state that holds no activation. The job names this version. */
    public static final int START_VERSION = 2;

    /** The newest rules of this release: the nonce release applies. */
    public static final int NEWEST_VERSION = 3;

    /**
     * The feature id of version 3 in {@code KardamomChainState}. Matches
     * Rust {@code kardamom_types::upgrades::FEATURE_SEALER_V3}.
     */
    public static final long FEATURE_SEALER_V3 = 2L;

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

    /** The version of the rules. */
    int version() {
        return version;
    }

    /** Whether the state holds an activation: the snapshot must then carry the rules. */
    boolean holdsActivation() {
        return activationOrigin != 0L;
    }

    /**
     * Take a feature activation record at cluster time {@code nowMs}, with
     * {@code stateOrigin} the L1 origin of the state.
     *
     * @return whether the rules switched to version 3 at this record
     */
    boolean onActivation(final FeatureActivation activation, final long stateOrigin, final long nowMs) {
        final boolean applies = activation.featureId() == FEATURE_SEALER_V3
            && version == START_VERSION
            && activation.l1Origin() >= stateOrigin
            && activation.l1Origin() > activationOrigin;
        if (!applies) {
            return false;
        }
        activationOrigin = activation.l1Origin();
        activationMs = activation.activationMs();
        return switchAt(nowMs);
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

    /**
     * A feature activation record: the upgrade transaction of the epoch at
     * {@code l1Origin} schedules {@code featureId} at {@code activationMs}.
     */
    public record FeatureActivation(long l1Origin, long featureId, long activationMs) {
    }
}
