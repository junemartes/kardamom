package io.kardamom.sealer;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * The switch between decision versions 3, 4 and 5. Two members
 * on two versions can decide one log entry differently, and the state
 * forks. So the version is replicated state: it changes only at a block
 * boundary, at the same log index on every member, and the snapshot carries
 * it. All the entries of one block run under one version.
 *
 * <p>Version 3 orders a transaction reference by its canonical id, with the
 * id window, the deadline and the contiguity guard. Version 4 orders it by
 * the replay window of its ingress image: see {@link SessionWindows}.
 * Version 5 is version 4 that also orders a full reference, the reference
 * with the bytes of its transaction.</p>
 *
 * <p>Each feature has its own schedule: {@link #FEATURE_SEALER_V4} turns
 * version 4 on, and {@link #FEATURE_SEALER_V5}, the carry flag, turns
 * version 5 on. Version 5 needs version 4, so the next block runs version 3
 * while version 4 is off, version 5 while both are on, and version 4
 * otherwise. The state takes an activation of the carry flag only while
 * version 4 or 5 runs ({@link Outcome#EARLY}). The rules below hold for
 * each schedule.</p>
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
 * boundary before the block of the deposit. The feature is on in block
 * N+1 when the visible activation has a time that is not 0 and is at or
 * below the time of block N, and off otherwise. A time ahead switches the
 * feature off and on again at its time, and a time of {@code 2^64 - 1}
 * keeps it off.</p>
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
 * <p>The state holds the visible activation of each feature and the ones
 * whose epoch is not sealed yet: at most {@link #MAX_HELD} of each feature. A record that comes when the
 * state holds that many changes nothing ({@link Outcome#FULL}).</p>
 *
 * <p>{@link #FEATURE_CARRY_WINDOW} sets the carry window flag, not a
 * version. The state takes its activation only while version 5 runs
 * ({@link Outcome#EARLY}), and never on a member that keeps its retained
 * frames in a file ({@link Outcome#UNSUPPORTED}). The flag changes at a
 * boundary by the same rules as a version. Its rules apply only while the
 * flag is on and version 5 runs. When version 5 stops, the flag keeps its
 * value, so its rules come back with version 5.</p>
 *
 * <p>A chain can run features from its genesis ({@link GenesisFeatures}).
 * The switch of such a chain holds one activation of each of these
 * features at L1 origin 0 with the time {@link #GENESIS_MS}. Such an
 * activation is visible at every boundary and its time is at or below the
 * time of every block, so the feature is on from the first block. It
 * stays until a real activation of the feature becomes visible. The held
 * activations put the switch in the snapshot, so a member that restores
 * one keeps the rules.</p>
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

    /**
     * The feature id of decision version 5, the carry flag, in
     * {@code KardamomChainState}. Matches Rust
     * {@code kardamom_types::upgrades::FEATURE_SEALER_V5}.
     */
    public static final long FEATURE_SEALER_V5 = 4L;

    /**
     * The feature id of the carry window in {@code KardamomChainState}.
     * Matches Rust {@code kardamom_types::upgrades::FEATURE_CARRY_WINDOW}.
     * Under version 5, the carry window keeps only the replay window of
     * retained frames, and refuses a transaction reference of record type 0.
     */
    public static final long FEATURE_CARRY_WINDOW = 5L;

    /** The version of an active carry flag: version 4 that relays the bytes of a full reference. */
    public static final int CARRY_VERSION = 5;

    /** The most activations of one feature that a state holds: the visible one and two that wait. */
    static final int MAX_HELD = 3;

    /** The time in ms of an activation at genesis: at or below the time of every block. */
    static final long GENESIS_MS = 1L;

    /** The activations of {@link #FEATURE_SEALER_V4}. */
    private final Schedule v4 = new Schedule();

    /** The activations of {@link #FEATURE_SEALER_V5}. */
    private final Schedule carry = new Schedule();

    /** The activations of {@link #FEATURE_CARRY_WINDOW}. */
    private final Schedule window = new Schedule();

    /** The carry window flag of the open block. Its rules apply only under version 5. */
    private boolean carryWindow;

    /**
     * Whether the member keeps its retained frames in a file. Such a
     * member takes no activation of {@link #FEATURE_CARRY_WINDOW}: its
     * snapshot has no place for the schedule. The setting must match on
     * every member, so every member gives the same outcome.
     */
    private boolean fileStore;

    /** The L1 origin of the last block that a boundary sealed. An activation at or below it is visible. */
    private long sealedOrigin;

    private int version;

    private VersionSwitch(final int version) {
        this.version = version;
    }

    /** The switch of a state that starts at genesis with no feature, or from a seed without the rules. */
    static VersionSwitch start() {
        return new VersionSwitch(OFF_VERSION);
    }

    /**
     * The switch of a state that starts at genesis with {@code features}:
     * one held activation of each feature at L1 origin 0 and time
     * {@link #GENESIS_MS}. The version and the carry window flag come from
     * the rule of a boundary.
     */
    public static VersionSwitch genesis(final GenesisFeatures features) {
        final VersionSwitch rules = start();
        rules.schedulesOf(features).forEach(schedule -> schedule.hold(0L, GENESIS_MS));
        rules.atBoundary(0L, 0L, GENESIS_MS);
        return rules;
    }

    /** A copy that changes apart from this switch. */
    VersionSwitch copy() {
        final VersionSwitch copy = new VersionSwitch(version);
        copy.carryWindow = carryWindow;
        copy.fileStore = fileStore;
        copy.sealedOrigin = sealedOrigin;
        copy.v4.copyFrom(v4);
        copy.carry.copyFrom(carry);
        copy.window.copyFrom(window);
        return copy;
    }

    /**
     * Whether the switch holds an activation of each feature of
     * {@code features}. A schedule that holds an activation always keeps
     * its newest visible one, so the switch of a chain that ran the
     * features from its genesis holds one of each for all time.
     */
    public boolean holdsEach(final GenesisFeatures features) {
        return schedulesOf(features).allMatch(schedule -> schedule.held > 0);
    }

    /**
     * Whether this switch is the switch of a chain that runs exactly
     * {@code features} from its genesis. It holds an activation of each
     * feature of the set, and no activation at L1 origin 0 of a feature
     * outside the set. Only a genesis activation has origin 0: the switch
     * ignores a record at or below the origin of its newest activation.
     */
    public boolean matchesGenesis(final GenesisFeatures features) {
        return holdsEach(features)
            && Stream.of(v4, carry, window).skip(features.count()).noneMatch(Schedule::holdsGenesis);
    }

    /** The schedules of {@code features}: the first of version 4, the carry flag and the carry window. */
    private Stream<Schedule> schedulesOf(final GenesisFeatures features) {
        return Stream.of(v4, carry, window).limit(features.count());
    }

    /** The version of the rules of the open block. */
    public int version() {
        return version;
    }

    /** Whether the state holds an activation: the snapshot must then carry the switch. */
    public boolean holdsActivation() {
        return v4.held > 0 || carry.held > 0;
    }

    /** Whether the state holds an activation of the carry flag: the snapshot must then carry its schedule. */
    public boolean holdsCarryActivation() {
        return carry.held > 0;
    }

    /** Whether the state holds an activation of the carry window: the snapshot must then carry its section. */
    public boolean holdsWindowActivation() {
        return window.held > 0;
    }

    /** Whether the rules of the carry window apply in the open block: the flag is on, and version 5 runs. */
    public boolean carryWindowRules() {
        return carryWindow && version == CARRY_VERSION;
    }

    /**
     * Mark the state of a member that keeps its retained frames in a file.
     * Call it before the first entry. Such a member holds no activation of
     * {@link #FEATURE_CARRY_WINDOW}: it answers each one with
     * {@link Outcome#UNSUPPORTED}. So it drops the schedule and the flag of
     * the carry window that a seed gives it, and holds what a peer with the
     * file holds.
     */
    void fileStore(final boolean file) {
        fileStore = file;
        if (file) {
            window.held = 0;
            carryWindow = false;
        }
    }

    /**
     * The state of each feature, for the admin endpoint: version 4, the
     * carry flag and the carry window, in feature id order.
     */
    public List<Feature> features() {
        return List.of(
            v4.feature(FEATURE_SEALER_V4, version >= ON_VERSION),
            carry.feature(FEATURE_SEALER_V5, version == CARRY_VERSION),
            windowFeature());
    }

    /** The state of the carry window, for the admin endpoint. */
    public Feature windowFeature() {
        return window.feature(FEATURE_CARRY_WINDOW, carryWindow);
    }

    /** The newest held activation of either feature, as a value, for the admin endpoint. */
    public View view() {
        final Schedule newest = carry.held > 0 && Long.compareUnsigned(carry.newest(), v4.newest()) > 0 ? carry : v4;
        return newest.held == 0
            ? new View(version, 0L, 0L)
            : new View(version, newest.origins[newest.held - 1], newest.times[newest.held - 1]);
    }

    /**
     * Take a feature activation record, with {@code stateOrigin} the L1
     * origin of the state and {@code openBlockMs} the time that the open
     * block executes with. The record only stores the schedule. The state
     * takes a record of the carry flag only while version 4 or 5 runs, and
     * a record of the carry window only while version 5 runs and only on a
     * member without the retained file.
     */
    Outcome onActivation(final FeatureActivation activation, final long stateOrigin, final long openBlockMs) {
        final Optional<Schedule> known = scheduleOf(activation.featureId());
        if (known.isEmpty()) {
            return Outcome.IGNORED;
        }
        final Schedule schedule = known.get();
        final long origin = activation.l1Origin();
        if (Long.compareUnsigned(origin, schedule.newest()) <= 0) {
            return Outcome.IGNORED;
        }
        if (stateOrigin != 0L && Long.compareUnsigned(origin, stateOrigin) < 0) {
            return Outcome.MISSED;
        }
        if (stateOrigin != 0L && origin != stateOrigin && origin != stateOrigin + 1) {
            return Outcome.IGNORED;
        }
        if (schedule == window && fileStore) {
            return Outcome.UNSUPPORTED;
        }
        if ((schedule == carry && version < ON_VERSION) || (schedule == window && version != CARRY_VERSION)) {
            return Outcome.EARLY;
        }
        if (schedule.held == MAX_HELD) {
            return Outcome.FULL;
        }
        final boolean late = Long.compareUnsigned(origin, sealedOrigin) <= 0;
        final long time = activation.activationMs();
        schedule.hold(origin, late && time == 0L ? openBlockMs : time);
        return late ? Outcome.STORED_LATE : Outcome.STORED;
    }

    /** The schedule of {@code featureId}, or empty for a feature that the switch does not follow. */
    private Optional<Schedule> scheduleOf(final long featureId) {
        if (featureId == FEATURE_SEALER_V4) {
            return Optional.of(v4);
        }
        if (featureId == FEATURE_SEALER_V5) {
            return Optional.of(carry);
        }
        return featureId == FEATURE_CARRY_WINDOW ? Optional.of(window) : Optional.empty();
    }

    /**
     * The version that {@link #atBoundary} would set for the same boundary.
     * It changes nothing, so a record that forces a boundary can be checked
     * under the rules of the block that it opens. Version 5 needs version 4:
     * when {@link #FEATURE_SEALER_V4} is off, the version is 3 whatever the
     * carry flag says.
     */
    int versionAtBoundary(final long l1Origin, final long sealedBlockMs, final long l2Timestamp) {
        if (!v4.onAt(l1Origin, sealedBlockMs, l2Timestamp, version >= ON_VERSION)) {
            return OFF_VERSION;
        }
        return carry.onAt(l1Origin, sealedBlockMs, l2Timestamp, version == CARRY_VERSION)
            ? CARRY_VERSION
            : ON_VERSION;
    }

    /**
     * Set the version of the next block at the boundary of a block with L1
     * origin {@code l1Origin}. {@code l2Timestamp} is the time that the next
     * block executes with, and {@code sealedBlockMs} the time that the
     * sealed block executed with. The newest visible activation of each
     * feature decides, and the older ones go. The carry window flag
     * follows its own schedule at every boundary, whatever the version.
     *
     * @return whether the version switched at this boundary
     */
    boolean atBoundary(final long l1Origin, final long sealedBlockMs, final long l2Timestamp) {
        final int due = versionAtBoundary(l1Origin, sealedBlockMs, l2Timestamp);
        carryWindow = window.onAt(l1Origin, sealedBlockMs, l2Timestamp, carryWindow);
        v4.settle(l1Origin, sealedBlockMs);
        carry.settle(l1Origin, sealedBlockMs);
        window.settle(l1Origin, sealedBlockMs);
        sealedOrigin = l1Origin;
        final boolean switched = due != version;
        version = due;
        return switched;
    }

    /** The length of the snapshot section that {@link #writeTo} writes. */
    int snapshotLen() {
        return Byte.BYTES + Long.BYTES + v4.snapshotLen();
    }

    /** The length of the snapshot section that {@link #writeCarryTo} writes. */
    int carrySnapshotLen() {
        return carry.snapshotLen();
    }

    /** The length of the snapshot section that {@link #writeWindowTo} writes. */
    int windowSnapshotLen() {
        return Byte.BYTES + window.snapshotLen();
    }

    /**
     * Write {@code [version:u8][sealed_origin:u64][held:u8]}, then
     * {@code [origin:u64][time_ms:u64]} for each held activation of
     * {@link #FEATURE_SEALER_V4}, oldest first.
     */
    void writeTo(final ByteBuffer buf) {
        buf.put((byte) version);
        buf.putLong(sealedOrigin);
        v4.writeTo(buf);
    }

    /**
     * Write {@code [held:u8]}, then {@code [origin:u64][time_ms:u64]} for
     * each held activation of {@link #FEATURE_SEALER_V5}, oldest first.
     */
    void writeCarryTo(final ByteBuffer buf) {
        carry.writeTo(buf);
    }

    /**
     * Write {@code [carry_window:u8][held:u8]}, then
     * {@code [origin:u64][time_ms:u64]} for each held activation of
     * {@link #FEATURE_CARRY_WINDOW}, oldest first.
     */
    void writeWindowTo(final ByteBuffer buf) {
        buf.put(carryWindow ? (byte) 1 : (byte) 0);
        window.writeTo(buf);
    }

    /** Read the section that {@link #writeTo} wrote. */
    static VersionSwitch readFrom(final ByteBuffer buf) {
        final VersionSwitch rules = new VersionSwitch(versionOf(buf.get()));
        rules.sealedOrigin = buf.getLong();
        rules.v4.readFrom(buf);
        return rules;
    }

    /**
     * Read the rules of a seed: the sections that {@link #writeTo},
     * {@link #writeCarryTo} and {@link #writeWindowTo} write, in that order.
     */
    static VersionSwitch readSeed(final ByteBuffer buf) {
        final VersionSwitch rules = readFrom(buf);
        rules.readCarryFrom(buf);
        rules.readWindowFrom(buf);
        return rules;
    }

    /** Read the section that {@link #writeCarryTo} wrote. */
    void readCarryFrom(final ByteBuffer buf) {
        carry.readFrom(buf);
    }

    /** Read the section that {@link #writeWindowTo} wrote. */
    void readWindowFrom(final ByteBuffer buf) {
        carryWindow = flagOf(buf.get());
        window.readFrom(buf);
    }

    private static boolean flagOf(final byte flag) {
        if (flag != 0 && flag != 1) {
            throw new IllegalArgumentException("bad snapshot carry window flag: " + flag);
        }
        return flag == 1;
    }

    private static int versionOf(final byte version) {
        if (version < OFF_VERSION || version > CARRY_VERSION) {
            throw new IllegalArgumentException("bad snapshot decision version: " + version);
        }
        return version;
    }

    /**
     * The held activations of one feature, oldest first, each above the one
     * before: the visible one and the ones whose epoch is not sealed yet.
     */
    private final class Schedule {
        private static final int HELD_LEN = Long.BYTES + Long.BYTES;

        /** The L1 origins of the held activations. */
        private final long[] origins = new long[MAX_HELD];

        /** The times in ms of the held activations, compared unsigned. A visible activation's time is resolved. */
        private final long[] times = new long[MAX_HELD];

        private int held;

        /** Whether the oldest held activation is a genesis activation, at L1 origin 0. */
        boolean holdsGenesis() {
            return held > 0 && origins[0] == 0L;
        }

        /** The origin of the newest held activation, or 0 when none. */
        long newest() {
            return held == 0 ? 0L : origins[held - 1];
        }

        /**
         * Whether the feature is on in the block after the boundary of a
         * block with L1 origin {@code l1Origin} and time
         * {@code sealedBlockMs}: the newest visible activation has a time
         * that is not 0 and is at or below {@code l2Timestamp}, the time of
         * the next block. With no visible activation, the feature stays as
         * {@code current}. The boundary allocates nothing.
         */
        boolean onAt(final long l1Origin, final long sealedBlockMs, final long l2Timestamp, final boolean current) {
            final int visible = visibleAt(l1Origin);
            if (visible == 0) {
                return current;
            }
            final long time = resolvedTime(visible - 1, sealedBlockMs);
            return time != 0L && Long.compareUnsigned(time, l2Timestamp) <= 0;
        }

        /** Resolve the time of the newest visible activation at the boundary, and drop the older ones. */
        void settle(final long l1Origin, final long sealedBlockMs) {
            final int visible = visibleAt(l1Origin);
            if (visible > 0) {
                times[visible - 1] = resolvedTime(visible - 1, sealedBlockMs);
                dropOldest(visible - 1);
            }
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

        /** The state of this feature, which is on when {@code on}, with its newest held activation. */
        Feature feature(final long id, final boolean on) {
            if (held == 0) {
                return Feature.none(id);
            }
            return new Feature(id, on ? Feature.ON : Feature.HELD_OFF, origins[held - 1], times[held - 1]);
        }

        void hold(final long origin, final long time) {
            origins[held] = origin;
            times[held] = time;
            held++;
        }

        void copyFrom(final Schedule other) {
            System.arraycopy(other.origins, 0, origins, 0, MAX_HELD);
            System.arraycopy(other.times, 0, times, 0, MAX_HELD);
            held = other.held;
        }

        int snapshotLen() {
            return Byte.BYTES + held * HELD_LEN;
        }

        void writeTo(final ByteBuffer buf) {
            buf.put((byte) held);
            for (int i = 0; i < held; i++) {
                buf.putLong(origins[i]).putLong(times[i]);
            }
        }

        void readFrom(final ByteBuffer buf) {
            final int count = buf.get();
            if (count < 0 || count > MAX_HELD) {
                throw new IllegalArgumentException("bad snapshot activation count: " + count);
            }
            for (int i = 0; i < count; i++) {
                hold(buf.getLong(), buf.getLong());
            }
        }
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
        FULL,
        /**
         * The record schedules the carry flag while version 4 is off, or the
         * carry window while version 5 is off: the record changed nothing.
         */
        EARLY,
        /** The record schedules the carry window on a member with the retained file: the record changed nothing. */
        UNSUPPORTED
    }

    /** A feature activation record: the upgrade transaction of the epoch at {@code l1Origin} schedules {@code featureId}. */
    public record FeatureActivation(long l1Origin, long featureId, long activationMs) {
    }

    /** The version, and the origin and the time of the newest held activation, both 0 when none. */
    public record View(int version, long activationOrigin, long activationMs) {
    }

    /**
     * The state of feature {@code id}: {@link #NONE}, {@link #HELD_OFF} or
     * {@link #ON}, and the origin and the time of its newest held
     * activation, both 0 when none.
     */
    public record Feature(long id, int state, long activationOrigin, long activationMs) {
        /** The state holds no activation of the feature. */
        public static final int NONE = 0;
        /** The state holds an activation, and the feature is off. */
        public static final int HELD_OFF = 1;
        /** The feature is on. */
        public static final int ON = 2;

        /** Feature {@code id} with no held activation. */
        public static Feature none(final long id) {
            return new Feature(id, NONE, 0L, 0L);
        }
    }
}
