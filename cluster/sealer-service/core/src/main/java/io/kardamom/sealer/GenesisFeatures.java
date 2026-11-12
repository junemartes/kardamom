package io.kardamom.sealer;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * The features of the version switch that a chain runs from its genesis:
 * the setting {@code kardamom.cluster.genesisFeatures}. Each set is a
 * prefix of features 3, 4 and 5, because feature 4 needs version 4 and
 * feature 5 needs version 5. Every member must run the same set, and the
 * set must stay for the life of the chain: a member that replays the log
 * from position 0 starts at genesis again.
 */
public enum GenesisFeatures {
    /** No feature: the chain starts on version 3. */
    NONE(""),
    /** Feature 3: the chain starts on version 4. */
    SEALER_V4("3"),
    /** Features 3 and 4: the chain starts on version 5. */
    SEALER_V5("3,4"),
    /** Features 3, 4 and 5: the chain starts on version 5 with the carry window. */
    CARRY_WINDOW("3,4,5");

    /** The setting that names the set. */
    public static final String SETTING = "kardamom.cluster.genesisFeatures";

    private final String value;

    GenesisFeatures(final String value) {
        this.value = value;
    }

    /**
     * The set that {@code value} names: {@code ""}, {@code "3"},
     * {@code "3,4"} or {@code "3,4,5"}. An unset value is {@link #NONE}.
     *
     * @throws IllegalArgumentException for any other value
     */
    public static GenesisFeatures parse(final String value) {
        final String trimmed = value == null ? "" : value.trim();
        return Arrays.stream(values())
            .filter(set -> set.value.equals(trimmed))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException(SETTING + " must be one of "
                + Arrays.stream(values()).map(set -> "\"" + set.value + "\"").collect(Collectors.joining(", "))
                + ", got \"" + value + "\""));
    }

    /** The setting value of this set, the empty string for {@link #NONE}. */
    public String value() {
        return value;
    }

    /**
     * The text of a refused start: the setting with this set, and
     * {@code cause}, which names the seed or the snapshot that does not
     * match the set and tells the operator what to do.
     */
    public String refusal(final String cause) {
        return SETTING + "=" + value + ", but the " + cause;
    }

    /** The number of features in the set: the set holds the first of features 3, 4 and 5 up to this count. */
    int count() {
        return ordinal();
    }

    /** Whether the set holds {@link VersionSwitch#FEATURE_CARRY_WINDOW}. */
    public boolean carryWindow() {
        return this == CARRY_WINDOW;
    }
}
