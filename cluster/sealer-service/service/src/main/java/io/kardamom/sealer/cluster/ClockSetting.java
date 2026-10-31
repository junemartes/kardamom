package io.kardamom.sealer.cluster;

import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusterClock;
import java.util.Optional;

/**
 * The clock of the consensus module ({@code -Dkardamom.cluster.clockClass}):
 * the name of a {@link ClusterClock} class with a public no-argument
 * constructor. Unset, the module keeps Aeron's default millisecond clock.
 * A test sets a clock that it moves forward, so the Raft timeouts of a
 * member pass in less wall time while their values stay the same.
 *
 * @param className the clock class, or empty for the default
 */
record ClockSetting(Optional<String> className) {

    static final String SETTING = "kardamom.cluster.clockClass";

    static ClockSetting fromProperties() {
        return new ClockSetting(Optional.ofNullable(System.getProperty(SETTING)).filter(s -> !s.isBlank()));
    }

    /** Give {@code ctx} the clock of this setting, or leave its default. */
    ConsensusModule.Context applyTo(final ConsensusModule.Context ctx) {
        return className.map(ClockSetting::instantiate).map(ctx::clusterClock).orElse(ctx);
    }

    private static ClusterClock instantiate(final String name) {
        try {
            return (ClusterClock) Class.forName(name).getConstructor().newInstance();
        } catch (final ReflectiveOperationException e) {
            throw new IllegalStateException(SETTING + ": cannot create " + name, e);
        }
    }
}
