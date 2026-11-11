package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.kardamom.sealer.VersionSwitch;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Feature 5 turns off with a later activation, then on again. Off, the
 * posted rule keeps every unposted frame again, and the canonical stream
 * goes on. On again, every member drains to the exact window at the
 * same entry.
 */
@ExtendWith(InterruptingTestCallback.class)
class SealerCarryWindowOffOnClusterTest {

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @Test
    @InterruptAfter(value = 60, unit = TimeUnit.SECONDS)
    void the_window_turns_off_and_on_at_the_same_entry_on_every_member() {
        try (CarryCluster carry = CarryCluster.atVersionFive(systemTestWatcher)) {
            final int from = carry.egress.relayedPayloads.size();
            carry.activate(VersionSwitch.FEATURE_CARRY_WINDOW, CarryCluster.ON);
            carry.awaitRunning(m -> m.metrics().carry().carryWindow());
            final List<FullRef> refs = new ArrayList<>(carry.blocks(3));
            carry.assertWindow(true);

            carry.activate(VersionSwitch.FEATURE_CARRY_WINDOW, CarryCluster.OFF);
            carry.awaitRunning(m -> !m.metrics().carry().carryWindow());
            refs.addAll(carry.blocks(6));
            carry.assertWindow(false);
            assertTrue(carry.synced().stream().allMatch(m -> m.metrics().carry().retainedFrames() > CarryCluster.WINDOW),
                () -> "off: no frame leaves before its block is posted: "
                    + carry.synced().stream().map(m -> m.metrics().carry()).toList());
            carry.assertSameState();

            carry.activate(VersionSwitch.FEATURE_CARRY_WINDOW, CarryCluster.ON);
            carry.awaitRunning(m -> m.metrics().carry().carryWindow());
            refs.addAll(carry.blocks(2));
            carry.assertWindow(true);
            carry.assertSameState();
            carry.assertRelayed(from, refs);
        }
    }
}
