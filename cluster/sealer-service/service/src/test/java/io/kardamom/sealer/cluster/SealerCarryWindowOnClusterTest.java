package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.cluster.TestNode;
import io.kardamom.sealer.VersionSwitch;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * The carry window turns on while a follower is down. The follower starts
 * again after the switch, replays the log, and switches at the same entry
 * as its peers: it ends with their state bytes and the exact window.
 */
@ExtendWith(InterruptingTestCallback.class)
class SealerCarryWindowOnClusterTest {

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @Test
    @InterruptAfter(value = 60, unit = TimeUnit.SECONDS)
    void a_follower_down_at_the_switch_starts_with_the_exact_window_of_its_peers() {
        try (CarryCluster carry = CarryCluster.atVersionFive(systemTestWatcher)) {
            final int from = carry.egress.relayedPayloads.size();
            final List<FullRef> refs = new ArrayList<>(carry.blocks(6));
            assertTrue(carry.synced().stream().allMatch(m -> m.metrics().carry().retainedFrames() > CarryCluster.WINDOW),
                "the posted rule keeps every unposted frame");

            final TestNode follower = carry.cluster.followers().get(0);
            final int followerId = follower.index();
            carry.cluster.stopNode(follower);
            carry.activate(VersionSwitch.FEATURE_CARRY_WINDOW, CarryCluster.ON);
            carry.awaitRunning(m -> m.metrics().carry().carryWindow());
            assertEquals(CarryCluster.MEMBER_COUNT - 1, carry.running().size(), "the follower is down at the switch");
            refs.addAll(carry.blocks(3));

            carry.restart(followerId);
            carry.awaitRunning(m -> m.metrics().carry().carryWindow());
            refs.addAll(carry.blocks(2));
            carry.assertSameState();
            carry.assertWindow(true);
            carry.assertRelayed(from, refs);
            assertEquals(0, carry.egress.refRefusedCount, "a reference with its bytes is never refused");
        }
    }
}
