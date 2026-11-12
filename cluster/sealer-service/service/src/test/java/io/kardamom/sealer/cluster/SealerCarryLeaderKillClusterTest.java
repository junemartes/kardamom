package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
 * Feature 5 turns off, then on, and the leader stops within two seconds
 * after it switches. A new leader keeps the canonical stream going, the
 * old leader starts again, and every member ends with the exact window
 * and the same state bytes.
 */
@ExtendWith(InterruptingTestCallback.class)
class SealerCarryLeaderKillClusterTest {

    private static final long KILL_AFTER_SWITCH_NS = TimeUnit.SECONDS.toNanos(2);

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @Test
    @InterruptAfter(value = 60, unit = TimeUnit.SECONDS)
    void a_leader_stopped_right_after_its_switch_leaves_every_member_with_the_window() {
        try (CarryCluster carry = CarryCluster.atVersionFive(systemTestWatcher)) {
            carry.activate(VersionSwitch.FEATURE_CARRY_WINDOW, CarryCluster.OFF);
            carry.awaitRunning(m -> m.metrics().carry().activationOrigin() != 0L);
            carry.assertWindow(false);
            final List<FullRef> refs = new ArrayList<>(carry.blocks(2));

            final TestNode leader = carry.cluster.awaitLeader();
            final SealerClusteredService leading = CarryCluster.service(leader);
            assertTrue(!leading.metrics().carry().carryWindow(), "the first read is off");
            carry.activate(VersionSwitch.FEATURE_CARRY_WINDOW, CarryCluster.ON);
            carry.await(() -> leading.metrics().carry().carryWindow());
            final long switched = System.nanoTime();
            final int from = carry.egress.relayedPayloads.size();
            carry.cluster.stopNode(leader);
            final long late = System.nanoTime() - switched;
            assertTrue(late <= KILL_AFTER_SWITCH_NS, "the stop came " + late + " ns after the switch");
            final TestNode elected = carry.elect(leader.index());
            assertNotEquals(leader.index(), elected.index());

            refs.addAll(carry.blocks(3));
            carry.restart(leader.index());
            carry.awaitRunning(m -> m.metrics().carry().carryWindow());
            refs.addAll(carry.blocks(2));
            carry.assertSameState();
            carry.assertWindow(true);
            carry.assertRelayed(from, refs.subList(2, refs.size()));
        }
    }
}
