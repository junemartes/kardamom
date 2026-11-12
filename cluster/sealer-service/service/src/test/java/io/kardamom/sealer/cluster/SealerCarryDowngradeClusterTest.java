package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.VersionSwitch;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Feature 4 turns off with a later activation: every member goes back to
 * decision version 4 at the same entry and reports it to the session.
 * The two references that a sequencer then releases, in the version 4
 * format without bytes, are both ordered in nonce order. Feature 4 turns
 * on again, and every member orders full references under version 5.
 */
@ExtendWith(InterruptingTestCallback.class)
class SealerCarryDowngradeClusterTest {

    private static final byte[] SENDER = FullRef.filled(CanonicalSealerState.SENDER_LEN, 0xAB);
    private static final long NONCE = 7L;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @Test
    @InterruptAfter(value = 60, unit = TimeUnit.SECONDS)
    void references_released_after_a_downgrade_are_ordered_under_version_four() {
        try (CarryCluster carry = CarryCluster.atVersionFive(systemTestWatcher)) {
            carry.await(() -> lastReport(carry) == VersionSwitch.CARRY_VERSION);
            carry.blocks(1);

            carry.activate(VersionSwitch.FEATURE_SEALER_V5, CarryCluster.OFF);
            carry.awaitRunning(m -> m.decisionRules().version() == 4);
            carry.await(() -> lastReport(carry) == 4);
            final int from = carry.egress.relayedPayloads.size();
            carry.offer(IngressFrames.txRefFrame(51, SENDER, NONCE, 4096L));
            carry.offer(IngressFrames.txRefFrame(52, SENDER, NONCE + 1, 4160L));
            carry.epoch();
            carry.awaitRelayed(from + 3);
            final List<byte[]> released = carry.egress.relayedPayloads.subList(from, from + 2);
            assertArrayEquals(IngressFrames.recordId(51), id(released.get(0)), "nonce n first");
            assertArrayEquals(IngressFrames.recordId(52), id(released.get(1)), "then the held nonce n + 1");
            released.forEach(p -> assertEquals(SealerWire.RT_TXREF, p[CanonicalSealerState.CANONICAL_ID_LEN]));
            assertEquals(0, carry.egress.refRefusedCount, "version 4 refuses no reference");
            carry.assertSameState();

            carry.activate(VersionSwitch.FEATURE_SEALER_V5, CarryCluster.ON);
            carry.awaitRunning(m -> m.decisionRules().version() == VersionSwitch.CARRY_VERSION);
            carry.await(() -> lastReport(carry) == VersionSwitch.CARRY_VERSION);
            final int again = carry.egress.relayedPayloads.size();
            carry.assertRelayed(again, carry.blocks(2));
            carry.assertSameState();
        }
    }

    private static int lastReport(final CarryCluster carry) {
        final List<Integer> versions = carry.egress.sealerInfoVersions;
        return versions.isEmpty() ? -1 : versions.get(versions.size() - 1);
    }

    private static byte[] id(final byte[] payload) {
        return java.util.Arrays.copyOf(payload, CanonicalSealerState.CANONICAL_ID_LEN);
    }
}
