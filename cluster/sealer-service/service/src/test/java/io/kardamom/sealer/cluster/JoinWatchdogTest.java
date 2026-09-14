package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.cluster.ElectionState;
import org.junit.jupiter.api.Test;

/** Pure-function tests for the join watchdog rule. No Aeron, no cluster. */
final class JoinWatchdogTest {
    private static final long WINDOW_MS = 60_000L;

    @Test
    void firesWhenInitPersistsPastTheWindow() {
        final JoinWatchdog w = new JoinWatchdog(WINDOW_MS);
        assertFalse(w.observe(ElectionState.INIT, 0L));
        assertFalse(w.observe(ElectionState.INIT, WINDOW_MS));
        assertTrue(w.observe(ElectionState.INIT, WINDOW_MS + 1L));
        assertEquals(WINDOW_MS + 1L, w.initForMs(WINDOW_MS + 1L));
    }

    @Test
    void aNormalElectionNeverFires() {
        final JoinWatchdog w = new JoinWatchdog(WINDOW_MS);
        assertFalse(w.observe(ElectionState.INIT, 0L));
        assertFalse(w.observe(ElectionState.CANVASS, 5L));
        assertFalse(w.observe(ElectionState.FOLLOWER_BALLOT, 50L));
        assertFalse(w.observe(ElectionState.FOLLOWER_CATCHUP, 10 * WINDOW_MS));
        assertFalse(w.observe(ElectionState.CLOSED, 20 * WINDOW_MS));
        assertEquals(0L, w.initForMs(20 * WINDOW_MS));
    }

    @Test
    void aLoneMemberWithQuorumLostDoesNotFire() {
        // cluster-quorum-loss-recover leaves one member canvassing for
        // minutes. CANVASS is not INIT, so the watchdog stays quiet.
        final JoinWatchdog w = new JoinWatchdog(WINDOW_MS);
        for (long t = 0; t < 10 * WINDOW_MS; t += 1_000L) {
            assertFalse(w.observe(ElectionState.CANVASS, t));
        }
    }

    @Test
    void leavingInitResetsTheClock() {
        final JoinWatchdog w = new JoinWatchdog(WINDOW_MS);
        assertFalse(w.observe(ElectionState.INIT, 0L));
        assertFalse(w.observe(ElectionState.CANVASS, WINDOW_MS / 2));
        // A second election (a leader change) starts INIT again. The
        // window restarts from this observation, not from the first.
        assertFalse(w.observe(ElectionState.INIT, WINDOW_MS));
        assertFalse(w.observe(ElectionState.INIT, 2 * WINDOW_MS));
        assertTrue(w.observe(ElectionState.INIT, 2 * WINDOW_MS + 1L));
    }

    @Test
    void anUnallocatedCounterIsNotInit() {
        final JoinWatchdog w = new JoinWatchdog(WINDOW_MS);
        assertFalse(w.observe(null, 0L));
        assertFalse(w.observe(null, 5 * WINDOW_MS));
        assertEquals(0L, w.initForMs(5 * WINDOW_MS));
    }

    @Test
    void rejectsANonPositiveWindow() {
        assertThrows(IllegalArgumentException.class, () -> new JoinWatchdog(0L));
    }
}
