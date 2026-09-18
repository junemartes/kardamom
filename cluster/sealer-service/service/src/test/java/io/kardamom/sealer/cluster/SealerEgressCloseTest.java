package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.cluster.ClusterStubs.StubCluster;
import io.kardamom.sealer.cluster.ClusterStubs.StubSession;
import java.util.concurrent.TimeUnit;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The deadline-then-close path of the egress. A consumer whose egress
 * publication stays back-pressured is closed after one deadline. The close
 * is a request to the consensus module: the session stays in the cluster's
 * session set until the close comes back through the log. Every frame in
 * between must skip that session instead of spinning the deadline on it
 * again. Issue #292 measured up to thirty closes per session id, each one a
 * full second on the single service thread, which stalls the boundary tick.
 */
class SealerEgressCloseTest {

    private static final long DEADLINE_NS = TimeUnit.SECONDS.toNanos(1);

    private StubCluster cluster;
    private SealerClusteredService service;
    private StubSession publisher;
    private StubSession healthy;
    private StubSession wedged;

    @BeforeEach
    void start() {
        cluster = new StubCluster();
        service = new SealerClusteredService(64, 250, 0);
        service.onStart(cluster, null);
        publisher = cluster.addSession(1);
        healthy = cluster.addSession(2);
        wedged = cluster.addSession(3);
        deliver(healthy, IngressFrames.subscribeFrame());
        deliver(wedged, IngressFrames.subscribeFrame());
    }

    private void deliver(final StubSession from, final byte[] frame) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        buf.putBytes(0, frame);
        service.onSessionMessage(from, 0, buf, 0, frame.length, null);
    }

    /** One record with nonce {@code n} from a non-zero sender, so the contiguity guard applies. */
    private void record(final int n) {
        final byte[] sender = new byte[CanonicalSealerState.SENDER_LEN];
        java.util.Arrays.fill(sender, (byte) 0xAA);
        deliver(publisher, IngressFrames.recordFrame(n, sender, n));
    }

    private void tick() {
        service.onTimerEvent(SealerClusteredService.BOUNDARY_TIMER_CORRELATION_ID, 250);
    }

    private static long count(final StubSession s, final byte kind) {
        return s.offered.stream().filter(f -> f[0] == kind).count();
    }

    @Test
    void aWedgedConsumerIsClosedOnceAndSkippedUntilTheCloseLands() {
        wedged.backPressured = true;

        final long firstStart = System.nanoTime();
        record(0);
        final long firstNs = System.nanoTime() - firstStart;
        assertTrue(wedged.closed, "the first frame runs out the deadline and closes the session");
        assertEquals(1, wedged.closes);
        assertTrue(firstNs >= DEADLINE_NS, "the first frame waits the full deadline: " + firstNs);

        final long restStart = System.nanoTime();
        for (int n = 1; n < 5; n++) {
            record(n);
        }
        tick();
        final long restNs = System.nanoTime() - restStart;
        assertEquals(1, wedged.closes, "a closing session is not closed again");
        assertTrue(restNs < DEADLINE_NS / 2,
                "frames after the close request must not spin the deadline again: " + restNs);
        assertEquals(5, count(healthy, SealerWire.EGRESS_KIND_RELAYED));
        assertEquals(1, count(healthy, SealerWire.EGRESS_KIND_BOUNDARY));
        assertTrue(count(wedged, SealerWire.EGRESS_KIND_RELAYED) == 0);
    }

    @Test
    void aHealthyConsumerIsNeverClosed() {
        for (int n = 0; n < 3; n++) {
            record(n);
        }
        tick();
        assertEquals(0, healthy.closes);
        assertEquals(0, wedged.closes);
        assertEquals(3, count(wedged, SealerWire.EGRESS_KIND_RELAYED));
    }
}
