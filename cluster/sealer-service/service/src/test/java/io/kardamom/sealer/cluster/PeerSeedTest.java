package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * A blank member whose peers do not answer keeps its peer seed rounds
 * going: the seed never returns, so the member never launches at log
 * position 0. {@link ClusterLogFactsTest} covers the seed from a running
 * cluster.
 */
@ExtendWith(InterruptingTestCallback.class)
final class PeerSeedTest {

    /** Three members on loopback ports that no process uses. */
    private static final String MEMBERS =
            "0,localhost:47100,localhost:47101,localhost:47102,localhost:47103,localhost:47104"
            + "|1,localhost:47110,localhost:47111,localhost:47112,localhost:47113,localhost:47114"
            + "|2,localhost:47120,localhost:47121,localhost:47122,localhost:47123,localhost:47124";
    private static final int MEMBER_ID = 0;
    private static final PeerSeed.Timing FAST = new PeerSeed.Timing(500, 100, 200);
    /** Long enough for several rounds of FAST. */
    private static final long WATCH_MS = 3_000;

    @TempDir
    Path dir;

    @Test
    @InterruptAfter(value = 30, unit = TimeUnit.SECONDS)
    void aBlankMemberWithoutPeersKeepsWaiting() throws InterruptedException {
        final MemberContexts contexts = new MemberContexts(
                dir.resolve("aeron").toString(),
                dir.resolve("cluster").toString(),
                dir.resolve("archive").toString(),
                ClusterNode.memberEndpoints(MEMBERS, MEMBER_ID));
        final PeerSeed seed = new PeerSeed(
                MEMBER_ID, ClusterNode.peerConsensusEndpoints(MEMBERS, MEMBER_ID), contexts, FAST);
        final Thread seeding = new Thread(seed::run, "peer-seed-test");
        seeding.start();

        seeding.join(WATCH_MS);
        final boolean stillSeeding = seeding.isAlive();
        seeding.interrupt();
        seeding.join();

        assertTrue(stillSeeding, "the seed must not return without a peer");
        assertFalse(contexts.clusterState().holdsRecordingLog(), "the member must stay blank");
    }
}
