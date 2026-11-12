package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
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
    private static final int STREAM_ID = 1_957;

    @TempDir
    Path dir;

    @Test
    @InterruptAfter(value = 30, unit = TimeUnit.SECONDS)
    void aBlankMemberWithoutPeersKeepsWaitingAndPreservesOtherArchives() throws IOException, InterruptedException {
        final Path executor = Files.createDirectories(dir.resolve("archive/dir"));
        Files.writeString(executor.resolve("archive.catalog"), "live executor catalog");
        Files.writeString(executor.resolve("0-0.rec"), "live executor recording");
        final Path stale = dir.resolve("archive/99999-0.rec");
        Files.writeString(stale, "stale Raft recording");
        final MemberContexts contexts = contexts();
        final PeerSeed seed = new PeerSeed(
                MEMBER_ID, ClusterNode.peerConsensusEndpoints(MEMBERS, MEMBER_ID), contexts, FAST,
                Optional.empty());
        final Thread seeding = new Thread(seed::run, "peer-seed-test");
        seeding.start();

        seeding.join(WATCH_MS);
        final boolean stillSeeding = seeding.isAlive();
        seeding.interrupt();
        seeding.join();

        assertTrue(stillSeeding, "the seed must not return without a peer");
        assertFalse(contexts.clusterState().holdsRecordingLog(), "the member must stay blank");
        assertFalse(Files.exists(stale), "the seed clears the old Raft recording");
        assertEquals("live executor catalog", Files.readString(executor.resolve("archive.catalog")));
        assertEquals("live executor recording", Files.readString(executor.resolve("0-0.rec")));
    }

    private MemberContexts contexts() {
        return new MemberContexts(
                dir.resolve("aeron").toString(),
                dir.resolve("cluster").toString(),
                dir.resolve("archive").toString(),
                ClusterNode.memberEndpoints(MEMBERS, MEMBER_ID));
    }

    @Test
    void anUnreadSnapshotCountsAsAFailedRoundUpToTheLimit() {
        final RetainedCopy copy = new RetainedCopy(List.of(), new SegmentDir(dir.resolve("retained")),
                RetainedToken.of("t").orElseThrow(), MEMBER_ID);
        final PeerSeed seed = new PeerSeed(MEMBER_ID, ClusterNode.peerConsensusEndpoints(MEMBERS, MEMBER_ID),
                contexts(), FAST, Optional.of(copy));
        assertTrue(seed.copiedRetained(new PeerSeed.NoSnapshot()), "a cluster with no snapshot needs no copy");
        IntStream.range(1, PeerSeed.MAX_COPY_FAILURES)
                .forEach(i -> assertFalse(seed.copiedRetained(new PeerSeed.Unread("the replay did not connect"))));
        assertThrows(IllegalStateException.class,
                () -> seed.copiedRetained(new PeerSeed.Unread("the replay did not connect")),
                "the last failed round stops the start");
    }

    @Test
    @InterruptAfter(value = 30, unit = TimeUnit.SECONDS)
    void theWaitForAReplayImageEndsAtItsDeadline() {
        try (MediaDriver driver = MediaDriver.launchEmbedded(new MediaDriver.Context()
                    .threadingMode(ThreadingMode.SHARED).dirDeleteOnStart(true).dirDeleteOnShutdown(true));
             Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
             ExclusivePublication publication = aeron.addExclusivePublication("aeron:ipc", STREAM_ID);
             Subscription absent = aeron.addSubscription(
                     ChannelUri.addSessionId("aeron:ipc", publication.sessionId() + 1), STREAM_ID);
             Subscription present = aeron.addSubscription(
                     ChannelUri.addSessionId("aeron:ipc", publication.sessionId()), STREAM_ID)) {
            final long timeoutNs = TimeUnit.MILLISECONDS.toNanos(200);
            final long start = System.nanoTime();
            assertTrue(new PeerSeed.ImageWait(absent, publication.sessionId() + 1, timeoutNs).await().isEmpty(),
                    "no publication has that session");
            assertTrue(System.nanoTime() - start >= timeoutNs, "the wait lasts until its deadline");
            assertTrue(new PeerSeed.ImageWait(present, publication.sessionId(), TimeUnit.SECONDS.toNanos(10))
                    .await().isPresent());
        }
    }
}
