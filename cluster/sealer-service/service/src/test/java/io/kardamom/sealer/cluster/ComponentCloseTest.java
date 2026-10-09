package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.Tests;
import java.io.File;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.CloseHelper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * A consensus module that throws in its start closes, and the process runs
 * on. Aeron records the error and calls no termination hook. The
 * {@link JoinWatchdogThread.Member} sees the closed component, and the stop
 * request stays clear, so the watchdog ends the process.
 *
 * <p>The module here fails because its archive client finds no archive on
 * its control stream. A member that times out while it loads its snapshot
 * from a slow archive takes the same path: the archive client throws in
 * {@code ConsensusModuleAgent.onStart}.</p>
 */
@ExtendWith(InterruptingTestCallback.class)
final class ComponentCloseTest {

    private static final String MEMBERS =
            "0,localhost:47200,localhost:47201,localhost:47202,localhost:47203,localhost:47204";
    private static final int MEMBER_ID = 0;
    /** A local archive control stream that no archive serves. */
    private static final int NO_ARCHIVE_STREAM_ID = 4_242;
    private static final long ARCHIVE_TIMEOUT_NS = TimeUnit.MILLISECONDS.toNanos(500);

    @TempDir
    Path dir;

    @Test
    @InterruptAfter(value = 30, unit = TimeUnit.SECONDS)
    void aModuleThatFailsInItsStartClosesWithNoStopRequest() {
        final MemberContexts contexts = new MemberContexts(
                dir.resolve("aeron").toString(),
                dir.resolve("cluster").toString(),
                dir.resolve("archive").toString(),
                ClusterNode.memberEndpoints(MEMBERS, MEMBER_ID));
        final StopRequest stop = new StopRequest();
        final AtomicBoolean hookRan = new AtomicBoolean();
        final ConsensusModule.Context consensus = new ConsensusModule.Context()
                .clusterMemberId(MEMBER_ID)
                .clusterMembers(MEMBERS)
                .clusterDir(new File(dir.resolve("cluster").toString()))
                .ingressChannel("aeron:udp")
                .logChannel("aeron:udp?term-length=64k")
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .archiveContext(new AeronArchive.Context()
                        .controlRequestChannel("aeron:ipc")
                        .controlRequestStreamId(NO_ARCHIVE_STREAM_ID)
                        .controlResponseChannel("aeron:ipc")
                        .messageTimeoutNs(ARCHIVE_TIMEOUT_NS))
                .terminationHook(() -> hookRan.set(true));
        ClusteredMediaDriver driver = null;
        ClusteredServiceContainer container = null;
        try {
            driver = ClusteredMediaDriver.launch(contexts.driver(), contexts.archive(), consensus);
            container = ClusteredServiceContainer.launch(new ClusteredServiceContainer.Context()
                    .aeronDirectoryName(contexts.driver().aeronDirectoryName())
                    .clusterDir(new File(dir.resolve("cluster").toString()))
                    .clusteredService(new SealerClusteredService()));
            final JoinWatchdogThread.Member member =
                    new JoinWatchdogThread.Member(MEMBER_ID, consensus, container.context(), stop);
            while (member.closedComponent().isEmpty()) {
                Tests.sleep(50);
            }

            assertEquals(Optional.of("CONSENSUS_MODULE"), member.closedComponent());
            assertFalse(hookRan.get(), "Aeron calls no termination hook for a module that fails in its start");
            assertFalse(stop.isRequested(), "a failed start is not a stop request");
        } finally {
            CloseHelper.quietCloseAll(container, driver);
        }
    }
}
