package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * The cluster-leader-kill case on three member processes: SIGKILL the
 * leader. The two others keep a quorum, so records commit again within the
 * progress bound. The killed member restarts on its own directories and
 * joins, and every member holds the same canonical order.
 */
@ExtendWith(InterruptingTestCallback.class)
class ClusterLeaderKillTest {

    @TempDir(cleanup = CleanupMode.ON_SUCCESS)
    Path dir;

    @Test
    @InterruptAfter(value = 60, unit = TimeUnit.SECONDS)
    void a_killed_leader_leaves_a_quorum_that_commits_and_the_member_rejoins() {
        try (LocalSealers group = new LocalSealers(dir, 3, LocalSealers.Settings.DEFAULT);
             CanonicalClient client = new CanonicalClient(dir.resolve("client"), group)) {
            group.startAll(List.of("-Dkardamom.cluster.bootstrap=true"));
            final SealerNode.View leader = group.awaitStarted(Duration.ofSeconds(20));
            IntStream.range(0, 5).forEach(n -> client.transfer(n, Duration.ofSeconds(10)));
            // Each transfer is one log entry. The leader recorded it, the
            // quorum committed it, and the service thread applied it: the
            // member has the two waits of each one.
            final SealerNode first = group.node(leader.memberId());
            assertTrue(first.steps("sealer.commit_wait") >= 5, "commit waits: " + first.steps("sealer.commit_wait"));
            assertTrue(first.steps("sealer.apply_wait") >= 5, "apply waits: " + first.steps("sealer.apply_wait"));

            final SealerNode victim = group.node(leader.memberId());
            victim.kill();
            assertFalse(victim.isAlive());
            IntStream.range(5, 10).forEach(n -> client.transfer(n, Duration.ofSeconds(30)));

            victim.start(List.of());
            group.awaitJoined(List.of(0, 1, 2), Duration.ofSeconds(20));
            group.assertOneLeaderPerTerm();
            MemberOrder.assertSame(group.nodes(), 0, io.kardamom.sealer.CanonicalSealerState.GENESIS_BLOCK_NUMBER,
                IntStream.range(0, 10).boxed().collect(Collectors.toSet()));
        }
    }
}
