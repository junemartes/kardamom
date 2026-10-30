package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.kardamom.sealer.CanonicalSealerState;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * The cluster-follower-kill case on three member processes. After the
 * cluster takes a snapshot, SIGKILL a follower. The quorum keeps
 * committing, and the restarted member restores the snapshot from its
 * intact directories, joins, and holds the same canonical order.
 */
@ExtendWith(InterruptingTestCallback.class)
class ClusterFollowerKillTest {

    private static final LocalSealers.Settings SNAPSHOTS = LocalSealers.Settings.SNAPSHOTS;

    @TempDir(cleanup = CleanupMode.ON_SUCCESS)
    Path dir;

    @Test
    @InterruptAfter(value = 60, unit = TimeUnit.SECONDS)
    void a_killed_follower_restores_its_snapshot_while_the_quorum_commits() {
        try (LocalSealers group = new LocalSealers(dir, 3, SNAPSHOTS);
             CanonicalClient client = new CanonicalClient(dir.resolve("client"), group)) {
            group.startAll(List.of("-Dkardamom.cluster.bootstrap=true"));
            final SealerNode.View leader = group.awaitLeader(Duration.ofSeconds(20));
            IntStream.range(0, 5).forEach(n -> client.transfer(n, Duration.ofSeconds(10)));
            Await.holds(Duration.ofSeconds(20), "a recorded cluster snapshot: the second one proves the first",
                () -> group.count(leader.memberId(), 0, "snapshot TAKEN") > 1);

            final SealerNode follower = group.node((leader.memberId() + 1) % 3);
            final String restored = "sealer snapshot RESTORED memberId=" + follower.id;
            follower.kill();
            final long killed = System.nanoTime();
            IntStream.range(5, 10).forEach(n -> client.transfer(n, Duration.ofSeconds(60)));
            System.out.printf("follower-kill: quorum committed 5 records %d ms after the kill%n",
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - killed));
            assertEquals(leader.memberId(), group.awaitLeader(Duration.ofSeconds(10)).memberId(),
                "the quorum changed its leader after a follower kill");

            final int start = follower.starts();
            follower.start(List.of());
            group.awaitJoined(List.of(0, 1, 2), Duration.ofSeconds(30));
            Await.holds(Duration.ofSeconds(10), "the restarted member logs '" + restored + "'",
                () -> group.count(follower.id, start, restored) > 0);
            group.assertOneLeaderPerTerm();
            MemberOrder.assertSame(group.nodes(), 0, CanonicalSealerState.GENESIS_BLOCK_NUMBER,
                IntStream.range(0, 10).boxed().collect(Collectors.toSet()));
        }
    }
}
