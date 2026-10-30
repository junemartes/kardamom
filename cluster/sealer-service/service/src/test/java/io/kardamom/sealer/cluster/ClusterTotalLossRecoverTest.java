package io.kardamom.sealer.cluster;

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
 * The cluster-total-loss-recover case on three member processes. SIGKILL
 * every member: the cluster must stall. Every member then returns with its
 * own log and snapshots, the members elect a leader among themselves, and
 * records commit again. The members hold the same canonical order, with the
 * records from before the loss.
 */
@ExtendWith(InterruptingTestCallback.class)
class ClusterTotalLossRecoverTest {

    private static final Duration STALL = Duration.ofSeconds(15);
    private static final LocalSealers.Settings SNAPSHOTS = LocalSealers.Settings.SNAPSHOTS;

    @TempDir(cleanup = CleanupMode.ON_SUCCESS)
    Path dir;

    @Test
    @InterruptAfter(value = 60, unit = TimeUnit.SECONDS)
    void every_member_returns_from_its_own_state_and_the_cluster_commits_again() {
        try (LocalSealers group = new LocalSealers(dir, 3, SNAPSHOTS);
             CanonicalClient client = new CanonicalClient(dir.resolve("client"), group::ingressEndpoints)) {
            group.startAll(List.of("-Dkardamom.cluster.bootstrap=true"));
            group.awaitLeader(Duration.ofSeconds(20));
            IntStream.range(0, 3).forEach(n -> client.transfer(n, Duration.ofSeconds(10)));
            // The second snapshot proves that the consensus module recorded the first one.
            Await.holds(Duration.ofSeconds(10), "two snapshots on every member", () -> group.nodes().stream()
                .allMatch(n -> group.count(n.id, 0, "snapshot TAKEN") > 1));
            Stage.log("total-loss: leader");

            final List<Integer> starts = group.nodes().stream().map(SealerNode::starts).toList();
            group.nodes().forEach(SealerNode::kill);
            group.assertStalled(client, 3, STALL);
            Stage.log("total-loss: stalled");

            group.startAll(List.of());
            group.awaitLeader(Duration.ofSeconds(30));
            Stage.log("total-loss: elected");
            group.nodes().forEach(n -> Await.holds(Duration.ofSeconds(5), "member " + n.id + " restores its snapshot",
                () -> group.count(n.id, starts.get(n.id), "sealer snapshot RESTORED memberId=" + n.id) > 0));
            client.transfer(3, Duration.ofSeconds(30));
            group.awaitJoined(List.of(0, 1, 2), Duration.ofSeconds(10));
            Stage.log("total-loss: committed");
            group.assertOneLeaderPerTerm();
            MemberOrder.assertSame(group.nodes(), 0, CanonicalSealerState.GENESIS_BLOCK_NUMBER,
                IntStream.range(0, 4).boxed().collect(Collectors.toSet()));
        }
    }
}
