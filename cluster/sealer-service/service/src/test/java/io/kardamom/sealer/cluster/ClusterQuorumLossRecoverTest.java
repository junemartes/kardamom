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
 * The cluster-quorum-loss-recover case on three member processes. SIGKILL
 * members 1 and 2: one member is left, so the cluster must stall with no
 * false progress. Member 1 returns, the quorum is back, and records commit
 * again with no gap. Member 2 returns last, and the three members hold the
 * same canonical order.
 */
@ExtendWith(InterruptingTestCallback.class)
class ClusterQuorumLossRecoverTest {

    /** The stall window of the chaos case. */
    private static final Duration STALL = Duration.ofSeconds(15);

    @TempDir(cleanup = CleanupMode.ON_SUCCESS)
    Path dir;

    @Test
    @InterruptAfter(value = 60, unit = TimeUnit.SECONDS)
    void a_cluster_without_a_quorum_stalls_and_commits_again_when_a_member_returns() {
        try (LocalSealers group = new LocalSealers(dir, 3, LocalSealers.Settings.DEFAULT);
             CanonicalClient client = new CanonicalClient(dir.resolve("client"), group::ingressEndpoints)) {
            group.startAll(List.of("-Dkardamom.cluster.bootstrap=true"));
            group.awaitLeader(Duration.ofSeconds(20));
            IntStream.range(0, 3).forEach(n -> client.transfer(n, Duration.ofSeconds(10)));
            Stage.log("quorum-loss: leader");

            group.node(1).kill();
            group.node(2).kill();
            group.assertStalled(client, 3, STALL);
            Stage.log("quorum-loss: stalled");

            group.node(1).start(List.of());
            client.transfer(3, Duration.ofSeconds(30));
            Stage.log("quorum-loss: quorum back");
            group.node(2).start(List.of());
            group.awaitJoined(List.of(0, 1, 2), Duration.ofSeconds(20));
            client.transfer(4, Duration.ofSeconds(10));
            Stage.log("quorum-loss: all back");
            group.assertOneLeaderPerTerm();
            MemberOrder.assertSame(group.nodes(), 0, CanonicalSealerState.GENESIS_BLOCK_NUMBER,
                IntStream.range(0, 5).boxed().collect(Collectors.toSet()));
        }
    }
}
