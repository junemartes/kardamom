package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.kardamom.sealer.CanonicalSealerState;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * The cpu-squeeze case on three member processes. Every member is starved
 * at once: in each 100 ms it runs 15 ms, and its threads stop for the
 * rest, while the client keeps offering records. Starvation may slow the
 * cluster, and sessions may lapse, but it must never fork the canonical
 * order. After the release, records commit within the recovery bound, each
 * term has one leader, and every member holds the same canonical order with
 * every record that any session saw relayed.
 */
@ExtendWith(InterruptingTestCallback.class)
class CpuSqueezeTest {

    private static final long SQUEEZE_MS = 20_000;
    private static final Duration RECOVER = Duration.ofSeconds(30);

    @TempDir(cleanup = CleanupMode.ON_SUCCESS)
    Path dir;

    @Test
    @InterruptAfter(value = 60, unit = TimeUnit.SECONDS)
    void starved_members_slow_down_but_never_fork_the_canonical_order() {
        try (LocalSealers group = new LocalSealers(dir, 3, LocalSealers.Settings.DEFAULT);
             CanonicalClient client = new CanonicalClient(dir.resolve("client"), group::ingressEndpoints)) {
            group.startAll(List.of("-Dkardamom.cluster.bootstrap=true"));
            group.awaitLeader(Duration.ofSeconds(20));
            final Set<Integer> committed = new HashSet<>();
            IntStream.range(0, 3).forEach(n -> {
                client.transfer(n, Duration.ofSeconds(10));
                committed.add(n);
            });

            final long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(SQUEEZE_MS);
            final int last;
            try (Squeeze squeeze = new Squeeze(group.nodes(), 15, 85)) {
                last = IntStream.iterate(3, n -> n + 1).takeWhile(n -> System.nanoTime() - end < 0)
                    .peek(n -> {
                        if (client.relayedWithin(n, Duration.ofSeconds(2))) {
                            committed.add(n);
                        }
                    })
                    .reduce(3, (a, b) -> b);
                System.out.printf("cpu-squeeze: %d periods, %d of %d records relayed during the squeeze%n",
                    squeeze.periods(), committed.size() - 3, last - 2);
                assertTrue(squeeze.periods() > 100, "the squeeze did not run");
            }
            final Duration recovered = client.transfer(last + 1, RECOVER);
            committed.add(last + 1);
            System.out.printf("cpu-squeeze: committed %d ms after the release%n", recovered.toMillis());
            group.awaitJoined(List.of(0, 1, 2), Duration.ofSeconds(10));
            group.assertOneLeaderPerTerm();
            MemberOrder.assertSame(group.nodes(), 0, CanonicalSealerState.GENESIS_BLOCK_NUMBER, committed);
        }
    }
}
