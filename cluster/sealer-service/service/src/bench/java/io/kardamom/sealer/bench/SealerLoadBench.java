package io.kardamom.sealer.bench;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.driver.MediaDriver;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * The sealer load bench. It starts real sealer members on this host, then
 * runs racing twins that offer the same transaction references, and one
 * consumer that reads the canonical stream. It prints the throughput, the
 * latency from the due time of a batch to the relayed frame, the reject
 * frames, and the GC pauses of each member.
 *
 * <p>Run it with {@code just sealer-bench}. The {@code bench.*} system
 * properties are in {@link BenchConfig}.</p>
 */
public final class SealerLoadBench {

    private static final long DRAIN_QUIET_MS = 2_000L;
    private static final long START_DELAY_MS = 200L;

    private final BenchConfig config;

    private SealerLoadBench(final BenchConfig config) {
        this.config = config;
    }

    public static void main(final String[] args) {
        new SealerLoadBench(BenchConfig.fromProperties()).run();
    }

    private void run() {
        resetWorkDir();
        try (LocalCluster cluster = new LocalCluster(config);
             MediaDriver driver = MediaDriver.launchEmbedded(new MediaDriver.Context()
                     .aeronDirectoryName(config.workDir().resolve("client-aeron").toString())
                     .dirDeleteOnStart(true)
                     .dirDeleteOnShutdown(true))) {
            cluster.start();
            measure(cluster, driver.aeronDirectoryName());
        }
    }

    private void measure(final LocalCluster cluster, final String aeronDir) {
        final RunClock clock = new RunClock(config.batchIntervalNs(), config.warmupSeconds() * 1_000_000_000L);
        final RecordStream stream = new RecordStream(config.senders(), System.nanoTime(), config.v4Ids());
        final AtomicBoolean stop = new AtomicBoolean();
        final Consumer consumer = new Consumer(config, clock).connect(clientContext(cluster, aeronDir));
        final List<Twin> twins = IntStream.range(0, config.twins())
                .mapToObj(t -> new Twin(config, stream, clock, stop).connect(clientContext(cluster, aeronDir)))
                .toList();
        final Thread consumerThread = startThread("consumer", consumer);
        final List<Thread> twinThreads = twins.stream().map(t -> startThread("twin", t)).toList();
        clock.start(START_DELAY_MS);
        final Window window = observe(consumer, clock);
        stop.set(true);
        twinThreads.forEach(SealerLoadBench::join);
        drain(consumer);
        consumer.stop();
        join(consumerThread);
        new Report(config, twins, consumer, window, cluster.gcSummaries()).print();
        twins.forEach(Twin::close);
        consumer.close();
    }

    /** The relayed counts at the end of the warm-up and at the end of the run. */
    record Window(long relayedAtWarmupEnd, long relayedAtEnd) {
    }

    private Window observe(final Consumer consumer, final RunClock clock) {
        sleepUntil(clock.startNs() + config.warmupSeconds() * 1_000_000_000L);
        final long atWarmupEnd = consumer.relayed();
        sleepUntil(clock.startNs() + (config.warmupSeconds() + config.seconds()) * 1_000_000_000L);
        return new Window(atWarmupEnd, consumer.relayed());
    }

    /** Wait until the relayed count stays the same for {@link #DRAIN_QUIET_MS}. */
    private static void drain(final Consumer consumer) {
        long last = -1;
        for (long now = consumer.relayed(); now != last; now = settle(consumer)) {
            last = now;
        }
    }

    private static long settle(final Consumer consumer) {
        LocalCluster.sleepQuietly(DRAIN_QUIET_MS);
        return consumer.relayed();
    }

    private AeronCluster.Context clientContext(final LocalCluster cluster, final String aeronDir) {
        return new AeronCluster.Context()
                .aeronDirectoryName(aeronDir)
                .ingressChannel("aeron:udp")
                .ingressEndpoints(cluster.ingressEndpoints())
                .egressChannel("aeron:udp?endpoint=127.0.0.1:0");
    }

    private void resetWorkDir() {
        final Path dir = config.workDir();
        try {
            if (Files.exists(dir)) {
                try (Stream<Path> paths = Files.walk(dir)) {
                    paths.sorted(Comparator.reverseOrder()).forEach(SealerLoadBench::delete);
                }
            }
            Files.createDirectories(dir);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void delete(final Path path) {
        try {
            Files.delete(path);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void sleepUntil(final long nanoTime) {
        final long ms = (nanoTime - System.nanoTime()) / 1_000_000L;
        LocalCluster.sleepQuietly(Math.max(0L, ms));
    }

    private static Thread startThread(final String name, final Runnable task) {
        final Thread thread = new Thread(task, name);
        thread.start();
        return thread;
    }

    private static void join(final Thread thread) {
        try {
            thread.join();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
