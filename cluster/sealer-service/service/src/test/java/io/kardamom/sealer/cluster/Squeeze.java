package io.kardamom.sealer.cluster;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A CPU squeeze on member processes, as a CFS quota throttles a container:
 * in each period every process runs for {@code runMs}, then all its threads
 * stop until the period ends. Closing the squeeze lets the processes run
 * again; it sends the last {@code CONT} on every exit path.
 */
final class Squeeze implements AutoCloseable {

    private final List<Long> pids;
    private final long runMs;
    private final long stopMs;
    private final Thread thread;
    private volatile boolean running = true;
    private long periods;

    Squeeze(final List<SealerNode> nodes, final long runMs, final long stopMs) {
        this.pids = nodes.stream().map(SealerNode::pid).toList();
        this.runMs = runMs;
        this.stopMs = stopMs;
        this.thread = new Thread(this::cycle, "kardamom-test-squeeze");
        thread.setDaemon(true);
        thread.start();
    }

    /** The count of whole run-and-stop periods so far. */
    long periods() {
        return periods;
    }

    private void cycle() {
        while (running) {
            period();
        }
    }

    /** One period: every process stops, then runs. */
    private void period() {
        signal("STOP");
        Await.pause(stopMs);
        signal("CONT");
        periods++;
        Await.pause(runMs);
    }

    private void signal(final String name) {
        final List<String> cmd = new ArrayList<>(List.of("kill", "-" + name));
        pids.forEach(pid -> cmd.add(Long.toString(pid)));
        try {
            new ProcessBuilder(cmd).start().waitFor();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        running = false;
        try {
            thread.join(5_000);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        signal("CONT");
    }
}
