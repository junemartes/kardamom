package io.kardamom.sealer.cluster;

import io.aeron.cluster.service.ClusterClock;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * A cluster clock for a member process that a test moves forward: the wall
 * clock plus an offset in milliseconds, which the test writes to the file of
 * {@code kardamom.test.clockOffsetFile}. The offset only grows, so the clock
 * never goes back. Moving every member forward by a timeout lets that
 * timeout pass at once, as the same wall time would.
 */
public final class OffsetClusterClock implements ClusterClock {

    private static final long REFRESH_MS = 5;
    private final Path file = Path.of(System.getProperty("kardamom.test.clockOffsetFile"));
    private volatile long offsetMs;

    public OffsetClusterClock() {
        offsetMs = read();
        final Thread thread = new Thread(this::refresh, "kardamom-test-clock");
        thread.setDaemon(true);
        thread.start();
    }

    @Override
    public TimeUnit timeUnit() {
        return TimeUnit.MILLISECONDS;
    }

    @Override
    public long time() {
        return System.currentTimeMillis() + offsetMs;
    }

    private void refresh() {
        while (!Thread.currentThread().isInterrupted()) {
            offsetMs = Math.max(offsetMs, read());
            Await.pause(REFRESH_MS);
        }
    }

    private long read() {
        try {
            return Files.exists(file) ? Long.parseLong(Files.readString(file).trim()) : 0;
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
