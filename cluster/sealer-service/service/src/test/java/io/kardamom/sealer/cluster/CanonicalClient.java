package io.kardamom.sealer.cluster;

import io.aeron.Publication;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;
import org.agrona.CloseHelper;
import org.agrona.ExpandableArrayBuffer;

/**
 * The sequencer side of the member-process tests: a real Aeron cluster
 * client on its own media driver. It offers ingress records in the
 * production wire format and reads the relayed frames from the egress. A
 * transfer retries through leader changes and reconnects, as the
 * production client does, within one bound; the sealer drops a repeated
 * canonical id, so a retry never adds a second record.
 */
final class CanonicalClient implements AutoCloseable {

    /** The wait for the relay of one offer before the client offers it again. */
    private static final long REOFFER_NS = Duration.ofSeconds(3).toNanos();

    private final MediaDriver driver;
    private final Supplier<String> endpoints;
    private final RecordingEgressListener egress = new RecordingEgressListener();
    private final Set<Integer> relayed = new HashSet<>();
    private AeronCluster client;
    private int reconnects;
    /** The relayed frames of the egress that {@link #relayed} already holds. */
    private int scanned;

    CanonicalClient(final Path aeronDir, final Supplier<String> endpoints) {
        this.endpoints = endpoints;
        this.driver = MediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(aeronDir.toString())
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true));
    }

    RecordingEgressListener egress() {
        return egress;
    }

    int reconnects() {
        return reconnects;
    }

    /**
     * Offer record {@code n} and wait until the egress relays it. Return the
     * time it took. Fail when {@code bound} passes first.
     */
    Duration transfer(final int n, final Duration bound) {
        return transfer(n, 0, bound);
    }

    /** {@link #transfer(int, Duration)} with {@code fillBytes} more bytes of payload after the id. */
    Duration transfer(final int n, final int fillBytes, final Duration bound) {
        final long start = System.nanoTime();
        final long deadline = start + bound.toNanos();
        while (!relayed.contains(n)) {
            if (System.nanoTime() - deadline > 0) {
                throw new AssertionError("record " + n + " was not relayed within " + bound.toMillis() + " ms");
            }
            attempt(n, fillBytes, Math.min(deadline, System.nanoTime() + REOFFER_NS));
        }
        return Duration.ofNanos(System.nanoTime() - start);
    }

    /** Offer record {@code n} for {@code window}, and return whether the egress relayed it in that time. */
    boolean relayedWithin(final int n, final Duration window) {
        final long deadline = System.nanoTime() + window.toNanos();
        while (!relayed.contains(n) && System.nanoTime() - deadline < 0) {
            attempt(n, 0, Math.min(deadline, System.nanoTime() + REOFFER_NS));
        }
        return relayed.contains(n);
    }

    /** Send a raw ingress frame on the current session, for a replay request. */
    void offer(final byte[] frame, final Duration bound) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer(frame.length);
        buf.putBytes(0, frame);
        final long deadline = System.nanoTime() + bound.toNanos();
        while (!offered(buf, frame.length)) {
            if (System.nanoTime() - deadline > 0) {
                throw new AssertionError("the ingress did not take a frame within " + bound.toMillis() + " ms");
            }
            Await.pause(5);
        }
    }

    /** Report the posted head {@code block}, as the batcher does after an L1 post. */
    void post(final long block) {
        offer(IngressFrames.postedCursorFrame(block), Duration.ofSeconds(10));
    }

    /** The highest boundary block of the egress so far. */
    long head() {
        poll();
        return egress.maxBoundaryBlockNumber;
    }

    /** Poll the egress until {@code done} holds, within {@code bound}. */
    void pollUntil(final Duration bound, final String what, final java.util.function.BooleanSupplier done) {
        Await.holds(bound, what, () -> {
            poll();
            return done.getAsBoolean();
        });
    }

    private void attempt(final int n, final int fillBytes, final long until) {
        final byte[] frame = IngressFrames.exemptRecordFrame(IngressFrames.canonicalId(n),
            java.util.Arrays.copyOf(IngressFrames.canonicalId(n), 32 + fillBytes));
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer(frame.length);
        buf.putBytes(0, frame);
        if (!offered(buf, frame.length)) {
            Await.pause(5);
            return;
        }
        while (!relayed.contains(n) && System.nanoTime() - until < 0 && session().isPresent()) {
            poll();
            Await.pause(1);
        }
    }

    private boolean offered(final ExpandableArrayBuffer buf, final int length) {
        return session().map(c -> {
            final long result = c.offer(buf, 0, length);
            if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
                drop();
            }
            c.pollEgress();
            return result > 0;
        }).orElse(false);
    }

    private void poll() {
        session().ifPresent(AeronCluster::pollEgress);
        egress.relayedPayloads.subList(scanned, egress.relayedPayloads.size()).stream()
            .filter(p -> p.length >= 32 && p[31] == (byte) 0x5A)
            .forEach(p -> relayed.add((p[0] & 0xFF) | (p[1] & 0xFF) << 8 | (p[2] & 0xFF) << 16 | (p[3] & 0xFF) << 24));
        scanned = egress.relayedPayloads.size();
    }

    /** The open session, connecting a new one when there is none; empty while no leader answers. */
    private java.util.Optional<AeronCluster> session() {
        if (client != null && client.isClosed()) {
            drop();
        }
        if (client == null) {
            try {
                client = AeronCluster.connect(new AeronCluster.Context()
                    .aeronDirectoryName(driver.aeronDirectoryName())
                    .ingressChannel("aeron:udp")
                    .ingressEndpoints(endpoints.get())
                    .egressChannel("aeron:udp?endpoint=127.0.0.1:0")
                    .messageTimeoutNs(Duration.ofSeconds(3).toNanos())
                    .egressListener(egress));
                reconnects++;
                resume();
            } catch (final RuntimeException e) {
                return java.util.Optional.empty();
            }
        }
        return java.util.Optional.of(client);
    }

    /**
     * Ask a new session for the stream after the last boundary that an
     * earlier session saw, as a consumer resumes from its cursor. A record
     * that committed while the old session was gone reaches this session in
     * the replay; the sealer drops its re-offer as a duplicate.
     */
    private void resume() {
        if (reconnects > 1) {
            final long[] cursor = egress.boundaries.isEmpty()
                ? new long[] {io.kardamom.sealer.CanonicalSealerState.GENESIS_BLOCK_NUMBER - 1, 0}
                : egress.boundaries.get(egress.boundaries.size() - 1);
            final byte[] frame = IngressFrames.replayRequestFrame(cursor[1], cursor[0] + 1);
            final ExpandableArrayBuffer buf = new ExpandableArrayBuffer(frame.length);
            buf.putBytes(0, frame);
            final long deadline = System.nanoTime() + REOFFER_NS;
            while (client.offer(buf, 0, frame.length) < 0 && System.nanoTime() - deadline < 0) {
                client.pollEgress();
                Await.pause(1);
            }
        }
    }

    private void drop() {
        CloseHelper.quietClose(client);
        client = null;
    }

    @Override
    public void close() {
        CloseHelper.quietClose(client);
        CloseHelper.quietClose(driver);
    }
}
