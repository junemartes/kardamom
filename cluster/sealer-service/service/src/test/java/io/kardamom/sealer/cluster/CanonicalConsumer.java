package io.kardamom.sealer.cluster;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import io.kardamom.sealer.CanonicalSealerState;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;

/**
 * A consumer of the canonical stream as the validator and the executors
 * read it: one cluster session, a cursor of the next index and the next
 * block, and a replay from that cursor at each new session. It applies
 * frames only in order. Before the answer to its replay request it drops
 * any frame that is not next, since the replay sends it again. After that
 * answer a frame past the cursor is a key gap, and the consumer asks for a
 * replay from its cursor again, as the production reader does; a frame
 * below the cursor is a duplicate that the cursor drops. The applied frames
 * are the input that the validator re-executes and checks. The consumer polls on its own
 * thread, as a validator does; the test reads its frames after
 * {@link #stop}.
 */
final class CanonicalConsumer implements EgressListener, AutoCloseable {

    private static final long CONNECT_TIMEOUT_NS = Duration.ofSeconds(3).toNanos();

    private final MediaDriver driver;
    private final Supplier<String> endpoints;
    private final List<byte[]> applied = new ArrayList<>();
    private final List<String> gaps = new java.util.concurrent.CopyOnWriteArrayList<>();
    private AeronCluster session;
    private volatile long nextIndex;
    private volatile long nextBlock = CanonicalSealerState.GENESIS_BLOCK_NUMBER;
    private volatile boolean running = true;
    private final Thread thread = new Thread(this::run, "kardamom-test-consumer");
    /** True from a new session until the answer to its replay request. */
    private boolean catchingUp;
    private volatile int sessions;

    CanonicalConsumer(final Path aeronDir, final Supplier<String> endpoints) {
        this.endpoints = endpoints;
        this.driver = MediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(aeronDir.toString())
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true));
        thread.setDaemon(true);
        thread.start();
    }

    private void run() {
        while (running) {
            pollOnce();
        }
    }

    private void pollOnce() {
        poll();
        Await.pause(1);
    }

    /** Stop the polling thread; the frames are then stable for the test thread. */
    void stop() {
        running = false;
        try {
            thread.join(10_000);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Poll the session, opening a new one with a replay from the cursor when the last one closed. */
    private void poll() {
        if (session != null && session.isClosed()) {
            CloseHelper.quietClose(session);
            session = null;
        }
        if (session == null) {
            open();
        }
        if (session != null) {
            session.pollEgress();
        }
    }

    /** Poll until the cursor reaches block {@code block}, within {@code bound}. */
    void awaitBlock(final long block, final Duration bound) {
        try {
            Await.holds(bound, "the consumer reaches block " + block, () -> nextBlock > block);
        } catch (final AssertionError e) {
            throw new AssertionError(e.getMessage() + "; next=(" + nextIndex + "," + nextBlock + ") sessions="
                + sessions + " gaps=" + gaps, e);
        }
    }

    /** The canonical frames that the consumer applied, in order. */
    List<byte[]> applied() {
        return List.copyOf(applied);
    }

    /** The key gaps that made the consumer ask for a replay again: a frame past the cursor after a replay answer. */
    List<String> gaps() {
        return List.copyOf(gaps);
    }

    int sessions() {
        return sessions;
    }

    private void open() {
        try {
            session = AeronCluster.connect(new AeronCluster.Context()
                .aeronDirectoryName(driver.aeronDirectoryName())
                .ingressChannel("aeron:udp")
                .ingressEndpoints(endpoints.get())
                .egressChannel("aeron:udp?endpoint=127.0.0.1:0")
                .messageTimeoutNs(CONNECT_TIMEOUT_NS)
                .egressListener(this));
        } catch (final RuntimeException e) {
            return;
        }
        sessions++;
        requestReplay();
    }

    /** Ask the session for the stream from the cursor; frames that are not next wait for the answer. */
    private void requestReplay() {
        catchingUp = true;
        final byte[] request = IngressFrames.replayRequestFrame(nextIndex, nextBlock);
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer(request.length);
        buf.putBytes(0, request);
        final long deadline = System.nanoTime() + CONNECT_TIMEOUT_NS;
        while (session.offer(buf, 0, request.length) < 0 && System.nanoTime() - deadline < 0) {
            session.pollEgress();
        }
    }

    @Override
    public void onMessage(
            final long clusterSessionId,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header) {
        final byte kind = buffer.getByte(offset);
        if (kind == SealerWire.EGRESS_KIND_REPLAY_DONE) {
            catchingUp = false;
        } else if (kind == SealerWire.EGRESS_KIND_RELAYED) {
            onFrame(buffer, offset, length, buffer.getLong(offset + 1, ByteOrder.LITTLE_ENDIAN), nextIndex);
        } else if (kind == SealerWire.EGRESS_KIND_BOUNDARY) {
            onBoundary(buffer, offset, length);
        }
    }

    private void onBoundary(final DirectBuffer buffer, final int offset, final int length) {
        final long block = buffer.getLong(offset + 1, ByteOrder.LITTLE_ENDIAN);
        final long endTx = buffer.getLong(offset + 1 + Long.BYTES, ByteOrder.LITTLE_ENDIAN);
        // A boundary of the next block is next only when every record of that block is applied.
        final long key = block >= nextBlock && endTx != nextIndex ? Long.MAX_VALUE : block;
        onFrame(buffer, offset, length, key, nextBlock);
    }

    /** Apply a frame whose key is the next key; drop an earlier one; note a later one as a gap after the replay. */
    private void onFrame(
            final DirectBuffer buffer, final int offset, final int length, final long key, final long next) {
        if (key == next) {
            final byte[] frame = new byte[length];
            buffer.getBytes(offset, frame);
            applied.add(frame);
            advance(frame[0]);
        } else if (key > next && !catchingUp) {
            gaps.add("kind " + buffer.getByte(offset) + " key " + key + " at next " + next);
            requestReplay();
        }
    }

    private void advance(final byte kind) {
        if (kind == SealerWire.EGRESS_KIND_RELAYED) {
            nextIndex++;
        } else {
            nextBlock++;
        }
    }

    @Override
    public void close() {
        stop();
        CloseHelper.quietClose(session);
        CloseHelper.quietClose(driver);
    }
}
