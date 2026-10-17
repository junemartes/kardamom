package io.kardamom.sealer.cluster;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import java.util.Optional;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.ControlledMessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;

/**
 * The feeder thread of the member feed. It owns its own Aeron client on
 * the media driver that the member shares with its executor, the feed
 * publication, and the control subscription. It reads the frame ring and
 * offers each frame. It passes each replay request to the service thread
 * through the control ring.
 *
 * <p>A back-pressured offer leaves the frame in the ring, and the feeder
 * tries again in its next duty cycle. Only this thread waits: the service
 * thread writes into the ring and never waits for it.</p>
 *
 * <p>The generation numbers each answer. The feeder starts a new one for
 * each request and for each loss of the subscriber, sends it with the
 * control message, and drops each frame of an older generation. A request
 * that comes while the feed has no subscriber still gets its answer: its
 * frames wait in the ring until the subscriber connects.</p>
 *
 * <p>The client is the only link to the shared driver. When the driver
 * goes, the client closes, and the feeder opens a new one every
 * {@link #RETRY_MS}. An error of the client never stops the member.</p>
 */
final class Feeder implements Agent {

    /** The time between two attempts to open the client. */
    static final long RETRY_MS = 1_000;
    /** The most frames that one duty cycle offers. */
    private static final int FRAME_BATCH = 64;
    /** The most control messages that one duty cycle takes. */
    private static final int REQUEST_BATCH = 8;

    /** The client, the feed publication and the control subscription. */
    private record Link(Aeron aeron, ExclusivePublication feed, Subscription control) implements AutoCloseable {
        @Override
        public void close() {
            CloseHelper.closeAll(control, feed, aeron);
        }
    }

    private final FeedConfig config;
    private final int memberId;
    /** Written by the service thread, read by this thread. */
    private final RingBuffer frames;
    /** Written by this thread, read by the service thread. */
    private final RingBuffer control;
    private final UnsafeBuffer message = new UnsafeBuffer(new byte[SealerWire.FEED_REQUEST_LEN]);
    private final ControlledMessageHandler offerHandler = this::offer;
    private final ControlledMessageHandler dropHandler = (type, buffer, index, length) ->
        ControlledMessageHandler.Action.CONTINUE;
    private final ControlledFragmentHandler requestHandler = this::onRequest;

    private Optional<Link> link = Optional.empty();
    private long nextAttemptMs;
    /** The generation of the current answer. The ring takes only a type above 0. */
    private int generation = 1;
    /** Whether the feed publication had a subscriber at the last check. */
    private boolean connected;
    /** Whether the last attempt to open the client failed; it logs the first failure only. */
    private boolean failing;

    Feeder(final FeedConfig config, final int memberId, final RingBuffer frames, final RingBuffer control) {
        this.config = config;
        this.memberId = memberId;
        this.frames = frames;
        this.control = control;
    }

    /** Start this agent on its own daemon thread. */
    AgentRunner start() {
        final AgentRunner runner = new AgentRunner(
            new BackoffIdleStrategy(100, 10, 1_000, 1_000_000),
            error -> System.out.println("sealer FEED ERROR memberId=" + memberId + " " + error),
            null,
            this);
        AgentRunner.startOnThread(runner, task -> {
            final Thread thread = new Thread(task, roleName());
            thread.setDaemon(true);
            return thread;
        });
        return runner;
    }

    @Override
    public String roleName() {
        return "sealer-feeder-" + memberId;
    }

    @Override
    public int doWork() {
        return link.map(this::work).orElseGet(this::reconnect);
    }

    @Override
    public void onClose() {
        link.ifPresent(Link::close);
    }

    /** One duty cycle with an open client. */
    private int work(final Link open) {
        if (open.aeron().isClosed()) {
            open.close();
            link = Optional.empty();
            log("CLOSED the client lost the driver");
            return 1;
        }
        final boolean now = open.feed().isConnected();
        if (now != connected) {
            return switchConnected(now);
        }
        return open.control().controlledPoll(requestHandler, REQUEST_BATCH)
            + frames.controlledRead(offerHandler, FRAME_BATCH);
    }

    /** One duty cycle without a client: end the answer, drop the frames, and open a client when it is time. */
    private int reconnect() {
        if (connected) {
            return switchConnected(false);
        }
        final int dropped = frames.controlledRead(dropHandler, FRAME_BATCH);
        final long nowMs = System.currentTimeMillis();
        if (nowMs < nextAttemptMs) {
            return dropped;
        }
        nextAttemptMs = nowMs + RETRY_MS;
        link = open();
        return dropped + 1;
    }

    private Optional<Link> open() {
        try {
            final Aeron aeron = Aeron.connect(new Aeron.Context()
                .aeronDirectoryName(config.aeronDir())
                .errorHandler(error -> System.out.println(
                    "sealer FEED CLIENT-ERROR memberId=" + memberId + " " + error)));
            final Link opened = links(aeron);
            failing = false;
            log("OPEN aeronDir=" + config.aeronDir());
            return Optional.of(opened);
        } catch (final RuntimeException e) {
            if (!failing) {
                log("NO-DRIVER aeronDir=" + config.aeronDir() + " " + e);
            }
            failing = true;
            return Optional.empty();
        }
    }

    /** Add the publication and the subscription. A failure closes the client. */
    private Link links(final Aeron aeron) {
        try {
            return new Link(aeron,
                aeron.addExclusivePublication(config.feedChannel(), SealerWire.FEED_STREAM_ID),
                aeron.addSubscription("aeron:ipc", SealerWire.FEED_CONTROL_STREAM_ID));
        } catch (final RuntimeException e) {
            CloseHelper.quietClose(aeron);
            throw e;
        }
    }

    /**
     * Record a change of the subscriber. A lost subscriber ends the answer:
     * the service turns idle, and this feeder drops the frames that it did
     * not send. The change waits while the control ring is full.
     */
    private int switchConnected(final boolean now) {
        if (!now && !sendIdle()) {
            return 0;
        }
        connected = now;
        log(now ? "CONNECTED" : "DISCONNECTED");
        return 1;
    }

    /** Pass one request to the service thread in a new generation. A full control ring holds the request back. */
    private ControlledFragmentHandler.Action onRequest(
            final DirectBuffer buffer, final int offset, final int length, final Header header) {
        final Optional<FeedRequest> request = FeedRequest.parse(buffer, offset, length);
        if (request.isEmpty()) {
            log("DROPPED a control message of " + length + " bytes, kind " + buffer.getByte(offset));
            return ControlledFragmentHandler.Action.CONTINUE;
        }
        return commit(request.get().write(message))
            ? ControlledFragmentHandler.Action.CONTINUE
            : ControlledFragmentHandler.Action.ABORT;
    }

    private boolean sendIdle() {
        message.putByte(0, FeedRequest.IDLE);
        return commit(Byte.BYTES);
    }

    /**
     * Write the first {@code length} bytes of {@link #message} into the
     * control ring in the next generation. On success the feeder takes that
     * generation.
     */
    private boolean commit(final int length) {
        final int next = Math.incrementExact(generation);
        if (!control.write(next, message, 0, length)) {
            return false;
        }
        generation = next;
        return true;
    }

    /** Offer one frame of the current generation; drop a frame of an older one. */
    private ControlledMessageHandler.Action offer(
            final int msgTypeId, final MutableDirectBuffer buffer, final int index, final int length) {
        if (msgTypeId != generation) {
            return ControlledMessageHandler.Action.CONTINUE;
        }
        return link.orElseThrow().feed().offer(buffer, index, length) > 0
            ? ControlledMessageHandler.Action.CONTINUE
            : ControlledMessageHandler.Action.ABORT;
    }

    private void log(final String what) {
        System.out.println("sealer FEED memberId=" + memberId + " " + what);
    }
}
