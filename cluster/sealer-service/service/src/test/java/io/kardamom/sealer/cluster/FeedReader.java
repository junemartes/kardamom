package io.kardamom.sealer.cluster;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.logbuffer.Header;
import io.aeron.test.Tests;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * The executor end of a member feed in a test: a subscription to the feed
 * and a publication on the control stream, on the media driver of one
 * member. It keeps every frame that it polls, in order.
 */
final class FeedReader implements AutoCloseable {

    /** One frame of the feed, with the fields that the tests read. */
    record Frame(byte[] bytes) {
        byte kind() {
            return bytes[0];
        }

        long a() {
            return new UnsafeBuffer(bytes).getLong(1, ByteOrder.LITTLE_ENDIAN);
        }

        long b() {
            return new UnsafeBuffer(bytes).getLong(1 + Long.BYTES, ByteOrder.LITTLE_ENDIAN);
        }

        long requestId() {
            return new UnsafeBuffer(bytes).getLong(1 + 2 * Long.BYTES, ByteOrder.LITTLE_ENDIAN);
        }

        /** Whether the frame is a retained frame: a relayed record or a boundary. */
        boolean canonical() {
            return kind() == SealerWire.EGRESS_KIND_RELAYED || kind() == SealerWire.EGRESS_KIND_BOUNDARY;
        }
    }

    private final Aeron aeron;
    private final Subscription feed;
    private final ExclusivePublication control;
    private final FragmentAssembler assembler = new FragmentAssembler(this::onFragment);
    private final List<Frame> frames = new ArrayList<>();

    FeedReader(final String aeronDir) {
        this.aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
        this.feed = aeron.addSubscription("aeron:ipc", SealerWire.FEED_STREAM_ID);
        this.control = aeron.addExclusivePublication("aeron:ipc?term-length=64k", SealerWire.FEED_CONTROL_STREAM_ID);
    }

    /** Send a replay request, after the feed has an image and the feeder subscribes to the control stream. */
    void request(final long fromIndex, final long fromBlock, final long requestId) {
        while (!feed.isConnected() || !control.isConnected()) {
            Tests.yield();
        }
        final UnsafeBuffer message = new UnsafeBuffer(new byte[SealerWire.FEED_REQUEST_LEN]);
        new FeedRequest(fromIndex, fromBlock, requestId).write(message);
        while (control.offer(message, 0, SealerWire.FEED_REQUEST_LEN) < 0) {
            Tests.yield();
        }
    }

    /** Poll until {@code done} holds for the frames so far. */
    void pollUntil(final Predicate<List<Frame>> done) {
        while (!done.test(frames)) {
            feed.poll(assembler, 64);
            Tests.yield();
        }
    }

    /** The frames so far. */
    List<Frame> frames() {
        return List.copyOf(frames);
    }

    private void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header) {
        final byte[] bytes = new byte[length];
        buffer.getBytes(offset, bytes);
        frames.add(new Frame(bytes));
    }

    @Override
    public void close() {
        CloseHelper.closeAll(control, feed, aeron);
    }
}
