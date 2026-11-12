package io.kardamom.sealer.cluster;

import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * The frames that one endpoint holds back, in arrival order. Only the
 * thread that polls the endpoint touches it. A frame leaves at its due
 * time, through the same handler that it would have reached on arrival.
 */
final class HeldFrames {

    /** The handler of the endpoint that takes a released frame. */
    interface Sink {
        void dispatch(UnsafeBuffer frame, int length, InetSocketAddress source, int transportIndex);
    }

    private record Frame(byte[] bytes, InetSocketAddress source, int transportIndex, long dueNs) {
    }

    private final ArrayDeque<Frame> frames = new ArrayDeque<>();
    private final LinkFaults faults;
    private final Sink sink;
    private long lastDueNs;

    HeldFrames(final LinkFaults faults, final Sink sink) {
        this.faults = faults;
        this.sink = sink;
    }

    boolean holds(final InetSocketAddress source) {
        return frames.stream().anyMatch(f -> f.source().equals(source));
    }

    /**
     * Handle one arrived frame: release the due frames first, then return
     * true when the frame is a tick, is dropped or is held, and false when
     * the endpoint handles it now.
     */
    boolean intercept(
            final UnsafeBuffer buffer, final int length, final InetSocketAddress source, final int transportIndex) {
        final long now = System.nanoTime();
        release(now);
        if (faults.isTick(source)) {
            return true;
        }
        final LinkFaults.Verdict verdict = faults.verdict(source, holds(source));
        if (verdict == LinkFaults.Verdict.HOLD) {
            // Due times never decrease, so the queue keeps the order of every link.
            lastDueNs = Math.max(lastDueNs, now + faults.delayNs(source));
            final byte[] copy = new byte[length];
            buffer.getBytes(0, copy);
            frames.addLast(new Frame(copy, source, transportIndex, lastDueNs));
        }
        return verdict != LinkFaults.Verdict.PASS;
    }

    private void release(final long now) {
        while (!frames.isEmpty() && frames.peekFirst().dueNs() <= now) {
            releaseFirst();
        }
    }

    private void releaseFirst() {
        final Frame frame = frames.removeFirst();
        faults.onReleased(frame.source());
        sink.dispatch(new UnsafeBuffer(frame.bytes()), frame.bytes().length, frame.source(), frame.transportIndex());
    }
}
