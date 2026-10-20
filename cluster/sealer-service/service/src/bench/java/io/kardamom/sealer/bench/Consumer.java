package io.kardamom.sealer.bench;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.logbuffer.Header;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * A canonical-stream consumer, as an executor is. It subscribes, then reads
 * every relayed record and boundary. For a record of the measured part of
 * an open-loop run, it records the time from the due time of the record's
 * batch to the arrival of the relayed frame.
 */
final class Consumer implements Runnable, EgressListener {

    private static final byte KIND_SUBSCRIBE = 2;
    private static final byte EGRESS_RELAYED = 1;
    private static final byte EGRESS_BOUNDARY = 2;
    /** The relayed payload starts after the kind, the index and the payload length. */
    private static final int RELAYED_PAYLOAD = 1 + Long.BYTES + Integer.BYTES;

    private final BenchConfig config;
    private final RunClock clock;
    private final AtomicBoolean stop = new AtomicBoolean();
    private final LatencyHistogram latency = new LatencyHistogram();
    private final long[] egressKinds = new long[32];
    private AeronCluster client;
    private volatile long relayed;
    private long blocks;

    Consumer(final BenchConfig config, final RunClock clock) {
        this.config = config;
        this.clock = clock;
    }

    /** Connect and subscribe to the canonical stream. */
    Consumer connect(final AeronCluster.Context context) {
        client = AeronCluster.connect(context.egressListener(this));
        final UnsafeBuffer frame = new UnsafeBuffer(new byte[] {KIND_SUBSCRIBE});
        while (client.offer(frame, 0, 1) < 0) {
            client.pollEgress();
        }
        return this;
    }

    @Override
    public void run() {
        while (!stop.get()) {
            pollOnce();
        }
    }

    @Override
    public void onMessage(
            final long clusterSessionId, final long timestamp, final DirectBuffer buffer,
            final int offset, final int length, final Header header) {
        final byte kind = buffer.getByte(offset);
        egressKinds[kind & 31]++;
        if (kind == EGRESS_RELAYED) {
            onRelayed(buffer, offset + RELAYED_PAYLOAD);
        } else if (kind == EGRESS_BOUNDARY) {
            blocks++;
        }
    }

    long relayed() {
        return relayed;
    }

    long blocks() {
        return blocks;
    }

    long egressKind(final int kind) {
        return egressKinds[kind];
    }

    LatencyHistogram latency() {
        return latency;
    }

    void stop() {
        stop.set(true);
    }

    void close() {
        client.close();
    }

    private void onRelayed(final DirectBuffer buffer, final int payload) {
        relayed++;
        if (!config.openLoop()) {
            return;
        }
        final long due = clock.scheduled(RecordStream.indexOf(buffer, payload) / config.batch());
        if (clock.measured(due)) {
            latency.record(System.nanoTime() - due);
        }
    }

    private void pollOnce() {
        if (client.pollEgress() == 0) {
            Thread.onSpinWait();
        }
    }
}
