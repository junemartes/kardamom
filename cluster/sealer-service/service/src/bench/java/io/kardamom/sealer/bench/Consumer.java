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
    private static final byte EGRESS_SEALER_INFO = 17;
    private static final byte KIND_ORIGIN_RECORD = 4;
    private static final byte KIND_FEATURE_ACTIVATION = 11;
    private static final long FEATURE_SEALER_V4 = 3L;
    private static final byte RT_EPOCH = 2;
    private static final long ACTIVATION_TIMEOUT_MS = 30_000L;
    private static final byte KIND_POSTED_CURSOR = 7;
    /** The batcher stand-in posts each block this many blocks after its boundary. */
    private static final long POST_LAG_BLOCKS = 4L;
    /** The relayed payload starts after the kind, the index and the payload length. */
    private static final int RELAYED_PAYLOAD = 1 + Long.BYTES + Integer.BYTES;
    /** The relayed payload of a transaction reference: the id, the record type and 13 bytes of fields. */
    private static final int REF_PAYLOAD_LEN = 32 + 1 + 13;

    private final BenchConfig config;
    private final RunClock clock;
    private final AtomicBoolean stop = new AtomicBoolean();
    private final LatencyHistogram latency = new LatencyHistogram();
    /** The highest latency and the record count of each second of the run, by due time. */
    private final long[] secondMax = new long[3600];
    private final long[] secondCount = new long[3600];
    private final long[] egressKinds = new long[32];
    private AeronCluster client;
    private volatile long relayed;
    private long blocks;
    private int sealerVersion;
    /** The newest block that a boundary closed, and the newest block posted. */
    private long sealedBlock;
    private long postedBlock;
    private final UnsafeBuffer cursorFrame = new UnsafeBuffer(new byte[1 + Long.BYTES]);

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

    /**
     * Switch the cluster to decision version 4 before the run: the
     * activation record of {@code FEATURE_SEALER_V4} at L1 origin 1, then the
     * epoch of origin 1. The version switches at the boundary after the
     * first block of the epoch. Wait until the sealer reports version 4.
     */
    void activateVersionFour() {
        final UnsafeBuffer frame = new UnsafeBuffer(new byte[64]);
        frame.putByte(0, KIND_FEATURE_ACTIVATION);
        frame.putLong(1, 1L, RecordStream.LE);
        frame.putLong(9, FEATURE_SEALER_V4, RecordStream.LE);
        frame.putLong(17, 1L, RecordStream.LE);
        offer(frame, 25);
        frame.setMemory(0, 64, (byte) 0);
        frame.putByte(0, KIND_ORIGIN_RECORD);
        frame.putByte(1, (byte) 0x7e);
        frame.putLong(33, 1L, RecordStream.LE);
        frame.putInt(41, 1, RecordStream.LE);
        frame.putByte(45, RT_EPOCH);
        offer(frame, 46);
        final long deadline = System.currentTimeMillis() + ACTIVATION_TIMEOUT_MS;
        while (sealerVersion != 4) {
            awaitEgress(deadline);
        }
    }

    private void offer(final UnsafeBuffer frame, final int length) {
        while (client.offer(frame, 0, length) < 0) {
            client.pollEgress();
        }
    }

    private void awaitEgress(final long deadline) {
        if (System.currentTimeMillis() > deadline) {
            throw new IllegalStateException("the sealer did not switch to decision version 4");
        }
        client.pollEgress();
        Thread.onSpinWait();
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
        if (kind == EGRESS_RELAYED && buffer.getInt(offset + 1 + Long.BYTES, RecordStream.LE) == REF_PAYLOAD_LEN) {
            onRelayed(buffer, offset + RELAYED_PAYLOAD);
        } else if (kind == EGRESS_BOUNDARY) {
            blocks++;
            sealedBlock = buffer.getLong(offset + 1, RecordStream.LE);
        } else if (kind == EGRESS_SEALER_INFO) {
            sealerVersion = buffer.getByte(offset + 1);
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

    /** One line for each second of the run: the records due in it and their highest latency. */
    String timeline() {
        final StringBuilder out = new StringBuilder("timeline (second: records max_ms)");
        for (int s = 0; s < secondCount.length && secondCount[s] > 0; s++) {
            out.append(String.format(" %d:%d/%.0f", s, secondCount[s], secondMax[s] / 1e6));
        }
        return out.toString();
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
        final long lat = System.nanoTime() - due;
        if (clock.measured(due)) {
            latency.record(lat);
        }
        final int second = (int) Math.min(secondMax.length - 1, Math.max(0, (due - clock.startNs()) / 1_000_000_000L));
        secondMax[second] = Math.max(secondMax[second], lat);
        secondCount[second]++;
    }

    private void pollOnce() {
        if (client.pollEgress() == 0) {
            Thread.onSpinWait();
        }
        postCursor();
    }

    /**
     * Stand in for the batcher: post the block {@link #POST_LAG_BLOCKS}
     * behind the newest boundary, so the sealer evicts its retained frames
     * as on a chain with a running batcher.
     */
    private void postCursor() {
        final long due = sealedBlock - POST_LAG_BLOCKS;
        if (due <= postedBlock) {
            return;
        }
        cursorFrame.putByte(0, KIND_POSTED_CURSOR);
        cursorFrame.putLong(1, due, RecordStream.LE);
        if (client.offer(cursorFrame, 0, cursorFrame.capacity()) > 0) {
            postedBlock = due;
        }
    }
}
