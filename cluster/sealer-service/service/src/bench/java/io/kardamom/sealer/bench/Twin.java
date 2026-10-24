package io.kardamom.sealer.bench;

import io.aeron.Publication;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.logbuffer.Header;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;

/**
 * One racing sequencer: it offers the whole record stream in kind-3 batches,
 * on the run's schedule. Every twin offers the same records, so the sealer
 * sees each record once from each twin. The twin counts the reject frames
 * that the sealer sends back to its session, by egress kind.
 */
final class Twin implements Runnable, EgressListener {

    private static final byte KIND_BATCH = 3;
    private static final int BATCH_HEADER = 1 + Short.BYTES;
    private static final int ENTRY_HEADER = Integer.BYTES;

    private final BenchConfig config;
    private final RecordStream stream;
    private final RunClock clock;
    private final AtomicBoolean stop;
    private final ExpandableDirectByteBuffer buf;
    private final long[] egressKinds = new long[32];
    private AeronCluster client;
    private long offered;
    private long refusals;
    private long maxLateNs;

    Twin(final BenchConfig config, final RecordStream stream, final RunClock clock, final AtomicBoolean stop) {
        this.config = config;
        this.stream = stream;
        this.clock = clock;
        this.stop = stop;
        this.buf = new ExpandableDirectByteBuffer(BATCH_HEADER + config.batch() * (ENTRY_HEADER + RecordStream.FRAME_LEN));
    }

    /** Connect before the run starts, so the connect time is not part of the measure. */
    Twin connect(final AeronCluster.Context context) {
        client = AeronCluster.connect(context.egressListener(this));
        return this;
    }

    @Override
    public void run() {
        clock.awaitStart();
        final long batches = config.totalRecords() / config.batch();
        for (long b = 0; b < batches && !stop.get(); b++) {
            offerBatch(b);
        }
    }

    @Override
    public void onMessage(
            final long clusterSessionId, final long timestamp, final DirectBuffer buffer,
            final int offset, final int length, final Header header) {
        egressKinds[buffer.getByte(offset) & 31]++;
    }

    long offered() {
        return offered;
    }

    long refusals() {
        return refusals;
    }

    long maxLateNs() {
        return maxLateNs;
    }

    long egressKind(final int kind) {
        return egressKinds[kind];
    }

    void close() {
        client.close();
    }

    private void offerBatch(final long batchIndex) {
        final long due = clock.scheduled(batchIndex);
        while (System.nanoTime() < due) {
            idleOnce();
        }
        maxLateNs = Math.max(maxLateNs, System.nanoTime() - due);
        final int length = fill(batchIndex * config.batch());
        while (!taken(client.offer(buf, 0, length))) {
            idleOnce();
        }
        offered += config.batch();
        client.pollEgress();
    }

    private int fill(final long first) {
        buf.putByte(0, KIND_BATCH);
        buf.putShort(1, (short) config.batch(), RecordStream.LE);
        int pos = BATCH_HEADER;
        for (int k = 0; k < config.batch(); k++) {
            pos = putEntry(pos, first + k);
        }
        return pos;
    }

    private int putEntry(final int pos, final long index) {
        final int length = stream.write(buf, pos + ENTRY_HEADER, index);
        buf.putInt(pos, length, RecordStream.LE);
        return pos + ENTRY_HEADER + length;
    }

    private boolean taken(final long result) {
        if (result > 0) {
            return true;
        }
        if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
            throw new IllegalStateException("the cluster ingress closed: " + result);
        }
        refusals++;
        return false;
    }

    private void idleOnce() {
        client.pollEgress();
        Thread.onSpinWait();
    }
}
