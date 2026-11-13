package io.kardamom.sealer.cluster;

import static io.kardamom.sealer.cluster.PromText.value;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.VoidLedger;
import io.kardamom.sealer.cluster.ClusterStubs.StubCluster;
import io.kardamom.sealer.cluster.ClusterStubs.StubSession;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;

/** The service thread fills the pipeline metrics from its callbacks. */
class SealerServiceMetricsTest {

    /** The ordering window: the three records wait in it until the boundary tick. */
    private static final int WINDOW = 4;
    private static final byte[] SENDER = new byte[CanonicalSealerState.SENDER_LEN];

    private final StubCluster cluster = new StubCluster();
    private final StubSession sequencer = cluster.addSession(1);

    static {
        Arrays.fill(SENDER, (byte) 0xAB);
    }

    private static void deliver(final SealerClusteredService svc, final StubSession from, final byte[] frame) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        buf.putBytes(0, frame);
        svc.onSessionMessage(from, 0, buf, 0, frame.length, null);
    }

    /** A batch of {@code entries}: {@code [kind:3][count:u16][len:u32][entry]...}. */
    private static byte[] batch(final byte[]... entries) {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        buf.putByte(0, SealerWire.KIND_BATCH);
        buf.putShort(1, (short) entries.length, ByteOrder.LITTLE_ENDIAN);
        final int length = Arrays.stream(entries).reduce(3, (pos, entry) -> {
            buf.putInt(pos, entry.length, ByteOrder.LITTLE_ENDIAN);
            buf.putBytes(pos + Integer.BYTES, entry);
            return pos + Integer.BYTES + entry.length;
        }, Integer::sum);
        final byte[] out = new byte[length];
        buf.getBytes(0, out);
        return out;
    }

    private String render(final SealerClusteredService svc) {
        final MetricsText text = new MetricsText(1);
        svc.metrics().renderTo(text);
        return text.toString();
    }

    @Test
    void theCallbacksCountMessagesFramesStepsAndBatches() {
        final SealerClusteredService svc = new SealerClusteredService(16, 250, 1, Set.of(), VoidLedger.Config.DISABLED,
            CanonicalSealerState.DEFAULT_INCLUSION_HORIZON_BLOCKS, WINDOW);
        svc.start(cluster, Optional.empty());
        deliver(svc, sequencer, IngressFrames.subscribeFrame());
        deliver(svc, sequencer, IngressFrames.recordFrame(1, SENDER, 0));
        deliver(svc, sequencer, batch(IngressFrames.recordFrame(2, SENDER, 1), IngressFrames.recordFrame(3, SENDER, 5)));
        deliver(svc, sequencer, new byte[] {10, 0, 0});
        assertEquals(3.0, value(render(svc), "kardamom_queue_depth", "queue=\"sealer.ordering_window\""));
        svc.onTimerEvent(SealerClusteredService.BOUNDARY_TIMER_CORRELATION_ID, 250);
        svc.onTerminate(cluster);

        final String text = render(svc);
        assertEquals(1.0, value(text, "kardamom_sealer_messages_total", "kind=\"0\""));
        assertEquals(1.0, value(text, "kardamom_sealer_messages_total", "kind=\"2\""));
        assertEquals(1.0, value(text, "kardamom_sealer_messages_total", "kind=\"3\""));
        assertEquals(1.0, value(text, "kardamom_sealer_messages_total", "kind=\"10\""));
        assertEquals(1.0, value(text, "kardamom_sealer_frames_total", "outcome=\"unknown_kind\""));
        assertEquals(1.0, value(text, "kardamom_sealer_frames_total", "outcome=\"contiguity_reject\""));
        assertEquals(4.0, value(text, "kardamom_step_seconds_count", "step=\"sealer.message\""));
        assertEquals(1.0, value(text, "kardamom_step_seconds_count", "step=\"sealer.boundary\""));
        assertEquals(1.0, value(text, "kardamom_step_seconds_count", "step=\"sealer.flush_window\""));
        assertEquals(1.0, value(text, "kardamom_batch_items_bucket", "batch=\"sealer.ingress_batch\",le=\"2\""));
        assertEquals(0.0, value(text, "kardamom_queue_depth", "queue=\"sealer.ordering_window\""));
        assertEquals(1L, svc.droppedUnknownKindCount());
        assertEquals(0.0, value(text, "kardamom_step_seconds_count", "step=\"sealer.commit_wait\""),
            "a service with no timeline records no wait");
    }

    @Test
    void aSessionMessageRecordsItsWaitsOnThisMember() {
        final AtomicLong clock = new AtomicLong();
        final AtomicLong append = new AtomicLong(4096);
        final AtomicLong commit = new AtomicLong(4096);
        final LogTimeline timeline = new LogTimeline(clock::get);
        timeline.follow(append::get, commit::get);
        final SealerClusteredService svc = new SealerClusteredService(16, 250, 1, Set.of(), VoidLedger.Config.DISABLED,
            CanonicalSealerState.DEFAULT_INCLUSION_HORIZON_BLOCKS, WINDOW).observing(timeline);
        svc.start(cluster, Optional.empty());
        timeline.sample();
        // The member records the entry, and 300 µs later the quorum commits it.
        clock.set(System.nanoTime() - 400_000);
        append.set(4224);
        timeline.sample();
        clock.addAndGet(300_000);
        commit.set(4224);
        timeline.sample();
        cluster.logPosition = 4224;
        deliver(svc, sequencer, IngressFrames.subscribeFrame());

        final String text = render(svc);
        assertEquals(1.0, value(text, "kardamom_step_seconds_count", "step=\"sealer.commit_wait\""));
        assertEquals(1.0, value(text, "kardamom_step_seconds_bucket", "step=\"sealer.commit_wait\",le=\"0.0005\""));
        assertEquals(0.0, value(text, "kardamom_step_seconds_bucket", "step=\"sealer.commit_wait\",le=\"0.00025\""));
        assertEquals(1.0, value(text, "kardamom_step_seconds_count", "step=\"sealer.apply_wait\""));
        assertEquals(0.0, value(text, "kardamom_step_seconds_bucket", "step=\"sealer.apply_wait\",le=\"0.00005\""),
            "the service applied the entry at least 100 µs after the commit");
    }
}
