package io.kardamom.sealer.cluster;

import static io.kardamom.sealer.cluster.PromText.lines;
import static io.kardamom.sealer.cluster.PromText.value;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.Publication;
import io.kardamom.sealer.cluster.ClusterStubs.StubSession;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class SealerMetricsTest {

    private final SealerMetrics metrics = new SealerMetrics();

    @Test
    void each_feed_state_has_its_fixed_gauge_value() {
        final List<FeedState> states =
            List.of(FeedState.OFF, FeedState.IDLE, FeedState.WALKING, FeedState.LIVE, FeedState.GAPPED);
        IntStream.range(0, states.size()).forEach(i -> {
            metrics.feed(states.get(i));
            assertEquals(i, value(render(), "kardamom_sealer_feed_state", ""), states.get(i).label());
        });
    }

    private String render() {
        final MetricsText text = new MetricsText(1);
        metrics.renderTo(text);
        return text.toString();
    }

    @Test
    void aFrameCountReturnsTheNewTotalOfItsOutcome() {
        assertEquals(1L, metrics.frame(SealerMetrics.Frame.PAST_DEADLINE));
        assertEquals(2L, metrics.frame(SealerMetrics.Frame.PAST_DEADLINE));
        assertEquals(1L, metrics.frame(SealerMetrics.Frame.MALFORMED));
        final String text = render();
        assertEquals(2.0, value(text, "kardamom_sealer_frames_total", "outcome=\"past_deadline\""));
        assertEquals(1.0, value(text, "kardamom_sealer_frames_total", "outcome=\"malformed\""));
        assertEquals(0.0, value(text, "kardamom_sealer_frames_total", "outcome=\"expired_ref\""));
    }

    @Test
    void aMessageKindShowsAfterItsFirstMessage() {
        metrics.message((byte) 0);
        metrics.message((byte) 0);
        metrics.message((byte) -1);
        final String text = render();
        assertEquals(2.0, value(text, "kardamom_sealer_messages_total", "kind=\"0\""));
        assertEquals(1.0, value(text, "kardamom_sealer_messages_total", "kind=\"255\""));
        assertEquals(0L, lines(text, "kardamom_sealer_messages_total{" + PromText.MEMBER + ",kind=\"3\"}"));
    }

    @Test
    void eachRetryableOfferResultHasItsOwnLabel() {
        metrics.egressRefused(Publication.NOT_CONNECTED);
        metrics.egressRefused(Publication.BACK_PRESSURED);
        metrics.egressRefused(Publication.BACK_PRESSURED);
        metrics.egressRefused(Publication.ADMIN_ACTION);
        final String text = render();
        final String name = "kardamom_sealer_egress_backpressure_total";
        assertEquals(1.0, value(text, name, "result=\"not_connected\""));
        assertEquals(2.0, value(text, name, "result=\"back_pressured\""));
        assertEquals(1.0, value(text, name, "result=\"admin_action\""));
    }

    @Test
    void theEgressOfferStepSamplesOneOfferInSixtyFour() {
        IntStream.range(0, 2 * SealerMetrics.EGRESS_OFFER_SAMPLE)
            .forEach(i -> metrics.egressOfferDone(metrics.egressOfferStart()));
        assertEquals(2.0, value(render(), "kardamom_step_seconds_count", "step=\"sealer.egress_offer\""));
    }

    @Test
    void aMessageAddsItsStepItsBusyTimeAndTheWindowDepth() {
        metrics.messageDone(System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(2), 7);
        metrics.ingressBatch(3);
        final String text = render();
        assertEquals(1.0, value(text, "kardamom_step_seconds_count", "step=\"sealer.message\""));
        assertEquals(0.0, value(text, "kardamom_step_seconds_bucket", "step=\"sealer.message\",le=\"0.001\""));
        assertTrue(value(text, "kardamom_loop_nanoseconds_total", "loop=\"sealer.service\",state=\"busy\"")
            >= TimeUnit.MILLISECONDS.toNanos(2));
        assertEquals(7.0, value(text, "kardamom_queue_depth", "queue=\"sealer.ordering_window\""));
        assertEquals(1.0, value(text, "kardamom_batch_items_bucket", "batch=\"sealer.ingress_batch\",le=\"4\""));
    }

    @Test
    void aFlushOfAnEmptyWindowRecordsNoStep() {
        metrics.windowFlushed(System.nanoTime(), 0);
        metrics.windowFlushed(System.nanoTime(), 2);
        assertEquals(1.0, value(render(), "kardamom_step_seconds_count", "step=\"sealer.flush_window\""));
    }

    @Test
    void eachFamilyHasOneHelpAndOneTypeLine() {
        final String text = render();
        assertEquals(1L, lines(text, "# HELP kardamom_step_seconds "));
        assertEquals(1L, lines(text, "# TYPE kardamom_step_seconds histogram"));
        assertEquals(5L, lines(text, "kardamom_step_seconds_count{"));
        assertEquals(1L, lines(text, "# TYPE kardamom_queue_depth gauge"));
    }

    /**
     * A wedged session queues its frame and counts the refusal. The drain
     * publishes the queued bytes. Past the stall deadline the session
     * closes, and the stall counts.
     */
    @Test
    void theBacklogsCountRefusalsQueuedBytesAndStalls() throws InterruptedException {
        final long stallNs = TimeUnit.MILLISECONDS.toNanos(20);
        final SessionBacklogs backlogs = new SessionBacklogs(0, stallNs, 1024, StartHold.released(), metrics);
        final StubSession session = new StubSession(1);
        session.backPressured = true;
        backlogs.offer(session, new byte[10]);
        backlogs.drain();
        String text = render();
        assertEquals(1.0, value(text, "kardamom_sealer_egress_backpressure_total", "result=\"back_pressured\""));
        assertEquals(10.0, value(text, "kardamom_queue_depth", "queue=\"sealer.egress_backlog\""));
        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(stallNs) * 2);
        backlogs.drain();
        text = render();
        assertTrue(session.closed);
        assertEquals(1.0, value(text, "kardamom_sealer_egress_stalls_total", ""));
        assertEquals(0.0, value(text, "kardamom_queue_depth", "queue=\"sealer.egress_backlog\""));
    }
}
