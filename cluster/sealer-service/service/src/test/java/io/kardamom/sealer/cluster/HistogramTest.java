package io.kardamom.sealer.cluster;

import static io.kardamom.sealer.cluster.PromText.value;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class HistogramTest {

    private static String render(final Histogram histogram) {
        final MetricsText text = new MetricsText(1);
        histogram.renderTo(text, "h", "step=\"s\"");
        return text.toString();
    }

    private static double bucket(final String text, final String le) {
        return value(text, "h_bucket", "step=\"s\",le=\"" + le + "\"");
    }

    @Test
    void aStepOfThreeMicrosecondsLandsInTheFiveMicrosecondBucket() {
        final Histogram histogram = new Histogram(Histogram.Layout.STEP);
        histogram.record(3_000L);
        final String text = render(histogram);
        assertEquals(0.0, bucket(text, "0.0000025"));
        assertEquals(1.0, bucket(text, "0.000005"));
        assertEquals(1.0, bucket(text, "10"));
        assertEquals(1.0, bucket(text, "+Inf"));
        assertEquals(0.000003, value(text, "h_sum", "step=\"s\""), 1e-12);
        assertEquals(1.0, value(text, "h_count", "step=\"s\""));
    }

    @Test
    void aValueOnABoundFallsInTheBucketOfThatBound() {
        final Histogram histogram = new Histogram(Histogram.Layout.STEP);
        histogram.record(1_000L);
        histogram.record(1_000_000_000L);
        final String text = render(histogram);
        assertEquals(1.0, bucket(text, "0.000001"));
        assertEquals(1.0, bucket(text, "0.5"));
        assertEquals(2.0, bucket(text, "1"));
    }

    @Test
    void aValueAboveTheLastBoundCountsOnlyInTheInfiniteBucket() {
        final Histogram histogram = new Histogram(Histogram.Layout.STEP);
        histogram.record(11_000_000_000L);
        final String text = render(histogram);
        assertEquals(0.0, bucket(text, "10"));
        assertEquals(1.0, bucket(text, "+Inf"));
        assertEquals(11.0, value(text, "h_sum", "step=\"s\""));
    }

    @Test
    void theCountLayoutShowsItemsAsIntegers() {
        final Histogram histogram = new Histogram(Histogram.Layout.COUNT);
        histogram.record(3L);
        histogram.record(65_536L);
        final String text = render(histogram);
        assertEquals(0.0, bucket(text, "2"));
        assertEquals(1.0, bucket(text, "4"));
        assertEquals(2.0, bucket(text, "65536"));
        assertEquals(65_539.0, value(text, "h_sum", "step=\"s\""));
        assertEquals(2.0, value(text, "h_count", "step=\"s\""));
    }
}
