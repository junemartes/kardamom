package io.kardamom.sealer.bench;

import java.util.List;

/** The printed result of one run. */
record Report(
        BenchConfig config,
        List<Twin> twins,
        Consumer consumer,
        SealerLoadBench.Window window,
        List<String> gc) {

    private static final int EGRESS_CONTIGUITY_REJECT = 5;
    private static final int EGRESS_PAST_DEADLINE = 7;
    private static final int EGRESS_WINDOW_FULL = 8;
    private static final int EGRESS_DA_LAG_REJECT = 10;
    private static final int EGRESS_RECORD_LAG_REJECT = 13;

    void print() {
        System.out.printf(
                "sealer-bench members=%d twins=%d rate=%s batch=%d warmup=%ds seconds=%d senders=%d v4Ids=%b%n",
                config.members(), config.twins(), config.openLoop() ? config.ratePerTwin() + "/s per twin" : "max",
                config.batch(), config.warmupSeconds(), config.seconds(), config.senders(), config.v4Ids());
        System.out.println("member jvm: " + String.join(" ", config.memberJvm()));
        System.out.println("member props: " + String.join(" ", config.memberProps()));
        twins.forEach(this::printTwin);
        printConsumer();
        gc.forEach(System.out::println);
    }

    private void printTwin(final Twin twin) {
        System.out.printf(
                "twin offered=%d refusals=%d max_late=%.1fms rejects: contiguity=%d past_deadline=%d "
                        + "window_full=%d da_lag=%d record_lag=%d%n",
                twin.offered(), twin.refusals(), twin.maxLateNs() / 1e6,
                twin.egressKind(EGRESS_CONTIGUITY_REJECT), twin.egressKind(EGRESS_PAST_DEADLINE),
                twin.egressKind(EGRESS_WINDOW_FULL), twin.egressKind(EGRESS_DA_LAG_REJECT),
                twin.egressKind(EGRESS_RECORD_LAG_REJECT));
    }

    private void printConsumer() {
        final long measured = window.relayedAtEnd() - window.relayedAtWarmupEnd();
        System.out.printf(
                "relayed total=%d blocks=%d measured_rate=%.0f/s%n",
                consumer.relayed(), consumer.blocks(), measured / (double) config.seconds());
        System.out.println(consumer.timeline());
        final LatencyHistogram h = consumer.latency();
        if (h.count() == 0) {
            return;
        }
        System.out.printf(
                "latency due->relayed (n=%d): p50=%s p90=%s p99=%s p99.9=%s p99.99=%s max=%s%n",
                h.count(), ms(h.quantile(0.50)), ms(h.quantile(0.90)), ms(h.quantile(0.99)),
                ms(h.quantile(0.999)), ms(h.quantile(0.9999)), ms(h.max()));
    }

    private static String ms(final long ns) {
        return String.format("%.3fms", ns / 1e6);
    }
}
