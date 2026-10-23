package io.kardamom.sealer.cluster;

/**
 * The text of {@code GET /metrics}: the disk gauge of the status, the log
 * positions, the pipeline metrics of the service thread, and the Aeron
 * counters of the member. The admin thread renders one per scrape.
 *
 * <p>The three positions show where the log waits. Append minus commit is
 * the wait for the replication quorum. Commit minus service is the wait
 * for the service thread to apply the log.</p>
 */
final class MetricsPage {
    private final int memberId;
    private final SealerMetrics pipeline;
    private final CncCounters counters;

    MetricsPage(final int memberId, final SealerMetrics pipeline, final CncCounters counters) {
        this.memberId = memberId;
        this.pipeline = pipeline;
        this.counters = counters;
    }

    /**
     * The report of one request: the verdict and the JSON of {@code status}
     * for a member within {@code lagBytes} of the commit position, and the
     * text of this page.
     */
    AdminServer.Report report(final MemberStatus status, final long lagBytes) {
        return new AdminServer.Report(status.isReady(lagBytes), status.toJson(lagBytes), render(status));
    }

    /** The text of one scrape, with {@code status} sampled for it. */
    String render(final MemberStatus status) {
        final MetricsText text = new MetricsText(memberId)
            .single("kardamom_sealer_commit_position", "gauge",
                "The log position that a quorum of the cluster appended.", status.commitPosition)
            .single("kardamom_sealer_service_position", "gauge",
                "The log position that the service thread applied.", status.servicePosition);
        pipeline.renderTo(text);
        counters.renderTo(text);
        return status.toMetrics() + text;
    }
}
