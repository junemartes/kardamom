package io.kardamom.sealer.cluster;

import static io.kardamom.sealer.cluster.PromText.value;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kardamom.sealer.DecisionRules;
import java.util.OptionalLong;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersManager;
import org.junit.jupiter.api.Test;

class MetricsPageTest {
    private static final MemberStatus STATUS = new MemberStatus(1, "LEADER", "CLOSED", 5000, 4000,
        MemberStatus.SnapshotVersions.of(0), DecisionRules.START_VIEW, OptionalLong.of(0));

    private static MetricsPage page() {
        final CncCounters none = new CncCounters(
            new CountersManager(new UnsafeBuffer(new byte[0]), new UnsafeBuffer(new byte[0])), 100);
        return new MetricsPage(1, new SealerMetrics(), none);
    }

    @Test
    void thePageKeepsTheDiskGaugeAndAddsThePositionsAndThePipeline() {
        final MemberStatus status = STATUS;
        final String text = page().render(status);
        assertTrue(text.startsWith(status.toMetrics()), text);
        assertEquals(5000.0, value(text, "kardamom_sealer_commit_position", ""));
        assertEquals(4000.0, value(text, "kardamom_sealer_service_position", ""));
        assertEquals(0.0, value(text, "kardamom_loop_nanoseconds_total", "loop=\"sealer.service\",state=\"busy\""));
    }

    @Test
    void theReportKeepsTheStatusVerdictAndCarriesThePage() {
        final AdminServer.Report report = page().report(STATUS, 500);
        assertEquals(STATUS.isReady(500), report.ready());
        assertEquals(STATUS.toJson(500), report.json());
        assertEquals(page().render(STATUS), report.metrics());
    }
}
