package io.kardamom.sealer.cluster;

import static io.kardamom.sealer.cluster.PromText.lines;
import static io.kardamom.sealer.cluster.PromText.value;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.aeron.AeronCounters;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.driver.status.SystemCounterDescriptor;
import java.nio.file.Path;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CncCountersTest {

    private static final int LOG_STREAM = 100;
    private static final int COUNTERS = 32;

    private final CountersManager counters = new CountersManager(
        new UnsafeBuffer(new byte[COUNTERS * CountersReader.METADATA_LENGTH]),
        new UnsafeBuffer(new byte[COUNTERS * CountersReader.COUNTER_LENGTH]));

    private void counter(final String label, final int typeId, final long value) {
        counters.setCounterValue(counters.allocate(label, typeId), value);
    }

    private void system(final SystemCounterDescriptor descriptor, final long value) {
        counters.setCounterValue(counters.allocate(descriptor.label(), SystemCounterDescriptor.SYSTEM_COUNTER_TYPE_ID,
            key -> key.putInt(0, descriptor.id())), value);
    }

    private String render(final CncCounters cnc) {
        final MetricsText text = new MetricsText(1);
        cnc.renderTo(text);
        return text.toString();
    }

    @Test
    void theAppendPositionIsTheRecordingOfTheLogStream() {
        counter("rec-pos: 7 11 " + LOG_STREAM + " aeron:udp?endpoint=a:1", AeronCounters.ARCHIVE_RECORDING_POSITION_TYPE_ID,
            4096L);
        counter("rec-pos: 8 12 106 aeron:ipc", AeronCounters.ARCHIVE_RECORDING_POSITION_TYPE_ID, 999_999L);
        assertEquals(4096.0, value(render(new CncCounters(counters, LOG_STREAM)), "kardamom_sealer_append_position", ""));
    }

    @Test
    void cycleTimesShowInSecondsAndCountersOfOneTypeAddUp() {
        counter("Cluster max cycle time in ns", AeronCounters.CLUSTER_MAX_CYCLE_TIME_TYPE_ID, 2_500_000L);
        counter("snd-bpe: 1", AeronCounters.DRIVER_SENDER_BPE_TYPE_ID, 3L);
        counter("snd-bpe: 2", AeronCounters.DRIVER_SENDER_BPE_TYPE_ID, 4L);
        final String text = render(new CncCounters(counters, LOG_STREAM));
        assertEquals(0.0025, value(text, "kardamom_sealer_consensus_max_cycle_seconds", ""), 1e-12);
        assertEquals(7.0, value(text, "kardamom_sealer_driver_sender_backpressure_total", ""));
    }

    @Test
    void aSystemCounterMatchesByTheIdInItsKey() {
        system(SystemCounterDescriptor.ERRORS, 2L);
        system(SystemCounterDescriptor.NAK_MESSAGES_SENT, 5L);
        final String text = render(new CncCounters(counters, LOG_STREAM));
        assertEquals(2.0, value(text, "kardamom_sealer_driver_errors_total", ""));
        assertEquals(5.0, value(text, "kardamom_sealer_driver_naks_sent_total", ""));
    }

    @Test
    void aProbeWithNoCounterWritesNoSeries() {
        final String text = render(new CncCounters(counters, LOG_STREAM));
        assertEquals("", text);
        assertEquals(0L, lines(text, "kardamom_sealer_archive_recorder_max_write_seconds"));
    }

    @Test
    void theMappedFileOfARunningDriverHoldsItsSystemCounters(@TempDir final Path dir) {
        final String aeronDir = dir.resolve("aeron").toString();
        try (MediaDriver driver = MediaDriver.launch(new MediaDriver.Context()
                .aeronDirectoryName(aeronDir)
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true))) {
            final String text = render(CncCounters.map(driver.aeronDirectoryName(), LOG_STREAM));
            assertEquals(0.0, value(text, "kardamom_sealer_driver_errors_total", ""));
            assertEquals(0.0, value(text, "kardamom_sealer_driver_short_sends_total", ""));
        }
    }
}
