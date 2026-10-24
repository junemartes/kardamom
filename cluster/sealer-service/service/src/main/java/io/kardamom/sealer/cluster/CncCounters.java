package io.kardamom.sealer.cluster;

import io.aeron.AeronCounters;
import io.aeron.CncFileDescriptor;
import io.aeron.driver.status.SystemCounterDescriptor;
import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

/**
 * The Aeron counters of the member, read from the CnC file of its media
 * driver. The driver, the archive, the consensus module and the service
 * share the file, so one scan reads all of them.
 *
 * <p>The admin thread scans the counters once per scrape. The service
 * thread does no work for them. The reader holds its own mapping of the
 * file, so a component that closes its Aeron client does not unmap the
 * memory under a scan. The mapping ends when the reader is collected.</p>
 */
final class CncCounters {

    /** One series that the scan fills from the counters of one type. */
    enum Probe {
        APPEND_POSITION("kardamom_sealer_append_position", "gauge",
            "The position that the archive recorded of the cluster log on this member.",
            AeronCounters.ARCHIVE_RECORDING_POSITION_TYPE_ID, Match.LOG_RECORDING, 1.0),
        CONSENSUS_MAX_CYCLE("kardamom_sealer_consensus_max_cycle_seconds", "gauge",
            "The longest duty cycle of the consensus module, in seconds.",
            AeronCounters.CLUSTER_MAX_CYCLE_TIME_TYPE_ID, Match.TYPE, 1e9),
        CONSENSUS_CYCLE_EXCEEDED("kardamom_sealer_consensus_cycle_threshold_exceeded_total", "counter",
            "The duty cycles of the consensus module above the cycle threshold.",
            AeronCounters.CLUSTER_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID, Match.TYPE, 1.0),
        SERVICE_MAX_CYCLE("kardamom_sealer_service_max_cycle_seconds", "gauge",
            "The longest duty cycle of the clustered service, in seconds.",
            AeronCounters.CLUSTER_CLUSTERED_SERVICE_MAX_CYCLE_TIME_TYPE_ID, Match.TYPE, 1e9),
        SERVICE_CYCLE_EXCEEDED("kardamom_sealer_service_cycle_threshold_exceeded_total", "counter",
            "The duty cycles of the clustered service above the cycle threshold.",
            AeronCounters.CLUSTER_CLUSTERED_SERVICE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID, Match.TYPE, 1.0),
        RECORDER_MAX_WRITE("kardamom_sealer_archive_recorder_max_write_seconds", "gauge",
            "The longest write of the archive recorder, in seconds.",
            AeronCounters.ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID, Match.TYPE, 1e9),
        RECORDER_WRITE_TIME("kardamom_sealer_archive_recorder_write_seconds_total", "counter",
            "The time that the archive recorder spent in writes, in seconds.",
            AeronCounters.ARCHIVE_RECORDER_TOTAL_WRITE_TIME_TYPE_ID, Match.TYPE, 1e9),
        RECORDER_WRITE_BYTES("kardamom_sealer_archive_recorder_write_bytes_total", "counter",
            "The bytes that the archive recorder wrote.",
            AeronCounters.ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID, Match.TYPE, 1.0),
        DRIVER_ERRORS("kardamom_sealer_driver_errors_total", "counter",
            "The errors that the media driver logged.",
            SystemCounterDescriptor.ERRORS),
        NAKS_SENT("kardamom_sealer_driver_naks_sent_total", "counter",
            "The NAK messages that the media driver sent.",
            SystemCounterDescriptor.NAK_MESSAGES_SENT),
        NAKS_RECEIVED("kardamom_sealer_driver_naks_received_total", "counter",
            "The NAK messages that the media driver received.",
            SystemCounterDescriptor.NAK_MESSAGES_RECEIVED),
        RETRANSMITS("kardamom_sealer_driver_retransmits_total", "counter",
            "The retransmits that the media driver sent.",
            SystemCounterDescriptor.RETRANSMITS_SENT),
        SHORT_SENDS("kardamom_sealer_driver_short_sends_total", "counter",
            "The sends that the socket took only in part.",
            SystemCounterDescriptor.SHORT_SENDS),
        SENDER_BACKPRESSURE("kardamom_sealer_driver_sender_backpressure_total", "counter",
            "The back-pressure events of the network publications of the driver, summed.",
            AeronCounters.DRIVER_SENDER_BPE_TYPE_ID, Match.TYPE, 1.0);

        private final String name;
        private final String type;
        private final String help;
        private final int typeId;
        private final Match match;
        /** The id of the system counter, for {@link Match#SYSTEM}. */
        private final int systemId;
        /** The counter units in one shown unit. */
        private final double unit;

        Probe(final String name, final String type, final String help, final int typeId, final Match match,
                final double unit) {
            this.name = name;
            this.type = type;
            this.help = help;
            this.typeId = typeId;
            this.match = match;
            this.systemId = 0;
            this.unit = unit;
        }

        Probe(final String name, final String type, final String help, final SystemCounterDescriptor system) {
            this.name = name;
            this.type = type;
            this.help = help;
            this.typeId = SystemCounterDescriptor.SYSTEM_COUNTER_TYPE_ID;
            this.match = Match.SYSTEM;
            this.systemId = system.id();
            this.unit = 1.0;
        }

        private static final Probe[] ALL = values();

        /** Two values of one scan as one: the highest for a gauge, the sum for a counter. */
        private long combine(final long a, final long b) {
            return "gauge".equals(type) ? Math.max(a, b) : a + b;
        }
    }

    /** How a probe picks the counters of its type. */
    private enum Match {
        /** Every counter of the type. The values add up, or the highest wins for a gauge. */
        TYPE,
        /** The system counter whose key holds the id of the probe. */
        SYSTEM,
        /** The recording position whose label names the stream of the cluster log. */
        LOG_RECORDING
    }

    private final CountersReader reader;
    /** The stream id of the cluster log, as a recording label shows it. */
    private final String logStreamId;

    /**
     * A reader of {@code reader}. A recording position counts when its
     * label names {@code logStreamId}: the label is
     * {@code rec-pos: <recordingId> <sessionId> <streamId> <channel>}.
     */
    CncCounters(final CountersReader reader, final int logStreamId) {
        this.reader = reader;
        this.logStreamId = Integer.toString(logStreamId);
    }

    /** Map the CnC file of the media driver in {@code aeronDir}. */
    static CncCounters map(final String aeronDir, final int logStreamId) {
        final MappedByteBuffer cnc = IoUtil.mapExistingFile(new File(aeronDir, CncFileDescriptor.CNC_FILE), "cnc");
        final UnsafeBuffer meta = CncFileDescriptor.createMetaDataBuffer(cnc);
        return new CncCounters(new CountersReader(
            CncFileDescriptor.createCountersMetaDataBuffer(cnc, meta),
            CncFileDescriptor.createCountersValuesBuffer(cnc, meta),
            StandardCharsets.US_ASCII), logStreamId);
    }

    /** The values of one scan, by probe, and whether a counter matched each. */
    private final class Scan {
        final long[] values = new long[Probe.ALL.length];
        final boolean[] found = new boolean[Probe.ALL.length];

        void accept(final int counterId, final int typeId, final DirectBuffer key, final String label) {
            final long value = reader.getCounterValue(counterId);
            Arrays.stream(Probe.ALL)
                .filter(probe -> probe.typeId == typeId && matches(probe, key, label))
                .forEach(probe -> add(probe, value));
        }

        private void add(final Probe probe, final long value) {
            final int i = probe.ordinal();
            values[i] = found[i] ? probe.combine(values[i], value) : value;
            found[i] = true;
        }
    }

    private boolean matches(final Probe probe, final DirectBuffer key, final String label) {
        return switch (probe.match) {
            case TYPE -> true;
            case SYSTEM -> key.getInt(0) == probe.systemId;
            case LOG_RECORDING -> logStreamOf(label);
        };
    }

    /** Whether the label of a recording position names the stream of the cluster log, as its third field. */
    private boolean logStreamOf(final String label) {
        final String[] fields = label.split(" ", 5);
        return fields.length == 5 && fields[3].equals(logStreamId);
    }

    /** Scan the counters, and write each probe that a counter matched. */
    void renderTo(final MetricsText text) {
        final Scan scan = new Scan();
        reader.forEach(scan::accept);
        Arrays.stream(Probe.ALL)
            .filter(probe -> scan.found[probe.ordinal()])
            .forEach(probe -> render(text, probe, scan.values[probe.ordinal()]));
    }

    private void render(final MetricsText text, final Probe probe, final long value) {
        if (probe.unit == 1.0) {
            text.single(probe.name, probe.type, probe.help, value);
            return;
        }
        text.single(probe.name, probe.type, probe.help, value / probe.unit);
    }
}
