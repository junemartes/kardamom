package io.kardamom.sealer.cluster;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.RecordingLog;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.agrona.concurrent.errors.ErrorLogReader;

/**
 * Reads one running cluster member's recording log, archive, and consensus
 * module error log from the test thread. Each archive read opens its own
 * Aeron client on the member's media driver, so the probe shares no state
 * with the member's agents.
 */
final class MemberProbe {

    private static final String REPLAY_CHANNEL = "aeron:ipc";
    private static final int REPLAY_STREAM_ID = 1_955;
    private static final int SEALER_SERVICE_ID = 0;
    private static final Pattern REPLAY_START = Pattern.compile("requested replay start position=(\\d+)");

    private final TestNode node;

    MemberProbe(final TestNode node) {
        this.node = node;
    }

    /** The first log position that the member's archive still holds. */
    long logStartPosition() {
        final long recordingId = withRecordingLog(RecordingLog::findLastTermRecordingId);
        return withArchive(archive -> archive.getStartPosition(recordingId));
    }

    /** The log position of the member's latest consensus module snapshot. */
    long latestSnapshotLogPosition() {
        return withRecordingLog(log ->
                Objects.requireNonNull(log.getLatestSnapshot(ConsensusModule.Configuration.SERVICE_ID)))
                .logPosition;
    }

    /**
     * The full payload of the member's latest sealer service snapshot: the
     * cluster markers, the session entries, and the sealer state. Two
     * members that snapshot at the same log position write equal bytes
     * only when their sealer states are equal.
     */
    byte[] latestServiceSnapshot() {
        final long recordingId = withRecordingLog(log ->
                Objects.requireNonNull(log.getLatestSnapshot(SEALER_SERVICE_ID)).recordingId);
        return withArchive(archive -> replayRecording(archive, recordingId));
    }

    /** The number of consensus module errors whose text contains {@code text}. */
    long errorObservations(final String text) {
        final MutableLong count = new MutableLong();
        ErrorLogReader.read(
                node.consensusModule().context().clusterMarkFile().errorBuffer(),
                (observations, firstTimestamp, lastTimestamp, encodedException) ->
                        count.addAndGet(encodedException.contains(text) ? observations : 0));
        return count.get();
    }

    /**
     * The replay start positions in the consensus module errors whose
     * text contains {@code text} and {@code "requested replay start
     * position="}.
     */
    List<Long> refusedReplayStarts(final String text) {
        final List<Long> starts = new ArrayList<>();
        ErrorLogReader.read(
                node.consensusModule().context().clusterMarkFile().errorBuffer(),
                (observations, firstTimestamp, lastTimestamp, encodedException) ->
                        REPLAY_START.matcher(encodedException).results()
                                .filter(match -> encodedException.contains(text))
                                .forEach(match -> starts.add(Long.parseLong(match.group(1)))));
        return starts;
    }

    private static byte[] replayRecording(final AeronArchive archive, final long recordingId) {
        final int sessionId = (int) archive.startReplay(
                recordingId, 0L, AeronArchive.NULL_LENGTH, REPLAY_CHANNEL, REPLAY_STREAM_ID);
        try (Subscription subscription = archive.context().aeron().addSubscription(
                ChannelUri.addSessionId(REPLAY_CHANNEL, sessionId), REPLAY_STREAM_ID)) {
            return SnapshotIo.readSnapshot(awaitImage(subscription, sessionId), YieldingIdleStrategy.INSTANCE);
        }
    }

    private static Image awaitImage(final Subscription subscription, final int sessionId) {
        Image image;
        while ((image = subscription.imageBySessionId(sessionId)) == null) {
            Tests.yield();
        }
        return image;
    }

    private <T> T withRecordingLog(final Function<RecordingLog, T> read) {
        try (RecordingLog log = new RecordingLog(node.consensusModule().context().clusterDir(), false)) {
            return read.apply(log);
        }
    }

    private <T> T withArchive(final Function<AeronArchive, T> read) {
        try (Aeron aeron = Aeron.connect(
                new Aeron.Context().aeronDirectoryName(node.mediaDriver().aeronDirectoryName()));
             AeronArchive archive = AeronArchive.connect(node.consensusModule().context().archiveContext()
                     .clone()
                     .aeron(aeron)
                     .ownsAeronClient(false))) {
            return read.apply(archive);
        }
    }
}
