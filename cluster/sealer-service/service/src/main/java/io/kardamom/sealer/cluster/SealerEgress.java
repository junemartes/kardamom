package io.kardamom.sealer.cluster;

import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.kardamom.sealer.Boundary;
import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.ClusterStatus;
import io.kardamom.sealer.Relayed;
import io.kardamom.sealer.VersionSwitch;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.LongHashSet;

/**
 * The egress layer of {@link SealerClusteredService}.
 * It frames relayed records, boundaries, and control messages, offers them
 * through the per-session backlogs of {@link SessionBacklogs}, targets the
 * record fan-out at announced consumers, and keeps a bounded set of framed
 * egress to serve client replay requests.
 *
 * <p>This class is single-threaded by design: every method here runs on the
 * one clustered-service thread (Aeron {@code ClusteredService} callbacks).
 * So the shared staging buffer, the retained frames, and the consumer set
 * are unsynchronized on purpose. Do not call into this class from any other
 * thread.</p>
 */
final class SealerEgress {

    private final Cluster cluster;
    /** This cluster member's id, logged on egress operational signals. */
    private final int memberId;
    /** The offer path, with one backlog for each session that pushes back. */
    private final SessionBacklogs backlogs;

    /**
     * Session ids that announced themselves as canonical-stream consumers
     * (a {@link SealerWire#KIND_SUBSCRIBE} frame, or any replay request).
     *
     * <p>This set changes only from logged session messages and
     * {@code onSessionClose}, and both are log-driven. So every member holds
     * the same set, and a new leader fans out to the same sessions.</p>
     *
     * <p>This set is not snapshotted on purpose. A restart from a snapshot
     * closes every client connection. Each client re-announces itself when
     * it opens its next session. Until the first announcement arrives,
     * {@link #offerToConsumers} falls back to a broadcast to all sessions, so
     * nothing goes unserved.</p>
     */
    private final LongHashSet consumerSessions = new LongHashSet();

    /** The sessions that get relayed records, and whether a session change made the list old. */
    private ClientSession[] targets = new ClientSession[0];
    private boolean targetsStale = true;

    /**
     * Retained egress frames, in emission order, for replay requests from
     * connecting or reconnecting clients.
     * Without replay, frames committed while a client had no session are
     * lost forever, leaving an unrecoverable gap in its canonical stream.
     * This is deterministic across members, since it is derived from the
     * replicated log.
     *
     * <p>The eviction rule follows the decision version and the carry
     * window of the state ({@link #setRules}):</p>
     * <ul>
     *   <li>Below version 5, or on a heap member while the carry window is
     *       off, the window of {@link #retentionCap} frames is a minimum,
     *       not a maximum: a frame past the window is evicted
     *       only when its block is posted to L1 ({@link #postedHead}).
     *       Nothing a batcher has not confirmed on L1 is dropped, so the
     *       batcher can always replay from its cursor. The DA-lag guard
     *       bounds the growth: the sealer refuses user records at
     *       {@code daLagBudgetBlocks} past the posted head, so the
     *       stretched window holds at most the budget's blocks plus one
     *       flush.</li>
     *   <li>Under version 5 with the carry window on, or on a member that
     *       carries the bytes ({@link RetainedFrames#carriesBytes}), the
     *       window is the whole rule: a frame past the window is evicted at
     *       once, at any posted head. So the
     *       retained frames have a fixed bound at any DA lag. A reader
     *       below the window gets REPLAY_UNAVAILABLE and recovers from
     *       another source. The DA-lag guard still refuses user records,
     *       but it holds no frame.</li>
     * </ul>
     *
     * <p>The retained frames ride the snapshot ({@link #writeSnapshot}), so
     * a member restored from a snapshot keeps the floor of the snapshot
     * instead of raising it to the restore point. In a
     * {@link RetainedFile}, the snapshot carries the index, and the frames
     * stay in the file.</p>
     */
    private final RetainedFrames retained;
    private final int retentionCap =
        Integer.getInteger("kardamom.cluster.retention", SealerWire.DEFAULT_RETENTION);
    /** First record index / boundary block still guaranteed retained. */
    private long firstRetainedIndex;
    private long firstRetainedBlock;
    /**
     * The last L2 block the batcher confirmed on L1, echoed from the state
     * machine. A frame of a block at or below it may leave the window.
     */
    private long postedHead;
    /**
     * Whether a frame past the window leaves at once, at any posted head:
     * decision version 5 with the carry window on, or on a member that
     * carries the bytes. Before the carry window, the executors and the
     * batcher can read references, which the egress cannot serve, so a
     * heap member keeps the posted rule. The state sets the version and the
     * carry window at a block boundary, and the service echoes them here at
     * the end of that log entry.
     */
    private boolean windowOnly;

    // Staging buffer for egress framing. Reuse it to avoid a per-message
    // allocation on the single cluster service thread.
    private final ExpandableArrayBuffer egressBuffer = new ExpandableArrayBuffer();
    /** The frame codec over {@link #egressBuffer}. */
    private final EgressFrames frames = new EgressFrames(egressBuffer);
    /** The member feed: it carries each retained frame to the paired executor. */
    private final Feed feed;
    /** The pipeline metrics of the service thread. */
    private final SealerMetrics metrics;

    SealerEgress(
            final Cluster cluster,
            final int memberId,
            final RetainedFrames retained,
            final Feed feed,
            final long firstRetainedIndex,
            final long firstRetainedBlock,
            final StartHold hold,
            final SealerMetrics metrics) {
        this.cluster = cluster;
        this.memberId = memberId;
        this.retained = retained;
        this.feed = feed;
        this.metrics = metrics;
        this.backlogs = new SessionBacklogs(
            memberId, SessionBacklogs.STALL_DEADLINE_NS, SessionBacklogs.BYTE_LIMIT, hold, metrics);
        this.firstRetainedIndex = firstRetainedIndex;
        this.firstRetainedBlock = firstRetainedBlock;
    }

    /** Mark a session as a canonical-stream consumer. */
    void addConsumer(final long sessionId) {
        if (consumerSessions.add(sessionId)) {
            targetsStale = true;
        }
    }

    /**
     * Adopt the posted head, and evict with the current rule what the
     * window no longer needs: under the posted rule, the frames past the
     * window whose block is now posted. Log-driven, so every member evicts
     * the same frames at the same point of the log.
     */
    void setPostedHead(final long postedHead) {
        this.postedHead = postedHead;
        prune();
    }

    /**
     * Adopt the eviction rule of decision version {@code version} with the
     * carry window {@code carryWindow}, and evict what the rule no longer
     * keeps. The service calls it at the start, after the retention is
     * restored and before the posted head, and at the end of each log entry
     * that switched the version or the carry window. So every member, from
     * the log or from a snapshot, evicts the same frames at the same point
     * of the log.
     */
    void setRules(final int version, final boolean carryWindow) {
        windowOnly = version == VersionSwitch.CARRY_VERSION && (carryWindow || retained.carriesBytes());
        prune();
    }

    /** Whether a frame past the window leaves at once, at any posted head. */
    boolean windowOnly() {
        return windowOnly;
    }

    /** The frames retained for replay. */
    long retainedCount() {
        return retained.count();
    }

    /** The bytes of the frames retained for replay. */
    long retainedBytes() {
        return retained.bytes();
    }

    /** The oldest record index still retained. */
    long firstRetainedIndex() {
        return firstRetainedIndex;
    }

    /** The oldest boundary block still retained. */
    long firstRetainedBlock() {
        return firstRetainedBlock;
    }

    /** Remove a closed session's consumer mark. */
    void removeConsumer(final long sessionId) {
        consumerSessions.remove(sessionId);
        targetsStale = true;
    }

    /**
     * Send the queued frames that each session takes now, and close each
     * session past a backlog limit. Call it only from a log-driven callback:
     * Aeron rejects an offer or a close from {@code doBackgroundWork}.
     */
    void drainBacklogs() {
        backlogs.drain();
    }

    /**
     * Drop the backlog of a closed session. A close that the service did not
     * ask for ends the session for good, so its response channel loses its
     * count of too-long closes too.
     */
    void dropBacklog(final ClientSession session, final CloseReason closeReason) {
        backlogs.drop(session.id());
        if (closeReason != CloseReason.SERVICE_ACTION) {
            backlogs.forgetChannel(session.responseChannel());
        }
    }

    /** Drop every backlog, because this member's role changed. */
    void dropBacklogs() {
        backlogs.dropAll();
    }

    /**
     * Serve a client replay request.
     * Re-offer every retained frame at or after the requested cursor to the
     * requesting session only, then send a REPLAY_DONE marker. Send
     * REPLAY_AHEAD with the head instead when the cursor is past the head.
     * Send REPLAY_UNAVAILABLE when the cursor does not name one point of
     * the stream, or when the retained frames do not reach it. This runs the
     * same way on every member, from the replicated log, but only the
     * leader's session offers reach the client. {@code upToIndex} and
     * {@code upToBlock} are the state machine's current canonical count and
     * block number, stamped into the REPLAY_DONE marker.
     *
     * <p>This method hands every wanted frame to the offer path at once, in
     * retained order. The frames that the session cannot take now wait in
     * its backlog, and live frames queue behind them, so the session gets
     * the replay, then the REPLAY_DONE marker, then the live stream. The
     * backlog keeps the retained arrays, not copies. A wedged session costs
     * the service thread no wait: its backlog closes it at the stall
     * deadline.</p>
     */
    void handleReplayRequest(
            final ClientSession session,
            final long fromIndex,
            final long fromBlock,
            final long upToIndex,
            final long upToBlock) {
        final ReplayVerdict verdict = replayVerdict(fromIndex, fromBlock, upToIndex, upToBlock);
        switch (verdict.kind()) {
            case AHEAD -> {
                logReplay(session, fromIndex, fromBlock, verdict.reason());
                offerControl(session, SealerWire.EGRESS_KIND_REPLAY_AHEAD, upToIndex, upToBlock);
                return;
            }
            case REFUSED -> {
                refuseReplay(session, fromIndex, fromBlock, verdict.reason());
                return;
            }
            case SERVE -> {
                // The retained frames follow.
            }
        }
        // Count each outcome apart. A dropped replay must not look the same
        // as a served one in these logs.
        final Map<SessionBacklogs.Outcome, Long> outcomes = retained.wanted(fromIndex, fromBlock)
            .map(retained::forSession)
            .map(frame -> backlogs.offer(session, frame))
            .collect(Collectors.groupingBy(
                Function.identity(),
                () -> new EnumMap<>(SessionBacklogs.Outcome.class),
                Collectors.counting()));
        System.out.println("cluster REPLAY memberId=" + memberId
            + " session=" + session.id() + " from=(" + fromIndex + "," + fromBlock
            + ") served=" + outcomes.getOrDefault(SessionBacklogs.Outcome.SENT, 0L)
            + " queued=" + outcomes.getOrDefault(SessionBacklogs.Outcome.QUEUED, 0L)
            + " dropped=" + outcomes.getOrDefault(SessionBacklogs.Outcome.DROPPED, 0L)
            + " retained=" + retained.count());
        offerControl(session, SealerWire.EGRESS_KIND_REPLAY_DONE, upToIndex, upToBlock);
    }

    /**
     * The answer to a replay request from {@code (fromIndex, fromBlock)},
     * with {@code (upToIndex, upToBlock)} as the head: the canonical count
     * and the open block.
     */
    ReplayVerdict replayVerdict(
            final long fromIndex, final long fromBlock, final long upToIndex, final long upToBlock) {
        if (fromIndex > upToIndex || fromBlock > upToBlock) {
            // A consumer ahead of the head applied records that this member
            // does not hold, for example after a wipe or a re-seed. A
            // REPLAY_DONE would let the consumer drop each new record below
            // its cursor as a duplicate and diverge silently. A repair from
            // this stream cannot serve it either, so REPLAY_AHEAD carries the
            // head and stops it.
            return new ReplayVerdict(ReplayVerdict.Kind.AHEAD,
                "AHEAD head=(" + upToIndex + "," + upToBlock + ")");
        }
        final long lowerEnd = retained.boundaryEnd(fromBlock - 1);
        final long upperEnd = retained.boundaryEnd(fromBlock);
        if (!cursorInsideBlock(fromIndex, lowerEnd, upperEnd)) {
            // A consumer resumes at a record index and a block number. The
            // two select frames on separate axes, so a pair that does not
            // name one point of the stream skips records, or applies them
            // twice, and no consumer-side check can see it: the consumer
            // seeds every counter from the same cursor. This member holds
            // the boundaries, so it is the one place that can refuse.
            return new ReplayVerdict(ReplayVerdict.Kind.REFUSED,
                "SKEWED block " + fromBlock + " spans (" + lowerEnd + "," + upperEnd + ")");
        }
        if (fromIndex < firstRetainedIndex || fromBlock < firstRetainedBlock) {
            return new ReplayVerdict(ReplayVerdict.Kind.REFUSED,
                "UNAVAILABLE floor=(" + firstRetainedIndex + "," + firstRetainedBlock + ")");
        }
        return ReplayVerdict.SERVE;
    }

    /**
     * Refuse a replay request with REPLAY_UNAVAILABLE, which carries the
     * retention floors, and log the reason.
     */
    private void refuseReplay(
            final ClientSession session, final long fromIndex, final long fromBlock, final String reason) {
        logReplay(session, fromIndex, fromBlock, reason);
        offerControl(session, SealerWire.EGRESS_KIND_REPLAY_UNAVAILABLE, firstRetainedIndex, firstRetainedBlock);
    }

    /**
     * Log the outcome of a replay request. The log goes to stdout, like the
     * role lines, so the chaos suite can grep it next to its other signals.
     * The service has no other logger.
     */
    private void logReplay(
            final ClientSession session, final long fromIndex, final long fromBlock, final String outcome) {
        System.out.println("cluster REPLAY memberId=" + memberId
            + " session=" + session.id() + " from=(" + fromIndex + "," + fromBlock + ") " + outcome);
    }

    /**
     * Whether a resume at {@code fromIndex} lies inside the block it names.
     * {@code lowerEnd} is the end index of the block before it, and
     * {@code upperEnd} the end index of the block itself; -1 means this
     * member does not retain that boundary, and that side is not checked.
     * A cold start sends the lower end exactly. A reconnect inside an open
     * block sends an index between the two.
     */
    static boolean cursorInsideBlock(final long fromIndex, final long lowerEnd, final long upperEnd) {
        return (lowerEnd < 0 || fromIndex >= lowerEnd) && (upperEnd < 0 || fromIndex <= upperEnd);
    }

    /** Frame and offer a control message {@code kind(1) | a(8) | b(8)}. */
    private void offerControl(final ClientSession session, final byte kind, final long a, final long b) {
        offerToSession(session, frames.control(kind, a, b));
    }

    /**
     * Frame and offer an {@link SealerWire#EGRESS_KIND_CONTIGUITY_REJECT}
     * ({@code kind(1) | sender(20) | nonce(8) | expected(8)}) to the offering
     * session.
     */
    void offerContiguityReject(
            final ClientSession session, final byte[] sender20, final long nonce, final long expected) {
        final MutableDirectBuffer buf = egressBuffer;
        int pos = 0;
        buf.putByte(pos, SealerWire.EGRESS_KIND_CONTIGUITY_REJECT);
        pos += Byte.BYTES;
        buf.putBytes(pos, sender20);
        pos += CanonicalSealerState.SENDER_LEN;
        buf.putLong(pos, nonce, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, expected, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        offerToSession(session, pos);
    }

    /**
     * Frame and offer a past-deadline reject to the offering session:
     * {@code [kind:7][sender:20][nonce:u64 LE][max_inclusion_block:u64 LE][at_block:u64 LE]}.
     */
    void offerPastDeadline(
            final ClientSession session,
            final byte[] sender20,
            final long nonce,
            final long maxInclusionBlock,
            final long atBlock) {
        final MutableDirectBuffer buf = egressBuffer;
        int pos = 0;
        buf.putByte(pos, SealerWire.EGRESS_KIND_PAST_DEADLINE);
        pos += Byte.BYTES;
        buf.putBytes(pos, sender20);
        pos += CanonicalSealerState.SENDER_LEN;
        buf.putLong(pos, nonce, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, maxInclusionBlock, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, atBlock, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        offerToSession(session, pos);
    }

    /**
     * Frame and offer the refusal of a transaction reference of record
     * type 0 to the offering session:
     * {@code [kind:18][canonical_id:32][sender:20][nonce:u64 LE][decision_version:u8]}.
     */
    void offerRefRefused(
            final ClientSession session,
            final byte[] canonicalId,
            final byte[] sender20,
            final long nonce,
            final int decisionVersion) {
        offerToSession(session, frames.refRefused(canonicalId, sender20, nonce, decisionVersion));
    }

    /**
     * Frame and offer a window-full reject to the offering session:
     * {@code [kind:8][sender:20][nonce:u64 LE]}. Back-pressure, not a
     * verdict: the sequencer republishes.
     */
    void offerWindowFull(final ClientSession session, final byte[] sender20, final long nonce) {
        final MutableDirectBuffer buf = egressBuffer;
        int pos = 0;
        buf.putByte(pos, SealerWire.EGRESS_KIND_WINDOW_FULL);
        pos += Byte.BYTES;
        buf.putBytes(pos, sender20);
        pos += CanonicalSealerState.SENDER_LEN;
        buf.putLong(pos, nonce, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        offerToSession(session, pos);
    }

    /**
     * Frame and offer an origin-gap reject to the offering session:
     * {@code [kind:12][offered_origin:u64 LE][expected_origin:u64 LE]}.
     */
    void offerOriginGap(final ClientSession session, final long offeredOrigin, final long expectedOrigin) {
        offerControl(session, SealerWire.EGRESS_KIND_ORIGIN_GAP, offeredOrigin, expectedOrigin);
    }

    /**
     * Frame and offer a remote-origin reject to the offering session:
     * {@code [kind:6][origin:u64 LE][first_seq:u64 LE][expected:u64 LE][reason:u8]}.
     */
    void offerRemoteOriginReject(
            final ClientSession session,
            final long originChainId,
            final long firstSeq,
            final long expectedNextSeq,
            final byte reason) {
        final MutableDirectBuffer buf = egressBuffer;
        int pos = 0;
        buf.putByte(pos, SealerWire.EGRESS_KIND_REMOTE_ORIGIN_REJECT);
        pos += Byte.BYTES;
        buf.putLong(pos, originChainId, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, firstSeq, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, expectedNextSeq, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putByte(pos, reason);
        pos += Byte.BYTES;
        offerToSession(session, pos);
    }

    /**
     * Retain the frame staged in {@link #egressBuffer} for future replays,
     * hand it to the member feed, and return its length. The window is
     * pruned after the add ({@link #prune}).
     */
    private int retain(final int length) {
        retained.append(egressBuffer, length);
        feed.onFrame(egressBuffer, length);
        prune();
        return length;
    }

    /**
     * Evict from the front while the window is over its cap and the oldest
     * frame may leave: at once with {@link #windowOnly}, else when it is posted.
     */
    private void prune() {
        while (retained.count() > retentionCap && (windowOnly || oldestIsPosted())) {
            evictFront();
        }
    }

    /** Evict the oldest frame, and move the floor of its axis past it. */
    private void evictFront() {
        final boolean boundary = retained.frontIsBoundary();
        final long key = retained.frontKey();
        retained.evictFront();
        if (boundary) {
            firstRetainedBlock = key + 1;
        } else {
            firstRetainedIndex = key + 1;
        }
    }

    /**
     * Whether the oldest retained frame belongs to a posted block. A record
     * at the front was emitted after the last evicted boundary and before
     * the boundary of {@link #firstRetainedBlock}, so that is its block.
     */
    private boolean oldestIsPosted() {
        final long block = retained.frontIsBoundary() ? retained.frontKey() : firstRetainedBlock;
        return block <= postedHead;
    }

    /**
     * Frame and broadcast the chain's status to every session, or offer it
     * to one session. The frame carries the record-lag tail and the
     * snapshot flag; with a retained file, the retained bytes follow
     * ({@link RetainedFrames#status}). Not retained: a session that
     * announces itself gets the current one.
     */
    void offerStatus(final ClusterStatus status) {
        broadcastStatus(status, false);
    }

    /**
     * Broadcast the status with the snapshot notice: a snapshot follows,
     * and no frame leaves the leader until the snapshot is complete.
     */
    void offerSnapshotNotice(final ClusterStatus status) {
        broadcastStatus(status, true);
    }

    /** {@link #offerStatus(ClusterStatus)} to one session. */
    void offerStatus(final ClientSession session, final ClusterStatus status) {
        offerToSession(session, retained.status(frames, status, false, postedHead));
    }

    private void broadcastStatus(final ClusterStatus status, final boolean snapshotFollows) {
        final int len = retained.status(frames, status, snapshotFollows, postedHead);
        for (final ClientSession session : cluster.clientSessions()) {
            offerToSession(session, len);
        }
    }

    /**
     * Frame and offer a DA-lag reject to the offering session:
     * {@code [kind:10][sender:20][nonce:u64 LE][sealed_head:u64 LE][posted_head:u64 LE][budget:u64 LE]}.
     */
    void offerDaLagReject(
            final ClientSession session,
            final byte[] sender20,
            final long nonce,
            final ClusterStatus status) {
        final MutableDirectBuffer buf = egressBuffer;
        int pos = 0;
        buf.putByte(pos, SealerWire.EGRESS_KIND_DA_LAG_REJECT);
        pos += Byte.BYTES;
        buf.putBytes(pos, sender20);
        pos += CanonicalSealerState.SENDER_LEN;
        buf.putLong(pos, nonce, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, status.sealedHead(), ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, status.postedHead(), ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, status.budgetBlocks(), ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        offerToSession(session, pos);
    }

    /**
     * Frame and offer a record-lag reject to the offering session:
     * {@code [kind:13][sender:20][nonce:u64 LE][sealed_index:u64 LE][recorded_index:u64 LE][budget:u64 LE]}.
     *
     * @param sealedIndex the last ordered canonical index
     */
    void offerRecordLagReject(
            final ClientSession session,
            final byte[] sender20,
            final long nonce,
            final long sealedIndex,
            final ClusterStatus status) {
        final MutableDirectBuffer buf = egressBuffer;
        int pos = 0;
        buf.putByte(pos, SealerWire.EGRESS_KIND_RECORD_LAG_REJECT);
        pos += Byte.BYTES;
        buf.putBytes(pos, sender20);
        pos += CanonicalSealerState.SENDER_LEN;
        buf.putLong(pos, nonce, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, sealedIndex, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, status.bestRecorded(), ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, status.recordLagBudget(), ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        offerToSession(session, pos);
    }

    /**
     * The retention section of a snapshot, after the state section. Every
     * member retains the same frames at the same log position, so every
     * member writes the same bytes.
     */
    byte[] writeSnapshot() {
        return retained.writeSnapshot(firstRetainedIndex, firstRetainedBlock);
    }

    /**
     * Restore the retention from the section that a snapshot carries, and
     * set the floors. The floors start at the restore point, where the
     * constructor put them.
     */
    void restore(final ByteBuffer section) {
        final RetainedFrames.Floors floors =
            retained.restore(section, new RetainedFrames.Floors(firstRetainedIndex, firstRetainedBlock));
        firstRetainedIndex = floors.index();
        firstRetainedBlock = floors.block();
    }

    /** Offer the sealer's rules, decision version {@code version}, to {@code session}. */
    void offerSealerInfo(final ClientSession session, final int version) {
        offerToSession(session, frames.sealerInfo(version, session.id()));
    }

    /** Offer the sealer's rules, decision version {@code version}, to every open session. */
    void offerSealerInfoToAll(final int version) {
        cluster.clientSessions().forEach(session -> offerSealerInfo(session, version));
    }

    /** Open an empty retention: a start at genesis or from a seed. */
    void openEmpty() {
        retained.clear();
    }

    /**
     * Retain a relayed record, and offer it to each consumer in the form
     * that an egress session gets ({@link RetainedFrames#forSession}).
     */
    void offerRelayed(final Relayed relayed) {
        offerToConsumers(retained.forSession(egressBuffer, retain(frames.relayed(relayed))));
    }

    /**
     * Relay {@code length} bytes of {@code src} at {@code offset} as the
     * payload of record {@code index}. The record path of decision version
     * 4 allocates nothing here: the frame goes from the staging buffer to
     * the retained ring and to each session.
     */
    void offerRelayed(final long index, final DirectBuffer src, final int offset, final int length) {
        offerToConsumers(retained.forSession(egressBuffer, retain(frames.relayed(index, src, offset, length))));
    }

    /**
     * Relay the full reference that {@code offer} views as the canonical
     * entry of record {@code index}. The record path of decision version 5
     * allocates nothing here: the id and the envelope go from the log
     * buffer to the staging buffer, then to the retained ring and to each
     * session.
     */
    void offerCarried(final long index, final TxRefOffer offer) {
        offerToConsumers(retained.forSession(egressBuffer, retain(frames.carried(index, offer))));
    }

    /**
     * Offer {@code frame} to every session that announced itself as a
     * canonical-stream consumer: the executors, the validator, and ingress
     * observers. This excludes the publisher-only sequencer sessions, which
     * drop every record they get. On a saturated leader, the per-session
     * unicast offer is the dominant cost, so cutting the session list this
     * way directly raises the ceiling.
     *
     * <p>This falls back to a broadcast to all sessions while no consumer
     * has announced itself, such as the window right after a restart, or a
     * mixed-version deploy whose clients never send SUBSCRIBE, so that
     * nothing goes unserved. The fan-out must reach beyond the sending
     * session because the executor replicas consume the canonical stream on
     * their own sessions.</p>
     */
    private void offerToConsumers(final int length) {
        final long startedNs = metrics.egressOfferStart();
        for (final ClientSession session : targets()) {
            backlogs.offer(session, egressBuffer, length);
        }
        metrics.egressOfferDone(startedNs);
    }

    /** The sessions of {@link #offerToConsumers}, built again after a session change. */
    private ClientSession[] targets() {
        if (targetsStale) {
            targets = cluster.clientSessions().stream()
                .filter(session -> consumerSessions.isEmpty() || consumerSessions.contains(session.id()))
                .toArray(ClientSession[]::new);
            targetsStale = false;
        }
        return targets;
    }

    /** A session opened or closed: the consumer fan-out builds its session list again. */
    void sessionsChanged() {
        targetsStale = true;
    }

    void offerBoundary(final Boundary boundary) {
        final int length = retain(frameBoundary(boundary));
        // Boundaries stay broadcast to every session, unlike relayed records.
        // There is at most one per tick, and the sequencer's boundary-only
        // lag feed (connect_with_egress_kind_filter) consumes them without a
        // SUBSCRIBE announcement. Filtering boundaries by consumer would
        // leave it unserved.
        cluster.clientSessions().forEach(session -> backlogs.offer(session, egressBuffer, length));
    }

    /**
     * Frame a {@link Boundary} into {@link #egressBuffer}:
     * {@code kind(1) | blockNumber(8) | endTxIdx(8) | l2Timestamp(8) | l1Origin(8)}.
     */
    private int frameBoundary(final Boundary boundary) {
        final MutableDirectBuffer buf = egressBuffer;
        int pos = 0;
        buf.putByte(pos, SealerWire.EGRESS_KIND_BOUNDARY);
        pos += Byte.BYTES;
        buf.putLong(pos, boundary.blockNumber, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, boundary.endTxIdx, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, boundary.l2Timestamp, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, boundary.l1Origin, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        return pos;
    }

    /**
     * Offer the frame staged in {@link #egressBuffer} to one session. The
     * frame is copied out first, because a backlog keeps the array it gets,
     * and the next frame reuses the staging buffer.
     */
    private void offerToSession(final ClientSession session, final int length) {
        backlogs.offer(session, frames.copy(length));
    }
}
