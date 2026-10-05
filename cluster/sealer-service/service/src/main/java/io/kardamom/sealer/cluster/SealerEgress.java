package io.kardamom.sealer.cluster;

import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.kardamom.sealer.Boundary;
import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.ClusterStatus;
import io.kardamom.sealer.Relayed;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
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
 * one clustered-service thread (Aeron {@code ClusteredService} callbacks),
 * just as when this logic lived inside the service. So the shared staging
 * buffer, the retained deque, and the consumer set are unsynchronized on
 * purpose. Do not call into this class from any other thread.</p>
 */
final class SealerEgress {


    /** One retained, already-framed egress frame (record or boundary). */
    private static final class RetainedFrame {
        final byte[] frame;
        final boolean boundary;
        final long key; // Record index, or boundary block number.

        RetainedFrame(final byte[] frame, final boolean boundary, final long key) {
            this.frame = frame;
            this.boundary = boundary;
            this.key = key;
        }

        /** Whether a replay from {@code (fromIndex, fromBlock)} includes this frame. */
        boolean wanted(final long fromIndex, final long fromBlock) {
            return boundary ? key >= fromBlock : key >= fromIndex;
        }
    }

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

    /**
     * Retained egress frames, in emission order, for replay requests from
     * connecting or reconnecting clients.
     * Without replay, frames committed while a client had no session are
     * lost forever, leaving an unrecoverable gap in its canonical stream.
     * This is deterministic across members, since it is derived from the
     * replicated log.
     *
     * <p>The window of {@link #retentionCap} frames is a minimum, not a
     * maximum: a frame past the window is evicted only when its block is
     * posted to L1 ({@link #postedHead}). Nothing a batcher has not
     * confirmed on L1 is dropped, so the batcher can always replay from
     * its cursor. The DA-lag guard bounds the growth: the sealer refuses
     * user records at {@code daLagBudgetBlocks} past the posted head, so
     * the stretched window holds at most the budget's blocks plus one
     * flush, in heap.</p>
     *
     * <p>The frames above the posted head ride the snapshot
     * ({@link #writeSnapshot}), so a member restored from a snapshot keeps
     * the floor at the posted head instead of raising it to the restore
     * point.</p>
     */
    private final java.util.ArrayDeque<RetainedFrame> retained = new java.util.ArrayDeque<>();
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

    // Staging buffer for egress framing. Reuse it to avoid a per-message
    // allocation on the single cluster service thread.
    private final ExpandableArrayBuffer egressBuffer = new ExpandableArrayBuffer();

    SealerEgress(
            final Cluster cluster,
            final int memberId,
            final long firstRetainedIndex,
            final long firstRetainedBlock) {
        this.cluster = cluster;
        this.memberId = memberId;
        this.backlogs = new SessionBacklogs(
            memberId, SessionBacklogs.STALL_DEADLINE_NS, SessionBacklogs.BYTE_LIMIT);
        this.firstRetainedIndex = firstRetainedIndex;
        this.firstRetainedBlock = firstRetainedBlock;
    }

    /** Mark a session as a canonical-stream consumer. */
    void addConsumer(final long sessionId) {
        consumerSessions.add(sessionId);
    }

    /**
     * Adopt the posted head as the retention floor, and evict what the
     * window no longer needs: frames past the window whose block is now
     * posted. Log-driven, so every member evicts the same frames at the
     * same point of the log.
     */
    void setPostedHead(final long postedHead) {
        this.postedHead = postedHead;
        prune();
    }

    /** The frames retained for replay. */
    int retainedCount() {
        return retained.size();
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
    }

    /**
     * Send the queued frames that each session takes now, and close each
     * session past a backlog limit. Call it only from a log-driven callback:
     * Aeron rejects an offer or a close from {@code doBackgroundWork}.
     */
    void drainBacklogs() {
        backlogs.drain();
    }

    /** Drop the backlog of a closed session. */
    void dropBacklog(final long sessionId) {
        backlogs.drop(sessionId);
    }

    /** Drop every backlog, because this member's role changed. */
    void dropBacklogs() {
        backlogs.dropAll();
    }

    /**
     * Serve a client replay request.
     * Re-offer every retained frame at or after the requested cursor to the
     * requesting session only, then send a REPLAY_DONE marker, or
     * REPLAY_UNAVAILABLE when eviction has outrun the request. This runs the
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
        final long lowerEnd = retainedBoundaryEnd(fromBlock - 1);
        final long upperEnd = retainedBoundaryEnd(fromBlock);
        if (!cursorInsideBlock(fromIndex, lowerEnd, upperEnd)) {
            // A consumer resumes at a record index and a block number. The
            // two select frames on separate axes, so a pair that does not
            // name one point of the stream skips records, or applies them
            // twice, and no consumer-side check can see it: the consumer
            // seeds every counter from the same cursor. This member holds
            // the boundaries, so it is the one place that can refuse.
            System.out.println("cluster REPLAY memberId=" + memberId
                + " session=" + session.id() + " from=(" + fromIndex + "," + fromBlock
                + ") SKEWED block " + fromBlock + " spans (" + lowerEnd + "," + upperEnd + ")");
            offerControl(session, SealerWire.EGRESS_KIND_REPLAY_UNAVAILABLE, firstRetainedIndex, firstRetainedBlock);
            return;
        }
        if (fromIndex < firstRetainedIndex || fromBlock < firstRetainedBlock) {
            // Log to stdout, like the role lines, so the chaos suite can grep
            // it next to its other signals. The service has no other logger.
            System.out.println("cluster REPLAY memberId=" + memberId
                + " session=" + session.id() + " from=(" + fromIndex + "," + fromBlock
                + ") UNAVAILABLE floor=(" + firstRetainedIndex + "," + firstRetainedBlock + ")");
            offerControl(session, SealerWire.EGRESS_KIND_REPLAY_UNAVAILABLE, firstRetainedIndex, firstRetainedBlock);
            return;
        }
        // Count each outcome apart. A dropped replay must not look the same
        // as a served one in these logs.
        final Map<SessionBacklogs.Outcome, Long> outcomes = retained.stream()
            .filter(f -> f.wanted(fromIndex, fromBlock))
            .map(f -> backlogs.offer(session, f.frame))
            .collect(Collectors.groupingBy(
                Function.identity(),
                () -> new EnumMap<>(SessionBacklogs.Outcome.class),
                Collectors.counting()));
        System.out.println("cluster REPLAY memberId=" + memberId
            + " session=" + session.id() + " from=(" + fromIndex + "," + fromBlock
            + ") served=" + outcomes.getOrDefault(SessionBacklogs.Outcome.SENT, 0L)
            + " queued=" + outcomes.getOrDefault(SessionBacklogs.Outcome.QUEUED, 0L)
            + " dropped=" + outcomes.getOrDefault(SessionBacklogs.Outcome.DROPPED, 0L)
            + " retained=" + retained.size());
        offerControl(session, SealerWire.EGRESS_KIND_REPLAY_DONE, upToIndex, upToBlock);
    }

    /** Byte offset of {@code endTxIdx} in a boundary frame: after the kind and the block number. */
    private static final int BOUNDARY_END_OFFSET = Byte.BYTES + Long.BYTES;

    /**
     * The end index of the retained boundary of {@code block}, or -1 when
     * this member does not retain it: block 0 has no boundary, an old one
     * aged out, and the open block has none yet.
     */
    private long retainedBoundaryEnd(final long block) {
        for (final RetainedFrame f : retained) {
            if (f.boundary && f.key == block) {
                return java.nio.ByteBuffer.wrap(f.frame)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .getLong(BOUNDARY_END_OFFSET);
            }
        }
        return -1L;
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
        final MutableDirectBuffer buf = egressBuffer;
        int pos = 0;
        buf.putByte(pos, kind);
        pos += Byte.BYTES;
        buf.putLong(pos, a, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, b, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        offerToSession(session, pos);
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
     * Retain an already-framed egress frame for future replays. The window
     * is pruned after the add: a frame leaves only when it is past the
     * window and its block is posted.
     */
    private void retain(final int length, final boolean boundary, final long key) {
        final byte[] copy = new byte[length];
        egressBuffer.getBytes(0, copy);
        retained.addLast(new RetainedFrame(copy, boundary, key));
        prune();
    }

    /** Evict from the front while the window is over its cap and the oldest frame is posted. */
    private void prune() {
        while (retained.size() > retentionCap && isPosted(retained.peekFirst())) {
            final RetainedFrame evicted = retained.removeFirst();
            if (evicted.boundary) {
                firstRetainedBlock = evicted.key + 1;
            } else {
                firstRetainedIndex = evicted.key + 1;
            }
        }
    }

    /**
     * Whether the oldest retained frame belongs to a posted block. A record
     * at the front of the deque was emitted after the last evicted boundary
     * and before the boundary of {@link #firstRetainedBlock}, so that is
     * its block.
     */
    private boolean isPosted(final RetainedFrame oldest) {
        final long block = oldest.boundary ? oldest.key : firstRetainedBlock;
        return block <= postedHead;
    }

    /**
     * Frame and broadcast the chain's status to every session, or offer it
     * to one session: {@code kind(1) | posted_head(8) | sealed_head(8) |
     * budget(8) | halted(1) | retained(8) | floor_index(8) | floor_block(8)}.
     * Not retained: a session that announces itself gets the current one.
     */
    void offerStatus(final ClusterStatus status) {
        final int len = frameStatus(status);
        for (final ClientSession session : cluster.clientSessions()) {
            offerToSession(session, len);
        }
    }

    /** {@link #offerStatus(ClusterStatus)} to one session. */
    void offerStatus(final ClientSession session, final ClusterStatus status) {
        offerToSession(session, frameStatus(status));
    }

    private int frameStatus(final ClusterStatus status) {
        final MutableDirectBuffer buf = egressBuffer;
        int pos = 0;
        buf.putByte(pos, SealerWire.EGRESS_KIND_STATUS);
        pos += Byte.BYTES;
        buf.putLong(pos, status.postedHead(), ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, status.sealedHead(), ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, status.budgetBlocks(), ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putByte(pos, status.halted() ? (byte) 1 : (byte) 0);
        pos += Byte.BYTES;
        buf.putLong(pos, status.retainedFrames(), ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, status.floorIndex(), ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, status.floorBlock(), ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        return pos;
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
     * Append the retained frames to a snapshot, after the state section:
     * {@code count(4) | count * (boundary(1) | key(8) | len(4) | frame)},
     * big-endian like the state. Every member retains the same frames at
     * the same log position, so every member writes the same bytes.
     */
    byte[] writeSnapshot() {
        int size = 4;
        for (final RetainedFrame f : retained) {
            size += 1 + 8 + 4 + f.frame.length;
        }
        final ByteBuffer buf = ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN);
        buf.putInt(retained.size());
        for (final RetainedFrame f : retained) {
            buf.put(f.boundary ? (byte) 1 : (byte) 0);
            buf.putLong(f.key);
            buf.putInt(f.frame.length);
            buf.put(f.frame);
        }
        return buf.array();
    }

    /**
     * Restore the retained frames a snapshot carries, and set the floors
     * from the oldest of them. A snapshot without a retention section (a
     * version before 8) leaves the floors at the restore point, where the
     * constructor put them.
     */
    void readSnapshot(final ByteBuffer buf) {
        if (!buf.hasRemaining()) {
            return;
        }
        final int count = buf.getInt();
        for (int i = 0; i < count; i++) {
            final boolean boundary = buf.get() != 0;
            final long key = buf.getLong();
            final byte[] frame = new byte[buf.getInt()];
            buf.get(frame);
            retained.addLast(new RetainedFrame(frame, boundary, key));
        }
        setFloorsFromRetained();
    }

    /**
     * The floors after a restore: the oldest retained record and boundary.
     * A record at the front belongs to the block of the first retained
     * boundary; with no retained boundary, the floors stay at the restore
     * point.
     */
    private void setFloorsFromRetained() {
        firstRetainedBlock = retained.stream()
            .filter(f -> f.boundary)
            .mapToLong(f -> f.key)
            .reduce(firstRetainedBlock, Math::min);
        firstRetainedIndex = retained.stream()
            .filter(f -> !f.boundary)
            .mapToLong(f -> f.key)
            .reduce(firstRetainedIndex, Math::min);
    }

    void offerRelayed(final Relayed relayed) {
        final int len = frameRelayed(relayed);
        retain(len, false, relayed.index);
        offerToConsumers(lastRetained());
    }

    /**
     * The frame that {@link #retain} added last. The offers send this array:
     * a backlog keeps the array it gets, and a retained array never changes.
     */
    private byte[] lastRetained() {
        return retained.peekLast().frame;
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
    private void offerToConsumers(final byte[] frame) {
        cluster.clientSessions().stream()
            .filter(session -> consumerSessions.isEmpty() || consumerSessions.contains(session.id()))
            .forEach(session -> backlogs.offer(session, frame));
    }

    /**
     * Frame a {@link Relayed} into {@link #egressBuffer}:
     * {@code kind(1) | index(8) | payloadLen(4) | payload[]}. The payload is
     * copied through as is.
     */
    private int frameRelayed(final Relayed relayed) {
        final MutableDirectBuffer buf = egressBuffer;
        int pos = 0;
        buf.putByte(pos, SealerWire.EGRESS_KIND_RELAYED);
        pos += Byte.BYTES;
        buf.putLong(pos, relayed.index, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putInt(pos, relayed.payload.length, ByteOrder.LITTLE_ENDIAN);
        pos += Integer.BYTES;
        if (relayed.payload.length > 0) {
            buf.putBytes(pos, relayed.payload);
            pos += relayed.payload.length;
        }
        return pos;
    }

    void offerBoundary(final Boundary boundary) {
        final int len = frameBoundary(boundary);
        retain(len, true, boundary.blockNumber);
        final byte[] frame = lastRetained();
        // Boundaries stay broadcast to every session, unlike relayed records.
        // There is at most one per tick, and the sequencer's boundary-only
        // lag feed (connect_with_egress_kind_filter) consumes them without a
        // SUBSCRIBE announcement. Filtering boundaries by consumer would
        // leave it unserved.
        cluster.clientSessions().forEach(session -> backlogs.offer(session, frame));
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
        backlogs.offer(session, staged(length));
    }

    /** A copy of the first {@code length} bytes of {@link #egressBuffer}. */
    private byte[] staged(final int length) {
        final byte[] frame = new byte[length];
        egressBuffer.getBytes(0, frame);
        return frame;
    }
}
