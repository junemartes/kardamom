package io.kardamom.sealer.cluster;

import io.aeron.Publication;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.kardamom.sealer.Boundary;
import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.Relayed;
import java.nio.ByteOrder;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.LongHashSet;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * The egress layer of {@link SealerClusteredService}.
 * It frames relayed records, boundaries, and control messages, offers them
 * with the deadline-then-close semantics documented on
 * {@link #OFFER_DEADLINE_NS}, targets the record fan-out at announced
 * consumers, and keeps a bounded set of framed egress to serve client replay
 * requests.
 *
 * <p>This class is single-threaded by design: every method here runs on the
 * one clustered-service thread (Aeron {@code ClusteredService} callbacks),
 * just as when this logic lived inside the service. So the shared staging
 * buffer, the retained deque, and the consumer set are unsynchronized on
 * purpose. Do not call into this class from any other thread.</p>
 */
final class SealerEgress {

    /**
     * Per-frame egress-offer deadline, per session.
     *
     * <p>Offers run on the single clustered-service thread. An unbounded
     * retry against one wedged client session (its egress image full because
     * the subscriber stopped draining) would block record relaying and the
     * boundary tick for the whole cluster. But silently dropping the frame
     * is worse: a client that misses a boundary seals two blocks as one, and
     * provably diverges. A retry bounded only by a small number of idle
     * cycles is not safe either, since it can expire during an ordinary,
     * sub-millisecond burst of back-pressure.</p>
     *
     * <p>So this retries up to a real deadline, and on timeout closes the
     * session. This is an explicit signal the client can act on: it
     * reconnects, and the executor's boundary-alignment fail-stop plus
     * archive crash recovery self-heals. This is never a silent gap. The
     * wall clock is safe to use here, because egress offers are member-local
     * IO (only the leader's offers reach clients), not replicated state.</p>
     *
     * <p>The one-second grace period keeps the cost of a dead session to one
     * deadline before it closes, while not closing a live client that is
     * only riding out a brief CPU spike. A close forces that client through
     * reconnect, fail-stop, and crash recovery, so false positives are
     * costly and should stay rare.</p>
     */
    private static final long OFFER_DEADLINE_NS = java.util.concurrent.TimeUnit.SECONDS.toNanos(1);

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
    }

    private final Cluster cluster;
    /** This cluster member's id, logged on egress operational signals. */
    private final int memberId;

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
     * replicated log. It is not snapshotted (v1): a member restarted from a
     * snapshot sets its retention floors from the restored state (see
     * {@link SealerClusteredService#onStart}) and serves REPLAY_UNAVAILABLE
     * for pre-restart ranges instead.
     */
    private final java.util.ArrayDeque<RetainedFrame> retained = new java.util.ArrayDeque<>();
    private final int retentionCap =
        Integer.getInteger("kardamom.cluster.retention", SealerWire.DEFAULT_RETENTION);
    /** First record index / boundary block still guaranteed retained. */
    private long firstRetainedIndex;
    private long firstRetainedBlock;

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
        this.firstRetainedIndex = firstRetainedIndex;
        this.firstRetainedBlock = firstRetainedBlock;
    }

    /** Mark a session as a canonical-stream consumer. */
    void addConsumer(final long sessionId) {
        consumerSessions.add(sessionId);
    }

    /** Remove a closed session's consumer mark. */
    void removeConsumer(final long sessionId) {
        consumerSessions.remove(sessionId);
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
     * <p>This method serves the replay synchronously. A prior attempt at a
     * timer-driven chunked drain also made live broadcasts skip mid-replay
     * sessions, which changed the steady-state egress flow and caused
     * consumers to freeze at their first record. Correctness matters more
     * than the leader-stall optimization that change was after. In
     * practice, the stall stays small: a wedged consumer costs one
     * {@link #OFFER_DEADLINE_NS} on its first frame and is then closed
     * (later offers return CLOSED and are skipped right away), and healthy
     * consumers drain retained frames at line rate.</p>
     */
    void handleReplayRequest(
            final ClientSession session,
            final long fromIndex,
            final long fromBlock,
            final long upToIndex,
            final long upToBlock) {
        if (fromIndex < firstRetainedIndex || fromBlock < firstRetainedBlock) {
            // Log to stdout, like the role lines, so the chaos suite can grep
            // it next to its other signals. The service has no other logger.
            System.out.println("cluster REPLAY memberId=" + memberId
                + " session=" + session.id() + " from=(" + fromIndex + "," + fromBlock
                + ") UNAVAILABLE floor=(" + firstRetainedIndex + "," + firstRetainedBlock + ")");
            offerControl(session, SealerWire.EGRESS_KIND_REPLAY_UNAVAILABLE, firstRetainedIndex, firstRetainedBlock);
            return;
        }
        long served = 0;
        long dropped = 0;
        for (final RetainedFrame f : retained) {
            final boolean wanted = f.boundary ? f.key >= fromBlock : f.key >= fromIndex;
            if (wanted) {
                // Count delivered frames, not attempted ones. An offer into a
                // closed or back-pressured session returns false. Counting it
                // as served would make a wholesale-dropped replay look the
                // same as a successful one in these logs.
                if (offerBytesToSession(session, f.frame)) {
                    served++;
                } else {
                    dropped++;
                }
            }
        }
        System.out.println("cluster REPLAY memberId=" + memberId
            + " session=" + session.id() + " from=(" + fromIndex + "," + fromBlock
            + ") served=" + served + " dropped=" + dropped
            + " retained=" + retained.size());
        offerControl(session, SealerWire.EGRESS_KIND_REPLAY_DONE, upToIndex, upToBlock);
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

    /** Retain an already-framed egress frame for future replays, up to a limit. */
    private void retain(final int length, final boolean boundary, final long key) {
        final byte[] copy = new byte[length];
        egressBuffer.getBytes(0, copy);
        retained.addLast(new RetainedFrame(copy, boundary, key));
        while (retained.size() > retentionCap) {
            final RetainedFrame evicted = retained.removeFirst();
            if (evicted.boundary) {
                firstRetainedBlock = evicted.key + 1;
            } else {
                firstRetainedIndex = evicted.key + 1;
            }
        }
    }

    void offerRelayed(final Relayed relayed) {
        final int len = frameRelayed(relayed);
        retain(len, false, relayed.index);
        offerToConsumers(len);
    }

    /**
     * Offer the frame staged in {@link #egressBuffer} to every session that
     * announced itself as a canonical-stream consumer: the executors, the
     * validator, and ingress observers. This excludes the publisher-only
     * sequencer sessions, which used to receive, and then drop, every
     * record. On a saturated leader, the per-session unicast offer is the
     * dominant cost, so cutting the session list this way directly raises
     * the ceiling.
     *
     * <p>This falls back to a broadcast to all sessions while no consumer
     * has announced itself, such as the window right after a restart, or a
     * mixed-version deploy whose clients never send SUBSCRIBE, so that
     * nothing goes unserved. The fan-out must reach beyond the sending
     * session because the executor replicas consume the canonical stream on
     * their own sessions.</p>
     */
    private void offerToConsumers(final int len) {
        if (consumerSessions.isEmpty()) {
            for (final ClientSession session : cluster.clientSessions()) {
                offerToSession(session, len);
            }
            return;
        }
        for (final ClientSession session : cluster.clientSessions()) {
            if (consumerSessions.contains(session.id())) {
                offerToSession(session, len);
            }
        }
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
        // Boundaries stay broadcast to every session, unlike relayed records.
        // There is at most one per tick, and the sequencer's boundary-only
        // lag feed (connect_with_egress_kind_filter) consumes them without a
        // SUBSCRIBE announcement. Filtering boundaries by consumer would
        // leave it unserved.
        for (final ClientSession session : cluster.clientSessions()) {
            offerToSession(session, len);
        }
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
     * Offer one frame to one session, with the deadline-then-close semantics
     * described on {@link #OFFER_DEADLINE_NS}. This is the single offer
     * loop: both egress paths, the staged buffer and the retained raw frame,
     * go through it, so the close semantics cannot drift between them.
     *
     * <p>On a terminal result, CLOSED means the session is already gone. Any
     * other terminal result, such as MAX_POSITION_EXCEEDED (the egress
     * publication hit its position limit and is now permanently dead), must
     * close the session. Returning silently would leave a zombie session,
     * kept alive by ingress keep-alives, while every frame for it is
     * dropped.</p>
     *
     * <p>When the deadline runs out under persistent back-pressure, this
     * session's subscriber has stopped draining. Close it instead of
     * dropping frames, since a gap would be silent corruption. Note that
     * the close event may never reach the client, because it rides the same
     * wedged egress. The client's delivered-frame liveness watchdog is the
     * actual recovery path.</p>
     */
    private boolean offerWithDeadline(
        final ClientSession session, final DirectBuffer buffer, final int length) {
        final long deadline = System.nanoTime() + OFFER_DEADLINE_NS;
        long result;
        do {
            result = session.offer(buffer, 0, length);
            if (result >= 0) {
                return true;
            }
            if (!retryable(result)) {
                if (result != Publication.CLOSED) {
                    closeSessionLoudly(session, "terminal offer result " + result);
                }
                return false;
            }
        } while (System.nanoTime() < deadline);
        closeSessionLoudly(session, "offer deadline exhausted (back-pressure)");
        return false;
    }

    /** Offer a retained raw frame through {@link #offerWithDeadline}. */
    private boolean offerBytesToSession(final ClientSession session, final byte[] frame) {
        return offerWithDeadline(session, new UnsafeBuffer(frame), frame.length);
    }

    /**
     * Close a session, and log the reason on stdout.
     * The SessionEvent(CLOSED) that the consensus module emits for the
     * client travels over the very egress publication that just failed, so
     * the client may never see it. Its liveness watchdog is what actually
     * recovers it. This log line is then the only durable record of why the
     * session died.
     */
    private void closeSessionLoudly(final ClientSession session, final String reason) {
        System.out.println("cluster EGRESS-CLOSE memberId=" + memberId
            + " session=" + session.id() + " reason=" + reason);
        session.close();
    }

    /** Offer the staged {@code egressBuffer} head through {@link #offerWithDeadline}. */
    private void offerToSession(final ClientSession session, final int length) {
        offerWithDeadline(session, egressBuffer, length);
    }

    /** Whether a negative offer result is retryable within the deadline. */
    private boolean retryable(final long offerResult) {
        // BACK_PRESSURED and ADMIN_ACTION are transient flow control.
        // NOT_CONNECTED is also retryable within the deadline: an egress
        // publication is legitimately unconnected for a moment at session
        // open, and after a leader failover while the new leader re-creates
        // it. But a session whose egress never (re)connects within the
        // deadline must be closed by the caller, not skipped. Its
        // keep-alives still flow through ingress, so the consensus module
        // keeps it alive while this service silently drops every frame for
        // it, a zombie session the client cannot detect. CLOSED and
        // MAX_POSITION_EXCEEDED stay terminal.
        if (offerResult == Publication.BACK_PRESSURED
                || offerResult == Publication.ADMIN_ACTION
                || offerResult == Publication.NOT_CONNECTED) {
            cluster.idleStrategy().idle();
            return true;
        }
        return false;
    }
}
