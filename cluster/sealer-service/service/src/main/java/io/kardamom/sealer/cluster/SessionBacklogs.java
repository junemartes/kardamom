package io.kardamom.sealer.cluster;

import io.aeron.Publication;
import io.aeron.cluster.service.ClientSession;
import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * The egress offer path of {@link SealerEgress}: one offer attempt per frame,
 * and a bounded backlog for each session that cannot take a frame now.
 *
 * <p>Offers run on the single clustered-service thread. That thread also
 * relays records and runs the boundary tick for every session, so an offer
 * never waits. A frame that meets back-pressure goes to the backlog of its
 * session. While a session has a backlog, every new frame for that session
 * goes to the backlog too, so the session gets its frames in emission order.
 * {@link #drain} sends backlog frames. The service calls it from its
 * log-driven callbacks, because Aeron rejects an egress offer and a session
 * close from {@code doBackgroundWork} and from {@code onRoleChange}.</p>
 *
 * <p>A live session never loses a frame: a consumer that misses a boundary
 * seals two blocks as one, and diverges. So a session that cannot take its
 * backlog is closed instead. The close is a signal the client can act on: it
 * reconnects, replays, and recovers. A session is closed once, when one of
 * these limits is reached:</p>
 * <ul>
 *   <li>No frame left its backlog for {@link #STALL_DEADLINE_NS}. The clock
 *       restarts at each frame that leaves, so a large replay to a client
 *       that drains at line rate stays open.</li>
 *   <li>The backlog holds more than its byte limit.</li>
 * </ul>
 *
 * <p>The backlogs are member-local IO state. They never change the
 * replicated state, what the service writes to the log, or the snapshot.
 * The wall clock is safe here for the same reason. Only the leader's offers
 * reach clients: a follower's offer is a mock that always succeeds, so a
 * follower holds no backlog. A role change drops every backlog, because the
 * clients reconnect to the new leader and replay from there. A session close
 * drops the backlog of that session.</p>
 *
 * <p>This class is single-threaded, like {@link SealerEgress}.</p>
 */
final class SessionBacklogs {

    /**
     * How long a backlog may make no progress before its session is closed.
     *
     * <p>One second keeps the cost of a dead session to one second of
     * queued frames, and does not close a live client that rides out a
     * brief CPU spike. A close forces the client through reconnect,
     * fail-stop, and crash recovery, so a false close is costly.</p>
     */
    static final long STALL_DEADLINE_NS = TimeUnit.SECONDS.toNanos(1);

    /**
     * The most bytes one backlog may hold. A full replay of the default
     * retention (65536 frames) fits well below it. Sessions share the frame
     * arrays of one emission, so this bounds how far one session may lag,
     * not the heap per session.
     */
    static final long BYTE_LIMIT = 64L * 1024 * 1024;

    /** What happened to one offered frame. */
    enum Outcome {
        /** The session's egress publication took the frame. */
        SENT,
        /** The frame waits in the session's backlog. */
        QUEUED,
        /** The session is closed or closing, so the frame goes nowhere. */
        DROPPED
    }

    /** The frames one session could not take yet, oldest first. */
    private final class Backlog {
        final ClientSession session;
        final ArrayDeque<byte[]> frames = new ArrayDeque<>();
        long bytes;
        /** When the backlog opened, or when a frame last left it. */
        long progressNs;

        Backlog(final ClientSession session, final long nowNs) {
            this.session = session;
            this.progressNs = nowNs;
        }

        void add(final byte[] frame) {
            frames.addLast(frame);
            bytes += frame.length;
        }

        /**
         * Send frames until the backlog is empty or the session pushes back,
         * then close the session if it is past a limit.
         *
         * @return whether this backlog is done: empty, or its session closed
         */
        boolean drain(final long nowNs) {
            final long result = flush(nowNs);
            if (frames.isEmpty()) {
                return true;
            }
            if (!retryable(result)) {
                closeOnTerminal(session, result);
                return true;
            }
            if (nowNs - progressNs > stallDeadlineNs) {
                closeLoudly(session, "offer deadline exhausted (back-pressure)");
                return true;
            }
            return false;
        }

        /** Send head frames while the session takes them; return the last offer result. */
        private long flush(final long nowNs) {
            long result = 0;
            while (!frames.isEmpty() && result >= 0) {
                result = sendHead(nowNs);
            }
            return result;
        }

        private long sendHead(final long nowNs) {
            final byte[] head = frames.peekFirst();
            final long result = attempt(session, head);
            if (result >= 0) {
                frames.removeFirst();
                bytes -= head.length;
                progressNs = nowNs;
            }
            return result;
        }
    }

    /** This cluster member's id, logged on each close. */
    private final int memberId;
    private final long stallDeadlineNs;
    private final long byteLimit;
    /** The open backlogs by session id. A session with no backlog has no entry. */
    private final Long2ObjectHashMap<Backlog> bySession = new Long2ObjectHashMap<>();
    /** A view that wraps each frame for the offer, with no allocation. */
    private final UnsafeBuffer view = new UnsafeBuffer(0, 0);

    SessionBacklogs(final int memberId, final long stallDeadlineNs, final long byteLimit) {
        this.memberId = memberId;
        this.stallDeadlineNs = stallDeadlineNs;
        this.byteLimit = byteLimit;
    }

    /**
     * Offer one frame to one session, or queue it behind the session's
     * backlog. The caller must not change {@code frame} later: a backlog
     * keeps the array, not a copy.
     */
    Outcome offer(final ClientSession session, final byte[] frame) {
        final Backlog backlog = bySession.get(session.id());
        if (backlog != null) {
            return append(backlog, frame);
        }
        // A close only asks the consensus module to close the session. The
        // session stays in the cluster's session set until the close comes
        // back through the log, and its publication is still the wedged one.
        if (session.isClosing()) {
            return Outcome.DROPPED;
        }
        final long result = attempt(session, frame);
        if (result >= 0) {
            return Outcome.SENT;
        }
        if (retryable(result)) {
            return append(open(session), frame);
        }
        closeOnTerminal(session, result);
        return Outcome.DROPPED;
    }

    /** Send what each backlog's session takes now, and close each session past a limit. */
    void drain() {
        final long nowNs = System.nanoTime();
        bySession.values().removeIf(backlog -> backlog.drain(nowNs));
    }

    /** Drop the backlog of a closed session. */
    void drop(final long sessionId) {
        bySession.remove(sessionId);
    }

    /** Drop every backlog, because this member's role changed. */
    void dropAll() {
        bySession.clear();
    }

    private Backlog open(final ClientSession session) {
        final Backlog backlog = new Backlog(session, System.nanoTime());
        bySession.put(session.id(), backlog);
        return backlog;
    }

    private Outcome append(final Backlog backlog, final byte[] frame) {
        backlog.add(frame);
        if (backlog.bytes <= byteLimit) {
            return Outcome.QUEUED;
        }
        bySession.remove(backlog.session.id());
        closeLoudly(backlog.session, "egress backlog over " + byteLimit + " bytes");
        return Outcome.DROPPED;
    }

    /**
     * One offer of {@code frame}. ADMIN_ACTION means the publication rotated
     * its term, and Aeron asks for a retry at once, so this retries once.
     */
    private long attempt(final ClientSession session, final byte[] frame) {
        view.wrap(frame);
        final long result = session.offer(view, 0, frame.length);
        return result == Publication.ADMIN_ACTION ? session.offer(view, 0, frame.length) : result;
    }

    /**
     * Whether a negative offer result can clear later.
     *
     * <p>BACK_PRESSURED and ADMIN_ACTION are transient flow control.
     * NOT_CONNECTED can clear too: an egress publication is unconnected for
     * a moment at session open, and after a failover while the new leader
     * re-creates it. A session whose egress never connects reaches the stall
     * deadline and is closed, not skipped. Its keep-alives still flow through
     * ingress, so a skipped session stays open while it gets no frames, and
     * the client cannot detect that. CLOSED and MAX_POSITION_EXCEEDED are
     * terminal.</p>
     */
    private static boolean retryable(final long offerResult) {
        return offerResult == Publication.BACK_PRESSURED
            || offerResult == Publication.ADMIN_ACTION
            || offerResult == Publication.NOT_CONNECTED;
    }

    /**
     * Close a session after a terminal offer result. CLOSED means the
     * session is already gone. Any other terminal result, such as
     * MAX_POSITION_EXCEEDED, means the egress publication is dead for good,
     * so the session must close. Otherwise ingress keep-alives keep it open
     * while it gets no frames.
     */
    private void closeOnTerminal(final ClientSession session, final long result) {
        if (result != Publication.CLOSED) {
            closeLoudly(session, "terminal offer result " + result);
        }
    }

    /**
     * Close a session, and log the reason on stdout. The close event for the
     * client goes over the same egress publication that failed, so the
     * client may never see it. The client's liveness watchdog recovers it.
     * This log line is then the only record of why the session closed.
     */
    private void closeLoudly(final ClientSession session, final String reason) {
        System.out.println("cluster EGRESS-CLOSE memberId=" + memberId
            + " session=" + session.id() + " reason=" + reason);
        session.close();
    }
}
