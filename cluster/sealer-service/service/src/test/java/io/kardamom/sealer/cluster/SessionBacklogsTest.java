package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kardamom.sealer.cluster.ClusterStubs.StubSession;
import io.kardamom.sealer.cluster.SessionBacklogs.Outcome;
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * The per-session backlog limits, with small limits so that each test runs
 * in milliseconds. A session past either limit is closed once, and a dropped
 * backlog closes nothing.
 */
class SessionBacklogsTest {

    private static final long STALL_NS = TimeUnit.MILLISECONDS.toNanos(20);
    private static final long BYTE_LIMIT = 100;

    private final SessionBacklogs backlogs = new SessionBacklogs(0, STALL_NS, BYTE_LIMIT);

    private static void pastTheStallDeadline() throws InterruptedException {
        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(STALL_NS) * 2);
    }

    private static StubSession wedged(final long id) {
        final StubSession session = new StubSession(id);
        session.backPressured = true;
        return session;
    }

    /**
     * While the member holds its outputs after its start, every frame waits,
     * and no session closes for the wait. After the hold the frames go out
     * in order.
     */
    @Test
    void theStartHoldQueuesEveryFrameAndClosesNothing() throws InterruptedException {
        final StartHold hold = new StartHold();
        final SessionBacklogs held = new SessionBacklogs(0, STALL_NS, BYTE_LIMIT, hold);
        final StubSession session = new StubSession(1);
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[8]);

        assertEquals(Outcome.QUEUED, held.offer(session, new byte[] {1}));
        assertEquals(Outcome.QUEUED, held.offer(session, buffer, 8));
        pastTheStallDeadline();
        held.drain();
        assertTrue(session.offered.isEmpty(), "nothing goes out during the hold");
        assertEquals(0, session.closes);

        hold.release();
        held.drain();
        assertEquals(2, session.offered.size());
        assertEquals(1, session.offered.get(0).length);
        assertEquals(0, session.closes, "the wait of the hold is no stall");
    }

    @Test
    void aBacklogOverTheByteLimitClosesItsSessionOnce() {
        final StubSession session = wedged(1);
        final byte[] frame = new byte[40];

        assertEquals(Outcome.QUEUED, backlogs.offer(session, frame));
        assertEquals(Outcome.QUEUED, backlogs.offer(session, frame));
        assertEquals(Outcome.DROPPED, backlogs.offer(session, frame));
        assertEquals(Outcome.DROPPED, backlogs.offer(session, frame));
        backlogs.drain();

        assertEquals(1, session.closes);
    }

    @Test
    void aDrainClosesOnlyTheSessionsThatStayWedged() throws InterruptedException {
        final List<StubSession> sessions =
            LongStream.range(1, 6).mapToObj(SessionBacklogsTest::wedged).toList();
        sessions.forEach(s -> backlogs.offer(s, new byte[] {(byte) s.id}));
        sessions.subList(0, 3).forEach(s -> s.backPressured = false);

        backlogs.drain();
        pastTheStallDeadline();
        backlogs.drain();
        backlogs.drain();

        sessions.subList(0, 3).forEach(s -> assertEquals(1, s.offered.size()));
        sessions.subList(0, 3).forEach(s -> assertEquals(0, s.closes));
        sessions.subList(3, 5).forEach(s -> assertEquals(1, s.closes));
        sessions.subList(3, 5).forEach(s -> assertTrue(s.offered.isEmpty()));
    }

    @Test
    void aDroppedBacklogClosesNothing() throws InterruptedException {
        final StubSession session = wedged(1);
        backlogs.offer(session, new byte[8]);

        backlogs.drop(session.id());
        pastTheStallDeadline();
        backlogs.drain();

        assertEquals(0, session.closes);
    }

    @Test
    void progressRestartsTheStallDeadline() throws InterruptedException {
        final StubSession session = wedged(1);
        backlogs.offer(session, new byte[8]);
        backlogs.offer(session, new byte[8]);
        pastTheStallDeadline();

        session.backPressured = false;
        session.credit = 1;
        backlogs.drain();
        assertEquals(0, session.closes, "a session that took a frame is not stalled");
        assertEquals(1, session.offered.size());

        pastTheStallDeadline();
        backlogs.drain();
        assertEquals(1, session.closes, "the deadline counts from the last frame taken");
    }

    /** A session whose egress publication takes at most {@code max} bytes in one message. */
    private static StubSession small(final long id, final int max) {
        final StubSession session = new StubSession(id);
        session.maxMessageLength = max;
        return session;
    }

    /** A relayed frame of {@code length} bytes for {@code index}. */
    private static byte[] relayed(final long index, final int length) {
        final byte[] frame = new byte[length];
        frame[0] = SealerWire.EGRESS_KIND_RELAYED;
        new UnsafeBuffer(frame).putLong(1, index, ByteOrder.LITTLE_ENDIAN);
        return frame;
    }

    @Test
    void aFrameAboveTheMaxMessageLengthClosesTheSessionAtOnce() {
        final StubSession session = small(1, 64);

        assertEquals(Outcome.DROPPED, backlogs.offer(session, relayed(7, 65)));

        assertEquals(1, session.closes);
        assertTrue(session.offered.isEmpty());
    }

    @Test
    void repeatedClosesOnOneIndexAreCountedForTheResponseChannel() {
        LongStream.range(1, 4).forEach(id -> backlogs.offer(small(id, 64), relayed(7, 65)));
        assertEquals(2, backlogs.tooLongRepeats(CHANNEL, RELAYED, 7));

        backlogs.offer(small(4, 64), relayed(8, 65));
        assertEquals(0, backlogs.tooLongRepeats(CHANNEL, RELAYED, 8));
        assertEquals(-1, backlogs.tooLongRepeats(CHANNEL, RELAYED, 7));
    }

    private static final String CHANNEL = "aeron:ipc";
    private static final byte RELAYED = SealerWire.EGRESS_KIND_RELAYED;

    /** A frame of {@code kind} with no canonical index, {@code length} bytes long. */
    private static byte[] unindexed(final byte kind, final int length) {
        final byte[] frame = new byte[length];
        frame[0] = kind;
        return frame;
    }

    @Test
    void framesOfTwoKindsWithNoIndexAreNotRepeats() {
        backlogs.offer(small(1, 64), unindexed(SealerWire.EGRESS_KIND_STATUS, 65));
        backlogs.offer(small(2, 64), unindexed(SealerWire.EGRESS_KIND_BOUNDARY, 65));

        assertEquals(0, backlogs.tooLongRepeats(CHANNEL, SealerWire.EGRESS_KIND_BOUNDARY, -1));
        assertEquals(-1, backlogs.tooLongRepeats(CHANNEL, SealerWire.EGRESS_KIND_STATUS, -1));
    }

    @Test
    void aRelayedReferenceIsKeyedByItsIndex() {
        final byte[] reference = relayed(7, 65);
        reference[0] = SealerWire.EGRESS_KIND_RELAYED_REF;
        backlogs.offer(small(1, 64), reference);
        backlogs.offer(small(2, 64), relayed(7, 65));

        assertEquals(0, backlogs.tooLongRepeats(CHANNEL, RELAYED, 7),
            "a relayed frame after a reference of the same index is no repeat");
    }

    @Test
    void forgettingTheChannelOrARoleChangeClearsTheCount() {
        backlogs.offer(small(1, 64), relayed(7, 65));
        backlogs.forgetChannel(CHANNEL);
        assertEquals(-1, backlogs.tooLongRepeats(CHANNEL, RELAYED, 7));

        backlogs.offer(small(2, 64), relayed(7, 65));
        backlogs.dropAll();
        assertEquals(-1, backlogs.tooLongRepeats(CHANNEL, RELAYED, 7));
    }
}
