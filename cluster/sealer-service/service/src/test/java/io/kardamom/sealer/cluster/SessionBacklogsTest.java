package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kardamom.sealer.cluster.ClusterStubs.StubSession;
import io.kardamom.sealer.cluster.SessionBacklogs.Outcome;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;
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
}
