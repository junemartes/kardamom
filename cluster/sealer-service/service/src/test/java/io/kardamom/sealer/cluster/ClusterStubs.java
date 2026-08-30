package io.kardamom.sealer.cluster;

import io.aeron.Aeron;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.YieldingIdleStrategy;

/**
 * Minimal in-memory {@link Cluster}/{@link ClientSession} stubs shared by the
 * driverless service tests ({@link SealerFanoutTest}, {@link SnapshotRestoreTest}).
 * These stubs implement only what {@code onStart}, session fan-out, and replay
 * touch. Every transport-level operation throws. {@link StubSession#offered}
 * records every egress frame verbatim, so tests can assert on raw frame bytes.
 */
final class ClusterStubs {

    private ClusterStubs() {
    }

    static final class StubSession implements ClientSession {
        final long id;
        final List<byte[]> offered = new ArrayList<>();
        boolean closed;

        StubSession(final long id) {
            this.id = id;
        }

        public long id() {
            return id;
        }

        public int responseStreamId() {
            return 0;
        }

        public String responseChannel() {
            return "aeron:ipc";
        }

        public byte[] encodedPrincipal() {
            return new byte[0];
        }

        public void close() {
            closed = true;
        }

        public boolean isClosing() {
            return closed;
        }

        public long offer(final DirectBuffer buffer, final int offset, final int length) {
            final byte[] copy = new byte[length];
            buffer.getBytes(offset, copy);
            offered.add(copy);
            return length;
        }

        public long offer(final io.aeron.DirectBufferVector[] vectors) {
            throw new UnsupportedOperationException();
        }

        public long tryClaim(final int length, final io.aeron.logbuffer.BufferClaim bufferClaim) {
            throw new UnsupportedOperationException();
        }
    }

    static final class StubCluster implements Cluster {
        final HashMap<Long, ClientSession> sessions = new HashMap<>();
        final IdleStrategy idleStrategy = new YieldingIdleStrategy();

        StubSession addSession(final long id) {
            final StubSession session = new StubSession(id);
            sessions.put(id, session);
            return session;
        }

        public int memberId() {
            return 0;
        }

        public Role role() {
            return Role.LEADER;
        }

        public long logPosition() {
            return 0;
        }

        public Aeron aeron() {
            throw new UnsupportedOperationException();
        }

        public io.aeron.cluster.service.ClusteredServiceContainer.Context context() {
            throw new UnsupportedOperationException();
        }

        public ClientSession getClientSession(final long clusterSessionId) {
            return sessions.get(clusterSessionId);
        }

        public Collection<ClientSession> clientSessions() {
            return sessions.values();
        }

        public void forEachClientSession(final Consumer<? super ClientSession> action) {
            sessions.values().forEach(action);
        }

        public boolean closeClientSession(final long clusterSessionId) {
            return sessions.remove(clusterSessionId) != null;
        }

        public long time() {
            return 0;
        }

        public TimeUnit timeUnit() {
            return TimeUnit.MILLISECONDS;
        }

        public boolean scheduleTimer(final long correlationId, final long deadline) {
            return true;
        }

        public boolean cancelTimer(final long correlationId) {
            return true;
        }

        public long offer(final DirectBuffer buffer, final int offset, final int length) {
            throw new UnsupportedOperationException();
        }

        public long offer(final io.aeron.DirectBufferVector[] vectors) {
            throw new UnsupportedOperationException();
        }

        public long tryClaim(final int length, final io.aeron.logbuffer.BufferClaim bufferClaim) {
            throw new UnsupportedOperationException();
        }

        public IdleStrategy idleStrategy() {
            return idleStrategy;
        }
    }
}
