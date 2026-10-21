package io.kardamom.sealer.cluster;

import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.LagBudgets;
import io.kardamom.sealer.VoidLedger;
import io.kardamom.sealer.cluster.ClusterStubs.StubCluster;
import io.kardamom.sealer.cluster.ClusterStubs.StubSession;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.agrona.ExpandableArrayBuffer;

/**
 * Driverless sealer members that apply one log of entries, each on its own
 * stub cluster, for the decision version tests. Each member has the session
 * of one sequencer, which gets every egress frame. {@link #stop} ends every
 * member that {@link #member} started.
 */
final class DriverlessLog {

    /** The dedup window of every member. */
    static final int CAPACITY = 16;

    /** One log entry: an ingress frame, or a boundary tick when {@code frame} is empty. */
    record Entry(String name, Optional<byte[]> frame, long timeMs) {
        static Entry frame(final String name, final byte[] frame, final long timeMs) {
            return new Entry(name, Optional.of(frame), timeMs);
        }

        static Entry tick(final String name, final long timeMs) {
            return new Entry(name, Optional.empty(), timeMs);
        }
    }

    /** The egress kinds that the session got for one entry, without the status frames, and the version after it. */
    record Outcome(String name, List<Byte> kinds, int decisionVersion) {
    }

    /** A member, its cluster, and the session of its sequencer. */
    record Member(SealerClusteredService svc, StubCluster cluster, StubSession sequencer) {
        Outcome apply(final Entry entry) {
            final int before = sequencer.offered.size();
            cluster.time = entry.timeMs();
            entry.frame().ifPresentOrElse(frame -> deliver(frame, entry.timeMs()), () -> svc.onTimerEvent(
                SealerClusteredService.BOUNDARY_TIMER_CORRELATION_ID, entry.timeMs()));
            final List<Byte> kinds = sequencer.offered.subList(before, sequencer.offered.size()).stream()
                .map(f -> f[0])
                .filter(kind -> kind != SealerWire.EGRESS_KIND_STATUS)
                .toList();
            return new Outcome(entry.name(), kinds, svc.decisionRules().version());
        }

        List<Outcome> applyAll(final List<Entry> log) {
            return log.stream().map(this::apply).toList();
        }

        /** Every egress frame that the session got, without the status frames. */
        List<byte[]> frames() {
            return sequencer.offered.stream().filter(f -> f[0] != SealerWire.EGRESS_KIND_STATUS).toList();
        }

        private void deliver(final byte[] frame, final long timeMs) {
            final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
            buf.putBytes(0, frame);
            svc.onSessionMessage(sequencer, timeMs, buf, 0, frame.length, null);
        }
    }

    private final List<Member> started = new ArrayList<>();

    /** A heap member with no ordering window, from {@code snapshot} or at genesis. */
    Member member(final Optional<byte[]> snapshot) {
        return member(snapshot, CanonicalSealerState.DEFAULT_ORDERING_WINDOW);
    }

    /** A heap member with an ordering window of {@code orderingWindow} records, from {@code snapshot} or at genesis. */
    Member member(final Optional<byte[]> snapshot, final int orderingWindow) {
        final StubCluster cluster = new StubCluster();
        cluster.role = io.aeron.cluster.service.Cluster.Role.FOLLOWER;
        final StubSession sequencer = cluster.addSession(1);
        final SealerClusteredService svc = new SealerClusteredService(
            CAPACITY, 250, 0, Set.of(), VoidLedger.Config.DISABLED,
            CanonicalSealerState.DEFAULT_INCLUSION_HORIZON_BLOCKS, orderingWindow, LagBudgets.DEFAULT);
        svc.start(cluster, snapshot);
        final Member member = new Member(svc, cluster, sequencer);
        started.add(member);
        return member;
    }

    /** End every member that {@link #member} started. */
    void stop() {
        started.forEach(member -> member.svc().onTerminate(member.cluster()));
    }
}
