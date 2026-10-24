package io.kardamom.sealer.cluster;

import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.GenesisFeatures;
import io.kardamom.sealer.LagBudgets;
import io.kardamom.sealer.SealerSeed;
import io.kardamom.sealer.VoidLedger;
import io.kardamom.sealer.cluster.ClusterStubs.StubCluster;
import io.kardamom.sealer.cluster.ClusterStubs.StubSession;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import org.agrona.ExpandableArrayBuffer;

/**
 * Driverless sealer members that apply one log of entries, each on its own
 * stub cluster, for the decision version tests. Each member has the session
 * of one sequencer, which gets every egress frame. {@link #stop} ends every
 * member that {@link #member} started.
 */
final class DriverlessLog {

    /** The dedup window of every member but a member with a feed. */
    static final int CAPACITY = 16;
    /** The dedup window of a member with a feed: its tests fill the replay window with records. */
    private static final int FEED_CAPACITY = 1 << 14;
    /** The peer chain that every member takes remote-origin records from. */
    static final long REMOTE_CHAIN = 10L;
    /** The segment size of the retained file of a member that carries the bytes. */
    private static final int SEGMENT_BYTES = 64 * 1024;

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

        /** The pipeline metrics of the member as Prometheus text, with the labels of member 1. */
        String metrics() {
            final MetricsText text = new MetricsText(1);
            svc.metrics().renderTo(text);
            return text.toString();
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
        return member(snapshot, orderingWindow, LagBudgets.DEFAULT);
    }

    /** A heap member with no ordering window and the lag guards of {@code budgets}, from {@code snapshot} or at genesis. */
    Member member(final Optional<byte[]> snapshot, final LagBudgets budgets) {
        return member(snapshot, CanonicalSealerState.DEFAULT_ORDERING_WINDOW, budgets);
    }

    private Member member(final Optional<byte[]> snapshot, final int orderingWindow, final LagBudgets budgets) {
        return member(CAPACITY, snapshot, orderingWindow, budgets, svc -> { });
    }

    /** A heap member with no ordering window and the member feed of {@code feed}, from {@code snapshot} or at genesis. */
    Member feeding(final Optional<byte[]> snapshot, final FeedConfig feed) {
        return member(FEED_CAPACITY, snapshot, CanonicalSealerState.DEFAULT_ORDERING_WINDOW, LagBudgets.DEFAULT,
            svc -> svc.feeding(feed));
    }

    /**
     * A member that carries the bytes and keeps its retained frames in a
     * file under {@code dir}, with the lag guards of {@code budgets}, from
     * {@code snapshot} or at genesis.
     */
    Member carrying(final Optional<byte[]> snapshot, final Path dir, final LagBudgets budgets) {
        return carrying(snapshot, dir, budgets, svc -> { });
    }

    /** A heap member from {@code snapshot}, or at genesis with the rules of {@code features}. */
    Member genesis(final Optional<byte[]> snapshot, final GenesisFeatures features) {
        return heap(snapshot, svc -> svc.genesisFeatures(features));
    }

    /** A member with the retained file under {@code dir}, from {@code snapshot}, or at genesis with {@code features}. */
    Member carryingGenesis(final Optional<byte[]> snapshot, final Path dir, final GenesisFeatures features) {
        return carrying(snapshot, dir, LagBudgets.DEFAULT, svc -> svc.genesisFeatures(features));
    }

    /** A heap member from {@code snapshot}, or from {@code seed}. */
    Member seeded(final Optional<byte[]> snapshot, final SealerSeed seed) {
        return heap(snapshot, svc -> svc.seededFrom(seed));
    }

    /** A member with the retained file under {@code dir}, from {@code snapshot}, or from {@code seed}. */
    Member carryingSeeded(final Optional<byte[]> snapshot, final Path dir, final SealerSeed seed) {
        return carrying(snapshot, dir, LagBudgets.DEFAULT, svc -> svc.seededFrom(seed));
    }

    private Member heap(final Optional<byte[]> snapshot, final Consumer<SealerClusteredService> setup) {
        return member(CAPACITY, snapshot, CanonicalSealerState.DEFAULT_ORDERING_WINDOW, LagBudgets.DEFAULT, setup);
    }

    private Member carrying(
            final Optional<byte[]> snapshot,
            final Path dir,
            final LagBudgets budgets,
            final Consumer<SealerClusteredService> setup) {
        return member(CAPACITY, snapshot, CanonicalSealerState.DEFAULT_ORDERING_WINDOW, budgets,
            setup.andThen(svc -> svc.carryingBytes(new RetainedFile.Config(dir, SEGMENT_BYTES))));
    }

    /** End {@code member}, then start a carrying member in its file in {@code dir} from {@code snapshot}. */
    Member restart(final Member member, final Path dir, final byte[] snapshot) {
        end(member);
        return carrying(Optional.of(snapshot), dir, LagBudgets.DEFAULT);
    }

    /** End {@code member}: its feed and its store close. */
    void end(final Member member) {
        started.remove(member);
        member.svc().onTerminate(member.cluster());
    }

    private Member member(
            final int capacity,
            final Optional<byte[]> snapshot,
            final int orderingWindow,
            final LagBudgets budgets,
            final Consumer<SealerClusteredService> setup) {
        final StubCluster cluster = new StubCluster();
        cluster.role = io.aeron.cluster.service.Cluster.Role.FOLLOWER;
        final StubSession sequencer = cluster.addSession(1);
        final SealerClusteredService svc = new SealerClusteredService(
            capacity, 250, 0, Set.of(REMOTE_CHAIN), VoidLedger.Config.DISABLED,
            CanonicalSealerState.DEFAULT_INCLUSION_HORIZON_BLOCKS, orderingWindow, budgets);
        setup.accept(svc);
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
