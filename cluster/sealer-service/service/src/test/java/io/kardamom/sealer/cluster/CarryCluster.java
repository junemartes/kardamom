package io.kardamom.sealer.cluster;

import static io.kardamom.sealer.cluster.ClusterTestHarness.awaitCondition;
import static io.kardamom.sealer.cluster.IngressFrames.offerFrame;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.VersionSwitch;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.stream.IntStream;

/**
 * Three real sealer members in one in-JVM {@link TestCluster}, driven by
 * the frames of the carry rollout: the activation of a feature by its
 * epoch, full references with their bytes, and positioned references
 * without bytes. Only epochs force a boundary, so every member switches
 * at the same log entry. Each member keeps a replay window of
 * {@link #WINDOW} frames.
 */
final class CarryCluster implements AutoCloseable {

    static final int MEMBER_COUNT = 3;
    /** The replay window of every member, in frames. */
    static final int WINDOW = 4;
    /** An activation time that has passed: the feature turns on at its boundary. */
    static final long ON = 1L;
    /** An activation time of {@code 2^64 - 1} ms: the feature is held and off. */
    static final long OFF = -1L;

    private static final int DEDUP_CAPACITY = 8192;
    /** No boundary tick fires inside a test: the epochs force each boundary. */
    private static final long TICK_MS = 600_000L;
    private static final String RETENTION = "kardamom.cluster.retention";

    final TestCluster cluster;
    final RecordingEgressListener egress = new RecordingEgressListener();
    private AeronCluster client;
    private long origin;
    private int tag = 100;
    private long position = 64L;

    private CarryCluster(final SystemTestWatcher watcher) {
        System.setProperty(RETENTION, Integer.toString(WINDOW));
        cluster = ClusterTestHarness.start(watcher, ClusterTestHarness.builder(MEMBER_COUNT, DEDUP_CAPACITY, TICK_MS));
        cluster.awaitLeader();
        cluster.egressListener(egress);
        client = cluster.connectClient();
    }

    /** Start the members, and switch every member to decision version 5 by features 3 and 4. */
    static CarryCluster atVersionFive(final SystemTestWatcher watcher) {
        final CarryCluster carry = new CarryCluster(watcher);
        carry.activate(VersionSwitch.FEATURE_SEALER_V4, ON);
        carry.awaitRunning(m -> m.decisionRules().version() == 4);
        carry.activate(VersionSwitch.FEATURE_SEALER_V5, ON);
        carry.awaitRunning(m -> m.decisionRules().version() == VersionSwitch.CARRY_VERSION);
        return carry;
    }

    /**
     * Offer the activation of {@code feature} at {@code activationMs} with
     * the next epoch, and the epoch after it, which forces the boundary
     * that seals the block of the activation.
     */
    void activate(final long feature, final long activationMs) {
        offerFrame(client, IngressFrames.featureActivationFrame(origin + 1, feature, activationMs));
        epoch();
        epoch();
    }

    /** Offer the next epoch: it forces the boundary of the open block. */
    void epoch() {
        origin++;
        offerFrame(client, IngressFrames.originRecordFrame(
            IngressFrames.canonicalId(1000 + (int) origin), origin, 1, new byte[] {0x02}));
    }

    /** Offer the next full reference, with the bytes of its transaction, and return it. */
    FullRef full() {
        position += 256L;
        final FullRef ref = FullRef.of(tag++, position);
        offerFrame(client, ref.frame());
        return ref;
    }

    /** Offer {@code frame} as it is. */
    void offer(final byte[] frame) {
        offerFrame(client, frame);
    }

    /** Offer {@code count} full references, each in its own block, and wait for their relay. */
    List<FullRef> blocks(final int count) {
        final int relayed = egress.relayedPayloads.size();
        final List<FullRef> refs = IntStream.range(0, count).mapToObj(i -> {
            final FullRef ref = full();
            epoch();
            return ref;
        }).toList();
        awaitRelayed(relayed + 2 * count);
        return refs;
    }

    /** Wait until the client got {@code count} relayed frames. */
    void awaitRelayed(final int count) {
        awaitCondition(client, () -> egress.relayedPayloads.size() >= count);
    }

    /** Wait until {@code done} holds, and keep the client polling meanwhile. */
    void await(final BooleanSupplier done) {
        awaitCondition(client, done);
    }

    /** Wait until {@code done} holds on the service of every running member. */
    void awaitRunning(final Predicate<SealerClusteredService> done) {
        awaitCondition(client, () -> running().stream().allMatch(done));
    }

    /** The services of the running members: the leader and the followers. */
    List<SealerClusteredService> running() {
        return IntStream.range(0, MEMBER_COUNT)
            .mapToObj(cluster::node)
            .filter(node -> node != null && !node.isClosed())
            .map(CarryCluster::service)
            .toList();
    }

    /** Wait until a member other than the stopped member {@code id} leads. Reconnect the client to it. */
    TestNode elect(final int id) {
        final TestNode leader = cluster.awaitLeader(id);
        client = cluster.reconnectClient();
        return leader;
    }

    /** Start the stopped member {@code id} from its own directories, and wait until it joins. */
    TestNode restart(final int id) {
        final TestNode node = cluster.startStaticNode(id, false);
        TestCluster.awaitElectionClosed(node);
        return node;
    }

    /** Wait until every running member applied the newest log entry that a member applied, and return them. */
    List<SealerClusteredService> synced() {
        final List<SealerClusteredService> members = running();
        final long applied = members.stream().mapToLong(SealerClusteredService::servicePosition).max().orElseThrow();
        awaitCondition(client, () -> members.stream().allMatch(m -> m.servicePosition() >= applied));
        return members;
    }

    /** The carry window of every running member, with its gauges, as {@code on} wants it. */
    void assertWindow(final boolean on) {
        final List<SealerClusteredService> members = synced();
        assertEquals(MEMBER_COUNT, members.size(), "every member runs");
        members.forEach(m -> {
            final String text = metrics(m);
            final MemberStatus.Carry carry = m.metrics().carry();
            assertEquals(on, carry.carryWindow(), "carryWindow");
            assertEquals(on ? 2.0 : 1.0, PromText.value(text, "kardamom_sealer_feature_state", "feature=\"5\""));
            assertEquals(on ? 1.0 : 0.0, PromText.value(text, "kardamom_sealer_window_exact", ""));
            assertEquals(5.0, PromText.value(text, "kardamom_sealer_decision_version", ""));
            assertEquals(0.0, PromText.value(text, "kardamom_sealer_ref_refused_total", ""), "no reference refused");
            assertTrue(!on || carry.retainedFrames() <= WINDOW, "the window keeps " + carry.retainedFrames());
        });
    }

    /** Wait until every running member applied the newest log entry, then compare their snapshots and rules. */
    void assertSameState() {
        final List<SealerClusteredService> members = synced();
        final SealerClusteredService first = members.get(0);
        members.forEach(m -> {
            assertArrayEquals(first.snapshot(), m.snapshot(), "the state of every member is the same");
            assertEquals(first.decisionRules(), m.decisionRules());
            assertEquals(first.metrics().carry(), m.metrics().carry());
        });
    }

    /** The relayed payloads after {@code from} are the canonical entries of {@code refs} and the epochs, in order. */
    void assertRelayed(final int from, final List<FullRef> refs) {
        final List<byte[]> carried = egress.relayedPayloads.subList(from, egress.relayedPayloads.size()).stream()
            .filter(p -> p[CanonicalSealerState.CANONICAL_ID_LEN] == SealerWire.RT_TX)
            .toList();
        assertEquals(refs.size(), carried.size(), "each full reference once");
        IntStream.range(0, refs.size()).forEach(i -> assertArrayEquals(refs.get(i).canonicalPayload(), carried.get(i)));
        final List<Long> indexes = egress.relayedIndexes.subList(from, egress.relayedIndexes.size());
        IntStream.range(1, indexes.size()).forEach(i -> assertEquals(indexes.get(i - 1) + 1, indexes.get(i),
            "the canonical indexes are contiguous: " + indexes));
    }

    /** The metrics page of {@code member}. */
    static String metrics(final SealerClusteredService member) {
        final MetricsText text = new MetricsText(1);
        member.metrics().renderTo(text);
        return text.toString();
    }

    static SealerClusteredService service(final TestNode node) {
        return ((SealerTestService) node.service()).delegate();
    }

    @Override
    public void close() {
        cluster.close();
        System.clearProperty(RETENTION);
    }
}
