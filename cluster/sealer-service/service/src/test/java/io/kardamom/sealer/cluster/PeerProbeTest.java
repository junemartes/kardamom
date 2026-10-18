package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** The peer probe against real admin servers on local ports. */
final class PeerProbeTest {
    private static final long LAG = 1024;
    private static final MemberStatus.SnapshotVersions SNAPSHOT = MemberStatus.SnapshotVersions.of(0);

    private static MemberStatus status(final int memberId, final String role, final String election, final long commit) {
        return new MemberStatus(memberId, role, election, commit, commit, SNAPSHOT, io.kardamom.sealer.DecisionRules.START_VIEW, OptionalLong.empty());
    }

    private static URI statusUri(final AdminServer admin) {
        return URI.create("http://127.0.0.1:" + admin.port() + "/status");
    }

    /** A local port with no listener: the probe's connect fails at once. */
    private static URI deadUri() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return URI.create("http://127.0.0.1:" + socket.getLocalPort() + "/status");
        }
    }

    @Test
    void findsAPeerWithAClosedElection() throws IOException {
        try (AdminServer electing = AdminServer.start(0, () -> status(1, "FOLLOWER", "CANVASS", 10), LAG, Optional.empty());
             AdminServer joined = AdminServer.start(0, () -> status(2, "FOLLOWER", "CLOSED", 10), LAG, Optional.empty())) {
            final PeerProbe probe = new PeerProbe(List.of(deadUri(), statusUri(electing), statusUri(joined)));
            assertEquals(Optional.of(2), probe.ask().closedPeer().map(PeerProbe.PeerStatus::memberId));
        }
    }

    @Test
    void aMemberThatShutsDownOrFailsIsNotLive() throws IOException {
        // A member on its way out reads CLOSED for its role and its
        // election. A failing status source answers an error object.
        try (AdminServer stopping = AdminServer.start(0, () -> status(1, "CLOSED", "CLOSED", 10), LAG, Optional.empty());
             AdminServer failing = AdminServer.start(0, () -> {
                 throw new IllegalStateException("counters gone");
             }, LAG, Optional.empty())) {
            final PeerProbe probe = new PeerProbe(List.of(statusUri(stopping), statusUri(failing), deadUri()));
            assertEquals(Optional.empty(), probe.ask().closedPeer());
        }
    }

    @Test
    void everyPeerMustAnswerForNoClosedElection() throws IOException {
        try (AdminServer electing = AdminServer.start(0, () -> status(1, "FOLLOWER", "CANVASS", 10), LAG, Optional.empty());
             AdminServer leading = AdminServer.start(0, () -> status(2, "LEADER", "LEADER_READY", 10), LAG, Optional.empty())) {
            assertEquals(JoinWatchdog.PeerElections.NONE_CLOSED,
                new PeerProbe(List.of(statusUri(electing), statusUri(leading))).ask().elections());
            assertEquals(JoinWatchdog.PeerElections.UNKNOWN,
                new PeerProbe(List.of(statusUri(electing), deadUri())).ask().elections(),
                "a peer that does not answer may have a closed election");
        }
    }

    @Test
    void aSilentPeerEndsTheRoundWithinItsBound() throws IOException {
        // The socket accepts the connection, and nobody answers.
        try (ServerSocket silent = new ServerSocket(0);
             AdminServer joined = AdminServer.start(0, () -> status(2, "FOLLOWER", "CLOSED", 10), LAG, Optional.empty())) {
            final URI silentUri = URI.create("http://127.0.0.1:" + silent.getLocalPort() + "/status");
            final long startNs = System.nanoTime();
            final PeerProbe.Round round = new PeerProbe(List.of(silentUri, statusUri(joined))).ask();
            final long tookMs = (System.nanoTime() - startNs) / 1_000_000L;
            assertEquals(JoinWatchdog.PeerElections.CLOSED, round.elections());
            assertEquals(1, round.answers().size());
            assertTrue(tookMs < 3_000L, "the round took " + tookMs + " ms");
        }
    }

    @Test
    void theCommitMarkGrowsOnlyWithANewHighestCommit() throws IOException {
        final AtomicReference<MemberStatus> replaying = new AtomicReference<>(status(1, "FOLLOWER", "FOLLOWER_REPLAY", 100));
        try (AdminServer peer = AdminServer.start(0, replaying::get, LAG, Optional.empty());
             AdminServer leader = AdminServer.start(0, () -> status(2, "LEADER", "LEADER_READY", 500), LAG, Optional.empty())) {
            final PeerProbe probe = new PeerProbe(List.of(statusUri(peer), statusUri(leader), deadUri()));
            assertEquals(600L, probe.commitMark());
            replaying.set(status(1, "FOLLOWER", "FOLLOWER_REPLAY", 40));
            assertEquals(600L, probe.commitMark(), "a new replay from a lower position is no progress");
            replaying.set(status(1, "FOLLOWER", "FOLLOWER_CATCHUP", 160));
            assertEquals(660L, probe.commitMark());
        }
    }

    @Test
    void theMembersListNamesEachOtherMemberAtTheAdminPort() throws IOException {
        try (AdminServer peer = AdminServer.start(0, () -> status(1, "LEADER", "CLOSED", 10), LAG, Optional.empty())) {
            final String members = "0,10.9.9.9:40200,10.9.9.9:40201,10.9.9.9:40202,10.9.9.9:40203,10.9.9.9:40204"
                + "|1,127.0.0.1:40200,127.0.0.1:40201,127.0.0.1:40202,127.0.0.1:40203,127.0.0.1:40204";
            assertEquals(Optional.of(1),
                PeerProbe.forMembers(members, 0, peer.port()).ask().closedPeer().map(PeerProbe.PeerStatus::memberId));
            assertEquals(JoinWatchdog.PeerElections.UNKNOWN, PeerProbe.forMembers(members, 0, 0).ask().elections(),
                "with the admin port off the probe knows no peer, so it has no answer");
            assertEquals(0L, PeerProbe.forMembers(members, 0, 0).commitMark());
        }
    }

    @Test
    void parsesTheStatusJsonOfThisRelease() {
        final MemberStatus sample = status(2, "LEADER", "LEADER_READY", 15_027_008L);
        final Optional<PeerProbe.PeerStatus> parsed = PeerProbe.PeerStatus.parse(sample.toJson(LAG));
        assertEquals(Optional.of(new PeerProbe.PeerStatus(2, "LEADER", "LEADER_READY", 15_027_008L)), parsed);
        assertTrue(PeerProbe.PeerStatus.parse("{\"error\":\"x\"}").isEmpty());
    }
}
