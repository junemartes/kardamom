package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** The peer probe against real admin servers on local ports. */
final class PeerProbeTest {
    private static final long LAG = 1024;
    private static final MemberStatus.SnapshotVersions SNAPSHOT = MemberStatus.SnapshotVersions.of(0);

    private static MemberStatus status(final int memberId, final String role, final String election, final long commit) {
        return new MemberStatus(memberId, role, election, commit, commit, SNAPSHOT);
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
        try (AdminServer electing = AdminServer.start(0, () -> status(1, "FOLLOWER", "CANVASS", 10), LAG);
             AdminServer joined = AdminServer.start(0, () -> status(2, "FOLLOWER", "CLOSED", 10), LAG)) {
            final PeerProbe probe = new PeerProbe(List.of(deadUri(), statusUri(electing), statusUri(joined)));
            assertEquals(Optional.of(2), probe.closedPeer().map(PeerProbe.PeerStatus::memberId));
        }
    }

    @Test
    void aMemberThatShutsDownOrFailsIsNotLive() throws IOException {
        // A member on its way out reads CLOSED for its role and its
        // election. A failing status source answers an error object.
        try (AdminServer stopping = AdminServer.start(0, () -> status(1, "CLOSED", "CLOSED", 10), LAG);
             AdminServer failing = AdminServer.start(0, () -> {
                 throw new IllegalStateException("counters gone");
             }, LAG)) {
            final PeerProbe probe = new PeerProbe(List.of(statusUri(stopping), statusUri(failing), deadUri()));
            assertEquals(Optional.empty(), probe.closedPeer());
        }
    }

    @Test
    void theCommitMarkGrowsOnlyWithANewHighestCommit() throws IOException {
        final AtomicReference<MemberStatus> replaying = new AtomicReference<>(status(1, "FOLLOWER", "FOLLOWER_REPLAY", 100));
        try (AdminServer peer = AdminServer.start(0, replaying::get, LAG);
             AdminServer leader = AdminServer.start(0, () -> status(2, "LEADER", "LEADER_READY", 500), LAG)) {
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
        try (AdminServer peer = AdminServer.start(0, () -> status(1, "LEADER", "CLOSED", 10), LAG)) {
            final String members = "0,10.9.9.9:40200,10.9.9.9:40201,10.9.9.9:40202,10.9.9.9:40203,10.9.9.9:40204"
                + "|1,127.0.0.1:40200,127.0.0.1:40201,127.0.0.1:40202,127.0.0.1:40203,127.0.0.1:40204";
            assertEquals(Optional.of(1),
                PeerProbe.forMembers(members, 0, peer.port()).closedPeer().map(PeerProbe.PeerStatus::memberId));
            assertEquals(Optional.empty(), PeerProbe.forMembers(members, 0, 0).closedPeer(),
                "with the admin port off the probe knows no peer");
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
