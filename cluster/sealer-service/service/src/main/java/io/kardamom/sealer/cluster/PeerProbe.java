package io.kardamom.sealer.cluster;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads the admin status of the other members over HTTP, for the join
 * watchdog.
 *
 * <p>The watchdog asks two questions. Does another member have a closed
 * election? Such a member follows a live leader, and it answers the backup
 * query of a peer seed. Does any other member make commit progress? A
 * member that replays or catches up moves its commit position, so a leader
 * that waits for it waits for a good reason.</p>
 *
 * <p>A peer that does not answer within the timeout counts as unknown: it
 * has no closed election and no progress. The probe runs on the watchdog
 * thread only.</p>
 */
final class PeerProbe {

    /** The bound on the connect and on the answer of one peer. */
    private static final Duration TIMEOUT = Duration.ofSeconds(1);

    /** The admin status URL of each other member. */
    private final List<URI> peers;
    private final HttpClient http;
    /** The highest commit position that each peer reported. */
    private final Map<URI, Long> highestCommit = new HashMap<>();

    PeerProbe(final List<URI> peers) {
        this.peers = peers;
        this.http = HttpClient.newBuilder().connectTimeout(TIMEOUT).build();
    }

    /**
     * The probe of the other members of {@code clusterMembers}: each member
     * entry is {@code id,ingress,consensus,log,catchup,archive}, and the
     * admin server of a member listens on the host of its ingress endpoint.
     * With an admin port of 0 the members serve no status, and the probe
     * knows no peer.
     */
    static PeerProbe forMembers(final String clusterMembers, final int memberId, final int adminPort) {
        if (adminPort == 0) {
            return new PeerProbe(List.of());
        }
        return new PeerProbe(ClusterNode.peerEntries(clusterMembers, memberId)
            .map(fields -> URI.create("http://" + fields[1].trim().split(":")[0] + ":" + adminPort + "/status"))
            .toList());
    }

    /** The first peer that reports a closed election, or empty when none does. */
    Optional<PeerStatus> closedPeer() {
        return peers.stream()
            .flatMap(peer -> status(peer).stream())
            .filter(PeerStatus::electionClosed)
            .findFirst();
    }

    /**
     * Ask each peer for its commit position. Return the sum of the highest
     * commit position of each peer. The sum grows when any peer makes
     * commit progress, and a peer that does not answer keeps its last value.
     */
    long commitMark() {
        peers.forEach(peer -> status(peer).ifPresent(answer ->
            highestCommit.merge(peer, answer.commitPosition(), Math::max)));
        return highestCommit.values().stream().mapToLong(Long::longValue).sum();
    }

    private Optional<PeerStatus> status(final URI peer) {
        final HttpRequest request = HttpRequest.newBuilder(peer).timeout(TIMEOUT).GET().build();
        try {
            return PeerStatus.parse(http.send(request, HttpResponse.BodyHandlers.ofString()).body());
        } catch (final IOException e) {
            return Optional.empty();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        }
    }

    /** The part of a peer's {@link MemberStatus} JSON that the watchdog reads. */
    record PeerStatus(int memberId, String role, String election, long commitPosition) {
        private static final Pattern FIELDS = Pattern.compile(
            "\"memberId\":(\\d+),\"role\":\"([A-Z_]+)\",\"election\":\"([A-Z_]+)\",\"commitPosition\":(-?\\d+)");

        /** The fields of a status answer, or empty for any other body, such as an error object. */
        static Optional<PeerStatus> parse(final String json) {
            final Matcher m = FIELDS.matcher(json);
            return m.find()
                ? Optional.of(new PeerStatus(
                    Integer.parseInt(m.group(1)), m.group(2), m.group(3), Long.parseLong(m.group(4))))
                : Optional.empty();
        }

        /**
         * Whether the peer runs with a closed election. A member that shuts
         * down reads CLOSED for its role and its election, so the role must
         * be a settled one.
         */
        boolean electionClosed() {
            return MemberStatus.ELECTION_CLOSED.equals(election)
                && (MemberStatus.ROLE_LEADER.equals(role) || MemberStatus.ROLE_FOLLOWER.equals(role));
        }
    }
}
