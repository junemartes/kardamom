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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
 * <p>Each round asks every peer at once and waits at most {@link #ROUND}.
 * The lookup of a peer name has no timeout of its own, so each request
 * runs on a thread of the probe: a lookup that hangs blocks that thread,
 * not the watchdog. A peer that does not answer within the round counts as
 * unknown.</p>
 */
final class PeerProbe {

    /** The bound on the connect and on the answer of one peer. */
    private static final Duration TIMEOUT = Duration.ofSeconds(1);
    /** The bound on one round, the name lookups included. */
    private static final Duration ROUND = Duration.ofSeconds(2);

    /** The admin status URL of each other member. */
    private final List<URI> peers;
    private final HttpClient http;
    /** The threads of the requests. A thread lives only while a lookup or a request runs. */
    private final ExecutorService requests = Executors.newCachedThreadPool(PeerProbe::daemon);
    /** The highest commit position that each peer reported; only the watchdog thread uses it. */
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

    /** Ask every peer once. */
    Round ask() {
        return new Round(peers.size(), answers().values().stream().flatMap(Optional::stream).toList());
    }

    /**
     * Ask each peer for its commit position. Return the sum of the highest
     * commit position of each peer. The sum grows when any peer makes
     * commit progress, and a peer that does not answer keeps its last value.
     */
    long commitMark() {
        answers().forEach((peer, answer) -> answer.ifPresent(status ->
            highestCommit.merge(peer, status.commitPosition(), Math::max)));
        return highestCommit.values().stream().mapToLong(Long::longValue).sum();
    }

    /** The answer of each peer within one round, or empty for a peer that did not answer. */
    private Map<URI, Optional<PeerStatus>> answers() {
        final Map<URI, CompletableFuture<Optional<PeerStatus>>> pending = new HashMap<>();
        peers.forEach(peer -> pending.put(peer, CompletableFuture
            .supplyAsync(() -> status(peer), requests)
            .completeOnTimeout(Optional.empty(), ROUND.toMillis(), TimeUnit.MILLISECONDS)));
        final Map<URI, Optional<PeerStatus>> answers = new HashMap<>();
        pending.forEach((peer, answer) -> answers.put(peer, answer.join()));
        return answers;
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

    private static Thread daemon(final Runnable task) {
        final Thread thread = new Thread(task, "kardamom-peer-probe");
        thread.setDaemon(true);
        return thread;
    }

    /**
     * The answers of one round.
     *
     * @param peers how many peers the probe asked.
     * @param answers the status of each peer that answered.
     */
    record Round(int peers, List<PeerStatus> answers) {

        /** The first peer that reports a closed election, or empty when none does. */
        Optional<PeerStatus> closedPeer() {
            return answers.stream().filter(PeerStatus::electionClosed).findFirst();
        }

        /**
         * What the round shows about the elections of the peers. It shows
         * no closed election only when the probe knows peers and every one
         * answered without one.
         */
        JoinWatchdog.PeerElections elections() {
            if (closedPeer().isPresent()) {
                return JoinWatchdog.PeerElections.CLOSED;
            }
            return peers > 0 && answers.size() == peers
                ? JoinWatchdog.PeerElections.NONE_CLOSED
                : JoinWatchdog.PeerElections.UNKNOWN;
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
