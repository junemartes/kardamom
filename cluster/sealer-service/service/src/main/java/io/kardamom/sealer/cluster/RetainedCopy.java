package io.kardamom.sealer.cluster;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

/**
 * The copy of the retained file from a peer, for a member that a peer
 * seeds. The Aeron ClusterBackup of the seed copies the snapshot and the
 * log. The snapshot holds only the index of the retained file, so this copy
 * takes every segment file from the admin endpoint of one peer
 * ({@link AdminServer#RETAINED_PATH}). Every member holds the same bytes at
 * the same position, so any peer serves.
 *
 * <p>The peer keeps appending during the copy, and its last segment can
 * hold bytes past the snapshot. The restore truncates the file at the head
 * position of the snapshot, and stops the member when the copy does not
 * hold every byte from the front of the snapshot.</p>
 *
 * @param peers    the admin endpoints of the other members, {@code http://host:port}
 * @param into     the directory of the retained file of this member
 * @param memberId this member's id, for the log lines
 */
record RetainedCopy(List<URI> peers, Path into, int memberId) {

    /** The longest wait for a connection or for one answer. */
    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private static final int OK = 200;

    /** Copy every segment from the first peer that serves them all. Return whether a peer did. */
    boolean run() {
        final HttpClient http = HttpClient.newBuilder().connectTimeout(TIMEOUT).build();
        return peers.stream().anyMatch(peer -> copyFrom(http, peer));
    }

    private boolean copyFrom(final HttpClient http, final URI peer) {
        try {
            new StateDir(into).clear();
            final List<String> names = list(http, peer);
            for (final String name : names) {
                fetch(http, peer, name);
            }
            System.out.println("cluster SEED retained-copy memberId=" + memberId + " peer=" + peer
                + " segments=" + names.size());
            return true;
        } catch (final IOException e) {
            System.out.println("cluster SEED retained-copy FAILED memberId=" + memberId + " peer=" + peer
                + ": " + e);
            return false;
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("retained-file copy interrupted", e);
        }
    }

    /** The segment names that {@code peer} lists. A line that is not a segment name fails the copy. */
    private static List<String> list(final HttpClient http, final URI peer)
            throws IOException, InterruptedException {
        final HttpResponse<String> response = http.send(
            request(peer.resolve(AdminServer.RETAINED_PATH)), HttpResponse.BodyHandlers.ofString());
        requireOk(response, AdminServer.RETAINED_PATH);
        final List<String> names = response.body().lines().map(line -> line.split(" ")[0]).toList();
        if (!names.stream().allMatch(name -> RetainedFile.SEGMENT_NAME.matcher(name).matches())) {
            throw new IOException("the peer lists a name that is not a segment: " + names);
        }
        return names;
    }

    private void fetch(final HttpClient http, final URI peer, final String name)
            throws IOException, InterruptedException {
        final Path target = into.resolve(name);
        final String path = AdminServer.RETAINED_PATH + "/" + name;
        final HttpResponse<Path> response = http.send(request(peer.resolve(path)),
            HttpResponse.BodyHandlers.ofFile(target));
        if (response.statusCode() != OK) {
            Files.deleteIfExists(target);
        }
        requireOk(response, path);
    }

    private static HttpRequest request(final URI uri) {
        return HttpRequest.newBuilder().uri(uri).timeout(TIMEOUT).GET().build();
    }

    private static void requireOk(final HttpResponse<?> response, final String path) throws IOException {
        if (response.statusCode() != OK) {
            throw new IOException("GET " + path + " answered " + response.statusCode());
        }
    }
}
