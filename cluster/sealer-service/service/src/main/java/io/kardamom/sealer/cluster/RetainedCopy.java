package io.kardamom.sealer.cluster;

import io.kardamom.sealer.CanonicalSealerState;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import org.agrona.IoUtil;

/**
 * The copy of the retained file from a peer, for a member that a peer
 * seeds. The Aeron ClusterBackup of the seed copies the snapshot and the
 * log. The snapshot holds only the index of the retained file, so this copy
 * takes every segment file from the admin endpoint of one peer
 * ({@link RetainedEndpoint}). Every member holds the same bytes at the same
 * position, so any peer serves.
 *
 * <p>The copy passes only when {@link RetainedWalk} finds exactly the frames
 * of the backed-up snapshot from its front to its head. A peer serves only
 * below its published head, and purges the segments below the front of its
 * snapshot before the last one. So a copy fails when the peer is behind the
 * snapshot, or took two snapshots since the backup. The seed then tries the
 * next peer, and then a new round.</p>
 *
 * @param peers    the admin endpoints of the other members, {@code http://host:port}
 * @param into     the directory of the retained file of this member
 * @param token    the shared token of the job
 * @param memberId this member's id, for the log lines
 */
record RetainedCopy(List<URI> peers, SegmentDir into, String token, int memberId) {

    /** The longest wait for a connection or for one answer. */
    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private static final int OK = 200;
    /** The offset of the version in a snapshot payload: after the magic. */
    private static final int VERSION_OFFSET = Integer.BYTES;

    /**
     * Copy the segments from the first peer whose copy matches
     * {@code snapshot}, the payload of the backed-up sealer snapshot. Return
     * whether a peer did.
     */
    boolean run(final byte[] snapshot) {
        if (ByteBuffer.wrap(snapshot).getInt(VERSION_OFFSET) != CanonicalSealerState.RETAINED_FILE_SNAPSHOT_VERSION) {
            System.out.println("cluster SEED retained-copy FAILED memberId=" + memberId
                + " — the backed-up snapshot holds no retained file");
            return false;
        }
        final RetainedSection section = RetainedSection.ofSnapshot(snapshot);
        final HttpClient http = HttpClient.newBuilder().connectTimeout(TIMEOUT).build();
        return peers.stream().anyMatch(peer -> copyFrom(http, peer, section));
    }

    private boolean copyFrom(final HttpClient http, final URI peer, final RetainedSection section) {
        try {
            new StateDir(into.dir()).clear();
            final List<String> names = list(http, peer);
            for (final String name : names) {
                fetch(http, peer, name);
            }
            into.sync();
            final Optional<String> problem = check(section);
            System.out.println("cluster SEED retained-copy" + problem.map(p -> " FAILED").orElse("")
                + " memberId=" + memberId + " peer=" + peer + " segments=" + names.size()
                + problem.map(p -> " — " + p).orElse(""));
            return problem.isEmpty();
        } catch (final IOException e) {
            System.out.println("cluster SEED retained-copy FAILED memberId=" + memberId + " peer=" + peer
                + ": " + e);
            return false;
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("retained-file copy interrupted", e);
        }
    }

    /** Walk the copied segments from the front to the head of {@code section}. */
    private Optional<String> check(final RetainedSection section) throws IOException {
        final TreeMap<Long, MappedByteBuffer> mapped = new TreeMap<>();
        try {
            for (final Long start : into.list().headMap(section.head(), false).keySet()) {
                mapped.put(start, into.map(start, FileChannel.MapMode.READ_ONLY));
            }
            return new RetainedWalk(mapped, section).problem();
        } finally {
            mapped.values().forEach(IoUtil::unmap);
        }
    }

    /** The segment names that {@code peer} lists. A line that is not a segment name fails the copy. */
    private List<String> list(final HttpClient http, final URI peer) throws IOException, InterruptedException {
        final HttpResponse<String> response = http.send(
            request(peer.resolve(RetainedEndpoint.PATH)), HttpResponse.BodyHandlers.ofString());
        requireOk(response, RetainedEndpoint.PATH);
        final List<String> names = response.body().lines().map(line -> line.split(" ")[0]).toList();
        if (!names.stream().allMatch(name -> SegmentDir.SEGMENT_NAME.matcher(name).matches())) {
            throw new IOException("the peer lists a name that is not a segment: " + names);
        }
        return names;
    }

    /** Fetch one segment, and sync it to disk. */
    private void fetch(final HttpClient http, final URI peer, final String name)
            throws IOException, InterruptedException {
        final var target = into.dir().resolve(name);
        final String path = RetainedEndpoint.PATH + "/" + name;
        final HttpResponse<Path> response = http.send(request(peer.resolve(path)),
            HttpResponse.BodyHandlers.ofFile(target));
        if (response.statusCode() != OK) {
            Files.deleteIfExists(target);
        }
        requireOk(response, path);
        try (FileChannel channel = FileChannel.open(target, StandardOpenOption.WRITE)) {
            channel.force(true);
        }
    }

    private HttpRequest request(final URI uri) {
        return HttpRequest.newBuilder().uri(uri).timeout(TIMEOUT)
            .header("Authorization", RetainedEndpoint.authorization(token))
            .GET().build();
    }

    private static void requireOk(final HttpResponse<?> response, final String path) throws IOException {
        if (response.statusCode() != OK) {
            throw new IOException("GET " + path + " answered " + response.statusCode());
        }
    }
}
