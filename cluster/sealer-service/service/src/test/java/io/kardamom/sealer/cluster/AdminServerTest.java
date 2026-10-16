package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The admin endpoint's routes and status codes, with a fixed status source. */
final class AdminServerTest {
    private static final long LAG = 1024;

    private static final MemberStatus.SnapshotVersions SNAPSHOT = MemberStatus.SnapshotVersions.of(0);
    private static final MemberStatus READY = new MemberStatus(0, "LEADER", "CLOSED", 10, 10, SNAPSHOT, 4096);
    private static final MemberStatus BEHIND =
        new MemberStatus(0, "FOLLOWER", "CLOSED", 5000, 10, SNAPSHOT, 4096);

    private static HttpResponse<String> get(final AdminServer admin, final String path)
            throws IOException, InterruptedException {
        final HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:" + admin.port() + path))
            .GET()
            .build();
        return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> serve(final Supplier<MemberStatus> status, final String path)
            throws IOException, InterruptedException {
        try (AdminServer admin = AdminServer.start(0, status, LAG)) {
            return get(admin, path);
        }
    }

    @Test
    void statusAnswersTheJson() throws Exception {
        final HttpResponse<String> response = serve(() -> BEHIND, "/status");
        assertEquals(200, response.statusCode());
        assertEquals("application/json", response.headers().firstValue("Content-Type").orElse(""));
        assertEquals(BEHIND.toJson(LAG), response.body());
    }

    @Test
    void readyAnswers200WhenReady() throws Exception {
        final HttpResponse<String> response = serve(() -> READY, "/ready");
        assertEquals(200, response.statusCode());
        assertEquals(READY.toJson(LAG), response.body());
    }

    @Test
    void readyAnswers503WhenNotReady() throws Exception {
        final HttpResponse<String> response = serve(() -> BEHIND, "/ready");
        assertEquals(503, response.statusCode());
        assertEquals(BEHIND.toJson(LAG), response.body());
    }

    @Test
    void otherPathsAnswer404() throws Exception {
        assertEquals(404, serve(() -> READY, "/other").statusCode());
        assertEquals(404, serve(() -> READY, "/retained").statusCode(), "no retained file");
    }

    @Test
    void failingSourceAnswers503WithAnError() throws Exception {
        final HttpResponse<String> response = serve(() -> {
            throw new IllegalStateException("counters \"gone\"");
        }, "/ready");
        assertEquals(503, response.statusCode());
        assertTrue(response.body().startsWith("{\"error\":\""), response.body());
        assertTrue(response.body().contains("\\\"gone\\\""), response.body());
    }

    @Test
    void metricsAnswerThePrometheusText() throws Exception {
        final HttpResponse<String> response = serve(() -> READY, "/metrics");
        assertEquals(200, response.statusCode());
        assertEquals(READY.toMetrics(), response.body());
    }

    @Test
    void theRetainedFileServesItsSegmentsToACopy(@TempDir final Path dir) throws Exception {
        final Path from = Files.createDirectories(dir.resolve("from"));
        final String first = RetainedFile.segmentName(0);
        final String second = RetainedFile.segmentName(5);
        Files.write(from.resolve(first), new byte[] {1, 2, 3, 4, 5});
        Files.write(from.resolve(second), new byte[] {6, 7});
        Files.write(from.resolve(SegmentPreallocator.PREPARED_NAME), new byte[] {9});
        try (AdminServer admin = AdminServer.start(0, () -> READY, LAG, Optional.of(from))) {
            assertEquals(first + " 5\n" + second + " 2\n", get(admin, "/retained").body());
            assertEquals(404, get(admin, "/retained/" + SegmentPreallocator.PREPARED_NAME).statusCode());
            assertEquals(404, get(admin, "/retained/" + RetainedFile.segmentName(7)).statusCode());

            final Path into = dir.resolve("into");
            final URI peer = URI.create("http://127.0.0.1:" + admin.port());
            final URI deadPeer = URI.create("http://127.0.0.1:1");
            assertTrue(new RetainedCopy(List.of(deadPeer, peer), into, 1).run(), "the second peer serves");
            assertArrayEquals(Files.readAllBytes(from.resolve(first)), Files.readAllBytes(into.resolve(first)));
            assertArrayEquals(Files.readAllBytes(from.resolve(second)), Files.readAllBytes(into.resolve(second)));
            try (Stream<Path> copied = Files.list(into)) {
                assertEquals(2, copied.count(), "the copy takes only the segments");
            }
        }
    }
}
