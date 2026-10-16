package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The admin endpoint's routes and status codes, with a fixed status source. */
final class AdminServerTest {
    private static final long LAG = 1024;

    private static final MemberStatus.SnapshotVersions SNAPSHOT = MemberStatus.SnapshotVersions.of(0);
    private static final MemberStatus READY =
        new MemberStatus(0, "LEADER", "CLOSED", 10, 10, SNAPSHOT, OptionalLong.of(4096));
    private static final MemberStatus BEHIND =
        new MemberStatus(0, "FOLLOWER", "CLOSED", 5000, 10, SNAPSHOT, OptionalLong.of(4096));
    private static final String TOKEN = "s3cret";

    private static HttpResponse<String> get(final AdminServer admin, final String path)
            throws IOException, InterruptedException {
        return get(admin, path, "none");
    }

    private static HttpResponse<String> get(final AdminServer admin, final String path, final String token)
            throws IOException, InterruptedException {
        final HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:" + admin.port() + path))
            .header("Authorization", RetainedEndpoint.authorization(token))
            .GET()
            .build();
        return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> serve(final Supplier<MemberStatus> status, final String path)
            throws IOException, InterruptedException {
        try (AdminServer admin = AdminServer.start(0, status, LAG, Optional.empty())) {
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
    void anUnreadableDiskReadsAsUnknownAndKeepsTheMemberReady() throws Exception {
        final MemberStatus unknown = new MemberStatus(0, "LEADER", "CLOSED", 10, 10, SNAPSHOT, OptionalLong.empty());
        assertEquals(200, serve(() -> unknown, "/ready").statusCode());
        assertTrue(serve(() -> unknown, "/status").body().contains("\"diskFreeBytes\":null"));
        assertTrue(serve(() -> unknown, "/metrics").body().endsWith("} NaN\n"));
    }

    @Test
    void theRetainedFileServesItsSegmentsBelowThePublishedHead(@TempDir final Path dir) throws Exception {
        final SegmentDir from = new SegmentDir(Files.createDirectories(dir.resolve("from")));
        Files.write(from.segment(0), new byte[] {1, 2, 3, 4, 5});
        Files.write(from.segment(5), new byte[] {6, 7, 0, 0});
        Files.write(from.segment(9), new byte[] {8});
        Files.write(from.dir().resolve(SegmentPreallocator.PREPARED_NAME), new byte[] {9});
        final RetainedEndpoint endpoint = RetainedEndpoint.start(from, () -> 7, TOKEN);
        try (AdminServer admin = AdminServer.start(0, () -> READY, LAG, Optional.of(endpoint))) {
            assertEquals(403, get(admin, "/retained").statusCode(), "a request without the token");
            assertEquals(SegmentDir.name(0) + " 5\n" + SegmentDir.name(5) + " 2\n",
                get(admin, "/retained", TOKEN).body(), "only the bytes below the head");
            assertEquals(2, get(admin, "/retained/" + SegmentDir.name(5), TOKEN).body().length());
            assertEquals(404, get(admin, "/retained/" + SegmentDir.name(9), TOKEN).statusCode());
            assertEquals(404, get(admin, "/retained/" + SegmentPreallocator.PREPARED_NAME, TOKEN).statusCode());
        }
    }
}
