package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/** The admin endpoint's routes and status codes, with a fixed status source. */
final class AdminServerTest {
    private static final long LAG = 1024;

    private static final MemberStatus.SnapshotVersions SNAPSHOT = MemberStatus.SnapshotVersions.of(0);
    private static final MemberStatus READY = new MemberStatus(0, "LEADER", "CLOSED", 10, 10, SNAPSHOT);
    private static final MemberStatus BEHIND =
        new MemberStatus(0, "FOLLOWER", "CLOSED", 5000, 10, SNAPSHOT);

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
        assertEquals(404, serve(() -> READY, "/metrics").statusCode());
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
}
