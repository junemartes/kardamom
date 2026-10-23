package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.cluster.ClusterBackup;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** The status of a backup node, and how the admin endpoint answers it. */
final class BackupStatusTest {
    private static final BackupStatus BACKING_UP = new BackupStatus(
        ClusterBackup.State.BACKING_UP, 4096, new BackupStatus.Snapshot(2048, 1));
    private static final BackupStatus QUERYING = new BackupStatus(
        ClusterBackup.State.BACKUP_QUERY, 0, BackupStatus.Snapshot.NONE);

    private static HttpResponse<String> serve(final BackupStatus status, final String path)
            throws IOException, InterruptedException {
        try (AdminServer admin = AdminServer.serve(0, status::report, Optional.empty())) {
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + admin.port() + path))
                .GET()
                .build();
            return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        }
    }

    /**
     * A backup that holds a snapshot stays ready through the query of each
     * interval. A first query, with no snapshot yet, is not ready.
     */
    @Test
    void theBackupIsReadyWhileItHoldsASnapshotAndBacksUp() {
        assertTrue(BACKING_UP.isReady());
        assertTrue(new BackupStatus(ClusterBackup.State.BACKUP_QUERY, 4096, BACKING_UP.snapshot()).isReady());
        assertFalse(QUERYING.isReady());
        assertFalse(new BackupStatus(ClusterBackup.State.SNAPSHOT_RETRIEVE, 4096, BACKING_UP.snapshot()).isReady());
    }

    @Test
    void theJsonNamesTheModeTheStateAndTheLatestSnapshot() {
        assertEquals("{\"mode\":\"backup\",\"state\":\"BACKING_UP\",\"stateCode\":5,\"liveLogPosition\":4096"
            + ",\"latestSnapshotLogPosition\":2048,\"latestSnapshotLeadershipTermId\":1,\"ready\":true}",
            BACKING_UP.toJson());
        assertEquals("{\"mode\":\"backup\",\"state\":\"BACKUP_QUERY\",\"stateCode\":0,\"liveLogPosition\":0"
            + ",\"latestSnapshotLogPosition\":-1,\"latestSnapshotLeadershipTermId\":-1,\"ready\":false}",
            QUERYING.toJson());
    }

    @Test
    void theLogLineChangesWithTheStateOrTheSnapshotOnly() {
        assertTrue(BACKING_UP.sameStage(new BackupStatus(ClusterBackup.State.BACKING_UP, 8192, BACKING_UP.snapshot())));
        // The query of each interval is routine: no log line.
        assertTrue(BACKING_UP.sameStage(new BackupStatus(ClusterBackup.State.BACKUP_QUERY, 4096, BACKING_UP.snapshot())));
        assertFalse(BACKING_UP.sameStage(
            new BackupStatus(ClusterBackup.State.SNAPSHOT_RETRIEVE, 4096, BACKING_UP.snapshot())));
        assertFalse(BACKING_UP.sameStage(
            new BackupStatus(ClusterBackup.State.BACKING_UP, 4096, new BackupStatus.Snapshot(3072, 1))));
        assertEquals("cluster BACKUP state=BACKING_UP liveLogPosition=4096 snapshotLogPosition=2048"
            + " snapshotLeadershipTermId=1", BACKING_UP.logLine());
    }

    @Test
    void readyAnswers200WhileBackingUpAnd503Else() throws Exception {
        final HttpResponse<String> ready = serve(BACKING_UP, "/ready");
        final HttpResponse<String> notReady = serve(QUERYING, "/ready");
        final HttpResponse<String> status = serve(QUERYING, "/status");

        assertEquals(200, ready.statusCode());
        assertEquals(BACKING_UP.toJson(), ready.body());
        assertEquals(503, notReady.statusCode());
        assertEquals(QUERYING.toJson(), notReady.body());
        assertEquals(200, status.statusCode());
        assertEquals(QUERYING.toJson(), status.body());
    }

    @Test
    void metricsAnswerTheThreeGauges() throws Exception {
        final HttpResponse<String> response = serve(BACKING_UP, "/metrics");

        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("\nkardamom_backup_state 5\n"), response.body());
        assertTrue(response.body().contains("\nkardamom_backup_live_log_position 4096\n"), response.body());
        assertTrue(response.body().contains("\nkardamom_backup_snapshot_log_position 2048\n"), response.body());
    }
}
