package io.kardamom.sealer.cluster;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 * The admin endpoint of a node: a small HTTP server that reports the
 * {@link MemberStatus} of a member, or the {@link BackupStatus} of a
 * backup node.
 *
 * <p>{@code GET /status} answers 200 with the status JSON. {@code GET /ready}
 * answers 200 when the node is ready and 503 when it is not, with the same
 * JSON, so a service check reads the verdict from the status code and an
 * operator reads the reason from the body. {@code GET /metrics} answers the
 * gauges in the Prometheus text format.</p>
 *
 * <p>With a retained file, {@link RetainedEndpoint} answers the paths
 * under {@code /retained} on its own threads. Every other path answers 404.
 * A failing status supplier answers 503 with an error object. The server
 * runs on one daemon thread, so it never keeps the JVM alive.</p>
 */
final class AdminServer implements AutoCloseable {
    private static final int OK = 200;
    private static final int NOT_FOUND = 404;
    private static final int UNAVAILABLE = 503;

    /**
     * One sample of a node, as the endpoint answers it.
     *
     * @param ready   the verdict of {@code /ready}
     * @param json    the body of {@code /status} and {@code /ready}
     * @param metrics the body of {@code /metrics}, in the Prometheus text format
     */
    record Report(boolean ready, String json, String metrics) {
    }

    private final HttpServer server;
    private final Supplier<Report> reports;
    /** The segments of the retained file, or empty when the member keeps the frames on the heap. */
    private final Optional<RetainedEndpoint> retained;

    private AdminServer(
            final HttpServer server,
            final Supplier<Report> reports,
            final Optional<RetainedEndpoint> retained) {
        this.server = server;
        this.reports = reports;
        this.retained = retained;
    }

    /**
     * Bind {@code 0.0.0.0:port} (0 picks a free port) and serve the member
     * status, with the segments of {@code retained} when it is present. A
     * member is ready within {@code lagBytes} of the commit position.
     *
     * @throws IOException when the bind fails
     */
    static AdminServer start(
            final int port,
            final Supplier<MemberStatus> status,
            final long lagBytes,
            final Optional<RetainedEndpoint> retained)
            throws IOException {
        return serve(port, () -> status.get().report(lagBytes), retained);
    }

    /**
     * Bind {@code 0.0.0.0:port} (0 picks a free port) and serve the reports
     * of {@code reports}, with the segments of {@code retained} when it is
     * present.
     *
     * @throws IOException when the bind fails
     */
    static AdminServer serve(
            final int port,
            final Supplier<Report> reports,
            final Optional<RetainedEndpoint> retained)
            throws IOException {
        final HttpServer http = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0);
        final AdminServer admin = new AdminServer(http, reports, retained);
        http.createContext("/", admin::handle);
        http.setExecutor(Executors.newSingleThreadExecutor(admin::daemonThread));
        http.start();
        return admin;
    }

    /** The port the server listens on. */
    int port() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
        retained.ifPresent(RetainedEndpoint::close);
    }

    private Thread daemonThread(final Runnable task) {
        final Thread t = new Thread(task, "kardamom-admin-endpoint");
        t.setDaemon(true);
        return t;
    }

    private void handle(final HttpExchange exchange) throws IOException {
        final String path = exchange.getRequestURI().getPath();
        if ("/status".equals(path) || "/ready".equals(path) || "/metrics".equals(path)) {
            respondWithReport(exchange, path);
        } else if (retained.isPresent()
                && (RetainedEndpoint.PATH.equals(path) || path.startsWith(RetainedEndpoint.PATH + "/"))) {
            retained.get().handle(exchange, path);
        } else {
            respond(exchange, NOT_FOUND, "{\"error\":\"not found\"}");
        }
    }

    /**
     * Answer one sample. {@code /metrics} answers the Prometheus text. The
     * other paths answer the status JSON: on {@code /ready} the status code
     * carries the verdict; on {@code /status} it is always 200.
     */
    private void respondWithReport(final HttpExchange exchange, final String path) throws IOException {
        final Report report;
        try {
            report = reports.get();
        } catch (final RuntimeException e) {
            respond(exchange, UNAVAILABLE, "{\"error\":\"" + escape(String.valueOf(e)) + "\"}");
            return;
        }
        if ("/metrics".equals(path)) {
            respond(exchange, OK, "text/plain; version=0.0.4", report.metrics());
            return;
        }
        final boolean ready = "/status".equals(path) || report.ready();
        respond(exchange, ready ? OK : UNAVAILABLE, report.json());
    }

    private static void respond(final HttpExchange exchange, final int code, final String body)
            throws IOException {
        respond(exchange, code, "application/json", body);
    }

    private static void respond(
            final HttpExchange exchange, final int code, final String contentType, final String body)
            throws IOException {
        final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(code, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    /** Make an exception message safe inside a JSON string. */
    private static String escape(final String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", " ");
    }
}
