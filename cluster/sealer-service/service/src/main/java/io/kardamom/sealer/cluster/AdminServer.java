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
 * The member's admin endpoint: a small HTTP server that reports the
 * {@link MemberStatus}.
 *
 * <p>{@code GET /status} answers 200 with the status JSON. {@code GET /ready}
 * answers 200 when the member is ready and 503 when it is not, with the same
 * JSON, so a service check reads the verdict from the status code and an
 * operator reads the reason from the body. {@code GET /metrics} answers the
 * metrics text in the Prometheus text format.</p>
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

    private final HttpServer server;
    private final Supplier<MemberStatus> status;
    /** The Prometheus text of one scrape. */
    private final Supplier<String> metrics;
    private final long lagBytes;
    /** The segments of the retained file, or empty when the member keeps the frames on the heap. */
    private final Optional<RetainedEndpoint> retained;

    private AdminServer(
            final HttpServer server,
            final Supplier<MemberStatus> status,
            final Supplier<String> metrics,
            final long lagBytes,
            final Optional<RetainedEndpoint> retained) {
        this.server = server;
        this.status = status;
        this.metrics = metrics;
        this.lagBytes = lagBytes;
        this.retained = retained;
    }

    /**
     * Bind {@code 0.0.0.0:port} (0 picks a free port) and start serving,
     * with the segments of {@code retained} when it is present. The
     * metrics text is the text of the status sample.
     *
     * @throws IOException when the bind fails
     */
    static AdminServer start(
            final int port,
            final Supplier<MemberStatus> status,
            final long lagBytes,
            final Optional<RetainedEndpoint> retained)
            throws IOException {
        return start(port, status, () -> status.get().toMetrics(), lagBytes, retained);
    }

    /**
     * {@link #start(int, Supplier, long, Optional)} with the metrics text of
     * {@code metrics}.
     *
     * @throws IOException when the bind fails
     */
    static AdminServer start(
            final int port,
            final Supplier<MemberStatus> status,
            final Supplier<String> metrics,
            final long lagBytes,
            final Optional<RetainedEndpoint> retained)
            throws IOException {
        final HttpServer http = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0);
        final AdminServer admin = new AdminServer(http, status, metrics, lagBytes, retained);
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
        if ("/status".equals(path)) {
            respondWithStatus(exchange, false);
        } else if ("/ready".equals(path)) {
            respondWithStatus(exchange, true);
        } else if ("/metrics".equals(path)) {
            respondWithMetrics(exchange);
        } else if (retained.isPresent()
                && (RetainedEndpoint.PATH.equals(path) || path.startsWith(RetainedEndpoint.PATH + "/"))) {
            retained.get().handle(exchange, path);
        } else {
            respond(exchange, NOT_FOUND, "{\"error\":\"not found\"}");
        }
    }

    private void respondWithMetrics(final HttpExchange exchange) throws IOException {
        final String text;
        try {
            text = metrics.get();
        } catch (final RuntimeException e) {
            respond(exchange, UNAVAILABLE, "{\"error\":\"" + escape(String.valueOf(e)) + "\"}");
            return;
        }
        respond(exchange, OK, "text/plain; version=0.0.4", text);
    }

    /**
     * Answer with the status JSON. On {@code /ready} the status code carries
     * the verdict; on {@code /status} it is always 200.
     */
    private void respondWithStatus(final HttpExchange exchange, final boolean verdictInCode)
            throws IOException {
        final MemberStatus sample;
        try {
            sample = status.get();
        } catch (final RuntimeException e) {
            respond(exchange, UNAVAILABLE, "{\"error\":\"" + escape(String.valueOf(e)) + "\"}");
            return;
        }
        final boolean ready = !verdictInCode || sample.isReady(lagBytes);
        respond(exchange, ready ? OK : UNAVAILABLE, sample.toJson(lagBytes));
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
