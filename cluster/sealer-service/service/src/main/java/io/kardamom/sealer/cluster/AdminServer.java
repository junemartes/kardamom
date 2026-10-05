package io.kardamom.sealer.cluster;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 * The member's admin endpoint: a small HTTP server that reports the
 * {@link MemberStatus}.
 *
 * <p>{@code GET /status} answers 200 with the status JSON. {@code GET /ready}
 * answers 200 when the member is ready and 503 when it is not, with the same
 * JSON, so a service check reads the verdict from the status code and an
 * operator reads the reason from the body. Every other path answers 404.
 * A handler never throws: a failing status supplier answers 503 with an
 * error object. The server runs on one daemon thread, so it never keeps
 * the JVM alive.</p>
 */
final class AdminServer implements AutoCloseable {
    private static final int OK = 200;
    private static final int NOT_FOUND = 404;
    private static final int UNAVAILABLE = 503;

    private final HttpServer server;
    private final Supplier<MemberStatus> status;
    private final long lagBytes;

    private AdminServer(
            final HttpServer server, final Supplier<MemberStatus> status, final long lagBytes) {
        this.server = server;
        this.status = status;
        this.lagBytes = lagBytes;
    }

    /**
     * Bind {@code 0.0.0.0:port} (0 picks a free port) and start serving.
     *
     * @throws IOException when the bind fails
     */
    static AdminServer start(
            final int port, final Supplier<MemberStatus> status, final long lagBytes)
            throws IOException {
        final HttpServer http = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0);
        final AdminServer admin = new AdminServer(http, status, lagBytes);
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
        } else {
            respond(exchange, NOT_FOUND, "{\"error\":\"not found\"}");
        }
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
        final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
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
