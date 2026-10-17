package io.kardamom.sealer.cluster;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Optional;

/**
 * The shared token that guards the retained file on the admin endpoint.
 * The job passes it in the env var {@value #ENV}, not in a system property,
 * because {@code ps} shows the properties. Its {@link #toString()} hides the
 * value, so a log line that prints a holder of the token does not print it.
 */
final class RetainedToken {

    /** The env var of the token. */
    static final String ENV = "KARDAMOM_RETAINED_TOKEN";

    private final String value;

    private RetainedToken(final String value) {
        this.value = value;
    }

    /** The token in {@code raw}, trimmed, or empty when it is unset or blank. */
    static Optional<RetainedToken> of(final String raw) {
        return Optional.ofNullable(raw).map(String::trim).filter(v -> !v.isEmpty()).map(RetainedToken::new);
    }

    /** The value of the {@code Authorization} header of a request. */
    String authorization() {
        return "Bearer " + value;
    }

    /** Whether {@code header}, the {@code Authorization} header of a request, carries this token. */
    boolean authorizes(final String header) {
        return header != null && MessageDigest.isEqual(
            header.getBytes(StandardCharsets.UTF_8), authorization().getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String toString() {
        return "RetainedToken[hidden]";
    }
}
