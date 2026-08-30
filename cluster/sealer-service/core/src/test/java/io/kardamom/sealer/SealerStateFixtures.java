package io.kardamom.sealer;

/**
 * Shared input builders for the {@link CanonicalSealerState} unit tests:
 * {@link CanonicalSealerStateTest}, {@link ContiguityGuardTest}, and
 * {@link OriginRecordTest}.
 * This class depends only on the core module and the JDK, not on Aeron or
 * service-module classes. This keeps the deterministic POJO tests runnable
 * even when the cluster transport does not build.
 */
final class SealerStateFixtures {

    private SealerStateFixtures() {
    }

    /** Build a 32-byte canonical id. Every byte equals {@code b}. */
    static byte[] id(int b) {
        byte[] out = new byte[CanonicalSealerState.CANONICAL_ID_LEN];
        java.util.Arrays.fill(out, (byte) b);
        return out;
    }

    /** Build a 20-byte sender id. Every byte equals {@code b}. */
    static byte[] sender(int b) {
        byte[] out = new byte[CanonicalSealerState.SENDER_LEN];
        java.util.Arrays.fill(out, (byte) b);
        return out;
    }

    static byte[] payload(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}
