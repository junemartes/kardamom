package io.kardamom.sealer;

/**
 * Shared input builders for the {@link CanonicalSealerState} unit tests
 * ({@link CanonicalSealerStateTest}, {@link ContiguityGuardTest},
 * {@link OriginRecordTest}). Depends ONLY on the core module and the JDK —
 * no Aeron, no service-module classes — so the deterministic POJO tests stay
 * runnable even when the cluster transport cannot be built.
 */
final class SealerStateFixtures {

    private SealerStateFixtures() {
    }

    /** Build a distinct 32-byte canonical id whose every byte is {@code b}. */
    static byte[] id(int b) {
        byte[] out = new byte[CanonicalSealerState.CANONICAL_ID_LEN];
        java.util.Arrays.fill(out, (byte) b);
        return out;
    }

    /** Build a distinct 20-byte sender whose every byte is {@code b}. */
    static byte[] sender(int b) {
        byte[] out = new byte[CanonicalSealerState.SENDER_LEN];
        java.util.Arrays.fill(out, (byte) b);
        return out;
    }

    static byte[] payload(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}
