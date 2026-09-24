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

    /**
     * A deadline no block can pass, for tests about everything except the
     * inclusion check. The sealer clamps what it stores to its own horizon,
     * so this does not pin an id in the window.
     */
    static final long NO_DEADLINE = Long.MAX_VALUE;

    /**
     * Rewrite a current snapshot as one an older version wrote: strip the
     * per-id deadline that version 7 added, and re-tag the version. Tests
     * that check an older tail section build their fixture this way,
     * because the id section's width is version-dependent.
     *
     * @param current    the bytes {@code takeSnapshot()} wrote
     * @param newVersion the version to claim, below 7
     * @return the downgraded snapshot
     */
    static byte[] downgradeToVersion(byte[] current, int newVersion) {
        java.nio.ByteBuffer in =
                java.nio.ByteBuffer.wrap(current).order(java.nio.ByteOrder.BIG_ENDIAN);
        int magic = in.getInt();
        in.getInt(); // the current version
        long canonicalCount = in.getLong();
        long blockNumber = in.getLong();
        int idCount = in.getInt();
        java.nio.ByteBuffer out =
                java.nio.ByteBuffer.allocate(current.length - idCount * 8)
                        .order(java.nio.ByteOrder.BIG_ENDIAN);
        out.putInt(magic);
        out.putInt(newVersion);
        out.putLong(canonicalCount);
        out.putLong(blockNumber);
        out.putInt(idCount);
        byte[] id = new byte[32];
        for (int i = 0; i < idCount; i++) {
            in.get(id);
            in.getLong(); // the deadline the older version did not carry
            out.put(id);
        }
        out.put(in);
        return out.array();
    }

    static byte[] payload(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}
