package io.kardamom.sealer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.IntStream;

/**
 * The seed a sealer cluster with no state starts from: the head of a state
 * rebuilt from L1. {@code kardamom-reconstruct --sealer-seed} writes it,
 * and {@code crates/reconstruct/src/seed.rs} holds the same layout.
 *
 * <p>Layout, big-endian: magic {@code "KSED"} (4) | version (4) |
 * chainId (8) | block (8) | endTxIdx (8) | l2Timestamp (8) | l1Origin (8)
 * | stateRoot (32) | senderCount (4) | senderCount * (sender (20) |
 * nextNonce (8)). The senders come eldest first.</p>
 *
 * <p>{@link #parse} is the one boundary: a seed it returns has a positive
 * block, no negative value, and exactly the bytes its sender count names.
 * The digest is the SHA-256 of the file, so it matches
 * {@code sha256sum} of the file.</p>
 *
 * @param head      the rebuilt head the sealer starts after
 * @param stateRoot the state root at the head, 32 bytes
 * @param senders   the senders the nonce guard starts with, eldest first
 * @param digest    the SHA-256 of the seed file, 32 bytes
 */
public record SealerSeed(Head head, byte[] stateRoot, List<Sender> senders, byte[] digest) {

    /** The first four bytes of a seed file: {@code "KSED"}. */
    static final int MAGIC = 0x4B53_4544;
    /** The layout version this sealer reads. */
    static final int VERSION = 1;
    /** Bytes before the first sender. */
    static final int HEADER_LEN = 4 + 4 + 5 * Long.BYTES + 32 + 4;
    /** Bytes of one sender entry. */
    static final int SENDER_ENTRY_LEN = CanonicalSealerState.SENDER_LEN + Long.BYTES;
    /** Length of the state root and of the digest. */
    public static final int HASH_LEN = 32;

    /**
     * The rebuilt head: block {@code H}, the canonical end {@code E_H} of
     * that block, its timestamp and its L1 origin, and the chain id.
     */
    public record Head(long chainId, long block, long endTxIdx, long l2Timestamp, long l1Origin) {
    }

    /** One sender and the nonce its next transaction carries. */
    public record Sender(byte[] address, long nextNonce) {
    }

    /**
     * Read and parse the seed file at {@code path}.
     *
     * @throws IOException              if the file cannot be read
     * @throws IllegalArgumentException if the file is not a valid seed
     */
    public static SealerSeed read(final Path path) throws IOException {
        return parse(Files.readAllBytes(path));
    }

    /**
     * Parse the seed file bytes.
     *
     * @throws IllegalArgumentException if the bytes are not a valid seed
     */
    public static SealerSeed parse(final byte[] file) {
        if (file.length < HEADER_LEN) {
            throw new IllegalArgumentException(
                "a seed has at least " + HEADER_LEN + " bytes, got " + file.length);
        }
        final ByteBuffer buf = ByteBuffer.wrap(file).order(ByteOrder.BIG_ENDIAN);
        final int magic = buf.getInt();
        if (magic != MAGIC) {
            throw new IllegalArgumentException("bad seed magic: 0x" + Integer.toHexString(magic));
        }
        final int version = buf.getInt();
        if (version != VERSION) {
            throw new IllegalArgumentException("unsupported seed version: " + version);
        }
        final Head head = new Head(
            nonNegative("chainId", buf.getLong()),
            nonNegative("block", buf.getLong()),
            nonNegative("endTxIdx", buf.getLong()),
            nonNegative("l2Timestamp", buf.getLong()),
            nonNegative("l1Origin", buf.getLong()));
        if (head.block() == 0 || head.block() == Long.MAX_VALUE) {
            // Block 0 is genesis, which needs no seed; the sealer opens
            // block + 1, so the maximum has no next block.
            throw new IllegalArgumentException("seed block must be in [1, 2^63 - 2], got " + head.block());
        }
        final byte[] stateRoot = new byte[HASH_LEN];
        buf.get(stateRoot);
        final long count = Integer.toUnsignedLong(buf.getInt());
        if (count * SENDER_ENTRY_LEN != buf.remaining()) {
            throw new IllegalArgumentException("seed names " + count + " senders, but holds "
                + buf.remaining() + " bytes after the header");
        }
        final List<Sender> senders = IntStream.range(0, (int) count)
            .mapToObj(i -> sender(buf))
            .toList();
        return new SealerSeed(head, stateRoot, senders, sha256(file));
    }

    /** The digest as lowercase hex, as {@code sha256sum} prints it. */
    public String digestHex() {
        return HexFormat.of().formatHex(digest);
    }

    /** The state root as {@code 0x}-prefixed hex. */
    public String stateRootHex() {
        return "0x" + HexFormat.of().formatHex(stateRoot);
    }

    private static Sender sender(final ByteBuffer buf) {
        final byte[] address = new byte[CanonicalSealerState.SENDER_LEN];
        buf.get(address);
        return new Sender(address, nonNegative("nextNonce", buf.getLong()));
    }

    /** A u64 from the file that Java holds as a long: refuse it at 2^63 and above. */
    private static long nonNegative(final String name, final long value) {
        if (value < 0) {
            throw new IllegalArgumentException(
                "seed " + name + " " + Long.toUnsignedString(value) + " does not fit a signed long");
        }
        return value;
    }

    private static byte[] sha256(final byte[] file) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(file);
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException("every Java runtime has SHA-256", e);
        }
    }
}
