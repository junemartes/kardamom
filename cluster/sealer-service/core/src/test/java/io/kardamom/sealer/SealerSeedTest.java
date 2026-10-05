package io.kardamom.sealer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;

/**
 * The seed file is a contract with {@code kardamom-reconstruct}. The golden
 * bytes are the ones the Rust test {@code the_seed_layout_matches_the_golden_bytes}
 * writes, so a change on one side fails the other side's test.
 */
class SealerSeedTest {

    /** The seed file the Rust encoder writes for its golden seed. */
    static final byte[] GOLDEN = HexFormat.of().parseHex(
        "4b534544"
            + "00000001"
            + "0000000000064aba"
            + "0000000000000007"
            + "0000000000000013"
            + "0000018bcfe568fa"
            + "000000000000002a"
            + "1111111111111111111111111111111111111111111111111111111111111111"
            + "00000002"
            + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            + "0000000000000003"
            + "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            + "0000000000000001");

    private static byte[] filled(int len, int b) {
        final byte[] out = new byte[len];
        Arrays.fill(out, (byte) b);
        return out;
    }

    @Test
    void the_golden_seed_parses_to_the_values_rust_wrote() throws Exception {
        final SealerSeed seed = SealerSeed.parse(GOLDEN);
        assertEquals(new SealerSeed.Head(412_346L, 7L, 19L, 1_700_000_000_250L, 42L), seed.head());
        assertArrayEquals(filled(32, 0x11), seed.stateRoot());
        assertEquals(2, seed.senders().size());
        assertArrayEquals(filled(20, 0xAA), seed.senders().get(0).address());
        assertEquals(3L, seed.senders().get(0).nextNonce());
        assertArrayEquals(filled(20, 0xBB), seed.senders().get(1).address());
        assertEquals(1L, seed.senders().get(1).nextNonce());
        assertArrayEquals(MessageDigest.getInstance("SHA-256").digest(GOLDEN), seed.digest());
        assertEquals(64, seed.digestHex().length());
    }

    @Test
    void a_seed_with_no_sender_parses() {
        final byte[] header = Arrays.copyOf(GOLDEN, SealerSeed.HEADER_LEN);
        ByteBuffer.wrap(header).putInt(SealerSeed.HEADER_LEN - 4, 0);
        assertTrue(SealerSeed.parse(header).senders().isEmpty());
    }

    @Test
    void a_damaged_seed_is_refused() {
        assertRefused("magic", copyWithInt(0, 0x4B53_4541));
        assertRefused("version", copyWithInt(4, 2));
        assertRefused("at least", Arrays.copyOf(GOLDEN, SealerSeed.HEADER_LEN - 1));
        assertRefused("2 senders", Arrays.copyOf(GOLDEN, GOLDEN.length - 1));
        assertRefused("2 senders", Arrays.copyOf(GOLDEN, GOLDEN.length + 1));
        assertRefused("3 senders", copyWithInt(SealerSeed.HEADER_LEN - 4, 3));
    }

    @Test
    void a_seed_at_genesis_or_past_a_signed_long_is_refused() {
        assertRefused("seed block", copyWithLong(16, 0L));
        assertRefused("seed block", copyWithLong(16, Long.MAX_VALUE));
        assertRefused("endTxIdx", copyWithLong(24, -1L));
        assertRefused("nextNonce", copyWithLong(SealerSeed.HEADER_LEN + 20, -1L));
    }

    private static byte[] copyWithInt(int offset, int value) {
        final byte[] out = GOLDEN.clone();
        ByteBuffer.wrap(out).putInt(offset, value);
        return out;
    }

    private static byte[] copyWithLong(int offset, long value) {
        final byte[] out = GOLDEN.clone();
        ByteBuffer.wrap(out).putLong(offset, value);
        return out;
    }

    private static void assertRefused(String expected, byte[] file) {
        final IllegalArgumentException e =
            assertThrows(IllegalArgumentException.class, () -> SealerSeed.parse(file));
        assertTrue(e.getMessage().contains(expected), e.getMessage());
    }
}
