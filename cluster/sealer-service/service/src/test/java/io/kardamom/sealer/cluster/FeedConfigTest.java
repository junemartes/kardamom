package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

class FeedConfigTest {

    @Test
    void an_unset_or_empty_dir_is_no_feed() {
        assertTrue(FeedConfig.parse(null, null, false).isEmpty());
        assertTrue(FeedConfig.parse(" ", null, true).isEmpty());
    }

    @Test
    void the_feed_takes_the_default_ring_and_the_term_of_the_largest_frame() {
        assertEquals(Optional.of(new FeedConfig("/pair/dir", 16 << 20, 8 << 20)),
            FeedConfig.parse(" /pair/dir ", "", true));
        assertEquals("aeron:ipc?term-length=8388608|sparse=false",
            FeedConfig.parse("/d", null, true).orElseThrow().feedChannel());
    }

    @Test
    void the_feed_without_the_retained_file_stops_the_start() {
        final IllegalArgumentException e =
            assertThrows(IllegalArgumentException.class, () -> FeedConfig.parse("/d", null, false));
        assertTrue(e.getMessage().contains(RetainedFile.Config.SETTING + "=true"), e.getMessage());
    }

    @Test
    void a_ring_below_the_largest_frame_or_not_a_power_of_two_stops_the_start() {
        assertThrows(IllegalArgumentException.class, () -> FeedConfig.parse("/d", "4194304", true));
        assertThrows(IllegalArgumentException.class, () -> FeedConfig.parse("/d", "25165824", true));
        assertThrows(NumberFormatException.class, () -> FeedConfig.parse("/d", "lots", true));
        assertEquals(32 << 20, FeedConfig.parse("/d", "33554432", true).orElseThrow().ringBytes());
    }

    @Test
    void a_request_parses_once_and_ignores_a_longer_tail() {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[SealerWire.FEED_REQUEST_LEN + 3]);
        final FeedRequest request = new FeedRequest(9, 4, 7);
        assertEquals(SealerWire.FEED_REQUEST_LEN, request.write(buffer));
        assertEquals(Optional.of(request), FeedRequest.parse(buffer, 0, SealerWire.FEED_REQUEST_LEN + 3));
        assertTrue(FeedRequest.parse(buffer, 0, SealerWire.FEED_REQUEST_LEN - 1).isEmpty(), "short");
        buffer.putByte(0, SealerWire.KIND_SUBSCRIBE);
        assertTrue(FeedRequest.parse(buffer, 0, SealerWire.FEED_REQUEST_LEN).isEmpty(), "another kind");
        buffer.putByte(0, FeedRequest.IDLE);
        assertTrue(FeedRequest.parse(buffer, 0, Byte.BYTES).isEmpty(), "idle");
    }
}
