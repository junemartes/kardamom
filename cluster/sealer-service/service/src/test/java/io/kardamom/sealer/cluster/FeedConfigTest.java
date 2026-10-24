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
        assertTrue(FeedConfig.parse(null, null, IdleCap.DEFAULT).isEmpty());
        assertTrue(FeedConfig.parse(" ", null, IdleCap.DEFAULT).isEmpty());
    }

    @Test
    void the_feed_takes_the_default_ring_and_the_term_of_the_largest_frame() {
        assertEquals(Optional.of(new FeedConfig("/pair/dir", 16 << 20, 8 << 20, IdleCap.DEFAULT)),
            FeedConfig.parse(" /pair/dir ", "", IdleCap.DEFAULT));
        assertEquals("aeron:ipc?term-length=8388608|sparse=false",
            FeedConfig.parse("/d", null, IdleCap.DEFAULT).orElseThrow().feedChannel());
    }

    @Test
    void the_dir_alone_turns_the_feed_on_without_the_retained_file() {
        assertEquals(Optional.of(new FeedConfig("/d", 16 << 20, 8 << 20, IdleCap.DEFAULT)),
            FeedConfig.parse("/d", null, IdleCap.DEFAULT));
    }

    @Test
    void a_ring_below_the_largest_frame_or_not_a_power_of_two_stops_the_start() {
        assertThrows(IllegalArgumentException.class,
            () -> FeedConfig.parse("/d", "4194304", IdleCap.DEFAULT));
        assertThrows(IllegalArgumentException.class,
            () -> FeedConfig.parse("/d", "25165824", IdleCap.DEFAULT));
        assertThrows(NumberFormatException.class,
            () -> FeedConfig.parse("/d", "lots", IdleCap.DEFAULT));
        assertEquals(32 << 20, FeedConfig.parse("/d", "33554432", IdleCap.DEFAULT).orElseThrow().ringBytes());
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
