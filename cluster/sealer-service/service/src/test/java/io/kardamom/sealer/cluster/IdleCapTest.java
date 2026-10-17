package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.junit.jupiter.api.Test;

class IdleCapTest {

    @Test
    void an_unset_or_blank_cap_is_50_us() {
        assertEquals(50_000L, IdleCap.parse(null).maxParkNs());
        assertEquals(IdleCap.DEFAULT, IdleCap.parse(" "));
    }

    @Test
    void the_cap_is_whole_microseconds() {
        assertEquals(200_000L, IdleCap.parse(" 200 ").maxParkNs());
        assertEquals(1_000L, IdleCap.parse("1").maxParkNs());
    }

    @Test
    void a_cap_below_1_us_or_not_a_number_stops_the_start() {
        assertThrows(IllegalArgumentException.class, () -> IdleCap.parse("0"));
        assertThrows(IllegalArgumentException.class, () -> IdleCap.parse("-5"));
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> IdleCap.parse("50us"));
        assertTrue(e.getMessage().contains(IdleCap.SETTING), e.getMessage());
    }

    @Test
    void the_strategy_parks_at_most_the_cap() {
        final BackoffIdleStrategy strategy = (BackoffIdleStrategy) IdleCap.parse("50").strategy();
        assertTrue(strategy.toString().contains("maxParkPeriodNs=50000"), strategy.toString());
    }
}
