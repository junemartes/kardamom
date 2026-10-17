package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class IdleCapTest {

    @Test
    void an_unset_cap_is_the_1_ms_of_aerons_own_backoff() {
        assertEquals(1_000_000L, IdleCap.parse(null).maxParkNs());
        assertEquals(IdleCap.DEFAULT, IdleCap.parse(" "));
    }

    @Test
    void the_cap_is_whole_microseconds() {
        assertEquals(50_000L, IdleCap.parse("50").maxParkNs());
    }

    @Test
    void a_bad_cap_names_the_property() {
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> IdleCap.parse("0"));
        assertTrue(e.getMessage().contains(IdleCap.SETTING), e.getMessage());
    }

    @Test
    void the_strategy_parks_at_most_the_cap() {
        final String text = IdleCap.parse("50").strategy().toString();
        assertTrue(text.contains("maxParkPeriodNs=50000"), text);
    }
}
