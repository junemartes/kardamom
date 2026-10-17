package io.kardamom.aeron;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * The idle strategy of the Aeron image. The build passes the shared cases
 * of {@code KARDAMOM_IDLE_CAP_US} in {@code kardamom.idleCapCases}, and the
 * image entrypoint in {@code kardamom.aeronEntrypoint}.
 */
final class CappedBackoffIdleStrategyTest {

    /** One case of the shared file: the raw value (null for unset) and the expected outcome. */
    private record Case(String raw, String expected) {
        static Case of(final String line) {
            final int bar = line.lastIndexOf('|');
            final String raw = line.substring(0, bar);
            return new Case("<unset>".equals(raw) ? null : raw, line.substring(bar + 1));
        }
    }

    private static List<Case> cases() throws IOException {
        return Files.readAllLines(Path.of(System.getProperty("kardamom.idleCapCases"))).stream()
            .filter(line -> !line.startsWith("#") && line.contains("|"))
            .map(Case::of)
            .toList();
    }

    @Test
    void capNs_takes_and_refuses_the_shared_cases() throws IOException {
        final List<Case> cases = cases();
        assertTrue(cases.size() > 10, "cases: " + cases);
        cases.forEach(CappedBackoffIdleStrategyTest::check);
    }

    private static void check(final Case c) {
        switch (c.expected()) {
            case "refuse" -> assertThrows(IllegalArgumentException.class,
                () -> CappedBackoffIdleStrategy.capNs("cap", c.raw()), "raw=" + c.raw());
            case "unset" -> assertEquals(1_000_000L, CappedBackoffIdleStrategy.capNs("cap", c.raw()), "raw=" + c.raw());
            default -> assertEquals(Long.parseLong(c.expected()) * 1_000L,
                CappedBackoffIdleStrategy.capNs("cap", c.raw()), "raw=" + c.raw());
        }
    }

    @Test
    void a_refusal_names_the_setting() {
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
            () -> CappedBackoffIdleStrategy.capNs("kardamom.cluster.idleCapUs", "abc"));
        assertTrue(e.getMessage().contains("kardamom.cluster.idleCapUs"), e.getMessage());
    }

    @Test
    void the_strategy_is_aerons_default_backoff_with_the_cap() {
        final String text = new CappedBackoffIdleStrategy(50_000L).toString();
        assertTrue(text.contains("maxSpins=10, maxYields=20, minParkPeriodNs=1000, maxParkPeriodNs=50000"), text);
    }

    @Test
    void the_entrypoint_names_eight_aeron_idle_strategy_properties() throws Exception {
        final String script = Files.readString(Path.of(System.getProperty("kardamom.aeronEntrypoint")));
        final Matcher loop = Pattern.compile("for agent in ([a-z. ]+); do").matcher(script);
        assertTrue(loop.find(), "the entrypoint has no agent loop");
        final List<String> names = Arrays.stream(loop.group(1).trim().split(" +"))
            .map(agent -> "aeron." + agent + ".idle.strategy")
            .toList();
        final Set<String> aeron = Stream.of(io.aeron.driver.Configuration.class, io.aeron.archive.Archive.Configuration.class)
            .flatMap(type -> Arrays.stream(type.getFields()))
            .filter(f -> Modifier.isStatic(f.getModifiers()) && f.getName().endsWith("IDLE_STRATEGY_PROP_NAME"))
            .map(CappedBackoffIdleStrategyTest::value)
            .collect(Collectors.toSet());
        assertEquals(8, names.size(), names.toString());
        assertTrue(aeron.containsAll(names), "names " + names + " not all in " + aeron);
    }

    private static String value(final Field field) {
        try {
            return (String) field.get(null);
        } catch (final IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }
}
