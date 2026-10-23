package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.fail;

import java.util.Arrays;

/** Reads the samples of a Prometheus text in the tests. */
final class PromText {
    /** The member labels of member 1, before the labels of a series. */
    static final String MEMBER = "service=\"sealer\",member_id=\"1\"";

    private PromText() {
    }

    /** The value of the series {@code name{MEMBER,labels}}; the test fails when the text has no such series. */
    static double value(final String text, final String name, final String labels) {
        final String series = name + "{" + MEMBER + (labels.isEmpty() ? "" : "," + labels) + "} ";
        return Arrays.stream(text.split("\n"))
            .filter(line -> line.startsWith(series))
            .mapToDouble(line -> Double.parseDouble(line.substring(series.length())))
            .findFirst()
            .orElseGet(() -> fail("no series " + series + " in:\n" + text));
    }

    /** How many lines of {@code text} start with {@code prefix}. */
    static long lines(final String text, final String prefix) {
        return Arrays.stream(text.split("\n")).filter(line -> line.startsWith(prefix)).count();
    }
}
