package io.kardamom.sealer.cluster;

/**
 * The Prometheus text of one scrape. Every series carries the labels of the
 * member: {@code service="sealer"} and {@code member_id}. The admin thread
 * builds one per request.
 */
final class MetricsText {
    private final StringBuilder out = new StringBuilder(16 * 1024);
    private final String memberLabels;

    MetricsText(final int memberId) {
        this.memberLabels = "service=\"sealer\",member_id=\"" + memberId + "\"";
    }

    /** Start the family {@code name}: its HELP and TYPE lines. Its samples follow. */
    MetricsText family(final String name, final String type, final String help) {
        out.append("# HELP ").append(name).append(' ').append(help).append('\n')
            .append("# TYPE ").append(name).append(' ').append(type).append('\n');
        return this;
    }

    /** One sample. {@code labels} is empty, or more labels after the member labels. */
    MetricsText sample(final String name, final String labels, final long value) {
        line(name, labels).append(value).append('\n');
        return this;
    }

    /** One sample with a fractional value. */
    MetricsText sample(final String name, final String labels, final double value) {
        line(name, labels).append(value).append('\n');
        return this;
    }

    /** A family of one sample with the member labels only. */
    MetricsText single(final String name, final String type, final String help, final long value) {
        return family(name, type, help).sample(name, "", value);
    }

    /** A family of one fractional sample with the member labels only. */
    MetricsText single(final String name, final String type, final String help, final double value) {
        return family(name, type, help).sample(name, "", value);
    }

    private StringBuilder line(final String name, final String labels) {
        out.append(name).append('{').append(memberLabels);
        if (!labels.isEmpty()) {
            out.append(',').append(labels);
        }
        return out.append("} ");
    }

    @Override
    public String toString() {
        return out.toString();
    }
}
