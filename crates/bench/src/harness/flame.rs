//! Flamegraph plumbing shared by the harness's two recorders: folded-text
//! merging (tracing-flame + pprof both emit a per-thread stack prefix),
//! ingress-frame filtering for the pprof report, and the common inferno
//! rendering options.

/// Inferno-flamegraph options shared by the tracing-flame and pprof
/// renderers. `min_width = 0.0` keeps narrow frames visible (matches
/// `inferno-flamegraph --minwidth 0`); `image_width = 2000` is enough
/// resolution for the dispatch-window stacks without bloating the SVG.
pub(crate) fn flamegraph_options() -> pprof::flamegraph::Options<'static> {
    let mut opts = pprof::flamegraph::Options::default();
    opts.min_width = 0.0;
    opts.image_width = Some(2_000);
    opts
}

/// Substring matched against demangled symbol names to decide whether a
/// pprof stack belongs to the ingress proxy. Matches
/// `kardamom_ingress::proxy::...`, `kardamom_ingress::sig_verify::...`, etc.
const INGRESS_FRAME_NEEDLE: &str = "kardamom_ingress";

pub(crate) struct FilteredReport {
    pub(crate) report: pprof::Report,
    pub(crate) kept_count: isize,
    pub(crate) dropped_count: isize,
}

/// Walk `report.data` and keep only entries whose `Frames` contains at
/// least one symbol whose demangled name contains `INGRESS_FRAME_NEEDLE`.
/// Samples that land entirely in bench-side / jsonrpsee-client / tokio /
/// hyper code are dropped.
pub(crate) fn filter_to_ingress(report: &pprof::Report) -> FilteredReport {
    let mut kept = std::collections::HashMap::new();
    let mut kept_count: isize = 0;
    let mut dropped_count: isize = 0;
    for (frames, count) in &report.data {
        if frames_contains_ingress(frames) {
            kept.insert(frames.clone(), *count);
            kept_count += *count;
        } else {
            dropped_count += *count;
        }
    }
    FilteredReport {
        report: pprof::Report {
            data: kept,
            timing: report.timing.clone(),
        },
        kept_count,
        dropped_count,
    }
}

fn frames_contains_ingress(frames: &pprof::Frames) -> bool {
    frames.frames.iter().any(|frame| {
        frame
            .iter()
            .any(|sym| sym.name().contains(INGRESS_FRAME_NEEDLE))
    })
}

/// Render a `pprof::Report` to inferno-flamegraph folded text, using the
/// same `thread_name;leaf;...;root count` layout that `Report::flamegraph`
/// builds internally. Output gets piped through `merge_folded_text` to
/// drop the thread prefix.
pub(crate) fn pprof_report_to_folded_text(report: &pprof::Report) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    for (key, value) in &report.data {
        let mut line = key.thread_name_or_id();
        for frame in key.frames.iter().rev() {
            for symbol in frame.iter().rev() {
                write!(&mut line, ";{symbol}").unwrap();
            }
        }
        write!(&mut line, " {value}").unwrap();
        out.push_str(&line);
        out.push('\n');
    }
    out
}

/// Inferno-flamegraph folded format is `"<thread>;<leaf>;...;<root> count"`.
/// This drops the `<thread>;` prefix, buckets by the remaining stack, and
/// sums counts. Used both for the `tracing-flame` `.folded` file and for
/// the on-CPU pprof report — both produce folded text with a per-thread
/// prefix, so the same merge collapses both correctly.
///
/// Lines with no `;` after the thread label (bare-root samples like
/// `ThreadId(N)-tokio-rt-worker 1234`) are dropped, matching the old
/// `grep ';'` recipe from the docs.
pub(crate) fn merge_folded_text(input: &str) -> String {
    use std::collections::BTreeMap;
    let mut merged: BTreeMap<String, u64> = BTreeMap::new();
    for line in input.lines() {
        let Some((stack, count_str)) = line.rsplit_once(' ') else {
            continue;
        };
        let Ok(count) = count_str.parse::<u64>() else {
            continue;
        };
        let Some((_, rest)) = stack.split_once(';') else {
            continue;
        };
        // tracing-flame uses `"; "` as the separator after the thread
        // label, so the rest has a leading space we trim defensively.
        let rest = rest.trim_start();
        if rest.is_empty() {
            continue;
        }
        *merged.entry(rest.to_string()).or_insert(0) += count;
    }
    let mut out = String::with_capacity(input.len());
    for (stack, count) in &merged {
        out.push_str(stack);
        out.push(' ');
        out.push_str(&count.to_string());
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_folded_text_collapses_per_thread_prefix() {
        let input = "\
ThreadId(1)-tokio-worker;decode;execute 100
ThreadId(2)-tokio-worker;decode;execute 250
ThreadId(3)-tokio-worker;decode;execute 50
";
        let out = merge_folded_text(input);
        // All three lines collapse into one with the prefix dropped and
        // counts summed.
        assert_eq!(out.trim(), "decode;execute 400");
    }

    #[test]
    fn merge_folded_text_drops_bare_root_samples() {
        let input = "\
ThreadId(1)-tokio-worker 9999
ThreadId(2)-tokio-worker 5555
";
        let out = merge_folded_text(input);
        // Neither line has a `;` — both are bare-root samples and get
        // dropped (matching the legacy `grep ';'` recipe).
        assert_eq!(out.trim(), "");
    }

    #[test]
    fn merge_folded_text_skips_unparseable_count() {
        let input = "\
thr;a;b not_a_number
thr;a;b 42
";
        let out = merge_folded_text(input);
        assert_eq!(out.trim(), "a;b 42");
    }

    #[test]
    fn merge_folded_text_sums_within_a_thread() {
        let input = "\
ThreadId(1);a;b 10
ThreadId(1);a;b 20
ThreadId(1);a;c 5
";
        let out = merge_folded_text(input);
        // Output is BTreeMap-sorted so deterministic for assertion.
        assert_eq!(out, "a;b 30\na;c 5\n");
    }
}
