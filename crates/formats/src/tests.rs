use super::*;

/// The text of a registry with one local format, `snap`, at the given
/// versions, and the `tail` lines after its code location.
fn text(versions: [u32; 3], tail: &str) -> String {
    let [writes, reads_min, reads_max] = versions;
    format!(
        "[format.snap]\nwrites = {writes}\nreads_min = {reads_min}\nreads_max = {reads_max}\nshared = false\ncode = [\"a.rs#X\"]\n{tail}"
    )
}

fn registry(versions: [u32; 3], tail: &str) -> Registry {
    Registry::parse(&text(versions, tail)).unwrap()
}

fn shared(versions: [u32; 3], tail: &str) -> Registry {
    Registry::parse(&text(versions, tail).replace("shared = false", "shared = true")).unwrap()
}

fn refusal(text: &str) -> RegistryError {
    Registry::parse(text).unwrap_err()
}

fn findings(base: &Registry, head: &Registry) -> Vec<(Rule, u32, u32)> {
    Comparison::new(base, head)
        .findings()
        .into_iter()
        .map(|f| (f.rule, f.head, f.base))
        .collect()
}

fn report(base: &Registry, head: &Registry) -> Report {
    Comparison::new(base, head).report()
}

const ONE_WAY_11: &str = "[one_way.snap]\nversion = 11\nreason = \"r\"\n";
const COORDINATED_11: &str = "[coordinated.snap]\nversion = 11\nreason = \"r\"\n";

#[test]
fn the_workspace_registry_loads_and_its_code_exists() {
    let registry = Registry::workspace().unwrap();
    assert_eq!(
        registry.missing_code(&Registry::root()),
        Vec::<String>::new()
    );
}

#[test]
fn a_symbol_matches_only_as_a_whole_word() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let location = |symbol: &str| Location {
        path: "src/lib.rs".to_owned(),
        symbol: symbol.to_owned(),
    };
    assert!(location("Registry").found(root));
    assert!(!location("Regist").found(root));
    assert!(!location("Registry").found(&root.join("src")));
}

#[test]
fn a_reader_that_ships_first_breaks_no_rule() {
    let base = shared([10, 1, 10], "");
    let head = shared([10, 1, 11], "");
    assert_eq!(findings(&base, &head), []);
    assert!(report(&base, &head).passed());
}

#[test]
fn a_local_writer_past_the_base_reader_is_one_way() {
    let base = registry([10, 1, 10], "");
    let head = registry([11, 1, 11], "");
    assert_eq!(findings(&base, &head), [(Rule::Rollback, 11, 10)]);
    assert!(!report(&base, &head).passed());
    assert!(report(&base, &registry([11, 1, 11], ONE_WAY_11)).passed());
}

#[test]
fn a_shared_writer_past_the_base_reader_is_also_coordinated() {
    let base = shared([10, 1, 10], "");
    let head = shared([11, 1, 11], "");
    assert_eq!(
        findings(&base, &head),
        [(Rule::Rollback, 11, 10), (Rule::MixedFleet, 11, 10)]
    );
    assert!(!report(&base, &shared([11, 1, 11], ONE_WAY_11)).passed());
    let both = format!("{ONE_WAY_11}{COORDINATED_11}");
    assert!(report(&base, &shared([11, 1, 11], &both)).passed());
}

#[test]
fn a_reader_that_drops_the_base_version_is_coordinated() {
    let base = registry([3, 3, 5], "");
    let head = registry([4, 4, 5], "");
    assert_eq!(findings(&base, &head), [(Rule::Rolling, 4, 3)]);
}

#[test]
fn a_schema_bump_breaks_both_rules() {
    let base = registry([3, 3, 3], "");
    let head = registry([4, 4, 4], "");
    assert_eq!(
        findings(&base, &head),
        [(Rule::Rollback, 4, 3), (Rule::Rolling, 4, 3)]
    );
}

#[test]
fn a_waiver_covers_only_its_own_version() {
    let base = registry([10, 1, 10], "");
    let stale = registry([12, 1, 12], ONE_WAY_11);
    let comparison = Comparison::new(&base, &stale);
    assert_eq!(comparison.waiver(&comparison.findings()[0]), None);
}

#[test]
fn a_waiver_from_the_base_does_not_cover_a_new_finding() {
    let base = registry([11, 1, 11], ONE_WAY_11);
    let head = registry([12, 1, 12], ONE_WAY_11.replace("11", "12").as_str());
    assert!(report(&base, &head).passed());
    let coordinated_4 = "[coordinated.snap]\nversion = 4\nreason = \"r\"\n";
    let base = registry([3, 3, 3], coordinated_4);
    let one_way_4 = "[one_way.snap]\nversion = 4\nreason = \"r\"\n";
    let head = registry([4, 4, 4], &format!("{one_way_4}{coordinated_4}"));
    let report = report(&base, &head);
    assert_eq!(report.waived.len(), 1, "{report:?}");
    assert_eq!(report.problems.len(), 1, "{report:?}");
}

#[test]
fn a_new_waiver_that_covers_no_finding_fails() {
    let base = registry([10, 1, 11], "");
    let head = registry([11, 1, 11], ONE_WAY_11);
    let report = report(&base, &head);
    assert_eq!(report.problems.len(), 1, "{:?}", report.problems);
    let carried = registry([11, 1, 11], ONE_WAY_11);
    assert!(Comparison::new(&carried, &head).report().passed());
}

#[test]
fn a_coordinated_waiver_does_not_cover_a_rollback() {
    let base = registry([10, 1, 10], "");
    let head = registry([11, 1, 11], COORDINATED_11);
    let comparison = Comparison::new(&base, &head);
    assert_eq!(comparison.waiver(&comparison.findings()[0]), None);
}

#[test]
fn a_new_format_breaks_no_rule() {
    let base = Registry::parse(&text([1, 1, 1], "").replace("snap", "other")).unwrap();
    let both = format!(
        "{}{}",
        text([5, 5, 5], ""),
        text([1, 1, 1], "").replace("snap", "other")
    );
    let head = Registry::parse(&both).unwrap();
    assert_eq!(findings(&base, &head), []);
}

#[test]
fn a_dropped_format_needs_a_retired_waiver() {
    let base = Registry::parse(&text([2, 1, 2], "").replace("snap", "old")).unwrap();
    let head = registry([1, 1, 1], "");
    assert_eq!(findings(&base, &head), [(Rule::Retired, 2, 2)]);
    assert!(!report(&base, &head).passed());
    let retired = registry([1, 1, 1], "[retired.old]\nversion = 2\nreason = \"r\"\n");
    assert!(report(&base, &retired).passed());
}

#[test]
fn a_permanent_format_never_raises_reads_min() {
    let permanent = "permanent = true\n";
    let base = registry([5, 4, 5], permanent);
    let head = registry([5, 5, 5], permanent);
    let report = report(&base, &head);
    assert_eq!(report.problems.len(), 1, "{:?}", report.problems);
}

#[test]
fn a_layout_change_needs_a_new_version() {
    let layout =
        |size: u32| format!("[format.snap.layout]\nRecord = \"rkyv size {size} align 8\"\n");
    let base = registry([1, 1, 1], &layout(16));
    assert!(!report(&base, &registry([1, 1, 1], &layout(24))).passed());
    assert!(report(&base, &registry([1, 1, 1], &layout(16))).passed());
}

#[test]
fn an_activation_past_the_base_reader_is_a_note() {
    let base = registry([1, 1, 1], "");
    let head = registry([1, 1, 2], "activation = { flag = \"f\", writes = 2 }\n");
    let report = report(&base, &head);
    assert!(report.passed());
    assert_eq!(report.notes.len(), 1);
}

#[test]
fn parse_refuses_bad_versions() {
    assert!(matches!(
        refusal(&text([11, 1, 10], "")),
        RegistryError::Order { .. }
    ));
    let activation = "activation = { flag = \"f\", writes = 2 }\n";
    assert!(matches!(
        refusal(&text([1, 1, 1], activation)),
        RegistryError::Activation { .. }
    ));
    let unshared = text([1, 1, 1], "").replace("shared = false\n", "");
    assert!(matches!(refusal(&unshared), RegistryError::Toml(_)));
}

#[test]
fn parse_refuses_bad_waivers() {
    let unknown = "[one_way.other]\nversion = 1\nreason = \"r\"\n";
    assert!(matches!(
        refusal(&text([1, 1, 1], unknown)),
        RegistryError::WaiverTarget { .. }
    ));
    let listed = "[retired.snap]\nversion = 1\nreason = \"r\"\n";
    assert!(matches!(
        refusal(&text([1, 1, 1], listed)),
        RegistryError::WaiverTarget { .. }
    ));
    let no_reason = "[coordinated.snap]\nversion = 1\nreason = \" \"\n";
    assert!(matches!(
        refusal(&text([1, 1, 1], no_reason)),
        RegistryError::NoReason { .. }
    ));
}

#[test]
fn parse_refuses_bad_code_locations() {
    let no_symbol = text([1, 1, 1], "").replace("a.rs#X", "a.rs");
    assert!(matches!(refusal(&no_symbol), RegistryError::Toml(_)));
    let no_code = text([1, 1, 1], "").replace("[\"a.rs#X\"]", "[]");
    assert!(matches!(refusal(&no_code), RegistryError::NoCode(_)));
}

#[test]
fn a_writer_below_the_base_reader_is_not_rollback_safe() {
    let base = shared([3, 3, 4], "");
    let head = shared([2, 2, 4], "");
    assert_eq!(
        findings(&base, &head),
        [(Rule::Rollback, 2, 3), (Rule::MixedFleet, 2, 3)]
    );
}

#[test]
fn a_reader_below_the_base_writer_is_not_rolling_safe() {
    let base = registry([4, 1, 4], "");
    let head = registry([3, 1, 3], "");
    assert_eq!(findings(&base, &head), [(Rule::Rolling, 3, 4)]);
}

#[test]
fn changing_the_shared_flag_does_not_hide_a_mixed_fleet() {
    let base = shared([10, 1, 10], "");
    let head = registry([11, 1, 11], ONE_WAY_11);
    assert!(!report(&base, &head).passed());
}

#[test]
fn permanent_formats_cannot_drop_newer_readers_or_the_designation() {
    let base = registry([5, 4, 6], "permanent = true\n");
    assert!(!report(&base, &registry([5, 4, 5], "permanent = true\n")).passed());
    assert!(!report(&base, &registry([5, 4, 6], "")).passed());
    let retired =
        Registry::parse("[format]\n[retired.snap]\nversion = 5\nreason = \"r\"\n").unwrap();
    assert!(!report(&base, &retired).passed());
}

#[test]
fn an_activation_below_the_base_reader_is_a_note() {
    let base = registry([3, 3, 3], "");
    let head = registry([3, 1, 3], "activation = { flag = \"f\", writes = 1 }\n");
    let report = report(&base, &head);
    assert!(report.passed());
    assert_eq!(report.notes.len(), 1);
}

#[test]
fn a_compatible_writer_downgrade_passes() {
    assert!(report(&registry([3, 1, 3], ""), &registry([2, 1, 3], "")).passed());
}

#[test]
fn a_code_location_needs_both_path_and_symbol() {
    for location in ["a.rs#", "#X", "#"] {
        assert!(Registry::parse(&text([1, 1, 1], "").replace("a.rs#X", location)).is_err());
    }
}
