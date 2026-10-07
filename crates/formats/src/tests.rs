use super::*;

/// The text of a registry with one format, `snap`, at the given
/// versions, and the `tail` lines after its code location.
fn text(versions: [u32; 3], tail: &str) -> String {
    let [writes, reads_min, reads_max] = versions;
    format!(
        "[format.snap]\nwrites = {writes}\nreads_min = {reads_min}\nreads_max = {reads_max}\ncode = [\"a.rs#X\"]\n{tail}"
    )
}

fn registry(versions: [u32; 3], tail: &str) -> Registry {
    Registry::parse(&text(versions, tail)).unwrap()
}

fn refusal(versions: [u32; 3], tail: &str) -> RegistryError {
    Registry::parse(&text(versions, tail)).unwrap_err()
}

fn findings(base: &Registry, head: &Registry) -> Vec<(Rule, u32, u32)> {
    Comparison::new(base, head)
        .findings()
        .into_iter()
        .map(|f| (f.rule, f.head, f.base))
        .collect()
}

#[test]
fn the_workspace_registry_loads_and_its_code_exists() {
    let registry = Registry::workspace().unwrap();
    assert_eq!(
        registry.missing_code(&Registry::root()),
        Vec::<String>::new()
    );
}

#[test]
fn a_reader_that_ships_first_breaks_no_rule() {
    let base = registry([10, 1, 10], "");
    let head = registry([10, 1, 11], "");
    assert_eq!(findings(&base, &head), []);
}

#[test]
fn a_writer_past_the_base_reader_is_one_way() {
    let base = registry([10, 1, 10], "");
    let head = registry([11, 1, 11], "");
    assert_eq!(findings(&base, &head), [(Rule::Rollback, 11, 10)]);
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
    let waived = registry(
        [11, 1, 11],
        "[one_way.snap]\nversion = 11\nreason = \"r\"\n",
    );
    let stale = registry(
        [12, 1, 12],
        "[one_way.snap]\nversion = 11\nreason = \"r\"\n",
    );
    let comparison = Comparison::new(&base, &waived);
    let finding = &comparison.findings()[0];
    assert_eq!(comparison.waiver(finding), Some("r"));
    let comparison = Comparison::new(&base, &stale);
    assert_eq!(comparison.waiver(&comparison.findings()[0]), None);
}

#[test]
fn a_coordinated_waiver_does_not_cover_a_rollback() {
    let base = registry([10, 1, 10], "");
    let head = registry(
        [11, 1, 11],
        "[coordinated.snap]\nversion = 11\nreason = \"r\"\n",
    );
    let comparison = Comparison::new(&base, &head);
    assert_eq!(comparison.waiver(&comparison.findings()[0]), None);
}

#[test]
fn a_new_format_breaks_no_rule() {
    let base = Registry::parse(&text([1, 1, 1], "").replace("snap", "old")).unwrap();
    let head = registry([5, 5, 5], "");
    assert_eq!(findings(&base, &head), []);
}

#[test]
fn parse_refuses_a_writer_outside_its_own_read_range() {
    assert!(matches!(
        refusal([11, 1, 10], ""),
        RegistryError::Order { .. }
    ));
}

#[test]
fn parse_refuses_an_activation_outside_the_read_range() {
    let activation = "activation = { flag = \"f\", writes = 2 }\n";
    assert!(matches!(
        refusal([1, 1, 1], activation),
        RegistryError::Activation { .. }
    ));
}

#[test]
fn parse_refuses_bad_waivers() {
    let unknown = "[one_way.other]\nversion = 1\nreason = \"r\"\n";
    assert!(matches!(
        refusal([1, 1, 1], unknown),
        RegistryError::UnknownWaiver { .. }
    ));
    let no_reason = "[coordinated.snap]\nversion = 1\nreason = \" \"\n";
    assert!(matches!(
        refusal([1, 1, 1], no_reason),
        RegistryError::NoReason { .. }
    ));
}

#[test]
fn parse_refuses_bad_code_locations() {
    let no_symbol = text([1, 1, 1], "").replace("a.rs#X", "a.rs");
    assert!(matches!(
        Registry::parse(&no_symbol),
        Err(RegistryError::Toml(_))
    ));
    let no_code = text([1, 1, 1], "").replace("[\"a.rs#X\"]", "[]");
    assert!(matches!(
        Registry::parse(&no_code),
        Err(RegistryError::NoCode(_))
    ));
}
