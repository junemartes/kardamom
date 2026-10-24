//! The rows of `testdata/feature_switch_blocks.txt`, the table of blocks
//! that the executor tests share. The exec-core test
//! `feature_switch_blocks` and the engine test
//! `the_table_switches_where_the_chain_switches` both parse it here.

/// One block of the table.
pub(crate) struct Row {
    pub(crate) number: u64,
    /// The header time of the block, in ms. The next block executes with
    /// it.
    pub(crate) ts: u64,
    /// The activation times of the `setFeature` deposits of the block, in
    /// log order.
    pub(crate) set_features: Vec<u64>,
    /// `true` when the block runs with `FEATURE_SEALER_V3` on.
    pub(crate) v3: bool,
}

impl Row {
    fn parse(line: &str) -> Self {
        let fields: Vec<&str> = line.split_whitespace().collect();
        assert_eq!(fields.len(), 5, "a block line has five fields: {line}");
        let set_features = match fields[3] {
            "-" => Vec::new(),
            times => times
                .split(',')
                .map(|time| time.parse().expect("a set_feature time"))
                .collect(),
        };
        Self {
            number: fields[0].parse().expect("a block number"),
            ts: fields[1].parse().expect("a header time"),
            set_features,
            v3: fields[4] == "1",
        }
    }

    /// Every row of the table, in block order.
    pub(crate) fn all() -> Vec<Self> {
        include_str!("../../testdata/feature_switch_blocks.txt")
            .lines()
            .filter(|line| !line.trim().is_empty() && !line.starts_with('#'))
            .map(Self::parse)
            .collect()
    }
}
