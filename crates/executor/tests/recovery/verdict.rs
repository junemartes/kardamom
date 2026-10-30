//! The invariants of the executor recovery cases: exact state parity
//! with an undisturbed replica, a dense and bijective state DB, and a
//! receipt stream with no lost transaction and no duplicate block.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use kardamom_state::integrity::{deep_compare_to, sweep};
use kardamom_state::{StateEnvBuilder, read_recovery_point};

use super::chain::TXS_PER_BLOCK;
use super::fleet::{NodeHandle, World};

/// The bound of one catch-up in these small chains.
pub(crate) const CATCH_UP: Duration = Duration::from_secs(20);

/// The receipt stream of one node, split into its processes.
pub(crate) struct Stream {
    /// The receipt hashes by canonical index, over every process.
    receipts: BTreeMap<u64, BTreeSet<String>>,
    /// The boundaries `(block, end)` of each process, in publication
    /// order.
    runs: Vec<Vec<(u64, u64)>>,
}

impl Stream {
    pub(crate) fn read(node: &NodeHandle) -> Self {
        let mut stream = Self {
            receipts: BTreeMap::new(),
            runs: Vec::new(),
        };
        node.receipts().lines().for_each(|line| stream.take(line));
        stream
    }

    fn take(&mut self, line: &str) {
        let fields: Vec<&str> = line.split(' ').collect();
        match fields.as_slice() {
            ["S", _] => self.runs.push(Vec::new()),
            ["R", index, hash] => {
                self.receipts
                    .entry(index.parse().unwrap())
                    .or_default()
                    .insert((*hash).to_string());
            }
            ["B", block, end] => {
                let boundary = (block.parse().unwrap(), end.parse().unwrap());
                self.runs.last_mut().unwrap().push(boundary);
            }
            _ => panic!("malformed receipt line {line:?}"),
        }
    }

    /// Each process publishes its boundaries in block order with no gap
    /// and at the canonical end of each block, the last process ends at
    /// `head`, and no transaction ever gets two different receipts.
    pub(crate) fn assert_exact(&self, name: &str, head: u64) {
        self.runs.iter().for_each(|run| {
            assert!(
                run.windows(2).all(|w| w[1].0 == w[0].0 + 1),
                "{name}: a process skipped a boundary: {run:?}"
            );
            assert!(
                run.iter().all(|(block, end)| *end == block * TXS_PER_BLOCK),
                "{name}: a boundary off its canonical end: {run:?}"
            );
        });
        let last = self.runs.iter().rev().find_map(|run| run.last());
        assert_eq!(last.map(|b| b.0), Some(head), "{name}: head boundary");
        self.receipts.iter().for_each(|(index, hashes)| {
            assert_eq!(
                hashes.len(),
                1,
                "{name}: transaction {index} has receipts {hashes:?}"
            );
        });
    }

    /// A process that kept its disk publishes no boundary again: every
    /// process starts above the boundaries of the processes before it.
    pub(crate) fn assert_no_republished(&self, name: &str) {
        let blocks: Vec<u64> = self.runs.iter().flatten().map(|b| b.0).collect();
        assert!(
            blocks.windows(2).all(|w| w[1] > w[0]),
            "{name}: a boundary went out twice: {blocks:?}"
        );
    }

    /// The boundaries of process `run` (from 1).
    pub(crate) fn run(&self, run: usize) -> Vec<u64> {
        self.runs[run - 1].iter().map(|b| b.0).collect()
    }
}

impl World {
    /// An undisturbed replica that runs from genesis through `head`,
    /// then stops. Its DB is the expected state of every recovered node.
    pub(crate) fn reference(&self, head: u64) -> NodeHandle {
        self.sealer.clear_floor();
        let mut reference = self.node("reference");
        reference.cfg.checkpoints = false;
        reference.start();
        reference.wait_head(head, CATCH_UP);
        reference.stop();
        reference
    }
}

impl NodeHandle {
    /// The stopped node's DB is internally whole and equals the
    /// reference's DB table by table at `head`. The receipt stream is
    /// exact.
    pub(crate) fn assert_matches(&self, reference: &NodeHandle, head: u64) {
        let open = |n: &NodeHandle| StateEnvBuilder::new(&n.cfg.state_dir).open().unwrap();
        let (mine, theirs) = (open(self), open(reference));
        let report = sweep(&mine).unwrap();
        assert!(report.is_clean(), "{}: {:?}", self.name(), report.problems);
        assert_eq!(report.last_committed_block, head, "{}", self.name());
        let diff = deep_compare_to(&theirs, &mine, head).unwrap();
        assert!(diff.is_empty(), "{}: state differs: {diff:?}", self.name());
        Stream::read(self).assert_exact(&self.name(), head);
    }

    /// The node kept its disk: no boundary went out twice.
    pub(crate) fn assert_no_republished(&self) {
        Stream::read(self).assert_no_republished(&self.name());
    }

    /// One JSON-RPC query to the node's committed-state endpoint, over
    /// real HTTP. Returns the `x-state-block` header and the body.
    pub(crate) fn query(&self, method: &str, params: &str) -> (u64, String) {
        use std::io::{Read, Write};
        let body = format!(r#"{{"jsonrpc":"2.0","id":1,"method":"{method}","params":{params}}}"#);
        let mut tcp = std::net::TcpStream::connect(self.cfg.rpc).unwrap();
        write!(
            tcp,
            "POST / HTTP/1.0\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        )
        .unwrap();
        let mut answer = String::new();
        tcp.read_to_string(&mut answer).unwrap();
        let (head, body) = answer.split_once("\r\n\r\n").unwrap();
        let block = head
            .lines()
            .find_map(|l| l.strip_prefix("x-state-block: "))
            .unwrap_or_else(|| panic!("{}: no state block in {answer:?}", self.name()))
            .trim()
            .parse()
            .unwrap();
        (block, body.to_string())
    }

    /// The durable block of the stopped node.
    pub(crate) fn durable_block(&self) -> u64 {
        let env = StateEnvBuilder::new(&self.cfg.state_dir).open().unwrap();
        read_recovery_point(&env).unwrap().last_committed_block
    }

    /// How often `needle` appears in the logs of every process.
    pub(crate) fn count(&self, needle: &str) -> usize {
        self.all_logs().matches(needle).count()
    }
}
