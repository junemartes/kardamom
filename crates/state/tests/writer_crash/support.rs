use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use metrics::{Counter, Gauge, Histogram, HistogramFn, Key, KeyName, Metadata, SharedString, Unit};

const BOUND: Duration = Duration::from_secs(20);

#[derive(Clone, Copy)]
pub(super) enum Cut {
    Accounts,
    Committed,
}

impl Cut {
    pub(super) fn parse(value: &str) -> Self {
        match value {
            "state.commit_accounts" => Self::Accounts,
            "state.commit_mdbx" => Self::Committed,
            _ => panic!("unknown writer crash cut: {value}"),
        }
    }

    fn step(self) -> &'static str {
        match self {
            Self::Accounts => "state.commit_accounts",
            Self::Committed => "state.commit_mdbx",
        }
    }
}

pub(super) struct CrashChild {
    child: Child,
    root: tempfile::TempDir,
}

impl CrashChild {
    pub(super) fn run(cut: Cut) -> Self {
        use std::os::unix::process::ExitStatusExt;

        let root = tempfile::tempdir().unwrap();
        let log = std::fs::File::create(root.path().join("child.log")).unwrap();
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "writer_crash_child",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("KARDAMOM_WRITER_CRASH_DIR", root.path())
            .env("KARDAMOM_WRITER_CRASH_CUT", cut.step())
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap();
        let mut fixture = Self { child, root };
        let deadline = Instant::now().checked_add(BOUND).unwrap();
        while !fixture.at_cut(cut, deadline) {
            std::thread::sleep(Duration::from_millis(5));
        }
        fixture.child.kill().unwrap();
        assert_eq!(fixture.child.wait().unwrap().signal(), Some(9));
        fixture
    }

    fn at_cut(&mut self, cut: Cut, deadline: Instant) -> bool {
        let exited = self.child.try_wait().unwrap();
        assert!(
            exited.is_none() && Instant::now() < deadline,
            "child exited or missed the crash cut: {exited:?}; {}",
            std::fs::read_to_string(self.root.path().join("child.log")).unwrap()
        );
        match std::fs::read_to_string(self.root.path().join("cut")) {
            Ok(value) => value == cut.step(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => panic!("read crash cut: {error}"),
        }
    }

    pub(super) fn root(&self) -> &Path {
        self.root.path()
    }
}

impl Drop for CrashChild {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub(super) struct Gate {
    armed: AtomicBool,
    path: PathBuf,
    cut: Cut,
}

impl Gate {
    pub(super) fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }
}

impl HistogramFn for Gate {
    fn record(&self, _: f64) {
        if !self.armed.swap(false, Ordering::AcqRel) {
            return;
        }
        // The production timing callback runs synchronously on the writer thread.
        // Holding it keeps the real transaction at this cut until the parent kills it.
        std::fs::write(&self.path, self.cut.step()).unwrap();
        std::thread::sleep(BOUND.checked_add(Duration::from_secs(5)).unwrap());
        panic!("the parent did not kill the writer at the crash cut");
    }
}

pub(super) struct Recorder(Arc<Gate>);

impl Recorder {
    pub(super) fn install(path: PathBuf, cut: Cut) -> Arc<Gate> {
        let gate = Arc::new(Gate {
            armed: AtomicBool::new(false),
            path,
            cut,
        });
        metrics::set_global_recorder(Self(gate.clone())).unwrap();
        gate
    }
}

impl metrics::Recorder for Recorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn register_counter(&self, _: &Key, _: &Metadata<'_>) -> Counter {
        Counter::noop()
    }
    fn register_gauge(&self, _: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }
    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> Histogram {
        if key.name() == "kardamom_step_seconds"
            && key
                .labels()
                .any(|label| label.key() == "step" && label.value() == self.0.cut.step())
        {
            return Histogram::from_arc(self.0.clone());
        }
        Histogram::noop()
    }
}
