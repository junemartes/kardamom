use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime};

#[derive(Clone, Copy)]
pub(super) enum Case {
    Auxiliary,
    State,
    Unsync,
}

impl Case {
    fn child(self) -> &'static str {
        match self {
            Self::Auxiliary => "checkpoint::disk_full_tests::disk_full_child",
            Self::State => "checkpoint::disk_full_tests::state::state_full_child",
            Self::Unsync => "checkpoint::disk_full_tests::state::unsync_full_child",
        }
    }

    fn proof(self) -> &'static str {
        match self {
            Self::Auxiliary => {
                "ENOSPC proof: checkpoint=28 block_log=28 committed_during=3 restored=5"
            }
            Self::State => {
                "State ENOSPC proof: commit=28 no_false_snapshot=1 recovered=1 resumed=2"
            }
            Self::Unsync => "Unsync ENOSPC proof: commit=28 prior_value=1 retry=1",
        }
    }
}

pub(super) struct Container {
    name: String,
    process: Child,
    log: tempfile::NamedTempFile,
}

impl Container {
    pub(super) fn run(case: Case) {
        let mut fixture = Self::start(case);
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(120))
            .unwrap();
        let status = std::iter::from_fn(|| Some(fixture.wait_step(deadline)))
            .flatten()
            .next()
            .unwrap();
        let output = std::fs::read_to_string(fixture.log.path()).unwrap();
        assert!(status.success(), "{output}");
        assert!(output.contains(case.proof()), "{output}");
        println!("{output}");
    }

    fn start(case: Case) -> Self {
        let token = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let name = format!("state-enospc-{}-{token}", std::process::id());
        let binary = std::env::current_exe().unwrap();
        let log = tempfile::NamedTempFile::new().unwrap();
        let mut command = Command::new("docker");
        command.args([
            "run",
            "--rm",
            "--name",
            &name,
            "--network",
            "none",
            "--shm-size",
            "8m",
            "--env",
            "KARDAMOM_CHECKPOINT_COMPONENT=1",
            "--env",
            "RUST_BACKTRACE=1",
            "--entrypoint",
            "/fixture/test",
        ]);
        Self::mount(&mut command, &binary, "/fixture/test");
        Self::libraries(&mut command, &binary);
        let output = log.reopen().unwrap();
        let process = command
            .args([
                "redis:7-alpine",
                "--exact",
                case.child(),
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .stdin(Stdio::null())
            .stdout(output.try_clone().unwrap())
            .stderr(output)
            .spawn()
            .unwrap();
        Self { name, process, log }
    }

    fn mount(command: &mut Command, source: &Path, target: &str) {
        command.args([
            "--mount",
            &format!(
                "type=bind,source={},target={target},readonly",
                std::fs::canonicalize(source).unwrap().display()
            ),
        ]);
    }

    fn libraries(command: &mut Command, binary: &Path) {
        let output = Command::new("ldd").arg(binary).output().unwrap();
        assert!(
            output.status.success(),
            "ldd must list the test binary's shared libraries"
        );
        String::from_utf8(output.stdout)
            .unwrap()
            .lines()
            .filter_map(|line| line.split_whitespace().find(|word| word.starts_with('/')))
            .for_each(|library| Self::mount(command, Path::new(library), library));
    }

    fn wait_step(&mut self, deadline: Instant) -> Option<std::process::ExitStatus> {
        assert!(
            Instant::now() < deadline,
            "container test timed out: {}",
            std::fs::read_to_string(self.log.path()).unwrap()
        );
        let status = self.process.try_wait().unwrap();
        std::thread::sleep(Duration::from_millis(10));
        status
    }
}

impl Drop for Container {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "--force", &self.name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}
