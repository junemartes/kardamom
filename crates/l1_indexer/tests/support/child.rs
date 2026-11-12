//! Follower instances as child processes of the test binary.
//!
//! Each instance is a real process: the production follower over its own
//! two providers and its own archive, and a production `Beacon` over its
//! process lifecycle. It sends its `l1_blocks` records and its `events`
//! records to the parent over one TCP connection, each frame encoded with
//! the production wire codec: one kind byte, a big-endian length, the
//! bytes. The parent appends the records to the stream's recording and
//! delivers the events to the board, one publisher id per connection, as
//! a restarted process opens a new Aeron publication.
//!
//! A freeze is SIGSTOP and a loss is SIGKILL; the parent checks the
//! process state and the signal that ended it.

use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicI32, Ordering};

use kardamom_batcher::da::DaProxy;
use kardamom_da_watcher::L1Endpoints;
use kardamom_l1_indexer::follow::{Follower, FollowerParts};
use kardamom_l1_indexer::sink::BlockSink;
use kardamom_l1_indexer::store::Store;
use kardamom_l1_indexer::{IndexerError, L1Block};
use kardamom_log::codec;
use kardamom_obs::events::{Beacon, Delivery, Identity, Publisher};
use kardamom_obs::lifecycle::process;
use kardamom_types::service::{HaltRef, ServiceEvent};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use super::rig::Rig;
use super::stream::Stream;

const RECORD: u8 = 0;
const EVENT: u8 = 1;

/// The environment of a child: what it reads and where it sends.
const RPCS: &str = "KARDAMOM_FOLLOWER_CHILD_RPCS";
const DA: &str = "KARDAMOM_FOLLOWER_CHILD_DA";
const DIR: &str = "KARDAMOM_FOLLOWER_CHILD_DIR";
const PARENT: &str = "KARDAMOM_FOLLOWER_CHILD_PARENT";
const INSTANCE: &str = "KARDAMOM_FOLLOWER_CHILD_INSTANCE";

/// The child's sink: each record, encoded, onto the writer task.
struct FrameSink(mpsc::UnboundedSender<(u8, rkyv::util::AlignedVec)>);

impl BlockSink for FrameSink {
    fn publish(&self, block: &L1Block) -> Result<(), IndexerError> {
        let frame = codec::encode(block).map_err(|e| IndexerError::Publish(e.to_string()))?;
        self.0
            .send((RECORD, frame))
            .map_err(|e| IndexerError::Publish(e.to_string()))
    }
}

/// The child's side: run the follower and its beacon until killed.
pub(crate) struct ChildMain;

impl ChildMain {
    pub(crate) async fn run() {
        let var = |name| std::env::var(name).unwrap();
        let stream = TcpStream::connect(var(PARENT)).await.unwrap();
        let (frames, rx) = mpsc::unbounded_channel();
        tokio::spawn(Self::write(stream, rx));
        let identity = Identity {
            service: HaltRef::L1_FOLLOWER.into(),
            instance: var(INSTANCE),
        };
        let events = frames.clone();
        let beacon = Beacon::new(identity, process().subscribe()).run(move |event| {
            let _ = events.send((EVENT, codec::encode(event).unwrap()));
        });
        tokio::spawn(beacon);
        let source = L1Endpoints {
            rpcs: var(RPCS).split(',').map(str::to_string).collect(),
            light_client: None,
        }
        .connect()
        .await
        .unwrap();
        Follower::open(FollowerParts {
            source,
            sink: FrameSink(frames),
            da: DaProxy::new(var(DA)).unwrap(),
            store: Store::open(var(DIR)).unwrap(),
            cfg: Rig::follow_config(),
        })
        .unwrap()
        .run()
        .await;
    }

    async fn write(
        mut stream: TcpStream,
        mut rx: mpsc::UnboundedReceiver<(u8, rkyv::util::AlignedVec)>,
    ) {
        while let Some((kind, bytes)) = rx.recv().await {
            let len = u32::try_from(bytes.len()).unwrap().to_be_bytes();
            let frame = [&[kind][..], &len, &bytes].concat();
            stream.write_all(&frame).await.unwrap();
        }
    }
}

/// The parent's side: accept each child's connection and route its
/// frames.
pub(crate) struct Hub {
    pub(crate) addr: std::net::SocketAddr,
}

/// The publisher id of the next connection.
static NEXT_PUBLISHER: AtomicI32 = AtomicI32::new(1);

impl Hub {
    pub(crate) async fn start(stream: Stream, board: mpsc::Sender<Delivery>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((conn, _)) = listener.accept().await {
                let publisher = Publisher(NEXT_PUBLISHER.fetch_add(1, Ordering::Relaxed));
                tokio::spawn(Self::read(conn, publisher, stream.clone(), board.clone()));
            }
        });
        Self { addr }
    }

    /// Route the frames of one connection until it closes.
    async fn read(
        mut conn: TcpStream,
        publisher: Publisher,
        stream: Stream,
        board: mpsc::Sender<Delivery>,
    ) {
        while let Some((kind, bytes)) = Self::frame(&mut conn).await {
            Self::route(kind, &bytes, publisher, &stream, &board).await;
        }
    }

    async fn frame(conn: &mut TcpStream) -> Option<(u8, Vec<u8>)> {
        let kind = conn.read_u8().await.ok()?;
        let len = conn.read_u32().await.ok()?;
        let mut bytes = vec![0; usize::try_from(len).unwrap()];
        conn.read_exact(&mut bytes).await.ok()?;
        Some((kind, bytes))
    }

    async fn route(
        kind: u8,
        bytes: &[u8],
        publisher: Publisher,
        stream: &Stream,
        board: &mpsc::Sender<Delivery>,
    ) {
        if kind == RECORD {
            let record = codec::materialize::<L1Block>(bytes).unwrap();
            stream.append(codec::encode(&record).unwrap());
        } else {
            let event = codec::materialize::<ServiceEvent>(bytes).unwrap();
            board.send(Delivery { publisher, event }).await.unwrap();
        }
    }
}

/// One follower instance: its process and its archive directory.
pub(crate) struct Instance {
    child: Child,
    pub(crate) dir: PathBuf,
}

/// What one instance reads and where it sends.
pub(crate) struct Launch<'a> {
    pub(crate) name: &'a str,
    pub(crate) rpcs: [String; 2],
    pub(crate) da: String,
    pub(crate) dir: &'a Path,
    pub(crate) hub: &'a Hub,
}

impl Instance {
    /// Start an instance: this test binary's ignored child entry point.
    pub(crate) fn spawn(launch: &Launch<'_>) -> Self {
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "follower_instance_child",
                "--nocapture",
            ])
            .env(RPCS, launch.rpcs.join(","))
            .env(DA, &launch.da)
            .env(DIR, launch.dir)
            .env(PARENT, launch.hub.addr.to_string())
            .env(INSTANCE, launch.name)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        Self {
            child,
            dir: launch.dir.to_path_buf(),
        }
    }

    fn signal(&self, signal: libc::c_int) {
        let pid = libc::pid_t::try_from(self.child.id()).unwrap();
        // SAFETY: the pid is this test's own live child.
        assert_eq!(unsafe { libc::kill(pid, signal) }, 0);
    }

    /// The process state letter from `/proc`: `T` while stopped.
    pub(crate) fn state(&self) -> char {
        let stat = std::fs::read_to_string(format!("/proc/{}/stat", self.child.id())).unwrap();
        stat.rsplit_once(") ")
            .and_then(|(_, rest)| rest.chars().next())
            .unwrap()
    }

    /// SIGSTOP, proven by the process state.
    pub(crate) async fn freeze(&self) {
        self.signal(libc::SIGSTOP);
        super::rig::wait(
            "the instance stops",
            std::time::Duration::from_secs(5),
            || self.state() == 'T',
        )
        .await;
    }

    /// SIGCONT.
    pub(crate) fn thaw(&self) {
        self.signal(libc::SIGCONT);
    }

    /// SIGKILL, proven by the signal that ended the process.
    pub(crate) fn kill(mut self) {
        self.child.kill().unwrap();
        assert_eq!(self.child.wait().unwrap().signal(), Some(libc::SIGKILL));
    }
}

impl Drop for Instance {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
