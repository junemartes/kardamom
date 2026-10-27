use alloy_primitives::{Address, B256, Bytes};
use alloy_provider::{ProviderBuilder, RootProvider};
use alloy_sol_types::SolValue;
use alloy_transport::mock::Asserter;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

use super::*;

/// Dispersal fails after recording the real packed payload. L1 can still
/// report a transaction sent by the process before it restarts.
struct FailedDispersal {
    url: String,
    payloads: mpsc::UnboundedReceiver<Vec<u8>>,
    task: tokio::task::JoinHandle<()>,
}

impl FailedDispersal {
    async fn serve() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (sent, payloads) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            loop {
                Self::refuse(&listener, &sent).await;
            }
        });
        Self {
            url,
            payloads,
            task,
        }
    }

    async fn refuse(listener: &TcpListener, sent: &mpsc::UnboundedSender<Vec<u8>>) {
        let (socket, _) = listener.accept().await.unwrap();
        let mut reader = BufReader::new(socket);
        let mut size = None;
        while Self::header(&mut reader, &mut size).await {}
        let mut body = vec![0; size.unwrap()];
        reader.read_exact(&mut body).await.unwrap();
        sent.send(body).unwrap();
        reader
            .write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
    }

    async fn header(
        reader: &mut BufReader<tokio::net::TcpStream>,
        size: &mut Option<usize>,
    ) -> bool {
        let mut line = String::new();
        assert_ne!(reader.read_line(&mut line).await.unwrap(), 0);
        if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
            *size = Some(value.trim().parse().unwrap());
        }
        line != "\r\n"
    }

    fn blocks(&mut self) -> Vec<u64> {
        let payload = self.payloads.try_recv().unwrap();
        crate::recon::reconstruct(&payload)
            .unwrap()
            .into_iter()
            .map(|block| block.block_number)
            .collect()
    }
}

impl Drop for FailedDispersal {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Restart {
    l1: Asserter,
    disk: tempfile::TempDir,
    da: FailedDispersal,
}

impl Restart {
    async fn new() -> Self {
        Self {
            l1: Asserter::new(),
            disk: tempfile::tempdir().unwrap(),
            da: FailedDispersal::serve().await,
        }
    }

    fn landed(&self, index: u64, first: u64, last: u64) {
        self.l1.push_success(&Bytes::from(index.abi_encode()));
        self.l1
            .push_success(&Bytes::from((first, last, B256::ZERO).abi_encode()));
    }

    fn feed(&self, restored: std::ops::Range<u64>) -> FeedLoop<RootProvider> {
        let spool = Spool::open(self.disk.path().join("spool")).unwrap();
        restored.for_each(|number| spool.append(&empty_block(number)).unwrap());
        let sender = LiveSender::new(
            ProviderBuilder::default().connect_mocked_client(self.l1.clone()),
            Address::ZERO,
            crate::da::DaProxy::new(&self.da.url).unwrap(),
            2,
            0,
            self.disk.path().join("cursor.json"),
        );
        let config = FeedConfig {
            blocks_per_batch: NonZeroUsize::new(1000).unwrap(),
            skip_through_block: 82,
            da_lag_due: None,
            ..cfg()
        };
        FeedLoop::new(sender, config, spool.clone(), spool.load().unwrap(), 53)
    }

    fn records() -> Receiver<ReaderToExec> {
        let (send, receive) = mpsc::channel(4);
        (83..=85).for_each(|block_number| {
            send.try_send(ReaderToExec::Boundary(BlockBoundaryStart {
                block_number,
                ..Default::default()
            }))
            .unwrap();
        });
        receive
    }

    fn cursor(&self) -> Option<BatchCursor> {
        BatchCursor::load(&self.disk.path().join("cursor.json")).unwrap()
    }

    async fn run(&self, feed: &mut FeedLoop<RootProvider>) -> anyhow::Error {
        tokio::time::timeout(Duration::from_secs(10), feed.run(Self::records()))
            .await
            .expect("the recovery and its failure stay bounded")
    }
}

#[tokio::test]
async fn a_restored_post_is_reconciled_before_live_blocks_can_extend_it() {
    let mut restart = Restart::new().await;
    restart.landed(3, 54, 82);
    restart.landed(4, 83, 85);
    let mut feed = restart.feed(54..83);
    let ended = restart.run(&mut feed).await;
    assert!(
        ended.to_string().contains("reader channel closed"),
        "{ended:#}"
    );
    assert_eq!(restart.da.blocks(), (54..=82).collect::<Vec<_>>());
    assert_eq!(restart.da.blocks(), [83, 84, 85]);
    assert_eq!(restart.cursor().unwrap().last_batch_index, 4);
    assert_eq!(restart.cursor().unwrap().next_block, 86);
    assert_eq!(*feed.posted.borrow(), 85);
    assert!(feed.spool.load().unwrap().blocks.is_empty());
    assert!(restart.l1.read_q().is_empty());
}

#[tokio::test]
async fn a_foreign_post_still_stops_recovery_before_any_live_block_is_consumed() {
    let mut restart = Restart::new().await;
    restart.landed(3, 54, 81);
    let mut feed = restart.feed(54..83);
    let ended = restart.run(&mut feed).await;
    assert!(
        ended.to_string().contains("second batcher is writing"),
        "{ended:#}"
    );
    assert_eq!(restart.da.blocks(), (54..=82).collect::<Vec<_>>());
    assert!(restart.cursor().is_none());
    assert_eq!(feed.spool.load().unwrap().blocks.len(), 29);
    assert_eq!(*feed.posted.borrow(), 53);
    assert!(restart.l1.read_q().is_empty());
}

#[tokio::test]
async fn an_exhausted_restored_post_keeps_its_spool_and_does_not_absorb_live_blocks() {
    let mut restart = Restart::new().await;
    restart.landed(2, 13, 53);
    let mut feed = restart.feed(54..83);
    let ended = restart.run(&mut feed).await;
    assert!(
        ended.is::<crate::live::sender::PostExhausted>(),
        "{ended:#}"
    );
    assert_eq!(restart.da.blocks(), (54..=82).collect::<Vec<_>>());
    assert!(restart.cursor().is_none());
    assert_eq!(feed.spool.load().unwrap().blocks.len(), 29);
    assert_eq!(*feed.posted.borrow(), 53);
    assert!(restart.l1.read_q().is_empty());
}

#[tokio::test]
async fn an_empty_spool_keeps_the_live_group_together() {
    let mut restart = Restart::new().await;
    restart.landed(3, 83, 85);
    let mut feed = restart.feed(0..0);
    let ended = restart.run(&mut feed).await;
    assert!(
        ended.to_string().contains("reader channel closed"),
        "{ended:#}"
    );
    assert_eq!(restart.da.blocks(), [83, 84, 85]);
    assert!(restart.da.payloads.try_recv().is_err());
    assert_eq!(restart.cursor().unwrap().last_batch_index, 3);
    assert_eq!(restart.cursor().unwrap().next_block, 86);
    assert!(feed.spool.load().unwrap().blocks.is_empty());
    assert!(restart.l1.read_q().is_empty());
}
