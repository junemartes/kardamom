//! Controlled JSON-RPC replies for the real archive client and actor.

use jsonrpsee::server::{RpcModule, Server, ServerHandle};
use jsonrpsee::types::ErrorObjectOwned;
use serde_json::{Value, json};

use super::*;

struct Call {
    method: &'static str,
    reply: oneshot::Sender<Value>,
}

impl Call {
    fn refuse(_call: Self) {}

    fn answer(self, result: Value) {
        self.reply.send(result).unwrap();
    }
}

struct Archive {
    url: String,
    calls: mpsc::Receiver<Call>,
    handle: ServerHandle,
}

impl Archive {
    async fn new() -> Self {
        let server = Server::builder().build("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", server.local_addr().unwrap());
        let (calls, receive) = mpsc::channel(4);
        let mut module = RpcModule::new(calls);
        for method in ["indexer_status", "indexer_batch", "indexer_l1_block"] {
            module
                .register_async_method(method, move |_, calls, _| async move {
                    let (reply, receive) = oneshot::channel();
                    calls.send(Call { method, reply }).await.unwrap();
                    receive.await.map_err(|error| {
                        ErrorObjectOwned::owned(-32000, error.to_string(), None::<()>)
                    })
                })
                .unwrap();
        }
        Self {
            url,
            calls: receive,
            handle: server.start(module),
        }
    }

    async fn next(&mut self, expected: &str) -> Call {
        let call = tokio::time::timeout(Duration::from_secs(5), self.calls.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(call.method, expected);
        call
    }

    async fn prefix(&mut self) -> Call {
        self.next("indexer_status")
            .await
            .answer(json!({"last_batch":1}));
        self.next("indexer_batch")
            .await
            .answer(json!({"l1_block":7}));
        self.next("indexer_l1_block").await
    }
}

impl Drop for Archive {
    fn drop(&mut self) {
        let _ = self.handle.stop();
    }
}

#[tokio::test]
async fn the_complete_archive_seed_publishes_without_waiting_for_another_tick() {
    let mut archive = Archive::new().await;
    let mut fixture = Fixture::using(IndexerClient::new(&archive.url));
    let call = archive.prefix().await;
    let record = Fixture::block();
    let timestamp = record.timestamp;
    let earliest = Fixture::age_now(timestamp);
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&record).unwrap();
    call.answer(json!(alloy_primitives::Bytes::copy_from_slice(&bytes)));
    let value = *tokio::time::timeout(
        Duration::from_secs(5),
        fixture.age.wait_for(Option::is_some),
    )
    .await
    .expect("a completed archive read publishes before the ten-second tick")
    .unwrap();
    assert!((earliest..=Fixture::age_now(timestamp)).contains(&value.unwrap()));
    fixture.close().await;
}

#[tokio::test]
async fn all_three_archive_requests_share_one_deadline() {
    let mut archive = Archive::new().await;
    let mut seed = seed::ArchiveSeed::new(Some(IndexerClient::new(&archive.url)));
    seed.start(EVERY);
    let status = archive.next("indexer_status").await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::time::resume();
    status.answer(json!({"last_batch":1}));
    let batch = archive.next("indexer_batch").await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::time::resume();
    batch.answer(json!({"l1_block":7}));
    let _last = archive.next("indexer_l1_block").await;
    tokio::time::pause();
    let error = tokio::time::timeout(Duration::from_secs(7), seed.next())
        .await
        .expect("earlier RPCs consume the final RPC's remaining budget")
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("exceeded its observation interval")
    );
}

#[tokio::test]
async fn a_late_archive_seed_cannot_replace_a_newer_live_post() {
    let mut archive = Archive::new().await;
    let mut fixture = Fixture::using(IndexerClient::new(&archive.url));
    let call = archive.prefix().await;
    let mut record = Fixture::block();
    let timestamp = record.timestamp;
    let earliest = Fixture::age_now(timestamp);
    tokio::time::pause();
    fixture
        .input
        .send((BPosition::from_index(1), record.clone()))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while fixture.input.capacity() != 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the pending archive reply must not block the live record");
    tokio::time::resume();
    record.timestamp = record.timestamp.checked_sub(3_600).unwrap();
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&record).unwrap();
    call.answer(json!(alloy_primitives::Bytes::copy_from_slice(&bytes)));
    let value = *tokio::time::timeout(
        Duration::from_secs(5),
        fixture.age.wait_for(Option::is_some),
    )
    .await
    .unwrap()
    .unwrap();
    assert!((earliest..=Fixture::age_now(timestamp)).contains(&value.unwrap()));
    fixture.close().await;
}

#[tokio::test]
async fn a_failed_archive_read_retries_with_the_same_client_and_recovers() {
    let mut archive = Archive::new().await;
    let mut seed = seed::ArchiveSeed::new(Some(IndexerClient::new(&archive.url)));
    seed.start(EVERY);
    Call::refuse(archive.next("indexer_status").await);
    let error = tokio::time::timeout(Duration::from_secs(5), seed.next())
        .await
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("indexer_status"));
    seed.start(EVERY);
    let call = archive.prefix().await;
    let record = Fixture::block();
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&record).unwrap();
    call.answer(json!(alloy_primitives::Bytes::copy_from_slice(&bytes)));
    let result = tokio::time::timeout(Duration::from_secs(5), seed.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result, Some(record.timestamp));
}
