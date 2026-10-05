use super::*;
use std::time::Duration;

/// A canned L1: one block, one receipt, two logs, for every call.
async fn fake_upstream() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        loop {
            let (sock, _) = listener.accept().await.unwrap();
            tokio::spawn(answer(sock));
        }
    });
    url
}

async fn answer(sock: TcpStream) {
    let mut conn = Connection::new(sock);
    while let Ok(Some(request)) = conn.read_request().await {
        let calls: Value = serde_json::from_slice(&request.body).unwrap();
        let reply = match &calls {
            Value::Array(calls) => Value::Array(calls.iter().map(canned).collect()),
            call => canned(call),
        };
        conn.write(&Response::json(200, &reply)).await.unwrap();
    }
}

fn canned(call: &Value) -> Value {
    let result = match call["method"].as_str() {
        Some("eth_getBlockByNumber") => serde_json::json!({
            "number": "0x10",
            "hash": format!("0x{}", "11".repeat(32)),
            "parentHash": format!("0x{}", "22".repeat(32)),
        }),
        Some("eth_getTransactionReceipt") => serde_json::json!({ "blockNumber": "0x10" }),
        Some("eth_getLogs") => serde_json::json!([
            { "address": "0x00000000000000000000000000000000000000aa" },
            { "address": "0x00000000000000000000000000000000000000bb" },
        ]),
        _ => serde_json::json!("0x10"),
    };
    serde_json::json!({ "jsonrpc": "2.0", "id": call["id"], "result": result })
}

async fn proxy() -> FaultProxy {
    let upstream = fake_upstream().await;
    FaultProxy::spawn(&upstream, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap()
}

async fn rpc(client: &reqwest::Client, url: &str, body: Value) -> (u16, Value) {
    let response = client.post(url).json(&body).send().await.unwrap();
    let status = response.status().as_u16();
    (status, response.json().await.unwrap())
}

fn call(id: u64, method: &str) -> Value {
    serde_json::json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": [] })
}

#[tokio::test]
async fn the_control_endpoint_sets_the_faults_the_pipe_applies() {
    let proxy = proxy().await;
    let client = reqwest::Client::new();
    let health: Value = client
        .get(format!("{}/health", proxy.url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(health["ok"], true);

    let (status, reply) = rpc(&client, &proxy.url(), call(1, "eth_getBlockByNumber")).await;
    assert_eq!(status, 200);
    assert_eq!(reply["result"]["hash"], format!("0x{}", "11".repeat(32)));

    let (status, active) = rpc(
        &client,
        &format!("{}/fault", proxy.url()),
        serde_json::json!({ "kind": "WrongBlockHash", "from_block": 16 }),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(active["active"][0]["kind"], "WrongBlockHash");
    assert_eq!(
        proxy.faults(),
        Faults::one(Fault::WrongBlockHash { from_block: 16 })
    );

    let (_, reply) = rpc(&client, &proxy.url(), call(2, "eth_getBlockByNumber")).await;
    assert_eq!(reply["result"]["hash"], format!("0x{}", "ee".repeat(32)));
    assert_eq!(reply["id"], 2);

    // A batch: each reply is matched to its call by id.
    let batch = Value::Array(vec![
        call(3, "eth_getLogs"),
        call(4, "eth_getBlockByNumber"),
    ]);
    proxy.set_faults(
        serde_json::from_value(serde_json::json!([
            { "kind": "SwallowLogs", "address": "0x00000000000000000000000000000000000000aa" },
            { "kind": "BrokenParentChain", "from_block": 0 },
        ]))
        .unwrap(),
    );
    let (_, reply) = rpc(&client, &proxy.url(), batch).await;
    assert_eq!(reply[0]["result"].as_array().map(Vec::len), Some(1));
    assert_eq!(
        reply[1]["result"]["parentHash"],
        format!("0x{}", "ab".repeat(32))
    );
    assert_eq!(proxy.served(), 3);

    // The list form clears with an empty list; `None` clears too.
    let (_, active) = rpc(
        &client,
        &format!("{}/fault", proxy.url()),
        serde_json::json!({ "kind": "None" }),
    )
    .await;
    assert_eq!(active["active"], serde_json::json!([]));
    assert!(proxy.faults().is_faithful());
    let bad = client
        .post(format!("{}/fault", proxy.url()))
        .body("{\"kind\":\"Nope\"}")
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status().as_u16(), 400);
}

#[tokio::test]
async fn a_rate_limit_answers_429_and_down_answers_503() {
    let proxy = proxy().await;
    let client = reqwest::Client::new();
    proxy.set_fault(Fault::RateLimit);
    let (status, reply) = rpc(&client, &proxy.url(), call(1, "eth_blockNumber")).await;
    assert_eq!(status, 429);
    assert_eq!(reply["error"]["message"], "rate limited");
    proxy.set_fault(Fault::Down);
    let (status, reply) = rpc(&client, &proxy.url(), call(2, "eth_blockNumber")).await;
    assert_eq!(status, 503);
    assert_eq!(reply["id"], 2);
    proxy.set_fault(Fault::None);
    let (status, _) = rpc(&client, &proxy.url(), call(3, "eth_blockNumber")).await;
    assert_eq!(status, 200);
}

#[tokio::test]
async fn an_unreachable_upstream_is_a_gateway_error_not_a_hang() {
    let dead = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", dead.local_addr().unwrap());
    drop(dead);
    let proxy = FaultProxy::spawn(&url, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let (status, reply) = rpc(&client, &proxy.url(), call(1, "eth_blockNumber")).await;
    assert_eq!(status, 502);
    assert!(
        reply["error"]["message"]
            .as_str()
            .unwrap()
            .starts_with("upstream:")
    );
}
