/// Canonical confirmation advances a sender while the executor is stopped.
/// Both transfers must be canonical before any execution receipt can exist.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack + anvil; run via `just test-e2e-local` or with --ignored"]
async fn s13h_canonical_prefix_without_execution_receipts() {
    use e2e::harness::{l2, metrics};
    use e2e::scenarios::{assert_receipt_ok, await_l2_receipt, metric_u64};
    let cfg = carry_stack_config(
        std::num::NonZeroUsize::MIN,
        IngressOptions::default().pending_receipt_timeout,
    );
    let stack = launch_l1_or_skip!(cfg);
    let t = target(&stack);
    let signer = l2::dev_signers_through(1).unwrap().remove(1);
    let payee = alloy_primitives::Address::repeat_byte(0xC8);
    let balance = t.rpc.get_balance(signer.address).await.result.unwrap();
    let before =
        alloy_primitives::U256::from_str_radix(balance.trim_start_matches("0x"), 16).unwrap();
    assert_eq!(t.rpc.get_balance(payee).await.result.unwrap(), "0x0");
    let warm = l2::sign_transfer(&signer, t.chain_id, 0, payee, 1).unwrap();
    assert_eq!(t.rpc.send_raw(&warm.raw).await.result.unwrap(), warm.hash);
    assert_receipt_ok(
        &await_l2_receipt(&t, warm.hash, "warm prefix")
            .await
            .unwrap(),
        "warm prefix",
    )
    .unwrap();
    let count = "kardamom_ingress_seal_latency_seconds_count";
    metrics::poll_until(
        "warm canonical observation",
        Duration::from_secs(10),
        Duration::from_millis(20),
        || async { Ok((metric_u64(t.ingress_metric(count).await?)? == 1).then_some(())) },
    )
    .await
    .unwrap();
    let txs: Vec<_> = (1..=2)
        .map(|nonce| l2::sign_transfer(&signer, t.chain_id, nonce, payee, 1).unwrap())
        .collect();
    stack.suspend_executor();
    let rpc = t.rpc.clone();
    let raws: Vec<_> = txs.iter().map(|tx| tx.raw.clone()).collect();
    let sends = tokio::spawn(async move {
        futures::future::join_all(raws.iter().map(|raw| rpc.send_raw(raw))).await
    });
    let canonical = metrics::poll_until(
        "both canonical without executor receipts",
        Duration::from_secs(10),
        Duration::from_millis(20),
        || async { Ok((metric_u64(t.ingress_metric(count).await?)? == 3).then_some(())) },
    )
    .await;
    stack.resume_executor();
    canonical.expect("canonical observation must release the sender suffix");
    let answers = sends.await.unwrap();
    assert_eq!(answers.len(), txs.len());
    for (answer, tx) in answers.into_iter().zip(&txs) {
        assert_eq!(answer.result.unwrap(), tx.hash);
        assert_receipt_ok(
            &await_l2_receipt(&t, tx.hash, "held suffix").await.unwrap(),
            "held suffix",
        )
        .unwrap();
    }
    assert_eq!(
        t.rpc
            .get_transaction_count(signer.address)
            .await
            .result
            .unwrap(),
        "0x3"
    );
    assert_eq!(t.rpc.get_balance(payee).await.result.unwrap(), "0x3");
    let balance = t.rpc.get_balance(signer.address).await.result.unwrap();
    let after =
        alloy_primitives::U256::from_str_radix(balance.trim_start_matches("0x"), 16).unwrap();
    assert_eq!(
        after,
        before - alloy_primitives::U256::from(3 * 21_000_000_000_001u64)
    );
    t.assert_validator_verdict("canonical prefix recovery")
        .await
        .unwrap();
}
