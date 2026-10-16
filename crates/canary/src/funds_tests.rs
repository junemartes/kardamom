use super::*;
use crate::outcome::Outcome;
use crate::test_support::FakeIngress;

#[tokio::test]
async fn no_probe_sends_before_the_first_balance_read() {
    let ingress = FakeIngress::start().await;
    ingress.fund(1_000);
    let ctx = ingress.context(ingress.ring().await);
    let rpc = &ingress.rpc;
    assert_eq!(ctx.lease(rpc).await.err(), Some(Outcome::Unfunded));
    assert_eq!(ingress.chain.lock().unwrap().sends, 0);
    let funds = Funds {
        ctx: Arc::clone(&ctx),
        floor: U256::from(1_000),
        topup: U256::from(1),
    };
    funds.refresh(rpc).await;
    let lease = ctx.lease(rpc).await.unwrap();
    ctx.transact("transfer", rpc, lease, FakeIngress::call())
        .await
        .unwrap();
    assert_eq!(ingress.chain.lock().unwrap().sends, 1);
}
