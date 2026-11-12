//! Measurements use unique transaction hashes and count every executor query.

use super::*;

struct RpcProfile {
    fresh: (Vec<u64>, U256),
    stale: (Vec<u64>, U256),
    admission: Vec<u64>,
    admission_tps: f64,
    queried: [usize; 2],
}

impl RpcProfile {
    const SAMPLES: usize = 400;

    async fn run(label: &str, round: usize) {
        let (_container, mut cfg) = kardamom_cache::testing::redis().await;
        cfg.max_stale_txs = NonZeroU64::new(8).unwrap();
        cfg.head_ttl_secs = NonZeroU64::new(60).unwrap();
        let query = QueryServer::start().await;
        let fixture = Fixture::with_query(&cfg, query.config()).await;
        let signer = PrivateKeySigner::random();
        fixture.seed(signer.address(), 3, LEGACY_COST * 10).await;
        Self::sample(&fixture, &query, &signer)
            .await
            .report(label, round);
    }

    async fn sample(fixture: &Fixture, query: &QueryServer, signer: &PrivateKeySigner) -> Self {
        let raw: Vec<_> = (3..403).map(|nonce| sign_legacy(signer, nonce)).collect();
        let fresh = Self::read_samples(fixture, signer.address()).await;
        let queried_fresh = query.calls.load(Ordering::Relaxed);
        fixture.advance_local_head();
        let stale = Self::read_samples(fixture, signer.address()).await;
        let queried_stale = query
            .calls
            .load(Ordering::Relaxed)
            .checked_sub(queried_fresh)
            .unwrap();
        let started = std::time::Instant::now();
        let mut admission = Vec::new();
        for transaction in raw {
            Self::admit_one(fixture, transaction, &mut admission).await;
        }
        let elapsed = started.elapsed().as_secs_f64();
        assert_eq!(
            fixture
                .shard_rx
                .iter()
                .map(tokio::sync::mpsc::UnboundedReceiver::len)
                .sum::<usize>(),
            Self::SAMPLES
        );
        assert_eq!(
            query.calls.load(Ordering::Relaxed),
            queried_fresh.checked_add(queried_stale).unwrap()
        );
        Self {
            fresh,
            stale,
            admission,
            admission_tps: f64::from(u32::try_from(Self::SAMPLES).unwrap()) / elapsed,
            queried: [queried_fresh, queried_stale],
        }
    }

    fn report(self, label: &str, round: usize) {
        let row = serde_json::json!({
            "label": label, "round": round, "samples": Self::SAMPLES,
            "build_profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "github_sha": std::env::var("GITHUB_SHA").ok(),
            "github_run": std::env::var("GITHUB_RUN_ID").ok(),
            "fresh_rpc_nonce": self.fresh.1.to_string(), "fresh_rpc": Self::stats(self.fresh.0),
            "stale_rpc_nonce": self.stale.1.to_string(), "stale_rpc": Self::stats(self.stale.0),
            "fresh_executor_queries": self.queried[0], "stale_executor_queries": self.queried[1],
            "validated_admission": Self::stats(self.admission),
            "validated_admission_tps": self.admission_tps
        });
        Self::record(&row);
        println!("ACCOUNT_PROFILE {row}");
    }

    async fn read_samples(fixture: &Fixture, address: Address) -> (Vec<u64>, U256) {
        let mut samples = Vec::new();
        let mut value = U256::ZERO;
        for _ in 0..Self::SAMPLES {
            value = Self::read_one(fixture, address, &mut samples).await;
        }
        (samples, value)
    }

    async fn admit_one(fixture: &Fixture, raw: alloy_primitives::Bytes, samples: &mut Vec<u64>) {
        let started = std::time::Instant::now();
        fixture
            .proxy
            .submit_validated("10.4.0.2".parse().unwrap(), raw)
            .await
            .unwrap();
        samples.push(u64::try_from(started.elapsed().as_nanos()).unwrap());
    }

    async fn read_one(fixture: &Fixture, address: Address, samples: &mut Vec<u64>) -> U256 {
        let started = std::time::Instant::now();
        let value = fixture
            .proxy
            .account_field(IP, address, BlockNumberOrTag::Latest, AccountField::Nonce)
            .await
            .unwrap();
        samples.push(u64::try_from(started.elapsed().as_nanos()).unwrap());
        value
    }

    fn record(row: &serde_json::Value) {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(std::env::temp_dir().join("kardamom-ingress-account-profile.jsonl"))
            .unwrap();
        writeln!(file, "{row}").unwrap();
    }

    fn stats(mut samples: Vec<u64>) -> serde_json::Value {
        samples.sort_unstable();
        serde_json::json!({"p50_ns":samples[Self::SAMPLES/2],"p95_ns":samples[Self::SAMPLES*95/100],"p99_ns":samples[Self::SAMPLES*99/100],"raw_ns":samples})
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; records account RPC and admission samples"]
async fn profile_account_cache_paths() {
    let label = std::env::var("KARDAMOM_PROFILE_LABEL").unwrap_or_else(|_| "ci".into());
    for round in 0..5 {
        RpcProfile::run(&label, round).await;
    }
}
