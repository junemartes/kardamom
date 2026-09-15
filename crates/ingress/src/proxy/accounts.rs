//! The account reads behind `eth_getTransactionCount` and
//! `eth_getBalance`: the local layer, then Redis when `[cache]` is on,
//! then the executor query.
//!
//! The ingress holds no history, so a block tag other than the head is
//! an invalid parameter. The reads take the per-IP rate limit like a
//! submit does: a cold address costs an mdbx snapshot on an executor,
//! and an unmetered read path would be an open door to that cost.

use std::net::IpAddr;

use alloy_primitives::{Address, U256};
use alloy_rpc_types_eth::BlockNumberOrTag;
use kardamom_cache::{AccountView, QueryAnswer, metrics as cache_metrics};

use crate::channels::{IngressPublication, IngressSubscription};
use crate::error::IngressError;

use super::IngressProxy;

/// Which committed value an RPC asks for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccountField {
    Nonce,
    Balance,
}

impl AccountField {
    fn of(self, view: &AccountView) -> U256 {
        match self {
            Self::Nonce => U256::from(view.nonce),
            Self::Balance => view.balance,
        }
    }
}

impl<P, S> IngressProxy<P, S>
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
{
    /// The value of `field` for `address` at `block`: the local layer,
    /// then Redis, then one executor query on a miss.
    ///
    /// # Errors
    ///
    /// Returns `RateLimited` past the client's token bucket, `Decode` for
    /// a block other than the head, and `StateUnavailable` when neither
    /// layer answers.
    pub async fn account_field(
        &self,
        client_ip: IpAddr,
        address: Address,
        block: BlockNumberOrTag,
        field: AccountField,
    ) -> Result<U256, IngressError> {
        if self.rate_limiter.check(client_ip).is_err() {
            return Err(IngressError::RateLimited(client_ip.to_string()));
        }
        self.check_block_tag(block)?;
        if let Some(view) = self.cached_view(address).await {
            return Ok(field.of(&view));
        }
        let Some(query) = &self.query else {
            return Err(IngressError::StateUnavailable(
                "no local entry and no executor query configured".into(),
            ));
        };
        let answer = match field {
            AccountField::Nonce => query.nonce(address).await,
            AccountField::Balance => query.balance(address).await,
        };
        answer
            .map(|QueryAnswer { value, .. }| value)
            .map_err(|e| IngressError::StateUnavailable(e.to_string()))
    }

    /// The two cache layers, each counted: the local layer, then Redis.
    async fn cached_view(&self, address: Address) -> Option<AccountView> {
        if let Some(view) = self.live.get(address) {
            cache_metrics::record_lookup("live", "hit");
            return Some(view);
        }
        cache_metrics::record_lookup("live", "miss");
        self.redis.as_ref()?.account(address).await
    }

    /// The ingress serves only the head. `latest`, `pending`, `safe`,
    /// and `finalized` all name it: there is no L2 reorg, so the head is
    /// final as soon as it exists. A number other than the latest block,
    /// or `earliest`, asks for history the ingress does not hold.
    fn check_block_tag(&self, block: BlockNumberOrTag) -> Result<(), IngressError> {
        let latest = self.latest_block_number();
        match block {
            BlockNumberOrTag::Latest
            | BlockNumberOrTag::Pending
            | BlockNumberOrTag::Safe
            | BlockNumberOrTag::Finalized => Ok(()),
            BlockNumberOrTag::Number(n) if n == latest => Ok(()),
            other => Err(IngressError::Decode(format!(
                "block {other} not served: the ingress answers only the head (block {latest})"
            ))),
        }
    }
}
