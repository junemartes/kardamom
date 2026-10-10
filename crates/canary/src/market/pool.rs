//! `swap` and `liquidity`. A swap alternates direction: 0.00001 ETH in,
//! then the token amount the pool quotes for 0.00001 ETH out. Each result
//! is checked against the formula from the reserves the pool's event
//! reports before the call; the product of the reserves must not fall;
//! the pool's ETH balance must hold its ETH reserve. A reserve snapshot
//! that moved since the canary's last call is activity of another
//! account, counted and not paged.

use alloy_primitives::{TxKind, U256};

use super::{MarketTask, OWNER, SWAPPER};
use crate::contracts::{CALL_GAS, LiquidityEvent, Pool, Reserves, SwapEvent};
use crate::metrics;
use crate::outcome::Outcome;
use crate::ring::Call;
use crate::rpc::Rpc;
use crate::wait::Poll;

/// The ETH a swap moves: 0.00001 ETH.
const SWAP_WEI: u64 = 10_000_000_000_000;
/// The ETH a `liquidity` run adds: 0.0001 ETH.
const LIQUIDITY_WEI: u64 = 100_000_000_000_000;

impl MarketTask {
    pub(super) async fn swap(&mut self, rpc: &Rpc, pool: Pool) -> Result<(), Outcome> {
        let eth_in = self.eth_in_next || self.last.is_none();
        let (value, input) = if eth_in {
            (U256::from(SWAP_WEI), Pool::swap_eth_for_token())
        } else {
            let (reserves, _) = self.last.ok_or(Outcome::SwapMismatch)?;
            let amount = Pool::amount_in_for(U256::from(SWAP_WEI), reserves.token, reserves.eth)
                .ok_or(Outcome::SwapMismatch)?;
            (U256::ZERO, Pool::swap_token_for_eth(amount))
        };
        let call = Call::new(TxKind::Call(pool.0), value, input, CALL_GAS);
        let done = self.send(rpc, SWAPPER, call).await?;
        let event: SwapEvent = pool
            .swap_in(&done.landed.receipt)
            .ok_or(Outcome::SwapMismatch)?;
        self.observe(event.before, event.after, event.seq);
        self.eth_in_next = !eth_in;
        event.check()?;
        self.balance_holds(rpc, pool, event.after).await
    }

    pub(super) async fn liquidity(&mut self, rpc: &Rpc, pool: Pool) -> Result<(), Outcome> {
        if let Some(shares) = self.state.shares {
            return self.remove_liquidity(rpc, pool, shares).await;
        }
        let (reserves, _) = self.last.ok_or(Outcome::LiquidityMismatch)?;
        let eth = U256::from(LIQUIDITY_WEI);
        let token = eth
            .checked_mul(reserves.token)
            .and_then(|t| t.checked_div(reserves.eth))
            .and_then(|t| t.checked_add(U256::from(1)))
            .ok_or(Outcome::LiquidityMismatch)?;
        let shares = self.add_liquidity(rpc, pool, eth, token).await?;
        self.state.shares = Some(shares);
        self.save().await;
        Ok(())
    }

    /// Add `eth` and `token` as the owner, check the shares, and return
    /// them.
    pub(super) async fn add_liquidity(
        &mut self,
        rpc: &Rpc,
        pool: Pool,
        eth: U256,
        token: U256,
    ) -> Result<U256, Outcome> {
        let call = Call::new(
            TxKind::Call(pool.0),
            eth,
            Pool::add_liquidity(token),
            CALL_GAS,
        );
        let done = self.send(rpc, OWNER, call).await?;
        let event = pool
            .liquidity_in(&done.landed.receipt)
            .ok_or(Outcome::LiquidityMismatch)?;
        let after = Self::after_liquidity(&event).ok_or(Outcome::LiquidityMismatch)?;
        self.observe(event.before, after, event.seq);
        if !event.shares_before.is_zero() {
            event.check()?;
        }
        self.balance_holds(rpc, pool, after).await?;
        Ok(event.shares)
    }

    async fn remove_liquidity(
        &mut self,
        rpc: &Rpc,
        pool: Pool,
        shares: U256,
    ) -> Result<(), Outcome> {
        let call = Call::new(
            TxKind::Call(pool.0),
            U256::ZERO,
            Pool::remove_liquidity(shares),
            CALL_GAS,
        );
        let done = self.send(rpc, OWNER, call).await?;
        self.state.shares = None;
        self.save().await;
        let event = pool
            .liquidity_in(&done.landed.receipt)
            .ok_or(Outcome::LiquidityMismatch)?;
        let after = Self::after_liquidity(&event).ok_or(Outcome::LiquidityMismatch)?;
        self.observe(event.before, after, event.seq);
        event.check()?;
        self.balance_holds(rpc, pool, after).await
    }

    /// The reserves after a liquidity change.
    fn after_liquidity(event: &LiquidityEvent) -> Option<Reserves> {
        let (eth, token) = if event.added {
            (
                event.before.eth.checked_add(event.eth)?,
                event.before.token.checked_add(event.token)?,
            )
        } else {
            (
                event.before.eth.checked_sub(event.eth)?,
                event.before.token.checked_sub(event.token)?,
            )
        };
        Some(Reserves { eth, token })
    }

    /// Note the reserves of a canary call: export them, and count the
    /// activity of another account when the snapshot before the call is
    /// not the one after the canary's last call.
    fn observe(&mut self, before: Reserves, after: Reserves, seq: U256) {
        let moved = self.last.is_some_and(|(reserves, last_seq)| {
            reserves != before || last_seq.checked_add(U256::from(1)) != Some(seq)
        });
        if moved {
            ::metrics::counter!(metrics::POOL_EXTERNAL_TOTAL).increment(1);
        }
        self.last = Some((after, seq));
        ::metrics::gauge!(metrics::POOL_RESERVE, "asset" => "eth").set(metrics::wei(after.eth));
        ::metrics::gauge!(metrics::POOL_RESERVE, "asset" => "kca").set(metrics::wei(after.token));
        if let Some(price) = after.price() {
            ::metrics::gauge!(metrics::POOL_PRICE).set(metrics::wei(price) / 1e18);
        }
    }

    /// The pool's ETH balance must hold its ETH reserve once the read
    /// shows the call. A balance above the reserve is ETH another account
    /// forced in: counted, not paged.
    async fn balance_holds(&self, rpc: &Rpc, pool: Pool, after: Reserves) -> Result<(), Outcome> {
        let equal = Poll::within(self.ctx.timing.read_timeout, self.ctx.timing.poll)
            .until(|| async {
                let balance = rpc.balance(pool.0).await.ok()?;
                (balance == after.eth).then_some(())
            })
            .await;
        if equal.is_some() {
            return Ok(());
        }
        let balance = rpc.balance(pool.0).await?;
        if balance < after.eth {
            return Err(Outcome::ReserveMismatch);
        }
        ::metrics::counter!(metrics::POOL_EXTERNAL_TOTAL).increment(1);
        Ok(())
    }
}
