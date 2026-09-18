//! The funded-account cursor. Genesis funds Anvil accounts #0 through
//! #17. The gate owns #0, the load stage #1 through #6, the chaos cases
//! #7 through #15, the ingress-churn re-smoke #16, and the fallback
//! churn re-smoke #17. Each case gets one dedicated account with a nonce
//! chain from 0 on the never-reset chain, so cases never collide and
//! never leave nonce gaps.

/// The shard of each funded account at shard map version 0: the first
/// 8 bytes of `keccak256(address)` as a big-endian u64, modulo the
/// partition count of 2. Fixed addresses and a fixed hash keep this
/// table stable. `check-contract.py` recomputes it.
pub const ACCT_SHARD: [u8; 16] = [0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 1, 0, 1];

/// The vslot of each funded account: `keccak256(address)[7]`. The shard
/// map turns a vslot into a lane; `ACCT_SHARD` is that map at version 0.
pub const ACCT_VSLOT: [u8; 16] = [
    40, 203, 173, 4, 226, 250, 74, 160, 16, 4, 123, 108, 103, 63, 240, 103,
];

/// The last funded chaos account.
const LAST: u32 = 15;
/// The load-reserve accounts the resize case may take on a chaos-only
/// shard.
const LOAD_RESERVE: [u32; 6] = [1, 2, 3, 4, 5, 6];

/// What a case needs from its account.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pin {
    /// Any account.
    Any,
    /// An account on shard 0, the shard whose replica the case kills or
    /// pauses. An arbitrary account lands on shard 0 or 1 by address
    /// hash, so half the runs would otherwise drive an untouched shard.
    Shard0,
    /// An account whose vslot moves to the new lane under the next map,
    /// so the resize case proves the move.
    MovesOnScaleOut,
}

/// The account cursor of one suite run.
#[derive(Debug, Clone)]
pub struct Accounts {
    next: u32,
    base: u32,
    /// Whether the load stage ran on this cluster, which decides whether
    /// the load reserve is untouched.
    run_load: bool,
}

impl Accounts {
    #[must_use]
    pub fn new(base: u32, run_load: bool) -> Self {
        Self {
            next: base,
            base,
            run_load,
        }
    }

    /// Take the next account that satisfies `pin`. Skipped accounts are
    /// burned, never reused: their nonce chains stay at 0. `moves` says
    /// whether an account's vslot moves under the next map.
    ///
    /// # Errors
    ///
    /// Returns an error when the funded accounts run out.
    pub fn take(
        &mut self,
        pin: Pin,
        case: &str,
        moves: impl Fn(u32) -> bool,
    ) -> anyhow::Result<u32> {
        self.skip_unpinned(pin, case, &moves);
        if self.next > LAST && pin == Pin::MovesOnScaleOut && !self.run_load {
            return self.take_reserve(case, &moves);
        }
        let account = self.next;
        self.next = account.saturating_add(1);
        anyhow::ensure!(
            account <= LAST,
            "{}: ran out of funded chaos accounts (#{account} > {LAST}); reduce the case list",
            crate::FAIL_PREFIX
        );
        Ok(account)
    }

    fn skip_unpinned(&mut self, pin: Pin, case: &str, moves: &impl Fn(u32) -> bool) {
        let fits = |a: u32| match pin {
            Pin::Any => true,
            Pin::Shard0 => ACCT_SHARD[a as usize] == 0,
            Pin::MovesOnScaleOut => moves(a),
        };
        while self.next <= LAST && !fits(self.next) {
            self.burn(pin, case);
        }
    }

    fn burn(&mut self, pin: Pin, case: &str) {
        let a = self.next;
        let why = match pin {
            Pin::Shard0 => format!("shard {}; case needs shard 0", ACCT_SHARD[a as usize]),
            _ => format!("vslot {} does not move", ACCT_VSLOT[a as usize]),
        };
        crate::log(format!("{case}: skipping funded account #{a} ({why})"));
        self.next = a.saturating_add(1);
    }

    /// The resize case runs last on a chaos-only shard, where the load
    /// reserve still sits at nonce 0. Take the first moved sender from
    /// it, and park the cursor past the end so a later case fails
    /// loudly.
    fn take_reserve(&mut self, case: &str, moves: &impl Fn(u32) -> bool) -> anyhow::Result<u32> {
        let spare = LOAD_RESERVE
            .into_iter()
            .find(|a| moves(*a))
            .ok_or_else(|| {
                crate::chaos_fail!(
                    "{case}: no moved sender in #{} .. #{LAST} nor in the load reserve",
                    self.base
                )
            })?;
        crate::log(format!(
            "{case}: no moved sender left in #{}..#{LAST}; taking load-reserve account #{spare} (vslot {}; the load harness never used it)",
            self.base, ACCT_VSLOT[spare as usize]
        ));
        self.next = LAST.saturating_add(1);
        Ok(spare)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pins_to_shard_zero_and_burns_the_skipped_accounts() {
        let mut accounts = Accounts::new(7, false);
        assert_eq!(accounts.take(Pin::Any, "a", |_| false).unwrap(), 7);
        assert_eq!(accounts.take(Pin::Any, "b", |_| false).unwrap(), 8);
        // #9 is shard 0, #10 is shard 1, #11 is shard 0.
        assert_eq!(accounts.take(Pin::Shard0, "c", |_| false).unwrap(), 9);
        assert_eq!(accounts.take(Pin::Shard0, "d", |_| false).unwrap(), 11);
    }

    #[test]
    fn the_resize_case_falls_back_to_the_load_reserve_on_a_chaos_only_shard() {
        let mut accounts = Accounts::new(15, false);
        // #15 does not move; the reserve's #3 does.
        let account = accounts
            .take(Pin::MovesOnScaleOut, "resize", |a| a == 3)
            .unwrap();
        assert_eq!(account, 3);
        assert!(accounts.take(Pin::Any, "later", |_| false).is_err());
        let mut with_load = Accounts::new(15, true);
        assert!(
            with_load
                .take(Pin::MovesOnScaleOut, "resize", |a| a == 3)
                .is_err()
        );
    }
}
