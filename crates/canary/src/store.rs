//! The canary's data directory: the files that outlive a restart. Each
//! file has one writer: the `contract` probe writes the contract
//! addresses, the `transfer` probe writes the anchor, and the market task
//! writes the token and pool state.

use std::path::{Path, PathBuf};

use alloy_primitives::{Address, B256, U256};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// The contracts the canary deployed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Contracts {
    pub counter: Option<Address>,
}

/// The state of the test RWA token and the pool, which the market task
/// owns: the addresses, the setup progress, the canary's ledger of the
/// token supply, and the liquidity shares its probe holds.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Market {
    pub rwa: Option<Address>,
    pub pool: Option<Address>,
    /// The number of setup steps done.
    pub setup: u8,
    /// The canary's mints less its burns.
    pub supply: U256,
    /// A supply change that was sent and not yet seen landed.
    pub pending: Option<SupplyChange>,
    /// The shares the `liquidity` probe added and has not removed.
    pub shares: Option<U256>,
}

/// One change of the token supply.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SupplyChange {
    Mint(U256),
    Burn(U256),
}

impl SupplyChange {
    /// The supply after this change of `supply`.
    #[must_use]
    pub fn apply(self, supply: U256) -> Option<U256> {
        match self {
            Self::Mint(amount) => supply.checked_add(amount),
            Self::Burn(amount) => supply.checked_sub(amount),
        }
    }
}

/// The data directory.
#[derive(Debug, Clone)]
pub struct Store {
    dir: PathBuf,
}

impl Store {
    #[must_use]
    pub fn new(dir: &Path) -> Self {
        Self {
            dir: dir.to_path_buf(),
        }
    }

    /// The directory of the ring's journals.
    #[must_use]
    pub fn ring_dir(&self) -> PathBuf {
        self.dir.join("ring")
    }

    /// The contract addresses; empty when the canary deployed nothing.
    ///
    /// # Errors
    ///
    /// Returns an error when the file exists and does not parse.
    pub async fn contracts(&self) -> anyhow::Result<Contracts> {
        self.load("contracts.json")
            .await
            .map(Option::unwrap_or_default)
    }

    /// Store the contract addresses.
    ///
    /// # Errors
    ///
    /// Returns an error when the write fails.
    pub async fn save_contracts(&self, contracts: &Contracts) -> anyhow::Result<()> {
        self.save("contracts.json", contracts).await
    }

    /// The state of the token and the pool; empty when the canary set up
    /// nothing.
    ///
    /// # Errors
    ///
    /// Returns an error when the file exists and does not parse.
    pub async fn market(&self) -> anyhow::Result<Market> {
        self.load("market.json")
            .await
            .map(Option::unwrap_or_default)
    }

    /// Store the state of the token and the pool.
    ///
    /// # Errors
    ///
    /// Returns an error when the write fails.
    pub async fn save_market(&self, market: &Market) -> anyhow::Result<()> {
        self.save("market.json", market).await
    }

    /// The hash of the first transaction the canary saw land: the old
    /// transaction whose receipt the `read` probe asks for.
    ///
    /// # Errors
    ///
    /// Returns an error when the file exists and does not parse.
    pub async fn anchor(&self) -> anyhow::Result<Option<B256>> {
        self.load("anchor.json").await
    }

    /// Store the anchor.
    ///
    /// # Errors
    ///
    /// Returns an error when the write fails.
    pub async fn save_anchor(&self, hash: B256) -> anyhow::Result<()> {
        self.save("anchor.json", &hash).await
    }

    async fn load<T: DeserializeOwned>(&self, name: &str) -> anyhow::Result<Option<T>> {
        let path = self.dir.join(name);
        match tokio::fs::read(&path).await {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|e| anyhow::anyhow!("{}: {e}", path.display())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(anyhow::anyhow!("{}: {e}", path.display())),
        }
    }

    async fn save<T: Serialize>(&self, name: &str, value: &T) -> anyhow::Result<()> {
        tokio::fs::create_dir_all(&self.dir).await?;
        let path = self.dir.join(name);
        let tmp = path.with_extension("tmp");
        tokio::fs::write(&tmp, serde_json::to_vec(value)?).await?;
        tokio::fs::rename(&tmp, &path).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_files_start_empty_and_keep_what_is_saved() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(dir.path());
        assert_eq!(store.contracts().await.unwrap(), Contracts::default());
        assert_eq!(store.anchor().await.unwrap(), None);
        let contracts = Contracts {
            counter: Some(Address::repeat_byte(5)),
        };
        store.save_contracts(&contracts).await.unwrap();
        store.save_anchor(B256::repeat_byte(6)).await.unwrap();
        assert_eq!(store.contracts().await.unwrap(), contracts);
        assert_eq!(store.anchor().await.unwrap(), Some(B256::repeat_byte(6)));
    }
}
