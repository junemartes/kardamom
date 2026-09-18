//! The `[cache]` and `[live_accounts]` config sections.

use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::CacheError;

/// The `[cache]` section: how a reader or the mirror reaches Redis. Off
/// when no address is configured, like the sequencer's `[lookup]`. A
/// reader with the section off makes no Redis call on any path.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, default)]
pub struct CacheConfig {
    /// Sentinel addresses, as `redis://host:port`. Empty means no
    /// Sentinel. The client asks them for the primary, and asks again
    /// after a failure.
    pub sentinels: Vec<String>,
    /// The master name the sentinels monitor.
    pub master_name: String,
    /// A direct `redis://host:port` address, for tests and single-node
    /// development. `sentinels` take precedence when both are set.
    pub url: Option<String>,
    /// The bound of one command, in ms. Never zero: serde rejects a `0`
    /// at parse time.
    pub timeout_ms: NonZeroU64,
    /// The staleness bound, in canonical positions. A reader that has
    /// seen a position this far beyond the mirror head treats the cache
    /// as stale and skips the admission checks.
    pub max_stale_txs: NonZeroU64,
    /// The ACL user. `None` means the default user.
    pub username: Option<String>,
    /// The environment variable that holds the password. Never the
    /// password itself: the config file is checked in.
    pub password_env: Option<String>,
    /// The receipt index TTL, in seconds.
    pub receipt_ttl_secs: NonZeroU64,
    /// The liveness TTL of a mirror head, in seconds. A dead mirror's
    /// head ages out of the readers' max.
    pub head_ttl_secs: NonZeroU64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            sentinels: Vec::new(),
            master_name: "kardamom".to_string(),
            url: None,
            timeout_ms: NonZeroU64::new(200).unwrap(),
            max_stale_txs: NonZeroU64::new(8_192).unwrap(),
            username: None,
            password_env: None,
            receipt_ttl_secs: NonZeroU64::new(600).unwrap(),
            head_ttl_secs: NonZeroU64::new(5).unwrap(),
        }
    }
}

impl CacheConfig {
    /// Whether any Redis address is configured.
    #[must_use]
    pub fn enabled(&self) -> bool {
        !self.sentinels.is_empty() || self.url.is_some()
    }

    /// The bound of one command, [`Self::timeout_ms`] as a `Duration`.
    #[must_use]
    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms.get())
    }

    /// The password, read from the environment named by
    /// [`Self::password_env`]. `None` when no variable is named.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::MissingPassword`] when the variable is named
    /// but not set.
    pub fn password(&self) -> Result<Option<String>, CacheError> {
        self.password_env
            .as_ref()
            .map(|name| std::env::var(name).map_err(|_| CacheError::MissingPassword(name.clone())))
            .transpose()
    }
}

/// The `[live_accounts]` section: the in-process layer. Independent of
/// `[cache]`: the local layer works with Redis off.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, default)]
pub struct LiveAccountsConfig {
    /// The bound on resident accounts. At capacity the oldest entry is
    /// evicted. Never zero. The default, 2^18 accounts, is about 30 MB
    /// and holds every account touched within the TTL at 4,800 tx/s.
    pub capacity: NonZeroUsize,
    /// How long an entry stays after its last write, in ms. This also
    /// bounds the damage of a missed frame: a stale entry expires. Never
    /// zero.
    pub ttl_ms: NonZeroU64,
}

impl Default for LiveAccountsConfig {
    fn default() -> Self {
        Self {
            capacity: NonZeroUsize::new(1 << 18).unwrap(),
            ttl_ms: NonZeroU64::new(30_000).unwrap(),
        }
    }
}

impl LiveAccountsConfig {
    /// [`Self::ttl_ms`] as a `Duration`.
    #[must_use]
    pub fn ttl(&self) -> Duration {
        Duration::from_millis(self.ttl_ms.get())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn off_by_default_and_on_with_any_address() {
        let cfg = CacheConfig::default();
        assert!(!cfg.enabled());
        let with_url = CacheConfig {
            url: Some("redis://127.0.0.1:6379".into()),
            ..CacheConfig::default()
        };
        assert!(with_url.enabled());
        let with_sentinels = CacheConfig {
            sentinels: vec!["redis://s:26379".into()],
            ..CacheConfig::default()
        };
        assert!(with_sentinels.enabled());
    }

    #[test]
    fn toml_rejects_a_zero_timeout_and_an_unknown_field() {
        let zero: Result<CacheConfig, _> = toml_from("timeout_ms = 0");
        assert!(zero.is_err());
        let unknown: Result<CacheConfig, _> = toml_from("nope = 1");
        assert!(unknown.is_err());
        let ok: CacheConfig = toml_from("url = \"redis://x:1\"").unwrap();
        assert_eq!(ok.timeout(), Duration::from_millis(200));
    }

    #[test]
    fn password_comes_from_the_named_variable() {
        let missing = CacheConfig {
            password_env: Some("KARDAMOM_CACHE_TEST_UNSET_VAR".into()),
            ..CacheConfig::default()
        };
        assert!(matches!(
            missing.password(),
            Err(CacheError::MissingPassword(_))
        ));
        assert_eq!(CacheConfig::default().password().unwrap(), None);
    }

    fn toml_from<T: serde::de::DeserializeOwned>(text: &str) -> Result<T, toml::de::Error> {
        toml::from_str(text)
    }
}
