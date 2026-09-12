//! The service records of discovery version 1: what a publisher, an
//! archive, and a cluster member advertise, and how each maps onto a
//! Consul service entry's address, port, and string metadata.

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};

use crate::error::LogError;

/// The metadata schema this runtime writes and accepts. A record from
/// another version is ignored.
pub const DISCOVERY_VERSION: &str = "1";

/// Consul service names, frozen for the infrastructure integration.
pub const PUBLISHER_SERVICE: &str = "kardamom-mdc-publisher";
pub const ARCHIVE_SERVICE: &str = "kardamom-aeron-archive";
pub const CLUSTER_MEMBER_SERVICE: &str = "kardamom-cluster-member";

/// One logical publication stream. Receipts and their block boundaries
/// are two topics: two publications share one control endpoint, and each
/// registers its own record.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Topic {
    TxData,
    TxReceipts,
    TxReceiptBoundaries,
    TxErrors,
    TxDeposits,
    TxRemoteEpochs,
    TxBal,
}

impl Topic {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TxData => "tx_data",
            Self::TxReceipts => "tx_receipts",
            Self::TxReceiptBoundaries => "tx_receipt_boundaries",
            Self::TxErrors => "tx_errors",
            Self::TxDeposits => "tx_deposits",
            Self::TxRemoteEpochs => "tx_remote_epochs",
            Self::TxBal => "tx_bal",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "tx_data" => Some(Self::TxData),
            "tx_receipts" => Some(Self::TxReceipts),
            "tx_receipt_boundaries" => Some(Self::TxReceiptBoundaries),
            "tx_errors" => Some(Self::TxErrors),
            "tx_deposits" => Some(Self::TxDeposits),
            "tx_remote_epochs" => Some(Self::TxRemoteEpochs),
            "tx_bal" => Some(Self::TxBal),
            _ => None,
        }
    }
}

impl std::fmt::Display for Topic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The discovery isolation boundary. Every record carries it, and every
/// query filters on it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Scope {
    pub cluster_id: String,
    pub chain_id: u64,
}

impl Scope {
    /// The metadata every record of this scope carries, and every query
    /// of this scope requires.
    #[must_use]
    pub fn meta(&self) -> BTreeMap<String, String> {
        BTreeMap::from([
            (
                "discovery_version".to_string(),
                DISCOVERY_VERSION.to_string(),
            ),
            ("cluster_id".to_string(), self.cluster_id.clone()),
            ("chain_id".to_string(), self.chain_id.to_string()),
        ])
    }
}

/// A Consul service id: unique per allocation, process incarnation,
/// topic, lane, and publication.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ServiceId(String);

impl ServiceId {
    #[must_use]
    pub fn new(id: String) -> Self {
        Self(id)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ServiceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// One catalog entry as both backends present it: the service id, name,
/// advertised address and port, and string metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceEntry {
    pub id: ServiceId,
    pub name: String,
    pub address: IpAddr,
    pub port: u16,
    pub meta: BTreeMap<String, String>,
}

impl ServiceEntry {
    #[must_use]
    pub fn socket_addr(&self) -> SocketAddr {
        SocketAddr::new(self.address, self.port)
    }

    fn meta_str(&self, key: &str) -> Result<&str, LogError> {
        self.meta
            .get(key)
            .map(String::as_str)
            .ok_or_else(|| LogError::Discovery(format!("record {}: missing meta {key}", self.id)))
    }

    fn meta_parsed<T: std::str::FromStr>(&self, key: &str) -> Result<T, LogError>
    where
        T::Err: std::fmt::Display,
    {
        let raw = self.meta_str(key)?;
        raw.parse().map_err(|e| {
            LogError::Discovery(format!("record {}: bad meta {key}={raw}: {e}", self.id))
        })
    }

    fn meta_optional<T: std::str::FromStr>(&self, key: &str) -> Result<Option<T>, LogError>
    where
        T::Err: std::fmt::Display,
    {
        if self.meta.contains_key(key) {
            self.meta_parsed(key).map(Some)
        } else {
            Ok(None)
        }
    }
}

/// A dynamic MDC publication: its control endpoint and the stream it
/// carries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublisherRecord {
    pub id: ServiceId,
    /// The publication's control endpoint, on the publisher's advertised
    /// interface.
    pub control: SocketAddr,
    pub topic: Topic,
    pub stream_id: i32,
    /// The physical transport lane of a `tx_data` publication. `None` on
    /// every other topic.
    pub lane: Option<u8>,
    /// A human label of the publishing process, for logs.
    pub publisher_id: String,
    /// The publication's Aeron session id, when known at registration.
    /// The received image header is the authority for positions; this is
    /// a hint for operators.
    pub session_id: Option<i32>,
}

impl PublisherRecord {
    /// The catalog entry this record registers as, under `scope`.
    #[must_use]
    pub fn entry(&self, scope: &Scope) -> ServiceEntry {
        let mut meta = scope.meta();
        meta.insert("topic".into(), self.topic.as_str().into());
        meta.insert("stream_id".into(), self.stream_id.to_string());
        meta.insert("publisher_id".into(), self.publisher_id.clone());
        if let Some(lane) = self.lane {
            meta.insert("lane_id".into(), lane.to_string());
        }
        if let Some(session) = self.session_id {
            meta.insert("session_id".into(), session.to_string());
        }
        ServiceEntry {
            id: self.id.clone(),
            name: PUBLISHER_SERVICE.into(),
            address: self.control.ip(),
            port: self.control.port(),
            meta,
        }
    }

    /// Parse a catalog entry. The scope and topic filters run in the
    /// query, so this only reads the publication fields.
    ///
    /// # Errors
    ///
    /// Returns an error if a required field is missing or malformed.
    pub fn from_entry(entry: &ServiceEntry) -> Result<Self, LogError> {
        let topic_raw = entry.meta_str("topic")?;
        let topic = Topic::parse(topic_raw).ok_or_else(|| {
            LogError::Discovery(format!("record {}: unknown topic {topic_raw}", entry.id))
        })?;
        Ok(Self {
            id: entry.id.clone(),
            control: entry.socket_addr(),
            topic,
            stream_id: entry.meta_parsed("stream_id")?,
            lane: entry.meta_optional("lane_id")?,
            publisher_id: entry.meta_str("publisher_id")?.to_string(),
            session_id: entry.meta_optional("session_id")?,
        })
    }
}

/// An Aeron Archive's control endpoint and the topics it records.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArchiveRecord {
    pub id: ServiceId,
    pub control: SocketAddr,
    /// Stable identity of the archive, independent of its address.
    pub archive_id: String,
    pub topics: Vec<Topic>,
}

impl ArchiveRecord {
    /// # Errors
    ///
    /// Returns an error if a required field is missing or malformed.
    pub fn from_entry(entry: &ServiceEntry) -> Result<Self, LogError> {
        let topics = entry
            .meta_str("topics")?
            .split(',')
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .map(|t| {
                Topic::parse(t).ok_or_else(|| {
                    LogError::Discovery(format!("record {}: unknown topic {t}", entry.id))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            id: entry.id.clone(),
            control: entry.socket_addr(),
            archive_id: entry.meta_str("archive_id")?.to_string(),
            topics,
        })
    }

    #[must_use]
    pub fn records(&self, topic: Topic) -> bool {
        self.topics.contains(&topic)
    }
}

/// One Aeron Cluster member's fixed identity and current ingress
/// endpoint. The membership set itself is configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterMemberRecord {
    pub id: ServiceId,
    pub member_id: i32,
    pub ingress: SocketAddr,
}

impl ClusterMemberRecord {
    /// # Errors
    ///
    /// Returns an error if the member id is missing or malformed.
    pub fn from_entry(entry: &ServiceEntry) -> Result<Self, LogError> {
        Ok(Self {
            id: entry.id.clone(),
            member_id: entry.meta_parsed("member_id")?,
            ingress: entry.socket_addr(),
        })
    }
}
