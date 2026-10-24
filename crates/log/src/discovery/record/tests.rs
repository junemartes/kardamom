use std::net::{IpAddr, Ipv4Addr};

use super::*;

const EVERY_TOPIC: [Topic; 12] = [
    Topic::TxData,
    Topic::TxReceipts,
    Topic::TxReceiptBoundaries,
    Topic::TxErrors,
    Topic::TxStatus,
    Topic::TxVerdicts,
    Topic::TxDeposits,
    Topic::TxRemoteEpochs,
    Topic::TxBal,
    Topic::ServiceEvents,
    Topic::ExecTxs,
    Topic::L1Blocks,
];

#[test]
fn every_topic_parses_back_from_its_name() {
    let back: Vec<Option<Topic>> = EVERY_TOPIC
        .iter()
        .map(|t| Topic::parse(t.as_str()))
        .collect();
    assert_eq!(back, EVERY_TOPIC.map(Some));
}

#[test]
fn the_executor_stream_has_its_wire_name_and_the_driver_term_length() {
    assert_eq!(Topic::ExecTxs.as_str(), "exec_txs");
    assert_eq!(Topic::parse("exec_txs"), Some(Topic::ExecTxs));
    assert_eq!(Topic::ExecTxs.term_length(), None);
}

#[test]
fn an_archive_record_of_the_executor_stream_parses() {
    let scope = Scope {
        cluster_id: "test".into(),
        chain_id: 7,
    };
    let mut meta = scope.meta();
    meta.insert("archive_id".into(), "executor-0".into());
    meta.insert("topics".into(), "exec_txs".into());
    let entry = ServiceEntry {
        id: ServiceId::new("archive-executor-0".into()),
        name: ARCHIVE_SERVICE.into(),
        address: IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)),
        port: 8010,
        meta,
    };
    let record = ArchiveRecord::from_entry(&entry).expect("a known topic parses");
    assert!(record.records(Topic::ExecTxs));
    assert!(!record.records(Topic::TxData));
}

/// A follower node's archive records `l1_blocks` beside the topic of its
/// other role; the record lists both, and an empty piece is skipped.
#[test]
fn an_archive_record_lists_the_follower_stream_beside_another_topic() {
    let scope = Scope {
        cluster_id: "test".into(),
        chain_id: 7,
    };
    let mut meta = scope.meta();
    meta.insert("archive_id".into(), "aux-0".into());
    meta.insert("topics".into(), "tx_deposits,l1_blocks".into());
    let entry = ServiceEntry {
        id: ServiceId::new("archive-aux-0".into()),
        name: ARCHIVE_SERVICE.into(),
        address: IpAddr::V4(Ipv4Addr::new(10, 0, 0, 6)),
        port: 8010,
        meta,
    };
    let record = ArchiveRecord::from_entry(&entry).expect("both topics parse");
    assert!(record.records(Topic::TxDeposits));
    assert!(record.records(Topic::L1Blocks));
    let mut only = entry;
    only.meta.insert("topics".into(), ",l1_blocks".into());
    let record = ArchiveRecord::from_entry(&only).expect("an empty piece is skipped");
    assert_eq!(record.topics, [Topic::L1Blocks]);
}

/// An archive entry of one node, with `topics` as its topic list.
fn archive_entry(topics: &str) -> ServiceEntry {
    let scope = Scope {
        cluster_id: "test".into(),
        chain_id: 7,
    };
    let mut meta = scope.meta();
    meta.insert("archive_id".into(), "ingress-0".into());
    meta.insert("topics".into(), topics.into());
    ServiceEntry {
        id: ServiceId::new("archive-ingress-0".into()),
        name: ARCHIVE_SERVICE.into(),
        address: IpAddr::V4(Ipv4Addr::new(10, 0, 0, 7)),
        port: 8010,
        meta,
    }
}

/// A node without the follower meta leaves the Nomad interpolation
/// unfilled. The record keeps its known topic.
#[test]
fn an_unfilled_interpolation_is_skipped_and_the_known_topic_stays() {
    let entry = archive_entry("tx_data,${meta.archive_topics_follower}");
    let record = ArchiveRecord::from_entry(&entry).expect("the known topic parses");
    assert_eq!(record.topics, [Topic::TxData]);
}

#[test]
fn an_empty_trailing_piece_is_skipped() {
    let record = ArchiveRecord::from_entry(&archive_entry("tx_data,")).expect("tx_data parses");
    assert_eq!(record.topics, [Topic::TxData]);
}

#[test]
fn a_record_with_only_unknown_topics_is_dropped() {
    let entry = archive_entry(",${meta.archive_topics_follower}");
    assert!(ArchiveRecord::from_entry(&entry).is_err());
    assert!(ArchiveRecord::from_entry(&archive_entry("nope,also_nope")).is_err());
}

#[test]
fn a_record_with_an_empty_topic_list_records_nothing() {
    let record = ArchiveRecord::from_entry(&archive_entry("")).expect("an empty list parses");
    assert_eq!(record.topics, [] as [Topic; 0]);
}
