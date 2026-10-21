use super::TxEnvelope;

#[test]
fn correlation_id_packs_ingress_id_in_high_bits() {
    // The low 48 bits are the sequence, and the top 16 bits are the
    // replica id.
    assert_eq!(TxEnvelope::pack_correlation_id(0, 0), 0);
    assert_eq!(TxEnvelope::pack_correlation_id(0, 41), 41);
    assert_eq!(TxEnvelope::pack_correlation_id(7, 0), 7u64 << 48);
    assert_eq!(TxEnvelope::pack_correlation_id(7, 5), (7u64 << 48) | 5);
    // This round-trips the replica id back out.
    let cases = [0u16, 1, 7, 256, u16::MAX]
        .into_iter()
        .flat_map(|id| [0u64, 1, 1_000_000, (1u64 << 48) - 1].map(move |seq| (id, seq)));
    for (id, seq) in cases {
        let c = TxEnvelope::pack_correlation_id(id, seq);
        assert_eq!(TxEnvelope::ingress_id_of(c), id, "id={id} seq={seq}");
        assert_eq!(c & 0x0000_FFFF_FFFF_FFFF, seq, "seq preserved");
    }
}

#[test]
fn distinct_replicas_never_collide_for_any_sequence() {
    // Two replicas that emit the same local sequence must produce
    // distinct ids.
    for seq in [0u64, 1, 99, 1u64 << 40] {
        assert_ne!(
            TxEnvelope::pack_correlation_id(1, seq),
            TxEnvelope::pack_correlation_id(2, seq)
        );
    }
}
