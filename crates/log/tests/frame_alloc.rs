//! Allocation comparison: `materialize::<TxEnvelope>` (the old tx_data hot
//! path) vs `TxFrame::new` + field reads (the zero-copy path). Run:
//!   cargo test -p kardamom-log --test frame_alloc --release -- --ignored --nocapture

use bytes::Bytes;
use kardamom_log::{TxFrame, codec};
use kardamom_types::TxEnvelope;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[test]
#[ignore = "profiling run"]
fn materialize_vs_frame() {
    let env = TxEnvelope {
        correlation_id: 7,
        raw_tx: Bytes::from(vec![0xAB; 240]), // typical signed transfer size
        sender: kardamom_types::Address::repeat_byte(0x11),
        tx_hash: kardamom_types::B256::repeat_byte(0x22),
    };
    let wire = codec::encode(&env).unwrap();
    const N: u64 = 20_000;

    let profiler = dhat::Profiler::builder().testing().build();

    let s0 = dhat::HeapStats::get();
    for _ in 0..N {
        let v: TxEnvelope = codec::materialize(&wire).unwrap();
        std::hint::black_box((v.correlation_id, v.sender, v.tx_hash, v.raw_tx.len()));
    }
    let s1 = dhat::HeapStats::get();

    for _ in 0..N {
        let f = TxFrame::new(&wire).unwrap();
        std::hint::black_box((f.correlation_id(), f.sender(), f.tx_hash(), f.raw_tx().len()));
    }
    let s2 = dhat::HeapStats::get();

    let m_allocs = (s1.total_blocks - s0.total_blocks) as f64 / N as f64;
    let m_bytes = (s1.total_bytes - s0.total_bytes) as f64 / N as f64;
    let f_allocs = (s2.total_blocks - s1.total_blocks) as f64 / N as f64;
    let f_bytes = (s2.total_bytes - s1.total_bytes) as f64 / N as f64;
    println!("materialize: {m_allocs:.2} allocs/frame, {m_bytes:.0} B/frame");
    println!("frame:       {f_allocs:.2} allocs/frame, {f_bytes:.0} B/frame");
    drop(profiler);
}
