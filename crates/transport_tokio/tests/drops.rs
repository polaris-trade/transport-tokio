//! Linux-only, `#[ignore]`. Overwhelms the socket rcvbuf by flooding
//! datagrams faster than we drain, then asserts `SO_RXQ_OVFL` reports a
//! non-zero cumulative drop count via `ReceiverStats::kernel_drops`.
//!
//! Run with `cargo nextest run --run-ignored ignored-only -p transport_tokio`
//! on Linux. Skipped in default test runs since kernel behavior under
//! artificial overflow is timing-sensitive.

#![cfg(target_os = "linux")]

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use transport_core::{BatchConfig, BindConfig, RecvBufConfig, SendBufConfig};
use transport_tokio::{RecvBatch, UdpTransport};

#[tokio::test]
#[ignore = "requires overwhelming kernel rcvbuf; run with --run-ignored"]
async fn kernel_drops_counter_monotonic() {
    let mut bind = BindConfig::default();
    bind.addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let mut rx = RecvBufConfig::default();
    rx.so_rcvbuf = Some(4096); // small buffer so overflow triggers fast
    rx.so_rxq_ovfl = true;
    let tx = SendBufConfig::default();
    let mut batch_cfg = BatchConfig::default();
    batch_cfg.recv_size = 8;

    let receiver = UdpTransport::bind(bind, rx, tx, batch_cfg)
        .await
        .expect("bind receiver");
    let recv_addr = receiver.local_addr().expect("local_addr");

    let sender = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("sender bind");

    // Flood many datagrams, then wait so the kernel definitely drops some.
    let payload = [0u8; 512];
    for _ in 0..2000 {
        let _ = sender.send_to(&payload, recv_addr).await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Drain in one batch to surface the ancillary counter.
    let mut batch = RecvBatch::with_capacity(8, 1500);
    let _ = receiver
        .recv_batch_linux(&mut batch)
        .await
        .expect("recv_batch");

    let stats = receiver.stats().snapshot();
    assert!(
        stats.kernel_drops > 0,
        "expected non-zero SO_RXQ_OVFL, got 0; kernel may have absorbed the flood, retry with tighter rcvbuf"
    );
}
