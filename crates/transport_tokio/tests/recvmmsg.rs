//! Linux-only. Sends a burst of 10 datagrams from an ephemeral sender and
//! asserts `recv_batch_linux` drains at least two in a single call, proving
//! the syscall is batching packets end-to-end.

#![cfg(target_os = "linux")]

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use transport_core::{BatchConfig, BindConfig, RecvBufConfig, SendBufConfig};
use transport_tokio::{RecvBatch, UdpTransport};

#[tokio::test]
async fn recvmmsg_batches_multiple_datagrams() {
    let mut bind = BindConfig::default();
    bind.addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let rx = RecvBufConfig::default();
    let tx = SendBufConfig::default();
    let mut batch_cfg = BatchConfig::default();
    batch_cfg.recv_size = 32;

    let receiver = UdpTransport::bind(bind, rx, tx, batch_cfg)
        .await
        .expect("bind receiver");
    let recv_addr = receiver.local_addr().expect("local_addr");

    let sender = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("sender bind");
    for i in 0..10u8 {
        sender.send_to(&[i], recv_addr).await.expect("send");
    }
    tokio::time::sleep(Duration::from_millis(30)).await;

    let mut batch = RecvBatch::with_capacity(32, 1500);
    let n = receiver
        .recv_batch_linux(&mut batch)
        .await
        .expect("recv_batch");
    assert!(n >= 2, "expected at least 2 batched datagrams, got {n}");
    assert_eq!(batch.count, n);

    let stats = receiver.stats().snapshot();
    assert_eq!(stats.packets_recv, n as u64);
    assert_eq!(stats.bytes_recv, n as u64, "each datagram is 1 byte");
}
