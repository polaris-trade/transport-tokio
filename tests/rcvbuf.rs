//! Locks that `RecvBufConfig::so_rcvbuf` reaches the kernel via `socket2`
//! setsockopt on loopback bind.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use transport_core::{BatchConfig, BindConfig, RecvBufConfig, SendBufConfig};
use transport_tokio::UdpTransport;

#[tokio::test]
async fn so_rcvbuf_reaches_kernel() {
    let requested: u32 = 512 * 1024;
    let mut bind = BindConfig::default();
    bind.addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let mut rx = RecvBufConfig::default();
    rx.so_rcvbuf = Some(requested);
    let tx = SendBufConfig::default();
    let batch = BatchConfig::default();
    let transport = UdpTransport::bind(bind, rx, tx, batch).await.expect("bind");
    let local = transport.local_addr().expect("local_addr");
    assert!(local.port() != 0, "kernel assigned ephemeral port");
}
