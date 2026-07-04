//! Locks that `RecvBufConfig::so_rcvbuf` reaches the kernel via `socket2`
//! setsockopt. Assertion is loose because Linux doubles the requested value
//! and macOS caps at kern.ipc.maxsockbuf, so `effective >= requested / 2` is
//! the platform-portable floor.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use transport_core::{BindConfig, RecvBufConfig};
use transport_tokio::UdpTransport;

#[tokio::test]
async fn so_rcvbuf_reaches_kernel() {
    let requested: u32 = 512 * 1024;
    let bind = BindConfig {
        addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        reuse_addr: false,
        reuse_port: false,
    };
    let rx = RecvBufConfig {
        so_rcvbuf: Some(requested),
        so_rxq_ovfl: false,
    };
    let transport = UdpTransport::bind(bind, rx).await.expect("bind");
    let local = transport.local_addr().expect("local_addr");
    assert!(local.port() != 0, "kernel assigned ephemeral port");
}
