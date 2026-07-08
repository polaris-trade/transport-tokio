//! Runs the shared `transport_core::testing` suites against `TokioTransport`:
//! the construction suite (`run_conformance_suite`, which auto-spins a
//! `127.0.0.1:0` peer for `connect_tcp`) and the datagram recv suite
//! (`run_datagram_source`), which drives `recv_burst` through the batch,
//! drain, pool-reclaim, and backpressure contract.

use std::{net::UdpSocket as StdUdpSocket, time::Duration};

use transport_core::{
    AffinityConfig, BatchConfig, BindConfig, RecvBufConfig, RingConfig, SendBufConfig,
    testing::{run_conformance_suite, run_datagram_source},
};
use transport_tokio::{TokioTransport, UdpTransport};

#[tokio::test]
async fn conformance_suite_all_cases_pass() {
    let report = run_conformance_suite::<TokioTransport>().await;
    assert!(report.all_passed(), "conformance suite failed: {report:?}");
    assert!(report.passed.contains(&"bind_udp"));
    assert!(report.passed.contains(&"name_non_empty"));
    assert!(report.passed.contains(&"connect_tcp"));
}

#[tokio::test]
async fn datagram_source_conformance_passes() {
    // `build(count)` binds a loopback receiver and floods it with `count`
    // datagrams from an ephemeral sender. Loopback delivery is not synchronous
    // with `send_to`, and the suite reaps in a tight sleepless loop, so give the
    // kernel a beat to land every datagram before handing over the source.
    let build = |count: usize| -> TokioTransport {
        let mut bind = BindConfig::default();
        bind.addr = "127.0.0.1:0".parse().expect("loopback addr");
        let udp = UdpTransport::bind_sync(
            bind,
            RecvBufConfig::default(),
            SendBufConfig::default(),
            RingConfig::default(),
            BatchConfig::default(),
            AffinityConfig::default(),
        )
        .expect("bind receiver");
        let addr = udp.local_addr().expect("local addr");

        let sender = StdUdpSocket::bind("127.0.0.1:0").expect("sender bind");
        for i in 0..count {
            sender.send_to(&[i as u8; 8], addr).expect("send datagram");
        }
        std::thread::sleep(Duration::from_millis(20));
        TokioTransport::Udp(udp)
    };

    let report = run_datagram_source(build);
    assert!(
        report.all_passed(),
        "datagram conformance failed: {:?}",
        report.failed
    );
    assert_eq!(report.passed.len(), 5);
}
