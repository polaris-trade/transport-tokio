//! Runs the shared `transport_core::testing::run_conformance_suite` against
//! `TokioTransport`. `bind_udp` and the transport name check must pass;
//! `connect_tcp` is expected to fail until the TCP path lands.

use transport_core::testing::run_conformance_suite;
use transport_tokio::TokioTransport;

#[tokio::test]
async fn conformance_suite_udp_paths_pass() {
    let report = run_conformance_suite::<TokioTransport>().await;

    assert!(
        report.passed.contains(&"bind_udp"),
        "bind_udp case must pass, report = {report:?}"
    );
    assert!(
        report.passed.contains(&"name_non_empty"),
        "name_non_empty case must pass, report = {report:?}"
    );
    assert!(
        report.failed.iter().any(|(c, _)| *c == "connect_tcp"),
        "connect_tcp expected to fail until TCP path lands, report = {report:?}"
    );
}
