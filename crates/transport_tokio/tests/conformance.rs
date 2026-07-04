//! Runs the shared `transport_core::testing::run_conformance_suite` against
//! `TokioTransport`. The suite auto-spins a `127.0.0.1:0` listener for
//! `connect_tcp`, so with UDP + TCP both wired the report should be all-pass.

use transport_core::testing::run_conformance_suite;
use transport_tokio::TokioTransport;

#[tokio::test]
async fn conformance_suite_all_cases_pass() {
    let report = run_conformance_suite::<TokioTransport>().await;
    assert!(report.all_passed(), "conformance suite failed: {report:?}");
    assert!(report.passed.contains(&"bind_udp"));
    assert!(report.passed.contains(&"name_non_empty"));
    assert!(report.passed.contains(&"connect_tcp"));
}
