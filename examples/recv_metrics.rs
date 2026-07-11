//! Run tokio UDP `recv_burst` with metrics live; scrape http://127.0.0.1:9464/metrics.
//! Self-floods a loopback socket (mirrors `benches/recv.rs`) so the run terminates
//! without a live exchange. Bounded: stops at 50_000 reaped frames or 5s, whichever first.

use std::{
    net::{SocketAddr, UdpSocket as StdUdpSocket},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use transport_core::{
    AffinityConfig, AsPayload, BatchConfig, BindConfig, DatagramSource, FrameBatch, RecvBufConfig,
    RingConfig, SendBufConfig, TransportError,
};
use transport_tokio::{TokioTransport, UdpFrame, UdpTransport};

const PAYLOAD_LEN: usize = 64;
const REAP_TARGET: u64 = 50_000;
const MAX_RUN: Duration = Duration::from_secs(5);

/// Flood `addr` with fixed-size datagrams until `stop` flips true.
fn spawn_flooder(addr: SocketAddr, stop: Arc<AtomicBool>) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let sender = StdUdpSocket::bind("127.0.0.1:0").expect("flooder bind");
        let payload = [0u8; PAYLOAD_LEN];
        while !stop.load(Ordering::Relaxed) {
            let _ = sender.send_to(&payload, addr);
        }
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _obs = observability::init(observability::ObsConfig {
        pipeline: observability::PipelineConfig {
            service_name: "transport-tokio-example".into(),
            level: "info".into(),
            otlp: None,
            logging: Some(observability::LoggingConfig {
                sinks: vec![observability::LogSink {
                    kind: observability::LogSinkKind::Stdout,
                    format: observability::LogFormat::Pretty,
                    level: None,
                }],
            }),
            tracing: None,
        },
        metrics: Some(observability::MetricsConfig {
            exporter: observability::MetricsExporter::Prometheus {
                bind: "127.0.0.1:9464".parse()?,
            },
        }),
    })?;
    // gate through transport-core's observability-core re-export, the same instance
    // the recv path reads. NOT observability::set_metrics_enabled - that re-export is
    // a separate crate copy (git-tag identity drives cargo dedup) and flips a dead gate.
    transport_core::observability_core::set_metrics_enabled(true);
    transport_core::observability_core::refresh_thread_gate();

    let mut bind = BindConfig::default();
    bind.addr = "127.0.0.1:0".parse().expect("loopback addr");
    let udp = UdpTransport::bind_sync(
        bind,
        RecvBufConfig::default(),
        SendBufConfig::default(),
        RingConfig::default(),
        BatchConfig::default(),
        AffinityConfig::default(),
    )?;
    let addr = udp.local_addr()?;
    let mut receiver = TokioTransport::Udp(udp);

    let stop = Arc::new(AtomicBool::new(false));
    let flooder = spawn_flooder(addr, Arc::clone(&stop));

    let mut batch: FrameBatch<UdpFrame> = FrameBatch::with_capacity(64);
    let mut reaped = 0u64;
    let start = Instant::now();
    while reaped < REAP_TARGET && start.elapsed() < MAX_RUN {
        match receiver.recv_burst(&mut batch, 64) {
            Ok(n) => {
                reaped += n as u64;
                for frame in batch.drain() {
                    std::hint::black_box(frame.payload().len());
                }
            }
            Err(TransportError::PoolExhausted { .. }) => continue,
            Err(e) => return Err(e.into()),
        }
    }

    stop.store(true, Ordering::Relaxed);
    flooder.join().expect("flooder thread");

    println!("reaped {reaped} frames; scrape counters at http://127.0.0.1:9464/metrics");
    Ok(())
}
