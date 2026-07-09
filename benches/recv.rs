//! Recv hot-path bench: ns/msg and allocs/msg for `DatagramSource::recv_burst`
//! at batch depths {1, 8, 32, 64} over a localhost UDP flood. Feeds the
//! recvmmsg-fast-path decision noted in `src/udp.rs` and shard-count sizing.

use std::{
    hint::black_box,
    net::{SocketAddr, UdpSocket as StdUdpSocket},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use transport_core::{
    AffinityConfig, AsPayload, BatchConfig, BindConfig, DatagramSource, FrameBatch, RecvBufConfig,
    RingConfig, SendBufConfig, TransportError,
};
use transport_tokio::{TokioTransport, UdpFrame, UdpTransport};

const BATCH_DEPTHS: [usize; 4] = [1, 8, 32, 64];
const PAYLOAD_LEN: usize = 64;

/// Flood `addr` with fixed-size datagrams until `stop` flips true. Blocking
/// send on loopback self-paces against the kernel receive queue, no explicit
/// backoff needed.
fn spawn_flooder(addr: SocketAddr, stop: Arc<AtomicBool>) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let sender = StdUdpSocket::bind("127.0.0.1:0").expect("flooder bind");
        let payload = [0u8; PAYLOAD_LEN];
        while !stop.load(Ordering::Relaxed) {
            let _ = sender.send_to(&payload, addr);
        }
    })
}

/// Reap exactly `depth` frames, looping across multiple `recv_burst` calls
/// when one call returns fewer than requested. The flood keeps the socket
/// topped up, so this rarely loops more than once or twice.
fn reap_full_batch(receiver: &mut TokioTransport, batch: &mut FrameBatch<UdpFrame>, depth: usize) {
    let mut got = 0;
    while got < depth {
        match receiver.recv_burst(batch, depth - got) {
            Ok(n) => got += n,
            Err(TransportError::PoolExhausted { .. }) => break,
            Err(e) => panic!("recv_burst failed: {e}"),
        }
    }
}

fn bench_recv_burst(c: &mut Criterion) {
    // `bind_sync` registers the socket with a reactor, so a runtime context
    // must be live; recv itself bypasses the reactor (see src/udp.rs PERF note).
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let _guard = rt.enter();

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
    let mut receiver = TokioTransport::Udp(udp);

    let stop = Arc::new(AtomicBool::new(false));
    let flooder = spawn_flooder(addr, Arc::clone(&stop));

    let mut group = c.benchmark_group("tokio_recv_burst");
    for depth in BATCH_DEPTHS {
        group.throughput(Throughput::Elements(depth as u64));
        let mut batch: FrameBatch<UdpFrame> = FrameBatch::with_capacity(depth);

        // one-shot steady-state check: recv_burst at this depth must not
        // allocate once the pool and batch are warm.
        let alloc_info = allocation_counter::measure(|| {
            reap_full_batch(&mut receiver, &mut batch, depth);
            for frame in batch.drain() {
                black_box(frame.payload().len());
            }
        });
        assert_eq!(
            alloc_info.count_total, 0,
            "tokio recv_burst depth={depth} allocated {} times on the steady-state path",
            alloc_info.count_total
        );

        group.bench_with_input(BenchmarkId::from_parameter(depth), &depth, |b, &depth| {
            b.iter(|| {
                reap_full_batch(&mut receiver, &mut batch, depth);
                for frame in batch.drain() {
                    black_box(frame.payload().len());
                }
            });
        });
    }
    group.finish();

    stop.store(true, Ordering::Relaxed);
    flooder.join().expect("flooder thread");
}

criterion_group!(benches, bench_recv_burst);
criterion_main!(benches);
