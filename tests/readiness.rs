//! Regression: `ready()` must reflect real socket state, not tokio's cached
//! readiness bit. `recv_burst`/`recv_into` hit the socket directly via raw
//! `socket2` syscalls and never clear that bit, so a naive `readable().await`
//! wrapper resolves instantly forever after the first packet. This drives two
//! send/drain cycles and asserts `ready()` blocks while idle in between.

use std::{mem::MaybeUninit, net::UdpSocket as StdUdpSocket, time::Duration};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use transport_core::{
    AffinityConfig, AsyncReady, BatchConfig, BindConfig, DatagramSource, FrameBatch, RecvBufConfig,
    RingConfig, SendBufConfig, StreamSource, TransportBind, TransportCore,
};
use transport_tokio::{TokioTransport, UdpTransport};

const IDLE_TIMEOUT: Duration = Duration::from_millis(150);

#[tokio::test]
async fn udp_ready_reflects_real_state_across_two_cycles() {
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
    .expect("bind_sync");
    let addr = udp.local_addr().expect("local_addr");
    let mut transport = TokioTransport::Udp(udp);

    let sender = StdUdpSocket::bind("127.0.0.1:0").expect("sender bind");
    let mut batch = FrameBatch::with_capacity(4);

    for _ in 0..2 {
        sender.send_to(b"hi", addr).expect("send datagram");

        transport.ready().await.expect("ready after send");
        let n = transport.recv_burst(&mut batch, 4).expect("recv_burst");
        assert_eq!(n, 1, "drained the sent datagram");
        for _ in batch.drain() {}
        // Drain again to Ok(0): confirms the socket is empty before the idle check.
        let drained = transport
            .recv_burst(&mut batch, 4)
            .expect("recv_burst drain");
        assert_eq!(drained, 0);

        // Old code: `readable().await` resolves instantly here (stale cached
        // bit from the first packet), so this timeout would never trip.
        let idle = tokio::time::timeout(IDLE_TIMEOUT, transport.ready()).await;
        assert!(idle.is_err(), "ready() must block while socket is idle");
    }
}

#[tokio::test]
async fn tcp_ready_reflects_real_state_across_two_cycles() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let peer_addr = listener.local_addr().expect("addr");
    // Echo until client closes (FIN -> Ok(0)); a fixed round count would drop
    // the socket right after round 2's write, racing round 2's drain probe.
    let echo = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.expect("accept");
        let mut buf = [0u8; 8];
        loop {
            match sock.read(&mut buf).await.expect("read") {
                0 => break,
                n => sock.write_all(&buf[..n]).await.expect("write"),
            }
        }
    });

    let mut bind = BindConfig::default();
    bind.addr = peer_addr;
    let mut transport = TokioTransport::connect_tcp(
        bind,
        RecvBufConfig::default(),
        SendBufConfig::default(),
        RingConfig::default(),
        AffinityConfig::default(),
    )
    .await
    .expect("connect_tcp");

    let mut buf = [MaybeUninit::<u8>::uninit(); 8];
    for _ in 0..2 {
        transport.send(b"hi").await.expect("send");

        transport.ready().await.expect("ready after send");
        let n = transport.recv_into(&mut buf).expect("recv_into");
        assert_eq!(n, 2, "drained the echoed bytes");
        // Drain again to Ok(0): confirms the stream has nothing more pending.
        let drained = transport.recv_into(&mut buf).expect("recv_into drain");
        assert_eq!(drained, 0);

        // Old code: `readable().await` resolves instantly here (stale cached
        // bit from the first read), so this timeout would never trip.
        let idle = tokio::time::timeout(IDLE_TIMEOUT, transport.ready()).await;
        assert!(idle.is_err(), "ready() must block while stream is idle");
    }

    // Close the client side so the echo task observes EOF and returns.
    drop(transport);
    echo.await.expect("echo task");
}
