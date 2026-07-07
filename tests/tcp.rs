//! Loopback echo. Spins a tokio TCP listener that reflects one write
//! back to the sender, then drives `TokioTransport::Tcp` through
//! `send` -> `poll_event` -> `next_frame`.

use std::future::poll_fn;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use transport_core::{
    AsPayload, BindConfig, RecvBufConfig, RingConfig, SendBufConfig, Transport, TransportBind,
};
use transport_tokio::{TokioEvent, TokioTransport};

#[tokio::test]
async fn tcp_loopback_echo() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let peer_addr = listener.local_addr().expect("addr");
    let echo = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.expect("accept");
        let mut buf = [0u8; 32];
        let n = sock.read(&mut buf).await.expect("read");
        sock.write_all(&buf[..n]).await.expect("write");
    });

    let mut bind_cfg = BindConfig::default();
    bind_cfg.addr = peer_addr;
    let mut transport = TokioTransport::connect_tcp(
        bind_cfg,
        RecvBufConfig::default(),
        SendBufConfig::default(),
        RingConfig::default(),
    )
    .await
    .expect("connect_tcp");
    assert_eq!(transport.name(), "tokio-tcp");

    transport.send(b"hello").await.expect("send");

    let event = poll_fn(|cx| transport.poll_event(cx))
        .await
        .expect("poll_event");
    match event {
        TokioEvent::Tcp(n) => assert_eq!(n, 5, "echoed bytes"),
        TokioEvent::Udp(_) => panic!("wrong event variant"),
    }
    let frame = transport.next_frame().expect("frame present");
    assert_eq!(frame.payload(), b"hello");

    echo.await.expect("echo task");
}
