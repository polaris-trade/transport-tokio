//! Loopback echo over the stream path. Spins a tokio TCP listener that
//! reflects one write back, then drives `TokioTransport::Tcp` through
//! `send` -> `ready` -> `recv_into`, checking the landed bytes match.

use std::mem::MaybeUninit;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use transport_core::{
    AffinityConfig, AsyncReady, BindConfig, RecvBufConfig, RingConfig, SendBufConfig, StreamSource,
    TransportBind, TransportCore,
};
use transport_tokio::TokioTransport;

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
        AffinityConfig::default(),
    )
    .await
    .expect("connect_tcp");
    assert_eq!(transport.name(), "tokio-tcp");

    transport.send(b"hello").await.expect("send");

    // Land the echoed bytes once into an uninitialised buffer via `recv_into`.
    let mut buf = [MaybeUninit::<u8>::uninit(); 32];
    let n = loop {
        transport.ready().await.expect("ready");
        let n = transport.recv_into(&mut buf).expect("recv_into");
        if n > 0 {
            break n;
        }
    };
    assert_eq!(n, 5, "echoed bytes");
    // SAFETY: recv_into initialised exactly the first `n` bytes.
    let got: Vec<u8> = buf[..n]
        .iter()
        .map(|b| unsafe { b.assume_init() })
        .collect();
    assert_eq!(got, b"hello");

    echo.await.expect("echo task");
}
