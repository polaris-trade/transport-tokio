//! Tokio-based Transport backend. Wraps `tokio::net::UdpSocket` (and
//! `tokio::net::TcpStream` in follow-up work) behind the `transport_core`
//! trait shape.

use std::task::{Context, Poll};

use transport_core::{
    AsPayload, BatchConfig, BindConfig, RecvBufConfig, RingConfig, Transport, TransportBind,
    TransportError,
};

pub mod pool;
pub mod udp;

pub use pool::{SharedVecPool, VecPool, VecSlab};
pub use udp::{UdpFrame, UdpTransport};

pub enum TokioTransport {
    Udp(UdpTransport),
}

pub enum TokioFrame<'a> {
    Udp(UdpFrame<'a>),
}

pub enum TokioEvent {
    Udp(std::net::SocketAddr),
}

impl AsPayload for TokioFrame<'_> {
    fn payload(&self) -> &[u8] {
        match self {
            TokioFrame::Udp(f) => f.payload(),
        }
    }

    fn sequence(&self) -> u64 {
        match self {
            TokioFrame::Udp(f) => f.sequence(),
        }
    }

    fn stream_id(&self) -> u8 {
        match self {
            TokioFrame::Udp(f) => f.stream_id(),
        }
    }
}

impl Transport for TokioTransport {
    type Frame<'a>
        = TokioFrame<'a>
    where
        Self: 'a;
    type Event = TokioEvent;

    fn poll_event(&mut self, cx: &mut Context<'_>) -> Poll<Result<Self::Event, TransportError>> {
        match self {
            TokioTransport::Udp(u) => match u.poll_recv(cx) {
                Poll::Ready(Ok(peer)) => Poll::Ready(Ok(TokioEvent::Udp(peer))),
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                Poll::Pending => Poll::Pending,
            },
        }
    }

    fn next_frame(&self) -> Option<Self::Frame<'_>> {
        match self {
            TokioTransport::Udp(u) => u.peek_frame().map(TokioFrame::Udp),
        }
    }

    fn name(&self) -> &'static str {
        match self {
            TokioTransport::Udp(_) => "tokio-udp",
        }
    }

    async fn send(&mut self, buf: &[u8]) -> Result<(), TransportError> {
        match self {
            TokioTransport::Udp(u) => u.send(buf).await.map(|_| ()),
        }
    }
}

impl TransportBind for TokioTransport {
    async fn bind_udp(
        bind: BindConfig,
        rx: RecvBufConfig,
        _ring: RingConfig,
        _batch: BatchConfig,
    ) -> Result<Self, TransportError> {
        let u = UdpTransport::bind(bind, rx).await?;
        Ok(TokioTransport::Udp(u))
    }

    async fn connect_tcp(
        _bind: BindConfig,
        _ring: RingConfig,
    ) -> Result<Self, TransportError> {
        Err(TransportError::Unsupported {
            name: "tcp_connect",
            reason: "TCP path not yet wired in this backend",
        })
    }
}
