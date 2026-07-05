//! Tokio-based Transport backend. Wraps `tokio::net::UdpSocket` and
//! `tokio::net::TcpStream` behind the `transport_core` trait shape.

use std::task::{Context, Poll};

use transport_core::{
    AsPayload, BatchConfig, BindConfig, RecvBufConfig, RingConfig, SendBufConfig, Transport,
    TransportBind, TransportError,
};

pub mod pool;
pub mod stats;
pub mod tcp;
pub mod udp;

pub use pool::{SharedVecPool, VecPool, VecSlab};
pub use stats::{ReceiverStats, ReceiverStatsSnapshot};
pub use tcp::{TcpFrame, TcpTransport};
pub use udp::{RecvBatch, UdpFrame, UdpTransport};

pub enum TokioTransport {
    Udp(UdpTransport),
    Tcp(TcpTransport),
}

pub enum TokioFrame<'a> {
    Udp(UdpFrame<'a>),
    Tcp(TcpFrame<'a>),
}

pub enum TokioEvent {
    Udp(std::net::SocketAddr),
    Tcp(usize),
}

impl AsPayload for TokioFrame<'_> {
    fn payload(&self) -> &[u8] {
        match self {
            TokioFrame::Udp(f) => f.payload(),
            TokioFrame::Tcp(f) => f.payload(),
        }
    }

    fn sequence(&self) -> u64 {
        match self {
            TokioFrame::Udp(f) => f.sequence(),
            TokioFrame::Tcp(f) => f.sequence(),
        }
    }

    fn stream_id(&self) -> u8 {
        match self {
            TokioFrame::Udp(f) => f.stream_id(),
            TokioFrame::Tcp(f) => f.stream_id(),
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
            TokioTransport::Tcp(t) => match t.poll_recv(cx) {
                Poll::Ready(Ok(n)) => Poll::Ready(Ok(TokioEvent::Tcp(n))),
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                Poll::Pending => Poll::Pending,
            },
        }
    }

    fn next_frame(&self) -> Option<Self::Frame<'_>> {
        match self {
            TokioTransport::Udp(u) => u.peek_frame().map(TokioFrame::Udp),
            TokioTransport::Tcp(t) => t.peek_frame().map(TokioFrame::Tcp),
        }
    }

    fn name(&self) -> &'static str {
        match self {
            TokioTransport::Udp(_) => "tokio-udp",
            TokioTransport::Tcp(_) => "tokio-tcp",
        }
    }

    async fn send(&mut self, buf: &[u8]) -> Result<(), TransportError> {
        match self {
            TokioTransport::Udp(u) => u.send(buf).await.map(|_| ()),
            TokioTransport::Tcp(t) => t.send(buf).await,
        }
    }
}

impl TransportBind for TokioTransport {
    async fn bind_udp(
        bind: BindConfig,
        rx: RecvBufConfig,
        tx: SendBufConfig,
        _ring: RingConfig,
        batch: BatchConfig,
    ) -> Result<Self, TransportError> {
        let u = UdpTransport::bind(bind, rx, tx, batch).await?;
        Ok(TokioTransport::Udp(u))
    }

    async fn connect_tcp(
        bind: BindConfig,
        rx: RecvBufConfig,
        tx: SendBufConfig,
        _ring: RingConfig,
    ) -> Result<Self, TransportError> {
        let t = TcpTransport::connect(bind, rx, tx).await?;
        Ok(TokioTransport::Tcp(t))
    }
}

// `transport_core::UdpTransport` (not the inner `crate::udp::UdpTransport`
// struct re-exported above). Multicast join + unconnected send for the Udp
// variant; the Tcp variant rejects both.
impl transport_core::UdpTransport for TokioTransport {
    async fn join_multicast(
        &mut self,
        group: std::net::IpAddr,
        interface: transport_core::MulticastInterface,
    ) -> Result<(), TransportError> {
        match self {
            TokioTransport::Udp(u) => u.join_multicast(group, interface),
            TokioTransport::Tcp(_) => Err(TransportError::Unsupported {
                name: "tokio-tcp",
                reason: "multicast join unsupported on TCP",
            }),
        }
    }

    async fn send_to(
        &mut self,
        buf: &[u8],
        addr: std::net::SocketAddr,
    ) -> Result<(), TransportError> {
        match self {
            TokioTransport::Udp(u) => u.send_to(buf, addr).await.map(|_| ()),
            TokioTransport::Tcp(_) => Err(TransportError::Unsupported {
                name: "tokio-tcp",
                reason: "send_to unsupported on TCP",
            }),
        }
    }
}
