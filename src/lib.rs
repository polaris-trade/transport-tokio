//! Tokio-based transport backend. Wraps `tokio::net::UdpSocket` and
//! `tokio::net::TcpStream` behind the `transport_core` recv seam:
//! `DatagramSource` for UDP, `StreamSource` for TCP, `AsyncReady` as the
//! readiness adapter, `PoolAccess` for the shared slab pool.

use std::mem::MaybeUninit;

use transport_core::{
    AffinityConfig, AsyncReady, BatchConfig, BindConfig, DatagramSource, FrameBatch, PoolAccess,
    RecvBufConfig, RingConfig, SendBufConfig, StreamSource, TransportBind, TransportCore,
    TransportError,
};

pub mod pool;
pub mod stats;
pub mod tcp;
pub mod udp;

pub use pool::{SharedVecPool, VecPool, VecSlab};
pub use stats::{ReceiverStats, ReceiverStatsSnapshot};
pub use tcp::TcpTransport;
pub use udp::{UdpFrame, UdpTransport};

/// Public backend enum consumers depend on. The `Udp` variant is the
/// `DatagramSource`; the `Tcp` variant is the `StreamSource`. Calling the wrong
/// recv shape for a variant returns `TransportError::Unsupported`.
pub enum TokioTransport {
    Udp(UdpTransport),
    Tcp(TcpTransport),
}

impl TransportCore for TokioTransport {
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

impl DatagramSource for TokioTransport {
    type Frame = UdpFrame;

    fn recv_burst(
        &mut self,
        out: &mut FrameBatch<UdpFrame>,
        max: usize,
    ) -> Result<usize, TransportError> {
        match self {
            TokioTransport::Udp(u) => u.recv_burst(out, max),
            TokioTransport::Tcp(_) => Err(TransportError::Unsupported {
                name: "tokio-tcp",
                reason: "recv_burst is datagram-only; use recv_into",
            }),
        }
    }
}

impl StreamSource for TokioTransport {
    fn recv_into(&mut self, dst: &mut [MaybeUninit<u8>]) -> Result<usize, TransportError> {
        match self {
            TokioTransport::Tcp(t) => t.recv_into(dst),
            TokioTransport::Udp(_) => Err(TransportError::Unsupported {
                name: "tokio-udp",
                reason: "recv_into is stream-only; use recv_burst",
            }),
        }
    }
}

impl AsyncReady for TokioTransport {
    async fn ready(&mut self) -> Result<(), TransportError> {
        match self {
            TokioTransport::Udp(u) => u.readable().await,
            TokioTransport::Tcp(t) => t.readable().await,
        }
    }
}

impl PoolAccess for TokioTransport {
    type Pool = SharedVecPool;

    fn pool(&self) -> &SharedVecPool {
        match self {
            TokioTransport::Udp(u) => u.pool(),
            TokioTransport::Tcp(t) => t.pool(),
        }
    }
}

impl TransportBind for TokioTransport {
    async fn bind_udp(
        bind: BindConfig,
        rx: RecvBufConfig,
        tx: SendBufConfig,
        ring: RingConfig,
        batch: BatchConfig,
        affinity: AffinityConfig,
    ) -> Result<Self, TransportError> {
        let u = UdpTransport::bind(bind, rx, tx, ring, batch, affinity).await?;
        Ok(TokioTransport::Udp(u))
    }

    async fn connect_tcp(
        bind: BindConfig,
        rx: RecvBufConfig,
        tx: SendBufConfig,
        ring: RingConfig,
        affinity: AffinityConfig,
    ) -> Result<Self, TransportError> {
        let t = TcpTransport::connect(bind, rx, tx, ring, affinity).await?;
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
