//! TCP path built on `tokio::net::TcpStream`.
//!
//! `TcpTransport::connect` opens a stream to `BindConfig::addr` and applies
//! `SO_RCVBUF` / `SO_SNDBUF` via `socket2::SockRef`. `recv_into` lands one read
//! directly into the caller's uninitialised buffer (a decode buffer's spare
//! capacity), the single copy in the stream path; protocol crates frame above.

use std::{mem::MaybeUninit, net::SocketAddr};

use socket2::SockRef;
use tokio::{
    io::{AsyncWriteExt, Interest},
    net::TcpStream,
};
use transport_core::{
    AffinityConfig, BindConfig, RecvBufConfig, RingConfig, SendBufConfig, TransportError,
};

use crate::{pool::SharedVecPool, udp::warn_affinity};

pub struct TcpTransport {
    stream: TcpStream,
    // Present for uniform `PoolAccess`; the stream path lands into caller-owned
    // buffers via `recv_into`, so it never draws slabs. Size it small via
    // `RingConfig` for a stream-only transport.
    pool: SharedVecPool,
    peer: SocketAddr,
}

impl TcpTransport {
    pub async fn connect(
        bind: BindConfig,
        rx: RecvBufConfig,
        tx: SendBufConfig,
        ring: RingConfig,
        affinity: AffinityConfig,
    ) -> Result<Self, TransportError> {
        warn_affinity(&affinity, "tokio-tcp");
        let stream =
            TcpStream::connect(bind.addr)
                .await
                .map_err(|e| TransportError::BindFailed {
                    addr: bind.addr.to_string(),
                    reason: e.to_string(),
                })?;
        let peer = stream.peer_addr().map_err(TransportError::Io)?;
        apply_tcp_socket_opts(&stream, &rx, &tx)?;
        Ok(Self {
            stream,
            pool: SharedVecPool::new(ring.slab_count, ring.slab_size),
            peer,
        })
    }

    /// Land one read into `dst` (typically a decode buffer's spare capacity).
    /// Returns the byte count written; the caller marks exactly that many bytes
    /// initialised. `Ok(0)` means nothing was ready. A clean peer close surfaces
    /// as `UnexpectedEof` so callers can react rather than spin on empty reads.
    pub fn recv_into(&mut self, dst: &mut [MaybeUninit<u8>]) -> Result<usize, TransportError> {
        let sock = SockRef::from(&self.stream);
        match sock.recv(dst) {
            Ok(0) => Err(TransportError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "peer closed",
            ))),
            Ok(n) => Ok(n),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(0),
            Err(e) => Err(TransportError::Io(e)),
        }
    }

    /// Resolve when stream is readable. Loops a raw `MSG_PEEK` probe via
    /// `try_io` so tokio's cached readiness bit clears on stale wake;
    /// `recv_into` bypasses the reactor and never clears that bit itself.
    pub async fn readable(&self) -> Result<(), TransportError> {
        loop {
            self.stream.readable().await.map_err(TransportError::Io)?;
            match self
                .stream
                .try_io(Interest::READABLE, || peek_ready(&self.stream))
            {
                Ok(()) => return Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(e) => return Err(TransportError::Io(e)),
            }
        }
    }

    pub fn pool(&self) -> &SharedVecPool {
        &self.pool
    }

    pub async fn send(&mut self, buf: &[u8]) -> Result<(), TransportError> {
        self.stream.write_all(buf).await.map_err(TransportError::Io)
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.peer
    }
}

/// `MSG_PEEK` probe: confirms real readiness without consuming stream bytes.
fn peek_ready(sock: &TcpStream) -> std::io::Result<()> {
    let mut buf = [MaybeUninit::<u8>::uninit(); 1];
    SockRef::from(sock).peek(&mut buf).map(|_| ())
}

fn apply_tcp_socket_opts(
    stream: &TcpStream,
    rx: &RecvBufConfig,
    tx: &SendBufConfig,
) -> Result<(), TransportError> {
    let sock = SockRef::from(stream);
    if let Some(req) = rx.so_rcvbuf {
        let req_usize = req as usize;
        sock.set_recv_buffer_size(req_usize)
            .map_err(TransportError::Io)?;
        let effective = sock.recv_buffer_size().map_err(TransportError::Io)?;
        if effective < req_usize {
            tracing::warn!(
                requested = req,
                effective,
                "kernel granted less SO_RCVBUF than requested on TCP"
            );
        }
    }
    if let Some(req) = tx.so_sndbuf {
        let req_usize = req as usize;
        sock.set_send_buffer_size(req_usize)
            .map_err(TransportError::Io)?;
        let effective = sock.send_buffer_size().map_err(TransportError::Io)?;
        if effective < req_usize {
            tracing::warn!(
                requested = req,
                effective,
                "kernel granted less SO_SNDBUF than requested on TCP"
            );
        }
    }
    Ok(())
}
