//! TCP path built on `tokio::net::TcpStream`.
//!
//! `TcpTransport::connect` opens a stream to `BindConfig::addr` and applies
//! `SO_RCVBUF` / `SO_SNDBUF` via `socket2::SockRef`. `poll_recv` reads one
//! chunk per call into an internal buffer; consumers observe the chunk via
//! `peek_frame`. TCP is stream-oriented so [`TcpFrame`] carries opaque bytes
//! with sequence and stream-id both zero; protocol crates handle framing.

use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use socket2::SockRef;
use tokio::io::{AsyncRead, AsyncWriteExt};
use tokio::net::TcpStream;
use transport_core::{AsPayload, BindConfig, RecvBufConfig, SendBufConfig, TransportError};

const MAX_TCP_CHUNK: usize = 64 * 1024;

pub struct TcpTransport {
    stream: TcpStream,
    buf: Vec<u8>,
    last_len: usize,
    has_frame: bool,
    peer: SocketAddr,
}

impl TcpTransport {
    pub async fn connect(
        bind: BindConfig,
        rx: RecvBufConfig,
        tx: SendBufConfig,
    ) -> Result<Self, TransportError> {
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
            buf: vec![0u8; MAX_TCP_CHUNK],
            last_len: 0,
            has_frame: false,
            peer,
        })
    }

    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Result<usize, TransportError>> {
        let mut rb = tokio::io::ReadBuf::new(&mut self.buf);
        match Pin::new(&mut self.stream).poll_read(cx, &mut rb) {
            Poll::Ready(Ok(())) => {
                let n = rb.filled().len();
                if n == 0 {
                    return Poll::Ready(Err(TransportError::Io(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "peer closed",
                    ))));
                }
                self.last_len = n;
                self.has_frame = true;
                Poll::Ready(Ok(n))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(TransportError::Io(e))),
            Poll::Pending => Poll::Pending,
        }
    }

    pub fn peek_frame(&self) -> Option<TcpFrame<'_>> {
        if self.has_frame {
            Some(TcpFrame {
                bytes: &self.buf[..self.last_len],
            })
        } else {
            None
        }
    }

    pub async fn send(&mut self, buf: &[u8]) -> Result<(), TransportError> {
        self.stream.write_all(buf).await.map_err(TransportError::Io)
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.peer
    }
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

pub struct TcpFrame<'a> {
    pub bytes: &'a [u8],
}

impl AsPayload for TcpFrame<'_> {
    fn payload(&self) -> &[u8] {
        self.bytes
    }

    fn sequence(&self) -> u64 {
        0
    }

    fn stream_id(&self) -> u8 {
        0
    }
}
