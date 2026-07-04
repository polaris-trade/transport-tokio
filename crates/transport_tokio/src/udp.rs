//! UDP path built on `tokio::net::UdpSocket`.
//!
//! `UdpTransport::bind` creates a `socket2::Socket`, applies `SO_REUSEADDR` /
//! `SO_REUSEPORT` / `SO_RCVBUF` / `SO_SNDBUF` / `SO_BUSY_POLL` (Linux) /
//! timestamping (Linux) via [`apply_socket_opts`], then hands off to the
//! tokio runtime. `poll_recv` drives receive; the latest datagram is peeked
//! as `UdpFrame` via [`peek_frame`].

use std::net::SocketAddr;
use std::task::{Context, Poll};

use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;
use transport_core::{
    AsPayload, BindConfig, RecvBufConfig, SendBufConfig, TimestampMode, TransportError,
};

const MAX_UDP_DGRAM: usize = 64 * 1024;

pub struct UdpTransport {
    sock: UdpSocket,
    buf: Vec<u8>,
    last_len: usize,
    last_peer: Option<SocketAddr>,
    has_frame: bool,
}

impl UdpTransport {
    pub async fn bind(
        bind: BindConfig,
        rx: RecvBufConfig,
        tx: SendBufConfig,
    ) -> Result<Self, TransportError> {
        let raw = create_socket(bind.addr)?;
        apply_socket_opts(&raw, &bind, &rx, &tx)?;
        raw.set_nonblocking(true).map_err(TransportError::Io)?;
        raw.bind(&bind.addr.into())
            .map_err(|e| TransportError::BindFailed {
                addr: bind.addr.to_string(),
                reason: e.to_string(),
            })?;
        let std_sock: std::net::UdpSocket = raw.into();
        let sock = UdpSocket::from_std(std_sock).map_err(TransportError::Io)?;
        Ok(Self {
            sock,
            buf: vec![0u8; MAX_UDP_DGRAM],
            last_len: 0,
            last_peer: None,
            has_frame: false,
        })
    }

    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Result<SocketAddr, TransportError>> {
        let mut rb = tokio::io::ReadBuf::new(&mut self.buf);
        match self.sock.poll_recv_from(cx, &mut rb) {
            Poll::Ready(Ok(peer)) => {
                self.last_len = rb.filled().len();
                self.last_peer = Some(peer);
                self.has_frame = true;
                Poll::Ready(Ok(peer))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(TransportError::Io(e))),
            Poll::Pending => Poll::Pending,
        }
    }

    pub fn peek_frame(&self) -> Option<UdpFrame<'_>> {
        if self.has_frame {
            Some(UdpFrame {
                bytes: &self.buf[..self.last_len],
            })
        } else {
            None
        }
    }

    pub async fn send(&self, buf: &[u8]) -> Result<usize, TransportError> {
        self.sock.send(buf).await.map_err(TransportError::Io)
    }

    pub async fn send_to(&self, buf: &[u8], addr: SocketAddr) -> Result<usize, TransportError> {
        self.sock
            .send_to(buf, addr)
            .await
            .map_err(TransportError::Io)
    }

    pub fn local_addr(&self) -> Result<SocketAddr, TransportError> {
        self.sock.local_addr().map_err(TransportError::Io)
    }

    pub fn last_peer(&self) -> Option<SocketAddr> {
        self.last_peer
    }
}

fn create_socket(addr: SocketAddr) -> Result<Socket, TransportError> {
    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    Socket::new(domain, Type::DGRAM, Some(Protocol::UDP)).map_err(TransportError::Io)
}

pub(crate) fn apply_socket_opts(
    sock: &Socket,
    bind: &BindConfig,
    rx: &RecvBufConfig,
    tx: &SendBufConfig,
) -> Result<(), TransportError> {
    if bind.reuse_addr {
        sock.set_reuse_address(true).map_err(TransportError::Io)?;
    }
    #[cfg(unix)]
    if bind.reuse_port {
        sock.set_reuse_port(true).map_err(TransportError::Io)?;
    }
    if let Some(req) = rx.so_rcvbuf {
        let req_usize = req as usize;
        sock.set_recv_buffer_size(req_usize)
            .map_err(TransportError::Io)?;
        let effective = sock.recv_buffer_size().map_err(TransportError::Io)?;
        if effective < req_usize {
            tracing::warn!(
                requested = req,
                effective,
                "kernel granted less SO_RCVBUF than requested"
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
                "kernel granted less SO_SNDBUF than requested"
            );
        }
    }
    apply_busy_poll(sock, rx)?;
    apply_timestamping(sock, rx);
    Ok(())
}

#[cfg(target_os = "linux")]
fn apply_busy_poll(sock: &Socket, rx: &RecvBufConfig) -> Result<(), TransportError> {
    use std::os::fd::AsRawFd;
    let Some(us) = rx.so_busy_poll_us else {
        return Ok(());
    };
    let fd = sock.as_raw_fd();
    let val: libc::c_int = us as libc::c_int;
    // SAFETY: fd owned by `sock`, `val` outlives the syscall, len matches type.
    let rc = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_BUSY_POLL,
            &val as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        tracing::warn!(requested = us, error = %err, "SO_BUSY_POLL setsockopt failed");
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn apply_busy_poll(_sock: &Socket, rx: &RecvBufConfig) -> Result<(), TransportError> {
    if rx.so_busy_poll_us.is_some() {
        tracing::warn!("SO_BUSY_POLL requested but only supported on Linux");
    }
    Ok(())
}

fn apply_timestamping(_sock: &Socket, rx: &RecvBufConfig) {
    // NOTE: real SO_TIMESTAMPING requires recvmsg + ancillary data parsing on
    // the recv path; wire that up alongside recvmmsg batching. For now only
    // warn so operators know the config knob is inert here.
    match rx.so_timestamping {
        TimestampMode::None => {}
        TimestampMode::KernelSw | TimestampMode::HardwareRx => {
            tracing::warn!(
                mode = ?rx.so_timestamping,
                "timestamping requested but recvmsg ancillary path not yet wired"
            );
        }
    }
}

pub struct UdpFrame<'a> {
    pub bytes: &'a [u8],
}

impl AsPayload for UdpFrame<'_> {
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
