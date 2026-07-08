//! UDP path built on `tokio::net::UdpSocket`.
//!
//! `UdpTransport::bind` creates a `socket2::Socket`, applies `SO_REUSEADDR` /
//! `SO_REUSEPORT` / `SO_RCVBUF` / `SO_SNDBUF` / `SO_BUSY_POLL` (Linux) /
//! `SO_RXQ_OVFL` (Linux) / timestamping (Linux) via [`apply_socket_opts`],
//! then hands off to the tokio runtime. `recv_burst` is the sync, batch-first
//! recv: it reaps ready datagrams into pool-owned [`UdpFrame`]s and returns the
//! count, `Ok(0)` when the socket is drained, `PoolExhausted` under backpressure.
//!
//! PERF: recv is a per-datagram `try_recv_from` loop today. A Linux `recvmmsg`
//! fast path (one syscall per burst) plus `SO_RXQ_OVFL` drop-count readback is a
//! measured follow-up gated on the recv benchmark, not a blind rewrite.

use std::{
    mem::MaybeUninit,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

use socket2::{Domain, Protocol, SockRef, Socket, Type};
use tokio::net::UdpSocket;
use transport_core::{
    AffinityConfig, AsPayload, BatchConfig, BindConfig, BufferPool, FrameBatch, MulticastInterface,
    RecvBufConfig, RingConfig, SendBufConfig, TimestampMode, TransportError,
};

use crate::{
    pool::{SharedVecPool, VecSlab},
    stats::ReceiverStats,
};

pub struct UdpTransport {
    sock: UdpSocket,
    pool: SharedVecPool,
    stats: Arc<ReceiverStats>,
    batch_recv_size: u32,
    last_peer: Option<SocketAddr>,
}

impl UdpTransport {
    /// Async binder used by `TransportBind`. The work is synchronous; this is a
    /// thin wrapper over [`UdpTransport::bind_sync`] so the trait's async shape
    /// holds while tests and conformance builders bind without a runtime await.
    pub async fn bind(
        bind: BindConfig,
        rx: RecvBufConfig,
        tx: SendBufConfig,
        ring: RingConfig,
        batch: BatchConfig,
        affinity: AffinityConfig,
    ) -> Result<Self, TransportError> {
        Self::bind_sync(bind, rx, tx, ring, batch, affinity)
    }

    /// Bind synchronously. Must run inside a tokio runtime context
    /// (`UdpSocket::from_std` registers with the reactor). `ring` sizes the
    /// slab pool recv lands into; `affinity` is honored where the backend can.
    pub fn bind_sync(
        bind: BindConfig,
        rx: RecvBufConfig,
        tx: SendBufConfig,
        ring: RingConfig,
        batch: BatchConfig,
        affinity: AffinityConfig,
    ) -> Result<Self, TransportError> {
        warn_affinity(&affinity, "tokio-udp");
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
            pool: SharedVecPool::new(ring.slab_count, ring.slab_size),
            stats: Arc::new(ReceiverStats::default()),
            batch_recv_size: batch.recv_size,
            last_peer: None,
        })
    }

    /// Reap up to `max` ready datagrams into `out`, each frame owning a pool
    /// slab it wrote into (zero further copy). Returns the count. `Ok(0)` means
    /// the socket had nothing ready; `PoolExhausted` means no landing slab was
    /// free while data was pending (backpressure, let the kernel drop).
    pub fn recv_burst(
        &mut self,
        out: &mut FrameBatch<UdpFrame>,
        max: usize,
    ) -> Result<usize, TransportError> {
        let cap = max.min(out.spare());
        let mut n = 0;
        while n < cap {
            let mut slab = match self.pool.acquire(self.pool.slab_size()) {
                Some(slab) => slab,
                None => {
                    if n == 0 {
                        return Err(TransportError::PoolExhausted {
                            in_use: self.pool.in_use(),
                            capacity: self.pool.capacity(),
                        });
                    }
                    break;
                }
            };
            // Hit the socket directly (nonblocking), bypassing tokio's cached
            // reactor readiness: a sync busy-poll recv must attempt the syscall
            // regardless of whether the runtime has observed the fd as readable.
            let buf = slab.buf_mut();
            // SAFETY: `&mut [u8]` and `&mut [MaybeUninit<u8>]` share layout; the
            // slab is already initialised and `recv_from` only writes into it.
            let dst = unsafe {
                std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut MaybeUninit<u8>, buf.len())
            };
            match SockRef::from(&self.sock).recv_from(dst) {
                Ok((len, from)) => {
                    let peer = from
                        .as_socket()
                        .unwrap_or_else(|| SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0));
                    slab.set_len(len);
                    self.stats.record_packet(len);
                    self.last_peer = Some(peer);
                    out.push(UdpFrame { slab, peer });
                    n += 1;
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // Nothing more ready; `slab` drops here, back to the pool.
                    break;
                }
                Err(e) => return Err(TransportError::Io(e)),
            }
        }
        Ok(n)
    }

    /// Resolve when the socket is readable, for `.await`-driven callers. The
    /// sync `recv_burst` never carries a waker; this is the optional adapter.
    pub async fn readable(&self) -> Result<(), TransportError> {
        self.sock.readable().await.map_err(TransportError::Io)
    }

    pub fn pool(&self) -> &SharedVecPool {
        &self.pool
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

    /// Join an IPv4 or IPv6 multicast group. `interface.v4` picks the local
    /// v4 interface addr (defaults to `INADDR_ANY`); `interface.v6_scope_id`
    /// picks the v6 interface index (defaults to 0 = any).
    pub fn join_multicast(
        &self,
        group: IpAddr,
        interface: MulticastInterface,
    ) -> Result<(), TransportError> {
        match group {
            IpAddr::V4(m) => {
                let iface = interface.v4.unwrap_or(Ipv4Addr::UNSPECIFIED);
                self.sock
                    .join_multicast_v4(m, iface)
                    .map_err(TransportError::Io)
            }
            IpAddr::V6(m) => {
                let scope = interface.v6_scope_id.unwrap_or(0);
                self.sock
                    .join_multicast_v6(&m, scope)
                    .map_err(TransportError::Io)
            }
        }
    }

    pub fn local_addr(&self) -> Result<SocketAddr, TransportError> {
        self.sock.local_addr().map_err(TransportError::Io)
    }

    pub fn last_peer(&self) -> Option<SocketAddr> {
        self.last_peer
    }

    pub fn stats(&self) -> &Arc<ReceiverStats> {
        &self.stats
    }

    pub fn batch_recv_size(&self) -> u32 {
        self.batch_recv_size
    }
}

/// Warn when CPU-affinity knobs are set on a backend that cannot honor them.
/// Tokio owns its worker threads, so `io_cpu`/`sqpoll_cpu` are informational
/// here; a busy-poll backend pins its own driver loop instead.
pub(crate) fn warn_affinity(affinity: &AffinityConfig, backend: &'static str) {
    if affinity.io_cpu.is_some() || affinity.sqpoll_cpu.is_some() {
        tracing::warn!(
            backend,
            "CPU affinity requested but tokio manages its own worker threads; ignoring"
        );
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
    apply_rxq_ovfl(sock, rx)?;
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

#[cfg(target_os = "linux")]
fn apply_rxq_ovfl(sock: &Socket, rx: &RecvBufConfig) -> Result<(), TransportError> {
    use std::os::fd::AsRawFd;
    if !rx.so_rxq_ovfl {
        return Ok(());
    }
    let fd = sock.as_raw_fd();
    let val: libc::c_int = 1;
    // SAFETY: fd owned by `sock`; `val` outlives the syscall.
    let rc = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RXQ_OVFL,
            &val as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        tracing::warn!(error = %err, "SO_RXQ_OVFL setsockopt failed");
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn apply_rxq_ovfl(_sock: &Socket, rx: &RecvBufConfig) -> Result<(), TransportError> {
    if rx.so_rxq_ovfl {
        tracing::warn!("SO_RXQ_OVFL requested but only supported on Linux");
    }
    Ok(())
}

fn apply_timestamping(_sock: &Socket, rx: &RecvBufConfig) {
    // NOTE: real SO_TIMESTAMPING needs recvmsg + ancillary parsing on the recv
    // path; it lands with the recvmmsg fast path. For now warn so operators know
    // the knob is inert here.
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

/// Owned UDP datagram: carries the pool slab it landed in, so it is
/// `Send + 'static` and returns the slab to the pool on `Drop`. Raw UDP has no
/// sequencing, so `sequence`/`stream_id` are zero; protocol crates layer those.
pub struct UdpFrame {
    slab: VecSlab,
    peer: SocketAddr,
}

impl UdpFrame {
    pub fn peer(&self) -> SocketAddr {
        self.peer
    }
}

impl AsPayload for UdpFrame {
    fn payload(&self) -> &[u8] {
        self.slab.as_ref()
    }

    fn sequence(&self) -> u64 {
        0
    }

    fn stream_id(&self) -> u8 {
        0
    }
}
