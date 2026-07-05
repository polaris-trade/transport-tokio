//! UDP path built on `tokio::net::UdpSocket`.
//!
//! `UdpTransport::bind` creates a `socket2::Socket`, applies `SO_REUSEADDR` /
//! `SO_REUSEPORT` / `SO_RCVBUF` / `SO_SNDBUF` / `SO_BUSY_POLL` (Linux) /
//! `SO_RXQ_OVFL` (Linux) / timestamping (Linux) via [`apply_socket_opts`],
//! then hands off to the tokio runtime. `poll_recv` drives the single-recv
//! path; `recv_batch_linux` drains a burst in one `recvmmsg` on Linux and
//! reports kernel drops via [`ReceiverStats`].

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::task::{Context, Poll};

use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;
use transport_core::{
    AsPayload, BatchConfig, BindConfig, MulticastInterface, RecvBufConfig, SendBufConfig,
    TimestampMode, TransportError,
};

use crate::stats::ReceiverStats;

const MAX_UDP_DGRAM: usize = 64 * 1024;

pub struct UdpTransport {
    sock: UdpSocket,
    buf: Vec<u8>,
    last_len: usize,
    last_peer: Option<SocketAddr>,
    has_frame: bool,
    stats: Arc<ReceiverStats>,
    batch_recv_size: u32,
}

impl UdpTransport {
    pub async fn bind(
        bind: BindConfig,
        rx: RecvBufConfig,
        tx: SendBufConfig,
        batch: BatchConfig,
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
            stats: Arc::new(ReceiverStats::default()),
            batch_recv_size: batch.recv_size,
        })
    }

    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Result<SocketAddr, TransportError>> {
        let mut rb = tokio::io::ReadBuf::new(&mut self.buf);
        match self.sock.poll_recv_from(cx, &mut rb) {
            Poll::Ready(Ok(peer)) => {
                self.last_len = rb.filled().len();
                self.last_peer = Some(peer);
                self.has_frame = true;
                self.stats.record_packet(self.last_len);
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

#[cfg(target_os = "linux")]
impl UdpTransport {
    /// Drain a burst via `recvmmsg`. `batch.capacity()` is the max slots
    /// filled per call; returned count is however many the kernel had ready.
    /// Kernel drop counter (`SO_RXQ_OVFL`) is copied into `batch.kernel_drops`
    /// and advanced on `self.stats`.
    pub async fn recv_batch_linux(&self, batch: &mut RecvBatch) -> Result<usize, TransportError> {
        use std::os::fd::AsRawFd;
        let fd = self.sock.as_raw_fd();
        let capacity = batch.capacity();
        loop {
            self.sock.readable().await.map_err(TransportError::Io)?;
            let result = self.sock.try_io(tokio::io::Interest::READABLE, || {
                let mut iovs: Vec<libc::iovec> = batch
                    .bufs
                    .iter_mut()
                    .map(|b| libc::iovec {
                        iov_base: b.as_mut_ptr() as *mut libc::c_void,
                        iov_len: b.len(),
                    })
                    .collect();
                let mut addrs: Vec<libc::sockaddr_storage> =
                    vec![unsafe { std::mem::zeroed() }; capacity];
                let mut controls: Vec<[u8; 64]> = vec![[0u8; 64]; capacity];
                let mut msgvec: Vec<libc::mmsghdr> = (0..capacity)
                    .map(|i| {
                        let mut hdr: libc::msghdr = unsafe { std::mem::zeroed() };
                        hdr.msg_name = &mut addrs[i] as *mut _ as *mut libc::c_void;
                        hdr.msg_namelen =
                            std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
                        hdr.msg_iov = &mut iovs[i];
                        hdr.msg_iovlen = 1;
                        hdr.msg_control = controls[i].as_mut_ptr() as *mut libc::c_void;
                        hdr.msg_controllen = controls[i].len();
                        hdr.msg_flags = 0;
                        libc::mmsghdr {
                            msg_hdr: hdr,
                            msg_len: 0,
                        }
                    })
                    .collect();

                // SAFETY: fd owned by `self.sock`; msgvec pointers reference
                // heap-owned buffers that outlive the call.
                let rc = unsafe {
                    libc::recvmmsg(
                        fd,
                        msgvec.as_mut_ptr(),
                        capacity as libc::c_uint,
                        libc::MSG_DONTWAIT,
                        std::ptr::null_mut(),
                    )
                };
                if rc < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                let n = rc as usize;
                let mut max_drops: u32 = 0;
                for i in 0..n {
                    let msg = &msgvec[i];
                    let len = msg.msg_len as usize;
                    batch.lens[i] = len;
                    batch.peers[i] = parse_peer(&addrs[i], msg.msg_hdr.msg_namelen);
                    if let Some(drops) = parse_scm_rxq_ovfl(&msg.msg_hdr) {
                        max_drops = max_drops.max(drops);
                    }
                    self.stats.record_packet(len);
                }
                batch.count = n;
                batch.kernel_drops = max_drops;
                if max_drops > 0 {
                    self.stats.advance_kernel_drops(max_drops as u64);
                }
                Ok(n)
            });
            match result {
                Ok(n) => return Ok(n),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(e) => return Err(TransportError::Io(e)),
            }
        }
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

#[cfg(target_os = "linux")]
fn parse_peer(addr: &libc::sockaddr_storage, namelen: libc::socklen_t) -> Option<SocketAddr> {
    if (namelen as usize) < std::mem::size_of::<libc::sa_family_t>() {
        return None;
    }
    match addr.ss_family as libc::c_int {
        libc::AF_INET => {
            // SAFETY: family AF_INET means addr is layout-compatible with sockaddr_in.
            let sin: &libc::sockaddr_in =
                unsafe { &*(addr as *const _ as *const libc::sockaddr_in) };
            let ip = std::net::Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr));
            let port = u16::from_be(sin.sin_port);
            Some(SocketAddr::V4(std::net::SocketAddrV4::new(ip, port)))
        }
        libc::AF_INET6 => {
            // SAFETY: family AF_INET6 means addr is layout-compatible with sockaddr_in6.
            let sin6: &libc::sockaddr_in6 =
                unsafe { &*(addr as *const _ as *const libc::sockaddr_in6) };
            let ip = std::net::Ipv6Addr::from(sin6.sin6_addr.s6_addr);
            let port = u16::from_be(sin6.sin6_port);
            Some(SocketAddr::V6(std::net::SocketAddrV6::new(
                ip,
                port,
                sin6.sin6_flowinfo,
                sin6.sin6_scope_id,
            )))
        }
        _ => None,
    }
}

#[cfg(target_os = "linux")]
fn parse_scm_rxq_ovfl(hdr: &libc::msghdr) -> Option<u32> {
    // SAFETY: iterate cmsg via CMSG_FIRSTHDR / CMSG_NXTHDR macros.
    unsafe {
        let mut cmsg = libc::CMSG_FIRSTHDR(hdr);
        while !cmsg.is_null() {
            if (*cmsg).cmsg_level == libc::SOL_SOCKET && (*cmsg).cmsg_type == libc::SO_RXQ_OVFL {
                let data = libc::CMSG_DATA(cmsg) as *const u32;
                return Some(std::ptr::read_unaligned(data));
            }
            cmsg = libc::CMSG_NXTHDR(hdr, cmsg);
        }
    }
    None
}

/// Per-datagram buffer set consumed by [`UdpTransport::recv_batch_linux`].
/// Preallocate once, reuse across calls.
pub struct RecvBatch {
    pub bufs: Vec<Vec<u8>>,
    pub lens: Vec<usize>,
    pub peers: Vec<Option<SocketAddr>>,
    pub count: usize,
    pub kernel_drops: u32,
}

impl RecvBatch {
    pub fn with_capacity(batch_size: usize, mtu: usize) -> Self {
        Self {
            bufs: (0..batch_size).map(|_| vec![0u8; mtu]).collect(),
            lens: vec![0; batch_size],
            peers: vec![None; batch_size],
            count: 0,
            kernel_drops: 0,
        }
    }

    pub fn capacity(&self) -> usize {
        self.bufs.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&[u8], SocketAddr)> {
        (0..self.count).map(move |i| {
            (
                &self.bufs[i][..self.lens[i]],
                self.peers[i].expect("peer set during recv_batch"),
            )
        })
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
