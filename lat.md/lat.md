# transport-tokio

Tokio-based backend for `transport_core`. Ships the `SharedVecPool` buffer-pool primitive, the UDP path, and the TCP path.

## Pool

Bounded slab pool: fixed slot array plus a free list, cheap `Drop`-based reclaim, backpressure via `acquire` returning `None`.

[[src/pool.rs#SharedVecPool]] is the reference-counted handle backends share across tasks; it wraps [[src/pool.rs#VecPool]] which owns a fixed slot array of `UnsafeCell<Vec<u8>>` plus a `parking_lot::Mutex<Vec<u32>>` free list. `Sync` is asserted manually because slot access is gated by the free list, not the compiler.

[[src/pool.rs#VecSlab]] is the owned slab handle. It carries `Arc<VecPool>`, a slot index, and a length. `Drop` returns the index to the free list, so the pool self-heals on task cancellation. `AsRef<[u8]>` returns the filled slice up to `len`.

`SharedVecPool::acquire(len)` returns `None` when `len` exceeds `slab_size` or when the free list is empty, giving backends a natural backpressure signal.

## UDP path

Wraps `tokio::net::UdpSocket` with socket-option application on bind: reuse, kernel buffers, busy-poll, timestamping.

[[src/udp.rs#UdpTransport]] wraps `tokio::net::UdpSocket`. `bind` builds a `socket2::Socket`, calls [[src/udp.rs#apply_socket_opts]] to install `SO_REUSEADDR`, `SO_REUSEPORT` (unix), `SO_RCVBUF`, `SO_SNDBUF`, `SO_BUSY_POLL` (Linux), and the timestamping request, then hands the raw fd to tokio.

`poll_recv` drains the socket into an internal scratch buffer and records the peer; `peek_frame` exposes the last datagram as [[src/udp.rs#UdpFrame]]. `UdpFrame` implements `AsPayload` with sequence and stream-id both zero: raw UDP has no sequencing, protocol crates layer that on top.

### Batched recv (Linux)

One `recvmmsg` syscall drains a burst of datagrams; ancillary data carries the kernel drop counter.

[[src/udp.rs#UdpTransport#recv_batch_linux]] issues one `recvmmsg` syscall via the `libc` FFI to drain a burst of datagrams. Uses `MSG_DONTWAIT` gated behind `tokio::UdpSocket::readable().await` so the async runtime schedules the wake-up correctly; `try_io` retries on spurious wake with `EAGAIN`.

[[src/udp.rs#RecvBatch]] holds the preallocated per-slot buffers, lens, peer addrs, and last-seen `SO_RXQ_OVFL` counter. Callers keep one `RecvBatch` per recv worker and call `recv_batch_linux` in a loop; `count` tells them how many slots the kernel filled.

Kernel drop counts are surfaced via `SO_RXQ_OVFL`: each datagram carries the current cumulative kernel-drop count in ancillary data. [[src/udp.rs#parse_scm_rxq_ovfl]] walks the cmsg list to extract it; the highest value seen in the batch advances [[src/stats.rs#ReceiverStats#advance_kernel_drops]] via CAS so parallel receivers do not race backwards.

### Socket-option helpers

Extra helpers layered on top of `apply_socket_opts` for the perf-tuning knobs.

[[src/udp.rs#apply_busy_poll]] is cfg-gated: Linux calls `libc::setsockopt(SOL_SOCKET, SO_BUSY_POLL, us)` directly, other platforms log a `tracing::warn!` when the field is set. Failed setsockopt does not fail bind; it warns and continues so the socket still binds under restricted sysctls.

[[src/udp.rs#apply_rxq_ovfl]] enables `SO_RXQ_OVFL` on Linux when `RecvBufConfig::so_rxq_ovfl` is set, so the kernel attaches the ancillary drop counter to every recv. Non-Linux warns and continues.

[[src/udp.rs#apply_timestamping]] currently only warns when `RecvBufConfig::so_timestamping` is `KernelSw` or `HardwareRx`; the real recvmsg ancillary-data parse lands alongside the `recvmmsg` batching path so both share one recv-side flow.

Kernel-buffer sizing (`SO_RCVBUF`, `SO_SNDBUF`) emits a `tracing::warn!` when the kernel grants less than requested. Operators tune `sysctl net.core.rmem_max` / `wmem_max` to lift the ceiling.

## TCP path

Wraps `tokio::net::TcpStream` with `SO_RCVBUF` / `SO_SNDBUF` applied via `socket2::SockRef` on the connected stream.

[[src/tcp.rs#TcpTransport]] opens a `TcpStream` to `BindConfig::addr` (interpreted as the remote peer for a client connect), then calls [[src/tcp.rs#apply_tcp_socket_opts]] to install the requested `SO_RCVBUF` and `SO_SNDBUF` sizes. `poll_recv` reads one chunk per poll into a 64 KiB scratch buffer; a zero-byte read is surfaced as `UnexpectedEof` so the caller can react to a graceful peer close.

[[src/tcp.rs#TcpFrame]] is the borrowed view. TCP is stream-oriented, so sequence and stream-id are both zero; the protocol crate (SoupBinTCP) handles record framing above.

## Receiver stats

Atomic counters shared between recv workers and observability consumers via `Arc<ReceiverStats>`.

[[src/stats.rs#ReceiverStats]] tracks `kernel_drops`, `packets_recv`, and `bytes_recv`. Every recv path (single `poll_recv` and batched `recv_batch_linux`) calls `record_packet(len)`; the batch path additionally calls `advance_kernel_drops` when `SO_RXQ_OVFL` reports a non-zero counter.

[[src/stats.rs#ReceiverStatsSnapshot]] is the plain-struct read-only copy returned by `ReceiverStats::snapshot`; observability code polls it instead of loading the atomics one by one.

## TokioTransport

Public enum that unifies UDP and TCP under a single `Transport` impl.

[[src/lib.rs#TokioTransport]] is the enum consumers depend on. `impl Transport` and `impl TransportBind` dispatch across the `Udp` and `Tcp` variants uniformly.

`impl transport_core::UdpTransport` adds multicast group join (`join_multicast`, dispatching IPv4 vs IPv6 to the inner socket's `join_multicast_v4`/`v6`) plus unconnected `send_to`. The `Tcp` variant rejects both with `TransportError::Unsupported`, so protocol crates that need multicast (MoldUDP) bound `T: UdpTransport` and get a compile error against a TCP-only backend.

[[src/lib.rs#TokioFrame]] and [[src/lib.rs#TokioEvent]] are the matching enums for the borrowed frame and per-poll event surface. `TokioEvent::Udp(SocketAddr)` carries the sender addr so protocol code can reply without a separate lookup; `TokioEvent::Tcp(usize)` carries the byte count.
