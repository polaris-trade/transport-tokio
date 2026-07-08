# transport-tokio

Tokio-based backend for `transport_core`. Ships the `SharedVecPool` buffer-pool primitive, the UDP datagram path, and the TCP stream path, all behind the `transport_core` recv seam.

## Pool

Bounded slab pool: fixed slot array plus a free list, cheap `Drop`-based reclaim, backpressure via `acquire` returning `None`. Slabs carry a write path so recv lands bytes directly into pooled memory.

[[src/pool.rs#SharedVecPool]] is the reference-counted handle backends share across tasks; it wraps [[src/pool.rs#VecPool]] which owns a fixed slot array of `UnsafeCell<Vec<u8>>` plus a `parking_lot::Mutex<Vec<u32>>` free list. `Sync` is asserted manually because slot access is gated by the free list, not the compiler. Each slot is zero-initialised once at build so recv has a full-width `&mut [u8]` to write into with no per-message allocation.

[[src/pool.rs#VecSlab]] is the owned slab handle. It carries `Arc<VecPool>`, a slot index, and a length. `Drop` returns the index to the free list, so the pool self-heals on task cancellation. `AsRef<[u8]>` returns the filled slice up to `len`; [[src/pool.rs#VecSlab#buf_mut]] hands recv the full backing slice and [[src/pool.rs#VecSlab#set_len]] records how many bytes landed.

`SharedVecPool::acquire(len)` returns `None` when `len` exceeds `slab_size` or when the free list is empty, giving backends a natural backpressure signal.

## UDP path

Wraps `tokio::net::UdpSocket` with socket-option application on bind: reuse, kernel buffers, busy-poll, timestamping. Recv is sync and batch-first.

[[src/udp.rs#UdpTransport]] wraps `tokio::net::UdpSocket`. `bind` builds a `socket2::Socket`, calls [[src/udp.rs#apply_socket_opts]] to install `SO_REUSEADDR`, `SO_REUSEPORT` (unix), `SO_RCVBUF`, `SO_SNDBUF`, `SO_BUSY_POLL` (Linux), and the timestamping request, then hands the raw fd to tokio. `bind_sync` is the runtime-context sync constructor conformance builders use; `bind` is the async wrapper `TransportBind` calls.

[[src/udp.rs#UdpTransport#recv_burst]] is the sync recv. It reaps up to `max` ready datagrams into a caller-owned `FrameBatch`, each datagram landing into a freshly acquired pool slab via `socket2` `recv_from` on the raw fd. Hitting the socket directly bypasses tokio's cached reactor readiness, which a sync busy-poll recv must not depend on. It returns the count reaped, `Ok(0)` when the socket is drained, and `TransportError::PoolExhausted` when no landing slab is free while data is pending. [[src/udp.rs#UdpFrame]] owns the slab it landed in, so it is `Send + 'static` and returns the slab to the pool on `Drop`; `AsPayload` reports sequence and stream-id as zero since raw UDP has no sequencing.

`readable` is the optional `.await` readiness adapter for async callers; the sync `recv_burst` core never carries a waker.

PERF: recv is a per-datagram `recv_from` loop. A Linux `recvmmsg` fast path (one syscall per burst) plus `SO_RXQ_OVFL` kernel-drop readback is a measured follow-up gated on the recv benchmark, not a blind rewrite.

### Socket-option helpers

Extra helpers layered on top of `apply_socket_opts` for the perf-tuning knobs.

[[src/udp.rs#apply_busy_poll]] is cfg-gated: Linux calls `libc::setsockopt(SOL_SOCKET, SO_BUSY_POLL, us)` directly, other platforms log a `tracing::warn!` when the field is set. Failed setsockopt does not fail bind; it warns and continues so the socket still binds under restricted sysctls.

[[src/udp.rs#apply_rxq_ovfl]] enables `SO_RXQ_OVFL` on Linux when `RecvBufConfig::so_rxq_ovfl` is set, so the kernel is ready to attach the ancillary drop counter once the recvmmsg ancillary read lands. Non-Linux warns and continues.

[[src/udp.rs#apply_timestamping]] currently only warns when `RecvBufConfig::so_timestamping` is `KernelSw` or `HardwareRx`; the real recvmsg ancillary-data parse lands alongside the `recvmmsg` batching path so both share one recv-side flow.

Kernel-buffer sizing (`SO_RCVBUF`, `SO_SNDBUF`) emits a `tracing::warn!` when the kernel grants less than requested. Operators tune `sysctl net.core.rmem_max` / `wmem_max` to lift the ceiling.

## TCP path

Wraps `tokio::net::TcpStream` with `SO_RCVBUF` / `SO_SNDBUF` applied via `socket2::SockRef` on the connected stream. Recv lands one read directly into caller memory.

[[src/tcp.rs#TcpTransport]] opens a `TcpStream` to `BindConfig::addr` (interpreted as the remote peer for a client connect), then calls [[src/tcp.rs#apply_tcp_socket_opts]] to install the requested `SO_RCVBUF` and `SO_SNDBUF` sizes. [[src/tcp.rs#TcpTransport#recv_into]] reads once via `socket2` `recv` into the caller's uninitialised buffer (a decode buffer's spare capacity), the single copy in the stream path; `Ok(0)` means nothing was ready and a zero-byte read surfaces as `UnexpectedEof` so the caller can react to a graceful peer close. It carries a `SharedVecPool` only to satisfy `PoolAccess` uniformly; the stream path never draws slabs.

## Receiver stats

Atomic counters shared between recv workers and observability consumers via `Arc<ReceiverStats>`.

[[src/stats.rs#ReceiverStats]] tracks `packets_recv` and `bytes_recv`; `recv_burst` calls `record_packet(len)` per datagram. [[src/stats.rs#ReceiverStatsSnapshot]] is the plain-struct read-only copy returned by `ReceiverStats::snapshot`; observability code polls it instead of loading the atomics one by one.

## TokioTransport

Public enum that unifies UDP and TCP under the `transport_core` recv seam.

[[src/lib.rs#TokioTransport]] is the enum consumers depend on. The `Udp` variant is the `DatagramSource` (`recv_burst`); the `Tcp` variant is the `StreamSource` (`recv_into`). `TransportCore` (name + async `send`), `AsyncReady` (`ready` via the inner socket's `readable`), `PoolAccess` (the variant's `SharedVecPool`), and `TransportBind` (`bind_udp` / `connect_tcp`) are implemented across both variants; calling the wrong recv shape for a variant returns `TransportError::Unsupported`.

`impl transport_core::UdpTransport` adds multicast group join (`join_multicast`, dispatching IPv4 vs IPv6 to the inner socket's `join_multicast_v4`/`v6`) plus unconnected `send_to`. The `Tcp` variant rejects both with `TransportError::Unsupported`, so protocol crates that need multicast (MoldUDP) bound `T: UdpTransport` and get a compile error against a TCP-only backend.
