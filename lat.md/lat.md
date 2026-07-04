# transport-tokio

Tokio-based backend for `transport_core`. Ships a `BufferPool` primitive plus the UDP path; TCP wiring lands in follow-up work.

## Pool

Bounded slab pool: fixed slot array plus a free list, cheap `Drop`-based reclaim, backpressure via `acquire` returning `None`.

[[crates/transport_tokio/src/pool.rs#SharedVecPool]] is the reference-counted handle backends share across tasks; it wraps [[crates/transport_tokio/src/pool.rs#VecPool]] which owns a fixed slot array of `UnsafeCell<Vec<u8>>` plus a `parking_lot::Mutex<Vec<u32>>` free list. `Sync` is asserted manually because slot access is gated by the free list, not the compiler.

[[crates/transport_tokio/src/pool.rs#VecSlab]] is the owned slab handle. It carries `Arc<VecPool>`, a slot index, and a length. `Drop` returns the index to the free list, so the pool self-heals on task cancellation. `AsRef<[u8]>` returns the filled slice up to `len`.

`SharedVecPool::acquire(len)` returns `None` when `len` exceeds `slab_size` or when the free list is empty, giving backends a natural backpressure signal.

## UDP path

[[crates/transport_tokio/src/udp.rs#UdpTransport]] wraps `tokio::net::UdpSocket`. `bind` builds a `socket2::Socket`, applies `SO_REUSEADDR`, `SO_REUSEPORT` (unix), and `SO_RCVBUF` via [[crates/transport_tokio/src/udp.rs#apply_socket_opts]], then hands the raw fd to tokio. A `tracing::warn!` fires when the kernel grants less `SO_RCVBUF` than requested.

`poll_recv` drains the socket into an internal scratch buffer and records the peer; `peek_frame` exposes the last datagram as [[crates/transport_tokio/src/udp.rs#UdpFrame]]. `UdpFrame` implements `AsPayload` with sequence and stream-id both zero: raw UDP has no sequencing, protocol crates layer that on top.

## TokioTransport

Public enum that unifies UDP (and, next, TCP) under a single `Transport` impl.

[[crates/transport_tokio/src/lib.rs#TokioTransport]] is the enum consumers depend on. `impl Transport` and `impl TransportBind` dispatch across the `Udp` variant today; the `Tcp` variant lands next. `connect_tcp` returns `TransportError::Unsupported` in the meantime, which is what the shared conformance suite expects.

[[crates/transport_tokio/src/lib.rs#TokioFrame]] and [[crates/transport_tokio/src/lib.rs#TokioEvent]] are the matching enums for the borrowed frame and per-poll event surface. `TokioEvent::Udp(SocketAddr)` carries the sender addr so protocol code can reply without a separate lookup.
