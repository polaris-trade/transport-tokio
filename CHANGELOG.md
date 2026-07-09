# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/).
## [0.3.2](https://github.com/polaris-trade/transport-tokio/compare/transport_tokio-v0.3.1...transport_tokio-v0.3.2) (2026-07-09)


### Build

* **deps:** bump criterion to 0.8.2 ([#16](https://github.com/polaris-trade/transport-tokio/issues/16)) ([1f81594](https://github.com/polaris-trade/transport-tokio/commit/1f81594703548378533f9471004f989c912c1d6a))

## [0.3.1](https://github.com/polaris-trade/transport-tokio/compare/transport_tokio-v0.3.0...transport_tokio-v0.3.1) (2026-07-09)


### Tests

* **bench:** add recv_burst throughput and allocation benchmark ([#14](https://github.com/polaris-trade/transport-tokio/issues/14)) ([acfb116](https://github.com/polaris-trade/transport-tokio/commit/acfb116abadcb266a5c4588808dccd64949180f2))

## [0.3.0](https://github.com/polaris-trade/transport-tokio/compare/transport_tokio-v0.2.1...transport_tokio-v0.3.0) (2026-07-08)


### ⚠ BREAKING CHANGES

* **recv:** Transport/TokioFrame/TokioEvent/TcpFrame and the RecvBatch recvmmsg helpers are gone; construct via TransportBind and consume frames through DatagramSource/StreamSource.

### Features

* **recv:** migrate tokio backend to the owned-frame recv seam ([#10](https://github.com/polaris-trade/transport-tokio/issues/10)) ([40bdb87](https://github.com/polaris-trade/transport-tokio/commit/40bdb8783f8fd2e106e0eb25791382a879c37c73))

## [0.2.1](https://github.com/polaris-trade/transport-tokio/compare/transport_tokio-v0.2.0...transport_tokio-v0.2.1) - 2026-07-07

### Refactor

- *(lib)* Restructure into single-crate layout ([#8](https://github.com/polaris-trade/transport-tokio/pull/8))


## [0.1.0](https://github.com/polaris-trade/transport-tokio/releases/tag/transport_tokio-v0.1.0) - 2026-07-04

### Features

- *(transport-tokio)* Add VecPool, UDP + TCP transports, and Linux recvmmsg batching ([#1](https://github.com/polaris-trade/transport-tokio/pull/1))


  First functional layer of the tokio backend for transport_core.

Pool:
- VecPool + SharedVecPool: bounded slab pool with UnsafeCell<Vec<u8>> slots gated by a parking_lot::Mutex<Vec<u32>> free list.
- VecSlab::drop returns the slot; SharedVecPool newtype satisfies the orphan rule for impl BufferPool.

UDP:
- UdpTransport wraps tokio::net::UdpSocket. apply_socket_opts installs SO_REUSEADDR, SO_REUSEPORT (unix), SO_RCVBUF, SO_SNDBUF, and SO_BUSY_POLL (Linux via libc::setsockopt).
- Kernel shortfalls and unsupported knobs emit tracing::warn! instead of failing bind.
- so_timestamping requests currently warn since the recvmsg ancillary-data path is not yet wired; lands with recvmmsg batching in a follow-up.
- Linux-only recv_batch_linux drains a burst via libc::recvmmsg, gated behind tokio's readable().await + try_io. Preallocated RecvBatch owns per-slot buffers, iovs, addrs, and cmsg storage so hot loops do not reallocate.

TCP:
- TcpTransport wraps tokio::net::TcpStream. connect opens the stream to BindConfig::addr (interpreted as the remote peer) and applies SO_RCVBUF / SO_SNDBUF via socket2::SockRef.
- poll_recv reads one chunk per poll; zero-byte reads surface as UnexpectedEof so callers can react to peer close.
- TCP is stream-oriented, so TcpFrame carries opaque bytes and the protocol crate handles framing.

Surface:
- TokioTransport enum dispatches Transport + TransportBind across the Udp and Tcp variants.
- TokioFrame mirrors the split; TokioEvent carries SocketAddr for UDP and byte count for TCP.

Stats:
- ReceiverStats holds atomic kernel_drops, packets_recv, bytes_recv counters shared via Arc; snapshot() returns a plain read-only copy for observability polling.
- apply_socket_opts installs SO_RXQ_OVFL on Linux when RecvBufConfig::so_rxq_ovfl is set; each recv carries the cumulative kernel-drop count in ancillary data.
- parse_scm_rxq_ovfl walks the cmsg list; the highest value seen in a batch advances kernel_drops monotonically via CAS.

Tests:
- Pool accounting under contention (drop reclaim, exhaustion, oversize rejection, 8-thread concurrent acquire).
- SO_RCVBUF loopback bind smoke.
- Loopback TCP echo through send / poll_event / next_frame.
- transport_core::testing::run_conformance_suite asserts BindUdp, ConnectTcp (via the suite's auto-spun peer), NameNonEmpty.
- recvmmsg.rs verifies burst batches >= 2 datagrams end-to-end.
- drops.rs (#[ignore]) floods rcvbuf and asserts non-zero kernel_drops.
