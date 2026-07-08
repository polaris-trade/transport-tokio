# transport-tokio

Tokio-based backend for the Polaris networking stack. Implements the
`transport_core` recv seam over `tokio::net`: UDP as a `DatagramSource`, TCP as a
`StreamSource`, with an `AsyncReady` adapter for `.await`-driven callers and a
bounded slab pool (`SharedVecPool`) so recv lands zero-copy into reused buffers.

## What it provides

- `TokioTransport`: the backend enum consumers hold. The `Udp` variant is the
  `DatagramSource` (`recv_burst`); the `Tcp` variant is the `StreamSource`
  (`recv_into`). It also implements `TransportCore`, `AsyncReady`, `PoolAccess`,
  `TransportBind`, and the multicast `UdpTransport` extension.
- `SharedVecPool`: bounded slab pool with `Drop`-based reclaim and `acquire`
  backpressure. Recv writes straight into a pooled slab; the yielded `UdpFrame`
  owns that slab (`Send + 'static`) and returns it on `Drop`.
- `ReceiverStats`: atomic packet and byte counters shared with observability.

## Recv model

Recv is a sync busy-poll core. `recv_burst` and `recv_into` hit the socket
directly (via `socket2` on the raw fd), so they attempt the syscall regardless
of the runtime's cached readiness; a caller that prefers to `.await` between
reaps calls `AsyncReady::ready` first. UDP recv is a portable `recv_from` loop
today; a Linux `recvmmsg` syscall-batching fast path is a measured follow-up.

## Usage

### UDP datagrams

Bind, then reap owned frames in a burst. Each frame owns its pool slab and is
`Send + 'static`, so it can be handed to another thread:

```rust
use transport_core::{
    AffinityConfig, AsPayload, BatchConfig, BindConfig, DatagramSource, FrameBatch,
    RecvBufConfig, RingConfig, SendBufConfig, TransportBind,
};
use transport_tokio::{TokioTransport, UdpFrame};

let mut transport = TokioTransport::bind_udp(
    BindConfig::new("0.0.0.0:4242".parse().unwrap()),
    RecvBufConfig::default(),
    SendBufConfig::default(),
    RingConfig::default(),
    BatchConfig::default(),
    AffinityConfig::default(),
)
.await?;

let mut batch: FrameBatch<UdpFrame> = FrameBatch::with_capacity(64);
match transport.recv_burst(&mut batch, 64)? {
    0 => { /* nothing ready, spin again */ }
    _n => {
        for frame in batch.drain() {
            let _payload: &[u8] = frame.payload();
        }
    }
}
```

### TCP stream

`recv_into` lands one read directly into caller memory (a decode buffer's spare
capacity). That single copy is the only one in the stream path:

```rust
use std::mem::MaybeUninit;
use transport_core::StreamSource;

let mut landing = [MaybeUninit::<u8>::uninit(); 64 * 1024];
let n = transport.recv_into(&mut landing)?;
// exactly the first `n` bytes of `landing` are initialised
```

## Dependency

```toml
[dependencies]
transport_tokio = { git = "https://github.com/polaris-trade/transport-tokio", tag = "transport_tokio-vX.Y.Z" }
```

Pulls `transport_core` by git tag. Distributed by git tag (`publish = false`).

## Dev commands

```bash
cargo nextest run
cargo clippy --all-targets -- -D warnings
lat check
```

MSRV `1.96.1` (pinned in `rust-toolchain.toml`).

## Docs

Architecture and design rationale live in [`lat.md/lat.md`](lat.md/lat.md).

## License

Dual-licensed under either [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE), at your option.
