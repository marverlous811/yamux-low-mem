# yamux-low-mem

Low-allocation implementation of the Yamux (Yet Another MUltipleXer) protocol, built on
`futures` I/O traits.

This crate targets workloads with many concurrent logical streams where **per-stream buffering
must stay bounded**.

See `SPEC.md` for the protocol spec and `IMPLEMENT.md` for the high-level design of this
implementation.

## Status

This project is experimental/WIP. The public API may change.

## Design highlights

- **Chunked buffers**: bytes are stored in fixed-size chunks to avoid large contiguous
  allocations and to make slicing cheap.
- **Incremental frame streaming**: data frames are surfaced as a header event followed by one
  or more data-chunk events, avoiding “read the whole payload into a new buffer”.
- **Futures-first API**:
  - `YamuxSession` implements `futures::Stream<Item = YamuxStream>` for accepting inbound
    streams while driving protocol progress.
  - `YamuxStream` implements `futures::AsyncRead` + `futures::AsyncWrite`.

## Usage

Add the dependency:

```toml
[dependencies]
yamux-low-mem = "0.1.0"
futures = "0.3"

# If you use Tokio, add:
# tokio = { version = "1", features = ["rt-multi-thread", "net", "macros", "io-util"] }
# tokio-util = { version = "0.7", features = ["compat"] }
```

Create a session and drive it:

```rust
use futures::{AsyncReadExt, AsyncWriteExt, StreamExt};
use tokio_util::compat::TokioAsyncReadCompatExt;
use yamux_low_mem::YamuxSession;

const MAX_WRITE_BUFFER: usize = 64 * 1024;

# async fn demo() -> std::io::Result<()> {
let tcp = tokio::net::TcpStream::connect("127.0.0.1:3000").await?;
let mut session = YamuxSession::client(tcp.compat(), MAX_WRITE_BUFFER);

// Outbound stream.
let mut stream = session.open_stream();

// The session must be polled to make progress. A simple pattern is to run it in
// a background task and ignore inbound streams if you don't expect any.
tokio::spawn(async move { while let Some(_inbound) = session.next().await {} });

stream.write_all(b"hello").await?;
stream.flush().await?;

let mut buf = [0u8; 5];
stream.read_exact(&mut buf).await?;
# Ok(())
# }
```

To accept inbound streams, poll the session in your event loop (e.g. with `tokio::select!`)
and handle each yielded `YamuxStream`.

## Examples

- Reverse-proxy tunnel over Yamux:
  - `cargo run --example proxy_server`
  - `cargo run --example proxy_client`
- Bench/throughput helpers:
  - `cargo run --example bench_server`
  - `cargo run --example bench_client`
  - `cargo run --example bench_yamux_server` (reference `tokio-yamux`)
  - `cargo run --example bench_yamux_client` (reference `tokio-yamux`)

## Interoperability

There are interop tests against the reference `tokio-yamux` implementation in
`tests/interop_yamux.rs`.
