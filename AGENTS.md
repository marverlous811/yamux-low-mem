# Agent Guide

## Goal

- Build a Rust futures-style Yamux multiplexing library that is low-memory,
  simple to use, and stable.

## Approach

- Use 4090-byte blocks to store incoming/outgoing data.
- Two block kinds: `ChunkOwned` for writing/ingest and `ChunkView` for
  lightweight references/clones.
- Maintain a `ChainedChunkBuffer` to push chunks, do byte-wise ops, and
  parse/construct frames without large allocations. Emit incremental
  `YamuxFrame` values:
  - `Header { version: u8, f_type: FrameType, flags: FrameFlags, stream_id: u32, length: u32 }`
  - `DataFrame { stream_id: u32, seq: usize, remain: usize /* 0 on last frame */, data: ChunkView }`
- Implement protocol on top of the chunked logic and expose `YamuxSession` with
  `futures::io::AsyncRead`/`AsyncWrite` using Tokio for integration.

## Usage Sketch

```rust
let mut session = YamuxSession::server(tcp_stream);
let mut out_stream = session.open_stream();
let mut in_stream = session.next().await?;
```

## Rules

- Always run `cargo fmt` after finish editing a file.
- Always run `cargo test` after finish implementing a feature or fixing a bug
  and fix all tests.
- Always run `cargo clippy` after all tests is pass, and fix all warns.

## Work Plan

- [ ] Create `ChunkOwned` and `ChunkView` mocks with tests.
- [ ] Create `ChainedChunkBuffer` mock with tests.
- [ ] Build `YamuxDataStream` = `Stream<Frame>` + `Sink<Frame>` for async
      send/recv.
- [ ] Build `YamuxStream` implementing `AsyncRead`/`AsyncWrite` using chunk
      storage to stay memory-light; add tests.
- [ ] Build `YamuxSession` over `YamuxDataStream`, implement Yamux logic, and
      write edge-case tests.
- [ ] Add self-test with client/server.
- [ ] Add e2e test against `yamux` crate (0.13.8) for interoperability.
