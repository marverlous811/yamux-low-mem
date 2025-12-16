Here is implement details and plans:

# Goal

Create a yamux multiplexing protocol library in Rust and futures style for:

- low memory consume
- simple to use
- stable (make it simple as much as possible)

# How to make it

For memory effecient, we use block is 4090 bytes for storing data (both incoming and outgoing). We have 2 kinds of block: ChunkOwned and ChunkView, were Owned is used for write (when reading form socket or encoding from outgoing queue), ChunkView is used for clone and reference to small part or data, which is very useful when parsing.

We create a ChainedChunkBuffer which can push more chunk, and we can do some byte wise operation in that, then we can easily parse or construct yamux frames from it. For avoiding malloc big memory for big data frame, we can pop data as incremental frames like:

```rust
enum YamuxFrame {
    Header {
        version: u8,
        f_type: FrameType,
        flags: FrameFlags,
        stream_id: u32,
        length: u32,
    },
    DataFrame {
        stream_id: u32,
        seq: usize,
        remain: usize, //remain = 0 for last data frame
        data: ChunkView
    }
}
```

Based on above chunked logic, we can implement Yamux protocol and provide futures::io::AsyncRead and futures::io::AsyncWrite with tokio for integrating with other system without headache.

```rust
let mut session = YamuxSession::server(tcp_stream);

// Create stream
let mut out_stream = session.open_stream();

// Receive stream as futures::Stream trait
let mut in_stream = session.next().await?;
```

# Plans

- [ ] Create ChunkOwned and ChunkView mock, write tests
- [ ] Create ChainedChunkBuffer mock, write tests
- [ ] Create YamuxDataStream = Stream<Frame> + Sink<Frame> for receiving and sending frames in async
- [ ] Create YamuxStream which is futures::io::AsyncRead + futures::io::AsyncWrite with internal is use ChunkOwned, ChunkView for storing incoming/outgoing data without big buffer, which can save alot of memory in case we have many concurrent streams, write tests for ensuring it works
- [ ] Create YamuxSession which take YamuxDataStream and implement yamux logic then create YamuxStream, write all tests include edge cases
- [ ] Write self-test with both server/client for make sure it works
- [ ] Write e2e test with https://docs.rs/yamux/0.13.8/yamux/ for ensuring works with other system
