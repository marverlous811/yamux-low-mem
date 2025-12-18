# High-level design

This document describes the *architecture* of `yamux-low-mem` and the key design choices that
keep allocations small. It intentionally avoids going into code-level details.

For the on-the-wire protocol, see `SPEC.md`.

## Goals

- **Bounded memory per stream**: avoid large per-stream buffers; keep buffering in small,
  reusable chunks.
- **Futures-first API**: expose `futures::AsyncRead` / `futures::AsyncWrite` for stream I/O and
  `futures::Stream` for session acceptance.
- **Simple integration**: work well in async applications (including Tokio via compat).

## Non-goals (current scope)

- Being a feature-complete drop-in replacement for every existing Yamux implementation.
- Aggressive performance tuning beyond eliminating large allocations.
- A fully stable API (this crate is still experimental).

## Architecture (layers)

The implementation is organized into a few layers, each with a single job:

1. **Chunked buffers**
   - The fundamental storage unit is a fixed-capacity chunk.
   - Incoming bytes are appended to a queue of chunks; parsing consumes from the front.
   - Outgoing bytes are encoded into chunks and drained to the underlying transport.

2. **Frame codec**
   - Parses and encodes Yamux headers using the chunked buffer interfaces.
   - Data payloads are *streamed* as a sequence of chunk views rather than copied into a
     contiguous buffer.

3. **Transport**
   - Wraps an `AsyncRead + AsyncWrite` stream and exposes `Sink<Frame> + Stream<Item = Frame>`.
   - Enforces a configurable maximum buffered write size for backpressure.

4. **Session**
   - Owns the transport and the set of active stream state machines.
   - Polling the session drives:
     - inbound frame handling (including new-stream acceptance), and
     - outbound flushing (control frames + stream data).
   - The session itself implements `Stream<Item = YamuxStream>` and yields newly accepted
     streams to the application.

5. **Streams**
   - Each logical Yamux stream is split into:
     - a session-driven “head” state machine that translates between protocol frames and a
       byte stream, and
     - a user-facing `YamuxStream` that implements `AsyncRead`/`AsyncWrite`.

## Data path and memory bounds

### Inbound (remote → local)

- The transport reads bytes from the underlying stream into fixed-size chunks.
- The frame codec parses headers without copying and then streams data payloads as chunk
  slices.
- Stream heads forward inbound data to the user-facing `YamuxStream` as chunk views.

Memory is bounded by:
- the amount of unread inbound data in the transport’s chunk queue, and
- per-stream queued data waiting for the application to read.

### Outbound (local → remote)

- The application writes to `YamuxStream`.
- Stream heads translate that into a sequence of Yamux DATA frames and enforce per-stream flow
  control.
- The session queues outbound frames and the transport encodes them into chunked buffers.

Memory is bounded by:
- per-stream buffered writes (chunk-sized), and
- a global maximum buffered writer size in the transport (backpressure).

## Flow control and stream lifecycle

- Each stream starts with an initial receive window (per the Yamux spec).
- Window update frames are emitted as inbound data is consumed so the peer can continue
  sending.
- Streams support half-close semantics via `FIN`, and can be hard-reset via `RST` when needed.

## Usage model (important)

`YamuxSession` is the “driver” of the protocol: it must be polled for the transport to read,
write, flush, and deliver inbound streams. Typical applications do one of:

- Run the session in a background task and handle inbound streams through a channel, or
- Poll the session in the main async loop (often using `select!`) and spawn a task per inbound
  `YamuxStream`.
