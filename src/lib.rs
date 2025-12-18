//! Low-allocation Yamux (Yet Another MUltipleXer) implementation.
//!
//! This crate provides a small-footprint implementation of the Yamux multiplexing protocol,
//! designed for scenarios with many concurrent streams where per-stream buffering must stay
//! bounded.
//!
//! The implementation follows the protocol described in `SPEC.md` and the design notes in
//! `IMPLEMENT.md`.
//!
//! # Design
//!
//! - Incoming and outgoing bytes are stored in fixed-size chunks (see [`chunk`]) to avoid
//!   allocating large contiguous buffers.
//! - Frames are parsed and emitted incrementally (see [`frame`]) so large data payloads can be
//!   streamed as a header followed by one or more data chunks.
//! - [`session::YamuxSession`] multiplexes [`stream::YamuxStream`] handles over an async
//!   transport.
//!
//! # Examples
//!
//! Open a client session and a stream:
//!
//! ```no_run
//! use futures::{AsyncReadExt, AsyncWriteExt, StreamExt};
//! use tokio_util::compat::TokioAsyncReadCompatExt;
//! use yamux_low_mem::session::YamuxSession;
//!
//! # async fn demo() -> std::io::Result<()> {
//! let tcp = tokio::net::TcpStream::connect("127.0.0.1:8080").await?;
//! let mut session = YamuxSession::client(tcp.compat(), 8 * 1024);
//! let mut stream = session.open_stream();
//! stream.write_all(b"hello").await?;
//! let mut buf = [0u8; 5];
//! stream.read_exact(&mut buf).await?;
//! # Ok(())
//! # }
//! ```
pub mod chunk;
pub mod frame;
pub mod packet;
pub mod session;
pub mod stream;
pub mod transport;
