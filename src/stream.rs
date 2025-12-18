//! Logical Yamux stream state and user-facing I/O handle.
//!
//! Yamux models each logical stream as a small state machine plus a byte-oriented handle.
//! This module splits that into two parts:
//! - [`YamuxStreamHead`] is driven by the session and converts between I/O intent and
//!   [`crate::frame::FrameStreamEvent`] values.
//! - [`YamuxStream`] is exposed to users and implements [`futures::AsyncRead`] and
//!   [`futures::AsyncWrite`].

use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{
    AsyncRead, AsyncWrite, SinkExt, Stream, StreamExt,
    channel::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender, channel, unbounded},
};
use thiserror::Error;

use crate::{
    chunk::{ChunkView, DEFAULT_CHUNK_CAPACITY},
    frame::FrameStreamEvent,
    packet::Flags,
};

// === Constants ===

/// Initial per-stream flow-control window in bytes.
pub const INITIAL_WINDOW: u32 = 256 * 1024;
pub const DEFAULT_WINDOW_UPDATE_THRESHOLD: u32 = INITIAL_WINDOW / 2;

// === Internal state ===

/// Tracks whether each half of a stream is still open.
struct State {
    local: bool,
    remote: bool,
}

/// Tracks per-stream send/receive flow-control windows.
struct Window {
    // store how many byte we can send
    send: usize,
    // store how many byte we already received
    recv: usize,
}

impl State {
    /// True when both the local and remote halves are closed.
    fn is_closed(&self) -> bool {
        !self.local && !self.remote
    }
}

// === Stream head (session-driven) ===

#[derive(Debug, Error, PartialEq, Eq)]
pub enum YamuxStreamHeadError {
    #[error("invalid frame type")]
    InvalidFrameType,
    #[error("invalid data chunk size")]
    InvalidDataChunkSize,
    #[error("internal channel error")]
    InternalChannelError,
}

/// Internal state machine that turns stream I/O into [`FrameStreamEvent`] values.
///
/// The session drives this type by:
/// - Feeding inbound events via [`YamuxStreamHead::on_input`].
/// - Polling it as a [`Stream`] to obtain outbound events.
pub struct YamuxStreamHead {
    tx: UnboundedSender<ChunkView>,
    rx: Receiver<ChunkView>,
    state: State,
    window: Window,
    outs: VecDeque<FrameStreamEvent>,
    recv_state: Option<usize>,
}

impl YamuxStreamHead {
    /// Creates a new outbound stream head (initiates with `SYN`).
    fn open(tx: UnboundedSender<ChunkView>, rx: Receiver<ChunkView>) -> Self {
        let init_pkt = FrameStreamEvent::WindowUpdate(Flags::syn(), INITIAL_WINDOW);
        Self {
            tx,
            rx,
            state: State { local: true, remote: false },
            window: Window {
                send: INITIAL_WINDOW as usize,
                recv: 0,
            },
            outs: VecDeque::from_iter([init_pkt]),
            recv_state: None,
        }
    }

    /// Creates a new inbound stream head (acknowledges with `ACK`).
    fn accept(tx: UnboundedSender<ChunkView>, rx: Receiver<ChunkView>) -> Self {
        let init_pkt = FrameStreamEvent::WindowUpdate(Flags::ack(), INITIAL_WINDOW);
        Self {
            tx,
            rx,
            state: State { local: true, remote: true },
            window: Window {
                send: INITIAL_WINDOW as usize,
                recv: 0,
            },
            outs: VecDeque::from_iter([init_pkt]),
            recv_state: None,
        }
    }

    /// Handles an incoming frame for this stream.
    ///
    /// # Errors
    ///
    /// Returns an I/O error with kind [`std::io::ErrorKind::InvalidData`] when the inbound
    /// sequence violates the expected Yamux data header/chunk ordering.
    pub fn on_input(&mut self, event: FrameStreamEvent) -> Result<(), YamuxStreamHeadError> {
        match event {
            FrameStreamEvent::Data(flags, size) => {
                if self.recv_state.is_some() {
                    return Err(YamuxStreamHeadError::InvalidFrameType);
                }

                self.handle_flag(flags);
                self.recv_state = Some(size as usize);

                Ok(())
            }
            FrameStreamEvent::DataChunk(chunk_view) => {
                if let Some(recv_state) = &mut self.recv_state {
                    if *recv_state < chunk_view.len() {
                        return Err(YamuxStreamHeadError::InvalidDataChunkSize);
                    }

                    *recv_state -= chunk_view.len();
                    let received_len = chunk_view.len();
                    if self.tx.unbounded_send(chunk_view).is_err() {
                        return Err(YamuxStreamHeadError::InternalChannelError);
                    }

                    if *recv_state == 0 {
                        self.recv_state = None;
                    }

                    self.mark_received_bytes(received_len);
                    Ok(())
                } else {
                    Err(YamuxStreamHeadError::InvalidFrameType)
                }
            }
            FrameStreamEvent::WindowUpdate(flags, delta) => {
                self.handle_flag(flags);
                self.window.send += delta as usize;

                Ok(())
            }
        }
    }

    /// Applies stream state transitions implied by control flags.
    fn handle_flag(&mut self, flag: Flags) {
        if flag.ack {
            log::info!("[YamuxStreamHead] received ack => remote opened");
            self.state.remote = true;
        }

        if flag.rst {
            log::info!("[YamuxStreamHead] received rst => both local and remote disabled");
            self.state.local = false;
            self.state.remote = false;
        }

        if flag.fin {
            log::warn!("[YamuxStreamHead] received fin => remote closed, send empty chunk");
            self.state.remote = false;
            let _ = self.tx.unbounded_send(vec![].into());
        }
    }

    /// Records received bytes and schedules window update frames when needed.
    fn mark_received_bytes(&mut self, received: usize) {
        self.window.recv += received;
        // auto send WindowUpdate when we received INITIAL_WINDOW
        if self.window.recv + DEFAULT_CHUNK_CAPACITY >= DEFAULT_WINDOW_UPDATE_THRESHOLD as usize {
            self.outs.push_back(FrameStreamEvent::WindowUpdate(Flags::ack(), self.window.recv as u32));
            self.window.recv = 0;
        }
    }
}

impl Stream for YamuxStreamHead {
    type Item = FrameStreamEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if let Some(out) = this.outs.pop_front() {
            return Poll::Ready(Some(out));
        }

        if this.state.is_closed() {
            // both local and remote are closed => stream is closed
            return Poll::Ready(None);
        }

        // We need to wait for remote to open (wait ack) before sending application data,
        // but we may still have control frames buffered in `outs` (e.g. initial SYN/ACK).
        if !this.state.remote {
            return Poll::Pending;
        }

        // We need to wait for more capacity to be available.
        // This implementation only sends in `DEFAULT_CHUNK_CAPACITY` sized increments.
        if this.window.send < DEFAULT_CHUNK_CAPACITY {
            return Poll::Pending;
        }

        if this.state.local {
            while let Poll::Ready(event) = this.rx.poll_next_unpin(cx) {
                match event {
                    Some(chunk) => {
                        this.window.send = this.window.send.saturating_sub(chunk.len());
                        this.outs.push_back(FrameStreamEvent::Data(Flags::empty(), chunk.len() as u32));
                        this.outs.push_back(FrameStreamEvent::DataChunk(chunk));
                    }
                    None => {
                        // Local side closed: send FIN once and keep the stream alive for inbound reads.
                        this.state.local = false;
                        this.outs.push_back(FrameStreamEvent::Data(Flags::fin(), 0));
                        break;
                    }
                }
            }
        }

        if let Some(out) = this.outs.pop_front() {
            Poll::Ready(Some(out))
        } else {
            Poll::Pending
        }
    }
}

// === User-facing stream handle ===

/// User-facing half of a logical Yamux stream.
#[derive(Debug)]
pub struct YamuxStream {
    tx: Sender<ChunkView>,
    rx: UnboundedReceiver<ChunkView>,
    recv_chunk: Option<(ChunkView, usize)>,
}

impl AsyncRead for YamuxStream {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();

        if this.recv_chunk.is_none() {
            if let Poll::Ready(event) = this.rx.poll_next_unpin(cx) {
                if let Some(chunk) = event {
                    this.recv_chunk = Some((chunk, 0));
                } else {
                    return Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "internal channel closed")));
                }
            }
        }

        if let Some((chunk, offset)) = &mut this.recv_chunk {
            let read_len = (chunk.len() - *offset).min(buf.len());
            buf[..read_len].copy_from_slice(&chunk[*offset..*offset + read_len]);
            *offset += read_len;
            if *offset == chunk.len() {
                this.recv_chunk = None;
            }
            return Poll::Ready(Ok(read_len));
        }

        Poll::Pending
    }
}

impl AsyncWrite for YamuxStream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();

        if let Poll::Ready(event) = this.tx.poll_ready(cx) {
            if let Err(e) = event {
                return Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe, e)));
            }
            let send_len = buf.len().min(DEFAULT_CHUNK_CAPACITY);
            if let Err(e) = this.tx.start_send(buf[..send_len].to_vec().into()) {
                return Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe, e)));
            }
            return Poll::Ready(Ok(send_len));
        }

        Poll::Pending
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        this.tx.poll_flush_unpin(cx).map_err(|e| std::io::Error::new(std::io::ErrorKind::BrokenPipe, e))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        this.tx.poll_close_unpin(cx).map_err(|e| std::io::Error::new(std::io::ErrorKind::BrokenPipe, e))
    }
}

/// Creates a paired head/stream used by the session and user-facing API.
pub(crate) fn open_stream() -> (YamuxStreamHead, YamuxStream) {
    let (tx, rx) = channel(1);
    let (tx2, rx2) = unbounded();
    (YamuxStreamHead::open(tx2, rx), YamuxStream { tx, rx: rx2, recv_chunk: None })
}

/// Creates a paired head/stream used by the session and user-facing API.
pub(crate) fn accept_stream() -> (YamuxStreamHead, YamuxStream) {
    let (tx, rx) = channel(1);
    let (tx2, rx2) = unbounded();
    (YamuxStreamHead::accept(tx2, rx), YamuxStream { tx, rx: rx2, recv_chunk: None })
}

#[cfg(test)]
mod tests {
    //! Stream tests are written as small, deterministic state-machine checks.
    //!
    //! Design goals:
    //! - Do not run an async runtime.
    //! - Drive state via `poll_*` with a `noop_waker` and an explicit [`Context`].
    //! - Keep poll counts small and predictable: most assertions are satisfied by the
    //!   very next `poll_next_unpin` call, otherwise we expect `Poll::Pending`.
    //! - Prefer validating observable protocol events ([`FrameStreamEvent`]) over
    //!   asserting on internal fields.

    use std::{
        pin::Pin,
        task::{Context, Poll},
    };

    use futures::{AsyncWrite, StreamExt, task::noop_waker};

    use crate::{
        chunk::DEFAULT_CHUNK_CAPACITY,
        frame::FrameStreamEvent,
        packet::Flags,
        stream::{DEFAULT_WINDOW_UPDATE_THRESHOLD, INITIAL_WINDOW, YamuxStreamHeadError, accept_stream, open_stream},
    };

    #[test]
    /// `open_stream()` immediately emits its initial `WindowUpdate(SYN, INITIAL_WINDOW)`.
    ///
    /// After that, it must remain `Pending` for application-data emission until the
    /// remote side acknowledges the stream (simulated by an inbound `ACK`).
    fn open_stream_should_wait_remote_open() {
        let (mut head, _stream) = open_stream();
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        assert_eq!(head.poll_next_unpin(&mut cx), Poll::Ready(Some(FrameStreamEvent::WindowUpdate(Flags::syn(), INITIAL_WINDOW))));
        assert!(matches!(head.poll_next_unpin(&mut cx), Poll::Pending));
        assert_eq!(head.state.remote, false);
        assert_eq!(head.on_input(FrameStreamEvent::WindowUpdate(Flags::ack(), 0)), Ok(()));

        assert_eq!(head.poll_next_unpin(&mut cx), Poll::Pending);
        assert_eq!(head.state.remote, true);
    }

    #[test]
    /// `accept_stream()` immediately emits its initial `WindowUpdate(ACK, INITIAL_WINDOW)`.
    ///
    /// A single `poll_write` on the user-facing [`YamuxStream`] should enqueue exactly
    /// one DATA header followed by one DATA chunk in the head's output stream.
    fn accept_stream_should_able_to_send_data() {
        let (mut head, mut stream) = accept_stream();
        // remote already open when accept_stream
        assert_eq!(head.state.remote, true);

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        assert_eq!(head.poll_next_unpin(&mut cx), Poll::Ready(Some(FrameStreamEvent::WindowUpdate(Flags::ack(), INITIAL_WINDOW))));

        let payload = b"hello-stream";
        assert_eq!(Pin::new(&mut stream).poll_write(&mut cx, payload).map_err(|e| e.to_string()), Poll::Ready(Ok(payload.len())));

        assert_eq!(head.poll_next_unpin(&mut cx), Poll::Ready(Some(FrameStreamEvent::Data(Flags::empty(), payload.len() as u32))));
        assert_eq!(head.poll_next_unpin(&mut cx), Poll::Ready(Some(FrameStreamEvent::DataChunk(payload.to_vec().into()))));
    }

    #[test]
    /// Dropping the user-facing stream closes the local half.
    ///
    /// The head should emit exactly one `FIN` (represented as a DATA header with the
    /// `FIN` flag and `size = 0`), then stop producing further events until something
    /// else changes (e.g. inbound frames).
    fn haft_close_should_send_fin() {
        let (mut head, stream) = accept_stream();
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Drain initial ACK.
        assert_eq!(head.poll_next_unpin(&mut cx), Poll::Ready(Some(FrameStreamEvent::WindowUpdate(Flags::ack(), INITIAL_WINDOW))));

        drop(stream);

        assert_eq!(head.poll_next_unpin(&mut cx), Poll::Ready(Some(FrameStreamEvent::Data(Flags::fin(), 0))));

        // FIN is only emitted once.
        assert_eq!(head.poll_next_unpin(&mut cx), Poll::Pending);
    }

    #[test]
    /// A DATA header declares a byte count that must be matched by subsequent DATA chunks.
    ///
    /// This test sends a header with `size = 1` but follows with a chunk of length 2,
    /// which must be rejected with `InvalidData`.
    fn wrong_data_chunk_size_should_return_error() {
        let (mut head, _stream) = accept_stream();

        assert_eq!(head.on_input(FrameStreamEvent::Data(Flags::empty(), 1)), Ok(()));

        assert_eq!(head.on_input(FrameStreamEvent::DataChunk(vec![0u8, 1u8].into())), Err(YamuxStreamHeadError::InvalidDataChunkSize));
    }

    #[test]
    /// Receiving a DATA chunk without first receiving a DATA header is invalid.
    fn wrong_data_chunk_state_should_return_error() {
        let (mut head, _stream) = accept_stream();

        assert_eq!(head.on_input(FrameStreamEvent::DataChunk(vec![0u8].into())), Err(YamuxStreamHeadError::InvalidFrameType));
    }

    #[test]
    /// Outbound sending is gated by the head's `send` window.
    ///
    /// When the available send window drops below one chunk, the head must return
    /// `Poll::Pending` and not emit partial frames.
    fn should_pending_after_exceed_window() {
        let (mut head, mut stream) = accept_stream();
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Drain initial ACK.
        assert_eq!(head.poll_next_unpin(&mut cx), Poll::Ready(Some(FrameStreamEvent::WindowUpdate(Flags::ack(), INITIAL_WINDOW))));

        head.window.send = DEFAULT_CHUNK_CAPACITY - 1;

        let payload = vec![7u8; DEFAULT_CHUNK_CAPACITY];
        assert_eq!(Pin::new(&mut stream).poll_write(&mut cx, &payload).map_err(|e| e.to_string()), Poll::Ready(Ok(DEFAULT_CHUNK_CAPACITY)));

        // head is pending because of window is limited
        assert_eq!(head.poll_next_unpin(&mut cx), Poll::Pending);
    }

    #[test]
    /// Inbound data consumption should trigger window updates once enough bytes are received.
    ///
    /// This test preloads the internal receive counter near the threshold, then delivers
    /// one full chunk to cross it, expecting the next polled output to be `WindowUpdate`.
    fn should_send_window_update_after_received_large_data() {
        let (mut head, _stream) = accept_stream();
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Drain initial ACK.
        assert_eq!(head.poll_next_unpin(&mut cx), Poll::Ready(Some(FrameStreamEvent::WindowUpdate(Flags::ack(), INITIAL_WINDOW))));

        head.window.recv = DEFAULT_WINDOW_UPDATE_THRESHOLD as usize - DEFAULT_CHUNK_CAPACITY;

        assert_eq!(head.on_input(FrameStreamEvent::Data(Flags::empty(), DEFAULT_CHUNK_CAPACITY as u32)), Ok(()));
        assert_eq!(head.on_input(FrameStreamEvent::DataChunk(vec![0u8; DEFAULT_CHUNK_CAPACITY].into())), Ok(()));

        assert_eq!(
            head.poll_next_unpin(&mut cx),
            Poll::Ready(Some(FrameStreamEvent::WindowUpdate(Flags::ack(), DEFAULT_WINDOW_UPDATE_THRESHOLD)))
        );
    }

    #[test]
    /// A `WindowUpdate` increases the send window and should unblock pending outbound data.
    ///
    /// We first force the head into a "not enough window" state (expecting `Pending`),
    /// then simulate an inbound `WindowUpdate(delta = 1)` that makes exactly one chunk
    /// sendable again.
    fn should_able_to_send_after_received_window_update() {
        let (mut head, mut stream) = accept_stream();
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Drain initial ACK.
        assert_eq!(head.poll_next_unpin(&mut cx), Poll::Ready(Some(FrameStreamEvent::WindowUpdate(Flags::ack(), INITIAL_WINDOW))));

        head.window.send = DEFAULT_CHUNK_CAPACITY - 1;

        let payload = vec![9u8; DEFAULT_CHUNK_CAPACITY];
        assert_eq!(Pin::new(&mut stream).poll_write(&mut cx, &payload).map_err(|e| e.to_string()), Poll::Ready(Ok(DEFAULT_CHUNK_CAPACITY)));

        assert_eq!(head.poll_next_unpin(&mut cx), Poll::Pending);

        assert_eq!(head.on_input(FrameStreamEvent::WindowUpdate(Flags::empty(), 1)), Ok(()));

        assert_eq!(head.poll_next_unpin(&mut cx), Poll::Ready(Some(FrameStreamEvent::Data(Flags::empty(), DEFAULT_CHUNK_CAPACITY as u32))));

        assert_eq!(head.poll_next_unpin(&mut cx), Poll::Ready(Some(FrameStreamEvent::DataChunk(payload.into()))));
    }
}
