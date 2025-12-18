//! Session orchestration for Yamux multiplexing.
//!
//! A [`YamuxSession`] drives a [`crate::transport::YamuxTransport`] and maintains the set of
//! active stream state machines (see [`crate::stream::YamuxStreamHead`]).
//!
//! The session itself implements [`futures::Stream`]; polling it advances both inbound and
//! outbound traffic and yields newly accepted [`crate::stream::YamuxStream`] handles.

use std::{
    collections::{HashMap, VecDeque},
    pin::Pin,
    task::{Context, Poll},
};

use futures::{AsyncRead, AsyncWrite, SinkExt, Stream, StreamExt};

use crate::{
    frame::{Frame, FrameSessionEvent},
    packet::{Flags, StreamID},
    stream::{YamuxStream, YamuxStreamHead, accept_stream, open_stream},
    transport::YamuxTransport,
};

// === Session type ===

/// Multiplexes logical Yamux streams over an async transport.
pub struct YamuxSession<T: AsyncRead + AsyncWrite + Unpin> {
    transport: YamuxTransport<T>,
    streams: HashMap<StreamID, YamuxStreamHead>,
    next_stream_id: u32,
    out_queue: VecDeque<Frame>,
    manual_close: bool,
}

impl<T: AsyncRead + AsyncWrite + Unpin> YamuxSession<T> {
    /// Creates a server-side session (even stream identifiers).
    pub fn server(stream: T, max_write_buffer: usize) -> Self {
        Self::new(stream, true, max_write_buffer)
    }

    /// Creates a client-side session (odd stream identifiers).
    pub fn client(stream: T, max_write_buffer: usize) -> Self {
        Self::new(stream, false, max_write_buffer)
    }

    /// Creates a session and selects the next outbound stream identifier.
    fn new(stream: T, is_server: bool, max_write_buffer: usize) -> Self {
        let start = if is_server {
            2
        } else {
            1
        };
        Self {
            transport: YamuxTransport::new(stream, max_write_buffer),
            streams: HashMap::new(),
            next_stream_id: start,
            out_queue: VecDeque::new(),
            manual_close: false,
        }
    }

    /// Opens a new outbound stream.
    ///
    /// The returned [`YamuxStream`] becomes usable once the remote acknowledges the stream.
    pub fn open_stream(&mut self) -> YamuxStream {
        let stream_id = StreamID(self.next_stream_id);
        self.next_stream_id = self.next_stream_id.wrapping_add(2);

        let (head, stream) = open_stream();
        self.streams.insert(stream_id, head);
        stream
    }

    /// Starts a graceful session shutdown by queueing a GoAway frame.
    ///
    /// After calling this, polling the session continues flushing queued frames
    /// until the underlying transport closes.
    pub fn close(&mut self, code: u32) {
        self.out_queue.push_back(Frame::Session(FrameSessionEvent::GoAway(code)));
        self.manual_close = true;
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> Stream for YamuxSession<T> {
    type Item = YamuxStream;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // first try to send
        let mut sent = false;
        while !this.out_queue.is_empty() && this.transport.poll_ready_unpin(cx).is_ready() {
            let out = this.out_queue.pop_front().expect("must have pop after check is_empty");
            if let Err(e) = this.transport.start_send_unpin(out) {
                log::error!("[YamuxSession] transport send frame error {e}");
                return Poll::Ready(None);
            }
            sent = true;
        }

        // we need to wait all pkt (end with GoAway) is sent
        if this.manual_close {
            if this.transport.poll_close_unpin(cx).is_ready() {
                log::info!("[YamuxSession] success close with sent GoAway");
                return Poll::Ready(None);
            } else {
                return Poll::Pending;
            }
        }

        while let Poll::Ready(event) = this.transport.poll_next_unpin(cx) {
            match event {
                Some(Ok(frame)) => match frame {
                    Frame::Session(event) => match event {
                        FrameSessionEvent::Ping(flags, code) => {
                            if flags.syn {
                                this.out_queue.push_back(Frame::Session(FrameSessionEvent::Ping(Flags::ack(), code)));
                            } else if flags.ack {
                                log::info!("[YamuxSession] got pong");
                            }
                        }
                        FrameSessionEvent::GoAway(code) => {
                            log::info!("[YamuxSession] close with code {code}");
                            return Poll::Ready(None);
                        }
                    },
                    Frame::Stream(stream_id, event) => {
                        if let Some(head) = this.streams.get_mut(&stream_id) {
                            if let Err(e) = head.on_input(event) {
                                log::error!("[YamuxSession] stream {stream_id} error {e}");
                                //TODO how to handle this?
                            }
                        } else if let Some(flags) = event.flags() {
                            if flags.syn {
                                log::info!("[YamuxSession] on incoming stream: {stream_id}");
                                let (mut head, stream) = accept_stream();
                                if let Err(e) = head.on_input(event) {
                                    log::error!("[YamuxSession] stream {stream_id} error {e}");
                                    //TODO how to handle this?
                                }
                                this.streams.insert(stream_id, head);

                                return Poll::Ready(Some(stream));
                            } else {
                                log::warn!("[YamuxSession] incomming message of unknown stream {stream_id} without Flags SYN");
                            }
                        }
                    }
                },
                Some(Err(e)) => {
                    log::error!("[YamuxSession] transport error: {}", e);
                    return Poll::Ready(None);
                }
                None => {
                    log::warn!("[YamuxSession] transport closed");
                    return Poll::Ready(None);
                }
            }
        }

        let mut closed_stream = vec![];
        for (stream_id, head) in this.streams.iter_mut() {
            while let Poll::Ready(event) = head.poll_next_unpin(cx) {
                if let Some(event) = event {
                    this.out_queue.push_back(Frame::Stream(*stream_id, event));
                } else {
                    log::info!("[YamuxSession] stream {stream_id} closed => remove");
                    closed_stream.push(*stream_id);
                }
            }
        }
        for stream_id in closed_stream {
            this.streams.remove(&stream_id);
        }

        // second try to send
        while !this.out_queue.is_empty() && this.transport.poll_ready_unpin(cx).is_ready() {
            let out = this.out_queue.pop_front().expect("must have pop after check is_empty");
            if let Err(e) = this.transport.start_send_unpin(out) {
                log::error!("[YamuxSession] transport send frame error {e}");
                return Poll::Ready(None);
            }
            sent = true;
        }

        if sent && let Poll::Ready(Err(e)) = this.transport.poll_flush_unpin(cx) {
            log::error!("[YamuxSession] transport flush frame error {e}");
            return Poll::Ready(None);
        }

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    //! Session tests are deterministic `poll_*` state-machine checks, similar to the stream and
    //! transport modules.
    //!
    //! Goals:
    //! - Avoid async/await and scheduling-dependent behavior.
    //! - Drive `YamuxSession` via `poll_next_unpin` using a `noop_waker`.
    //! - Make backpressure explicit by wrapping I/O so `poll_write` returns `Pending` a known
    //!   number of times.
    //! - Assert on externally observable effects (stream acceptance, GoAway-driven shutdown).

    use std::task::{Context, Poll};

    use futures::StreamExt;
    use futures::task::noop_waker;
    use tokio_util::compat::TokioAsyncReadCompatExt;

    use crate::session::YamuxSession;

    #[test]
    fn should_able_to_open_stream_and_receive_stream() {
        let (left, right) = tokio::io::duplex(64 * 1024);
        let mut client = YamuxSession::client(left.compat(), 64 * 1024);
        let mut server = YamuxSession::server(right.compat(), 64 * 1024);

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Opening a stream is a local API call; the "wire-visible" SYN only appears once the
        // session is polled and flushes the head's initial WindowUpdate frame.
        let _client_stream = client.open_stream();
        assert!(matches!(client.poll_next_unpin(&mut cx), Poll::Pending));

        // Server should yield the accepted stream
        assert!(matches!(server.poll_next_unpin(&mut cx), Poll::Ready(Some(_))));
    }

    #[test]
    fn should_able_to_close_and_wait_sent_out() {
        let (left, right) = tokio::io::duplex(64 * 1024);

        let mut client = YamuxSession::client(left.compat(), 64 * 1024);
        let mut server = YamuxSession::server(right.compat(), 64 * 1024);

        client.close(0);

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        assert!(matches!(client.poll_next_unpin(&mut cx), Poll::Ready(None)));
        assert!(matches!(server.poll_next_unpin(&mut cx), Poll::Ready(None)));
    }
}
