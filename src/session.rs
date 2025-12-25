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
    time::Duration,
};

use futures::{AsyncRead, AsyncWrite, SinkExt, Stream, StreamExt};
use tokio::time::Interval;

use crate::{
    frame::{Frame, FrameSessionEvent},
    packet::{Flags, StreamID},
    stream::{YamuxStream, YamuxStreamHead, accept_stream, open_stream},
    transport::YamuxTransport,
};

mod rtt;
pub use rtt::KeepAliveConfig;

#[derive(Debug, Clone)]
pub struct YamuxSessionConfig {
    pub max_write_buffer: usize,
    pub keep_alive_config: Option<KeepAliveConfig>,
}

// === Session type ===

/// Multiplexes logical Yamux streams over an async transport.
pub struct YamuxSession<T: AsyncRead + AsyncWrite + Unpin> {
    transport: YamuxTransport<T>,
    streams: HashMap<StreamID, YamuxStreamHead>,
    next_stream_id: u32,
    out_queue: VecDeque<Frame>,
    manual_close: bool,
    rtt: rtt::Rtt,
    interval: Option<KeepAliveInterval>,
}

impl<T: AsyncRead + AsyncWrite + Unpin> YamuxSession<T> {
    /// Creates a server-side session (even stream identifiers).
    pub fn server(stream: T, cfg: YamuxSessionConfig) -> Self {
        Self::new(stream, true, cfg)
    }

    /// Creates a client-side session (odd stream identifiers).
    pub fn client(stream: T, cfg: YamuxSessionConfig) -> Self {
        Self::new(stream, false, cfg)
    }

    /// Creates a session and selects the next outbound stream identifier.
    fn new(stream: T, is_server: bool, cfg: YamuxSessionConfig) -> Self {
        let start = if is_server {
            2
        } else {
            1
        };

        let interval = cfg.keep_alive_config.as_ref().map(|c| KeepAliveInterval::new(c.interval));
        Self {
            transport: YamuxTransport::new(stream, cfg.max_write_buffer),
            streams: HashMap::new(),
            next_stream_id: start,
            out_queue: VecDeque::new(),
            manual_close: false,
            rtt: rtt::Rtt::new(cfg.keep_alive_config.unwrap_or_default()),
            interval,
        }
    }

    /// Opens a new outbound stream.
    ///
    /// The returned [`YamuxStream`] becomes usable once the remote acknowledges the stream.
    pub fn open_stream(&mut self) -> YamuxStream {
        let stream_id = StreamID(self.next_stream_id);
        self.next_stream_id = self.next_stream_id.wrapping_add(2);
        if self.next_stream_id == 0 {
            self.next_stream_id = 2;
        }

        let (head, stream) = open_stream(stream_id);
        self.streams.insert(stream_id, head);
        log::info!("[YamuxSession] opened stream {stream_id}, total streams: {}", self.streams.len());
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

    pub fn on_ping(&mut self, flags: Flags, code: u32) {
        if flags.ack {
            // pong received
            self.rtt.handle_pong(code);
        } else if flags.syn {
            // ping received, send pong
            log::info!("[YamuxSession] enqueue pong frame");
            self.out_queue.push_back(Frame::Session(FrameSessionEvent::Ping(Flags::ack(), code)));
        }
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> Stream for YamuxSession<T> {
    type Item = YamuxStream;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // check keep-alive
        let tick_event = if let Some(interval) = &mut this.interval {
            match Pin::new(interval).as_mut().poll_next(cx) {
                Poll::Pending => SessionTickEvent::Idle,
                Poll::Ready(Some(())) => {
                    if let Some(event) = this.rtt.next_ping() {
                        match event {
                            rtt::RttEvent::KeepAlive(nonce) => {
                                log::debug!("[YamuxSession] enqueue ping frame");
                                this.out_queue.push_back(Frame::Session(FrameSessionEvent::Ping(Flags::syn(), nonce)));
                                SessionTickEvent::NeedWake
                            }
                            rtt::RttEvent::Close => {
                                log::info!("[YamuxSession] keep-alive timeout, close session");
                                SessionTickEvent::Close
                            }
                        }
                    } else {
                        SessionTickEvent::NeedWake
                    }
                }
                Poll::Ready(None) => SessionTickEvent::Close,
            }
        } else {
            SessionTickEvent::Idle
        };

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
                            this.on_ping(flags, code);
                        }
                        FrameSessionEvent::GoAway(code) => {
                            log::debug!("[YamuxSession] close with code {code}");
                            return Poll::Ready(None);
                        }
                    },
                    Frame::Stream(stream_id, event) => {
                        if let Some(head) = this.streams.get_mut(&stream_id) {
                            log::debug!("[YamuxSession] got frame for stream {stream_id}");
                            if let Err(e) = head.on_input(event) {
                                log::error!("[YamuxSession] stream {stream_id} error {e}");
                                //TODO how to handle this?
                            }
                        } else if let Some(flags) = event.flags() {
                            if flags.syn {
                                log::info!("[YamuxSession] on incoming stream: {stream_id}");
                                let (mut head, stream) = accept_stream(stream_id);
                                if let Err(e) = head.on_input(event) {
                                    log::error!("[YamuxSession] stream {stream_id} error {e}");
                                    //TODO how to handle this?
                                }
                                this.streams.insert(stream_id, head);
                                log::info!("[YamuxSession] accepted stream {stream_id}, total streams: {}", this.streams.len());

                                return Poll::Ready(Some(stream));
                            } else {
                                log::warn!("[YamuxSession] incomming message of unknown stream {stream_id}, event {event}");
                            }
                        } else {
                            log::warn!("[YamuxSession] incomming message of unknown stream {stream_id}, event {event}");
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
                    break;
                }
            }
        }
        for stream_id in closed_stream {
            this.streams.remove(&stream_id);
            log::info!("[YamuxSession] removed stream {stream_id} => total streams: {}", this.streams.len());
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

        match tick_event {
            SessionTickEvent::Close => {
                this.close(0);
                Poll::Pending
            }
            SessionTickEvent::NeedWake => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            SessionTickEvent::Idle => Poll::Pending,
        }
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> Drop for YamuxSession<T> {
    fn drop(&mut self) {
        log::info!("[YamuxSession] drop");
    }
}

enum SessionTickEvent {
    Idle,
    NeedWake,
    Close,
}

struct KeepAliveInterval {
    interval: Interval,
}

impl KeepAliveInterval {
    fn new(duration: Duration) -> Self {
        Self { interval: tokio::time::interval(duration) }
    }
}

impl Stream for KeepAliveInterval {
    type Item = ();

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.interval.poll_tick(cx) {
            Poll::Ready(_) => Poll::Ready(Some(())),
            Poll::Pending => Poll::Pending,
        }
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

    use crate::session::{YamuxSession, YamuxSessionConfig};

    #[test]
    fn should_able_to_open_stream_and_receive_stream() {
        let (left, right) = tokio::io::duplex(64 * 1024);

        let cfg = YamuxSessionConfig {
            max_write_buffer: 64 * 1024,
            keep_alive_config: None,
        };
        let mut client = YamuxSession::client(left.compat(), cfg.clone());
        let mut server = YamuxSession::server(right.compat(), cfg);

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

        let cfg = YamuxSessionConfig {
            max_write_buffer: 64 * 1024,
            keep_alive_config: None,
        };

        let mut client = YamuxSession::client(left.compat(), cfg.clone());
        let mut server = YamuxSession::server(right.compat(), cfg);

        client.close(0);

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        assert!(matches!(client.poll_next_unpin(&mut cx), Poll::Ready(None)));
        assert!(matches!(server.poll_next_unpin(&mut cx), Poll::Ready(None)));
    }
}
