use std::{
    collections::{HashMap, VecDeque},
    pin::Pin,
    task::{Context, Poll},
};

use futures::{AsyncRead, AsyncWrite, SinkExt, Stream, StreamExt};

use crate::{
    frame::{Frame, FrameSessionEvent},
    packet::{FlagsBuilder, StreamID},
    stream::{YamuxStream, YamuxStreamHead, accept_stream, open_stream},
    transport::YamuxTransport,
};

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
    pub fn open_stream(&mut self) -> YamuxStream {
        let stream_id = StreamID(self.next_stream_id);
        self.next_stream_id = self.next_stream_id.wrapping_add(2);

        let (head, stream) = open_stream();
        self.streams.insert(stream_id, head);
        stream
    }

    /// Close connection
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
        while !this.out_queue.is_empty() && this.transport.poll_ready_unpin(cx).is_ready() {
            let out = this.out_queue.pop_front().expect("must have pop after check is_empty");
            if let Err(e) = this.transport.start_send_unpin(out) {
                log::error!("[YamuxSession] transport send frame error {e}");
                return Poll::Ready(None);
            }
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
                                this.out_queue
                                    .push_back(Frame::Session(FrameSessionEvent::Ping(FlagsBuilder::default().ack(true).build().expect("should build flags"), code)));
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
        }

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn should_able_to_open_stream_and_receive_stream() {
        //TODO: create 2 session, open stream from client, accept stream from server
    }

    #[test]
    fn should_able_to_close_and_wait_sent_out() {
        //TODO: create 2 session, close manual, wait sent out
    }
}
