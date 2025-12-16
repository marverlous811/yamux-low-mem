use std::{
    collections::{HashMap, VecDeque},
    io,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{AsyncRead, AsyncWrite, Sink, Stream};

use crate::{
    frame::{Frame, FrameStreamEvent},
    packet::StreamID,
    stream::{YamuxStream, YamuxStreamHead, build_stream},
    transport::YamuxTransport,
};

/// Multiplexes logical Yamux streams over an async transport.
pub struct YamuxSession<T: AsyncRead + AsyncWrite> {
    transport: YamuxTransport<T>,
    streams: HashMap<StreamID, YamuxStreamHead>,
    incoming: VecDeque<YamuxStream>,
    next_stream_id: u32,
    closed: bool,
}

impl<T: AsyncRead + AsyncWrite> YamuxSession<T> {
    /// Creates a server-side session (even stream identifiers).
    pub fn server(stream: T) -> Self {
        Self::new(stream, true)
    }

    /// Creates a client-side session (odd stream identifiers).
    pub fn client(stream: T) -> Self {
        Self::new(stream, false)
    }

    fn new(stream: T, is_server: bool) -> Self {
        let start = if is_server {
            2
        } else {
            1
        };
        Self {
            transport: YamuxTransport::new(stream),
            streams: HashMap::new(),
            incoming: VecDeque::new(),
            next_stream_id: start,
            closed: false,
        }
    }

    /// Opens a new outbound stream.
    pub fn open_stream(&mut self) -> YamuxStream {
        let stream_id = StreamID(self.next_stream_id);
        self.next_stream_id = self.next_stream_id.wrapping_add(2);

        let (head, stream) = build_stream();
        self.streams.insert(stream_id, head);
        stream
    }

    fn handle_frame(&mut self, frame: Frame) {
        match frame {
            Frame::Stream { stream_id, event } => {
                let is_syn = matches!(event, FrameStreamEvent::Data { flags, .. } if flags.syn);
                if is_syn && !self.streams.contains_key(&stream_id) {
                    let (mut head, stream) = build_stream();
                    head.on_input(event.clone());
                    self.streams.insert(stream_id, head);
                    self.incoming.push_back(stream);
                }
                if let Some(head) = self.streams.get_mut(&stream_id) {
                    head.on_input(event);
                }
            }
            Frame::Session(_) => {}
        }
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> YamuxSession<T> {
    fn poll_outgoing(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut frames = Vec::new();
        for (stream_id, head) in self.streams.iter_mut() {
            while let Poll::Ready(Some(event)) = Pin::new(&mut *head).poll_next(cx) {
                frames.push(Frame::Stream { stream_id: *stream_id, event });
            }
        }

        let mut transport = Pin::new(&mut self.transport);
        for frame in frames {
            if let Err(err) = transport.as_mut().start_send(frame) {
                return Poll::Ready(Err(err));
            }
        }

        match transport.as_mut().poll_flush(cx) {
            Poll::Ready(res) => Poll::Ready(res),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> Stream for YamuxSession<T> {
    type Item = YamuxStream;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.closed {
            return Poll::Ready(this.incoming.pop_front());
        }

        match this.poll_outgoing(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(_)) => {
                this.closed = true;
                return Poll::Ready(None);
            }
            Poll::Pending => {}
        }

        if let Some(stream) = this.incoming.pop_front() {
            return Poll::Ready(Some(stream));
        }

        loop {
            match Pin::new(&mut this.transport).poll_next(cx) {
                Poll::Ready(Some(Ok(frame))) => {
                    this.handle_frame(frame);
                    if let Some(stream) = this.incoming.pop_front() {
                        return Poll::Ready(Some(stream));
                    }
                }
                Poll::Ready(Some(Err(_))) => {
                    this.closed = true;
                    return Poll::Ready(None);
                }
                Poll::Ready(None) => {
                    this.closed = true;
                    return Poll::Ready(this.incoming.pop_front());
                }
                Poll::Pending => break,
            }
        }

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{Stream, future::poll_fn, io::AsyncWriteExt};
    use std::task::Poll;
    use tokio_util::compat::TokioAsyncReadCompatExt;

    #[tokio::test]
    async fn delivers_incoming_stream_and_data() {
        let (client_io, server_io) = tokio::io::duplex(8192);
        let mut client = YamuxSession::client(client_io.compat());
        let mut server = YamuxSession::server(server_io.compat());

        let mut client_stream = client.open_stream();
        client_stream.write_all(b"hi").await.unwrap();

        let mut server_stream = poll_fn(|cx| -> Poll<Option<YamuxStream>> {
            let _ = Stream::poll_next(Pin::new(&mut client), cx);
            match Stream::poll_next(Pin::new(&mut server), cx) {
                Poll::Ready(Some(stream)) => Poll::Ready(Some(stream)),
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            }
        })
        .await
        .expect("stream should arrive");

        let mut buf = [0u8; 2];
        poll_fn(|cx| -> Poll<io::Result<usize>> {
            let _ = Stream::poll_next(Pin::new(&mut client), cx);
            let _ = Stream::poll_next(Pin::new(&mut server), cx);
            Pin::new(&mut server_stream).poll_read(cx, &mut buf)
        })
        .await
        .unwrap();

        assert_eq!(&buf, b"hi");
    }
}
