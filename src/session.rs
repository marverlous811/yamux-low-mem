use std::{
    collections::{HashMap, VecDeque},
    io,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{AsyncRead, AsyncWrite, Stream};

use crate::{
    packet::StreamID,
    stream::{YamuxStream, YamuxStreamHead, build_stream},
    transport::YamuxTransport,
};

/// Multiplexes logical Yamux streams over an async transport.
pub struct YamuxSession<T: AsyncRead + AsyncWrite + Unpin> {
    transport: YamuxTransport<T>,
    streams: HashMap<StreamID, YamuxStreamHead>,
    next_stream_id: u32,
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
}

impl<T: AsyncRead + AsyncWrite + Unpin> Stream for YamuxSession<T> {
    type Item = YamuxStream;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}
