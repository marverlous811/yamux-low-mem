use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{AsyncRead, AsyncWrite, Sink, Stream};

use crate::chunk::{ChainedChunkBufferReader, ChainedChunkBufferWriter, ChunkView};
use crate::frame::{Frame, FrameReader, FrameWriter};

/// Adapts an `AsyncRead`/`AsyncWrite` stream into Yamux frames using chunked buffers.
///
/// The transport encodes frames via [`FrameWriter`] into `ChunkOwned` blocks and writes
/// them to the underlying stream. Incoming bytes are parsed incrementally with
/// [`FrameReader`], yielding metadata and data chunks as they arrive.
pub struct YamuxTransport<T: AsyncRead + AsyncWrite> {
    stream: T,
    writer: FrameWriter<ChainedChunkBufferWriter>,
    reader: FrameReader<ChainedChunkBufferReader>,
}

impl<T: AsyncRead + AsyncWrite> YamuxTransport<T> {
    /// Wraps an async stream with Yamux framing.
    pub fn new(stream: T) -> Self {
        Self {
            stream,
            writer: FrameWriter::new(ChainedChunkBufferWriter::new()),
            reader: FrameReader::new(ChainedChunkBufferReader::new()),
        }
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> Sink<Frame> for YamuxTransport<T> {
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        todo!()
    }

    fn start_send(self: Pin<&mut Self>, _item: Frame) -> Result<(), Self::Error> {
        todo!()
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        todo!()
    }

    fn poll_close(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        todo!()
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> Stream for YamuxTransport<T> {
    type Item = Result<Frame, io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}
