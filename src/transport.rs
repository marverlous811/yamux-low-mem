use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{AsyncRead, AsyncWrite, Sink, Stream};

use crate::chunk::{ChainedChunkBufferReader, ChainedChunkBufferWriter, ChunkBufferWriter};
use crate::frame::{Frame, FrameReader, FrameWriter};

/// Adapts an `AsyncRead`/`AsyncWrite` stream into Yamux frames using chunked buffers.
///
/// The transport encodes frames via [`FrameWriter`] into `ChunkOwned` blocks and writes
/// them to the underlying stream. Incoming bytes are parsed incrementally with
/// [`FrameReader`], yielding metadata and data chunks as they arrive.
pub struct YamuxTransport<T: AsyncRead + AsyncWrite> {
    stream: T,
    max_writer_buffer: usize,
    writer: FrameWriter<ChainedChunkBufferWriter>,
    reader: FrameReader<ChainedChunkBufferReader>,
}

impl<T: AsyncRead + AsyncWrite + Unpin> YamuxTransport<T> {
    /// Wraps an async stream with Yamux framing.
    pub fn new(stream: T, max_writer_buffer: usize) -> Self {
        Self {
            max_writer_buffer,
            stream,
            writer: FrameWriter::new(ChainedChunkBufferWriter::new()),
            reader: FrameReader::new(ChainedChunkBufferReader::new()),
        }
    }

    fn poll_write(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        // pop front as long as we can write
        while let Some(front) = self.writer.buffer_mut().front_slice() {
            match Pin::new(&mut self.stream).poll_write(cx, front)? {
                Poll::Ready(written) => {
                    self.writer.buffer_mut().consume_front(written);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
        Poll::Ready(Ok(()))
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> Sink<Frame> for YamuxTransport<T> {
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();

        match this.poll_write(cx)? {
            Poll::Ready(_) => Poll::Ready(Ok(())),
            Poll::Pending => {
                // if we have buffer more than max_writer_buffer, we need to wait for the next poll
                if this.writer.buffer_mut().len() >= this.max_writer_buffer {
                    return Poll::Pending;
                } else {
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }

    fn start_send(self: Pin<&mut Self>, item: Frame) -> Result<(), Self::Error> {
        let this = self.get_mut();
        item.write(this.writer.buffer_mut());
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();

        this.poll_write(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();

        match this.poll_write(cx)? {
            Poll::Ready(_) => Pin::new(&mut this.stream).poll_close(cx),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> Stream for YamuxTransport<T> {
    type Item = Result<Frame, io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        let mut buf = [0; 4096];
        while let Poll::Ready(event) = Pin::new(&mut this.stream).poll_read(cx, &mut buf) {
            match event {
                Ok(len) => {
                    this.reader.push_back(buf[..len].to_vec().into());
                    match this.reader.next_frame() {
                        Ok(Some(frame)) => return Poll::Ready(Some(Ok(frame))),
                        Ok(None) => continue,
                        Err(e) => return Poll::Ready(Some(Err(io::Error::new(io::ErrorKind::BrokenPipe, e)))),
                    }
                }
                Err(e) => return Poll::Ready(Some(Err(e))),
            }
        }
        Poll::Pending
    }
}
