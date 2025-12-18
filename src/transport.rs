use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{AsyncRead, AsyncWrite, Sink, Stream};
use thiserror::Error;

use crate::chunk::{ChainedChunkBufferReader, ChainedChunkBufferWriter, ChunkBufferSource, ChunkBufferWriter};
use crate::frame::{Frame, FrameReader, FrameWriter};
use crate::packet::ParserError;

// === Transport ===

#[derive(Debug, Error, PartialEq, Eq)]
pub enum YamuxTransportError {
    #[error("io error {0}")]
    Io(String),
    #[error("parser error {0}")]
    ParserError(#[from] ParserError),
}

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

    /// Flushes buffered encoded bytes to the underlying stream.
    ///
    /// This method drains the internal [`ChainedChunkBufferWriter`] by repeatedly polling
    /// the wrapped I/O object. It returns [`Poll::Pending`] as soon as the underlying
    /// stream would block.
    fn poll_write(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), YamuxTransportError>> {
        // pop front as long as we can write
        while let Some(front) = self.writer.buffer_mut().front_slice() {
            match Pin::new(&mut self.stream).poll_write(cx, front).map_err(|e| YamuxTransportError::Io(e.to_string()))? {
                Poll::Ready(written) => {
                    log::debug!("[YamuxTransport] written {written} bytes");
                    self.writer.buffer_mut().consume_front(written);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
        Poll::Ready(Ok(()))
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> Sink<Frame> for YamuxTransport<T> {
    type Error = YamuxTransportError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();

        match this.poll_write(cx)? {
            Poll::Ready(_) => Poll::Ready(Ok(())),
            Poll::Pending => {
                // if we have buffer more than max_writer_buffer, we need to wait
                if this.writer.buffer_mut().filled_len() >= this.max_writer_buffer {
                    return Poll::Pending;
                } else {
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }

    fn start_send(self: Pin<&mut Self>, item: Frame) -> Result<(), Self::Error> {
        let this = self.get_mut();
        log::debug!("[YamuxTransport] send frame {item}");
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
            Poll::Ready(_) => Pin::new(&mut this.stream).poll_close(cx).map_err(|e| YamuxTransportError::Io(e.to_string())),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> Stream for YamuxTransport<T> {
    type Item = Result<Frame, YamuxTransportError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        match this.reader.next_frame() {
            Ok(Some(frame)) => {
                log::debug!("[YamuxTransport] got frame {frame}");
                return Poll::Ready(Some(Ok(frame)));
            }
            Ok(None) => {}
            Err(e) => return Poll::Ready(Some(Err(YamuxTransportError::ParserError(e)))),
        }

        let mut buf = [0; 4096];
        while let Poll::Ready(event) = Pin::new(&mut this.stream).poll_read(cx, &mut buf) {
            match event {
                Ok(len) => {
                    if len == 0 {
                        log::info!("[YamuxTransport] read 0 bytes => close");
                        return Poll::Ready(None);
                    }
                    log::debug!("[YamuxTransport] read {len} bytes");
                    this.reader.push_back(buf[..len].to_vec().into());
                    match this.reader.next_frame() {
                        Ok(Some(frame)) => {
                            log::debug!("[YamuxTransport] got frame {frame}");
                            return Poll::Ready(Some(Ok(frame)));
                        }
                        Ok(None) => continue,
                        Err(e) => return Poll::Ready(Some(Err(YamuxTransportError::ParserError(e)))),
                    }
                }
                Err(e) => {
                    log::error!("[YamuxTransport] stream error {e}");
                    return Poll::Ready(Some(Err(YamuxTransportError::Io(e.to_string()))));
                }
            }
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    //! Transport tests focus on polling semantics and frame round-tripping.
    //!
    //! We intentionally avoid async/await: the transport itself is a `Sink<Frame> + Stream<Item = Frame>`,
    //! so we drive it via `poll_*` with a `noop_waker` to keep the number of polls small and predictable.

    use std::task::{Context, Poll};

    use futures::{SinkExt, StreamExt, task::noop_waker};
    use tokio_util::compat::TokioAsyncReadCompatExt;

    use crate::{
        chunk::ChunkView,
        frame::{Frame, FrameSessionEvent, FrameStreamEvent},
        packet::{Flags, StreamID},
        transport::YamuxTransport,
    };

    #[test]
    /// Writes a single frame into one transport and reads it back from the peer transport.
    ///
    /// This test is deliberately "one hop":
    /// - `poll_ready` must be `Ready` before `start_send`.
    /// - `poll_flush` must be `Ready` to ensure bytes are pushed into the underlying stream.
    /// - The peer must yield exactly one frame on the next `poll_next`.
    fn pipe_2_streams() {
        let (left, right) = tokio::io::duplex(64 * 1024);
        let mut left = YamuxTransport::new(left.compat(), 64 * 1024);
        let mut right = YamuxTransport::new(right.compat(), 64 * 1024);

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        let ping_frame = Frame::Session(FrameSessionEvent::Ping(Flags::empty(), 12345));
        assert_eq!(left.poll_ready_unpin(&mut cx), Poll::Ready(Ok(())));

        // write to left
        assert_eq!(left.start_send_unpin(ping_frame.clone()), Ok(()));

        // flush left
        assert_eq!(left.poll_flush_unpin(&mut cx), Poll::Ready(Ok(())));

        // read from right
        assert_eq!(right.poll_next_unpin(&mut cx), Poll::Ready(Some(Ok(ping_frame))));
    }

    #[test]
    /// Writes a data frame into one transport and reads it back from the peer transport.
    ///
    /// This test is deliberately "one hop":
    /// - `poll_ready` must be `Ready` before `start_send`.
    /// - `poll_flush` must be `Ready` to ensure bytes are pushed into the underlying stream.
    /// - The peer must yield exactly one frame on the next `poll_next`.
    fn pipe_2_streams_data() {
        let (left, right) = tokio::io::duplex(64 * 1024);
        let mut left = YamuxTransport::new(left.compat(), 64 * 1024);
        let mut right = YamuxTransport::new(right.compat(), 64 * 1024);

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        let data_frame = Frame::Stream(StreamID(1), FrameStreamEvent::Data(Flags::empty(), 5));
        let data_chunk = Frame::Stream(StreamID(1), FrameStreamEvent::DataChunk(ChunkView::from(vec![0; 5])));
        assert_eq!(left.poll_ready_unpin(&mut cx), Poll::Ready(Ok(())));

        // write to left
        assert_eq!(left.start_send_unpin(data_frame.clone()), Ok(()));
        assert_eq!(left.start_send_unpin(data_chunk.clone()), Ok(()));

        // flush left
        assert_eq!(left.poll_flush_unpin(&mut cx), Poll::Ready(Ok(())));

        // read from right
        assert_eq!(right.poll_next_unpin(&mut cx), Poll::Ready(Some(Ok(data_frame))));
        assert_eq!(right.poll_next_unpin(&mut cx), Poll::Ready(Some(Ok(data_chunk))));
        assert_eq!(right.poll_next_unpin(&mut cx), Poll::Pending);
    }
}
