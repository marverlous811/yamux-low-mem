//! Yamux frame parsing and encoding.
//!
//! Frames are read and written using [`ChunkBufferReader`] and
//! [`ChunkBufferWriter`] so that callers can operate directly on chunked
//! buffers without intermediate copies.

use crate::{
    chunk::{ChunkBufferReader, ChunkBufferSink, ChunkBufferSource, ChunkBufferWriter, ChunkView},
    packet::{Flags, FrameType, Header, ParserError, StreamID},
};

#[derive(Debug)]
struct PendingPayload {
    stream_id: StreamID,
    remain: u32,
}

/// Stateful reader that yields metadata and payload chunks separately.
///
/// A data frame is surfaced as [`Frame::Stream`] with [`FrameStreamEvent::Data`]
/// followed by one or more [`FrameStreamEvent::DataChunk`] values until the
/// declared `size` is exhausted. Control frames are returned as session events.
pub struct FrameReader<R> {
    buffer: R,
    pending: Option<PendingPayload>,
}

impl<R: ChunkBufferReader + ChunkBufferSink> FrameReader<R> {
    /// Creates a new reader over the provided buffer.
    pub fn new(buffer: R) -> Self {
        Self { buffer, pending: None }
    }

    /// Pushes a new chunk to the front of the queue.
    pub fn push_back(&mut self, chunk: ChunkView) {
        self.buffer.push_back(chunk);
    }

    /// Parses the next frame or payload chunk if available.
    ///
    /// Returns `Ok(None)` when more data is required to produce the next item.
    pub fn next_frame(&mut self) -> Result<Option<Frame>, ParserError> {
        todo!()
    }
}

/// Stateful writer that owns a buffer and enforces header/data sequencing.
#[derive(Debug)]
pub struct FrameWriter<W> {
    pending: Option<PendingPayload>,
    buffer: W,
}

impl<W: ChunkBufferWriter + ChunkBufferSource> FrameWriter<W> {
    /// Creates a new writer backed by the provided buffer.
    pub fn new(buffer: W) -> Self {
        Self { pending: None, buffer }
    }

    /// Return buffer
    pub fn buffer_mut(&mut self) -> &mut W {
        &mut self.buffer
    }

    pub fn take(self) -> W {
        self.buffer
    }

    /// Pops the next chunk from the front of the queue.
    pub fn pop_front(&mut self) -> Option<ChunkView> {
        self.buffer.pop_front()
    }

    /// Writes the next frame or chunk into the owned buffer, enforcing payload sizes.
    pub fn write(&mut self, _frame: Frame) -> Result<(), ParserError> {
        todo!()
    }
}

/// Stream-directed events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameStreamEvent {
    /// Announces an incoming data payload of `size` bytes.
    Data { flags: Flags, size: u32 },
    /// Carries a slice of data for the current payload.
    DataChunk { remain: u32, chunk: ChunkView },
    /// Updates the flow-control window by `delta`.
    WindowUpdate { flags: Flags, delta: u32 },
}

/// Session-directed events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameSessionEvent {
    /// Ping frame (opaque payload not yet modeled).
    Ping { flags: Flags },
    /// GoAway frame (error payload not yet modeled).
    GoAway { flags: Flags },
}

/// Yamux frame variants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    /// Session-level control event.
    Session(FrameSessionEvent),
    /// Stream-level event tagged with the destination stream.
    Stream { stream_id: StreamID, event: FrameStreamEvent },
}

impl Frame {
    /// Attempts to parse a full frame.
    ///
    /// Returns `Ok(None)` when the buffer does not yet hold a complete header.
    /// The payload of a data frame remains in the buffer; use [`FrameReader`]
    /// to stream data chunks without allocation.
    pub fn read(buffer: &mut impl ChunkBufferReader) -> Result<Option<Self>, ParserError> {
        let header = match Header::read(buffer)? {
            Some(header) => header,
            None => return Ok(None),
        };

        match header.type_ {
            FrameType::Data => Ok(Some(Frame::Stream {
                stream_id: header.stream_id,
                event: FrameStreamEvent::Data {
                    flags: header.flags,
                    size: header.length,
                },
            })),
            FrameType::WindowUpdate => Ok(Some(Frame::Stream {
                stream_id: header.stream_id,
                event: FrameStreamEvent::WindowUpdate {
                    flags: header.flags,
                    delta: header.length,
                },
            })),
            FrameType::Ping => Ok(Some(Frame::Session(FrameSessionEvent::Ping { flags: header.flags }))),
            FrameType::GoAway => Ok(Some(Frame::Session(FrameSessionEvent::GoAway { flags: header.flags }))),
        }
    }

    /// Writes the frame into the provided buffer.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError::LengthOverflow`] if any payload length exceeds `u32`.
    ///
    /// This helper does not encode data payloads; prefer [`FrameWriter`] for
    /// data frames to ensure headers and chunks are emitted consistently.
    pub fn write(&self, buffer: &mut impl ChunkBufferWriter) -> Result<(), ParserError> {
        match self {
            Frame::Session(event) => match event {
                FrameSessionEvent::Ping { flags } => {
                    Header::new(FrameType::Ping, *flags, StreamID(0), 0).write(buffer);
                    Ok(())
                }
                FrameSessionEvent::GoAway { flags } => {
                    Header::new(FrameType::GoAway, *flags, StreamID(0), 0).write(buffer);
                    Ok(())
                }
            },
            Frame::Stream { stream_id, event } => match event {
                FrameStreamEvent::Data { flags, size } => {
                    Header::new(FrameType::Data, *flags, *stream_id, *size).write(buffer);
                    Ok(())
                }
                FrameStreamEvent::DataChunk { chunk, .. } => {
                    buffer.write_chunk(chunk);
                    Ok(())
                }
                FrameStreamEvent::WindowUpdate { flags, delta } => {
                    Header::new(FrameType::WindowUpdate, *flags, *stream_id, *delta).write(buffer);
                    Ok(())
                }
            },
        }
    }
}
