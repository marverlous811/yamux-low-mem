//! Yamux frame parsing and encoding.
//!
//! Frames are read and written using [`ChunkBufferReader`] and
//! [`ChunkBufferWriter`] so that callers can operate directly on chunked
//! buffers without intermediate copies.

use derive_more::Display;

use crate::{
    chunk::{ChunkBufferReader, ChunkBufferSink, ChunkBufferSource, ChunkBufferWriter, ChunkView},
    packet::{Flags, FrameType, Header, ParserError, StreamID},
};

#[derive(Debug)]
struct PendingPayload {
    stream_id: StreamID,
    remain: usize,
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
        // We need to continue reading data chunks utils it finishes
        if let Some(pending) = self.pending.as_mut() {
            match self.buffer.next_chunk(pending.remain) {
                None => return Ok(None),
                Some(chunk) => {
                    pending.remain -= chunk.len();
                    let output = Frame::Stream(pending.stream_id, FrameStreamEvent::DataChunk(chunk));
                    if pending.remain == 0 {
                        self.pending = None;
                    }
                    return Ok(Some(output));
                }
            }
        }

        // Parse new frame from buffer
        match Frame::read(&mut self.buffer) {
            Ok(Some(frame)) => {
                if let Frame::Stream(stream_id, FrameStreamEvent::Data(_, size)) = frame {
                    // Start tracking this data frame for chunked delivery
                    self.pending = Some(PendingPayload { stream_id, remain: size as usize });
                }
                return Ok(Some(frame));
            }
            Ok(None) => return Ok(None),
            Err(e) => return Err(e),
        }
    }
}

pub enum FrameWriterError {
    InvalidStreamId,
    UnexpectedFrameType,
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
    pub fn write(&mut self, frame: Frame) -> Result<(), FrameWriterError> {
        if let Some(pending) = self.pending.as_mut() {
            // Handle pending data chunk
            if let Frame::Stream(stream_id, FrameStreamEvent::DataChunk(chunk)) = &frame {
                if *stream_id == pending.stream_id {
                    pending.remain -= chunk.len();
                    if pending.remain == 0 {
                        self.pending = None;
                    }
                } else {
                    // Unexpected stream ID for pending data
                    return Err(FrameWriterError::InvalidStreamId);
                }
            } else {
                // Non-data frame while waiting for data chunk
                // This is an error - we should only receive data chunks when we have a pending payload
                return Err(FrameWriterError::UnexpectedFrameType);
            }
        } else if let Frame::Stream(stream_id, FrameStreamEvent::Data(_, size)) = &frame {
            // Track pending data for chunked delivery
            self.pending = Some(PendingPayload {
                stream_id: *stream_id,
                remain: *size as usize,
            });
        }
        frame.write(&mut self.buffer);
        Ok(())
    }
}

/// Stream-directed events.
#[derive(Debug, Clone, PartialEq, Eq, Display)]
pub enum FrameStreamEvent {
    /// Announces an incoming data payload of `size` bytes.
    #[display("Data({_0}, {_1})")]
    Data(Flags, u32),
    /// Carries a slice of data for the current payload.
    #[display("DataChunk({_0})")]
    DataChunk(ChunkView),
    /// Updates the flow-control window by `delta`.
    #[display("WindowUpdate({_0}, {_1})")]
    WindowUpdate(Flags, u32),
}

/// Session-directed events.
#[derive(Debug, Clone, PartialEq, Eq, Display)]
pub enum FrameSessionEvent {
    /// Ping frame (opaque payload not yet modeled).
    Ping(Flags),
    /// GoAway frame (error payload not yet modeled).
    GoAway(Flags),
}

/// Yamux frame variants.
#[derive(Debug, Clone, PartialEq, Eq, Display)]
pub enum Frame {
    /// Session-level control event.
    #[display("Frame::Session({_0})")]
    Session(FrameSessionEvent),
    /// Stream-level event tagged with the destination stream.
    #[display("Frame::Stream({_0}, {_1})")]
    Stream(StreamID, FrameStreamEvent),
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
            FrameType::Data => Ok(Some(Frame::Stream(header.stream_id, FrameStreamEvent::Data(header.flags, header.length)))),
            FrameType::WindowUpdate => Ok(Some(Frame::Stream(header.stream_id, FrameStreamEvent::WindowUpdate(header.flags, header.length)))),
            FrameType::Ping => Ok(Some(Frame::Session(FrameSessionEvent::Ping(header.flags)))),
            FrameType::GoAway => Ok(Some(Frame::Session(FrameSessionEvent::GoAway(header.flags)))),
        }
    }

    /// Writes the frame into the provided buffer.
    ///
    /// This helper does not encode data payloads; prefer [`FrameWriter`] for
    /// data frames to ensure headers and chunks are emitted consistently.
    pub fn write(&self, buffer: &mut impl ChunkBufferWriter) {
        match self {
            Frame::Session(event) => match event {
                FrameSessionEvent::Ping(flags) => {
                    Header::new(FrameType::Ping, *flags, StreamID(0), 0).write(buffer);
                }
                FrameSessionEvent::GoAway(flags) => {
                    Header::new(FrameType::GoAway, *flags, StreamID(0), 0).write(buffer);
                }
            },
            Frame::Stream(stream_id, event) => match event {
                FrameStreamEvent::Data(flags, size) => {
                    Header::new(FrameType::Data, *flags, *stream_id, *size).write(buffer);
                }
                FrameStreamEvent::DataChunk(chunk) => {
                    buffer.write_chunk(chunk);
                }
                FrameStreamEvent::WindowUpdate(flags, delta) => {
                    Header::new(FrameType::WindowUpdate, *flags, *stream_id, *delta).write(buffer);
                }
            },
        }
    }
}
