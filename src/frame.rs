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

// === Internal state ===

/// Tracks payload bytes remaining for a data frame currently being streamed.
#[derive(Debug)]
struct PendingPayload {
    stream_id: StreamID,
    remain: usize,
}

// === Reader ===

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

    /// Pushes a new chunk to the back of the queue.
    pub fn push_back(&mut self, chunk: ChunkView) {
        log::debug!("[FrameReader] push back chunk {} bytes", chunk.len());
        self.buffer.push_back(chunk);
    }

    /// Parses the next frame or payload chunk if available.
    ///
    /// Returns `Ok(None)` when more data is required to produce the next item.
    pub fn next_frame(&mut self) -> Result<Option<Frame>, ParserError> {
        // We need to continue reading data chunks utils it finishes
        if let Some(pending) = self.pending.as_mut() {
            log::debug!(
                "[FrameReader] pending data frame with stream id {} and remaining {} bytes, current buffer {}",
                pending.stream_id,
                pending.remain,
                self.buffer.len()
            );
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
                    log::info!("[FrameReader] got data frame with stream id {} and size {} bytes", stream_id, size);
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

// === Writer ===

/// Errors returned by [`FrameWriter::write`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameWriterError {
    /// The stream identifier for a [`FrameStreamEvent::DataChunk`] did not match
    /// the stream currently being written.
    InvalidStreamId,
    /// A data chunk exceeded the remaining declared payload size.
    ChunkTooLarge,
    /// A non-chunk frame was provided while a payload was still pending.
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

    /// Returns a mutable reference to the underlying buffer.
    pub fn buffer_mut(&mut self) -> &mut W {
        &mut self.buffer
    }

    /// Returns the inner buffer, discarding writer state.
    pub fn take(self) -> W {
        self.buffer
    }

    /// Pops the next chunk from the front of the queue.
    pub fn pop_front(&mut self) -> Option<ChunkView> {
        self.buffer.pop_front()
    }

    /// Writes the next frame or chunk into the owned buffer, enforcing payload sizes.
    ///
    /// # Errors
    ///
    /// Returns [`FrameWriterError::InvalidStreamId`] if a data chunk is written for a
    /// different stream than the most recent data header, or [`FrameWriterError::UnexpectedFrameType`]
    /// if a non-chunk frame is written while a payload is pending.
    pub fn write(&mut self, frame: Frame) -> Result<(), FrameWriterError> {
        if let Some(pending) = self.pending.as_mut() {
            // Handle pending data chunk
            if let Frame::Stream(stream_id, FrameStreamEvent::DataChunk(chunk)) = &frame {
                if *stream_id == pending.stream_id {
                    if chunk.len() > pending.remain {
                        return Err(FrameWriterError::ChunkTooLarge);
                    }
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
        } else if matches!(frame, Frame::Stream(_, FrameStreamEvent::DataChunk(_))) {
            // Data chunk without a preceding header is invalid.
            return Err(FrameWriterError::UnexpectedFrameType);
        }
        frame.write(&mut self.buffer);
        Ok(())
    }
}

// === Events ===

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

impl FrameStreamEvent {
    /// Returns flags for events that carry them.
    pub fn flags(&self) -> Option<Flags> {
        match self {
            Self::Data(flags, _) | Self::WindowUpdate(flags, _) => Some(*flags),
            _ => None,
        }
    }
}

/// Session-directed events.
#[derive(Debug, Clone, PartialEq, Eq, Display)]
pub enum FrameSessionEvent {
    /// Ping frame (opaque payload not yet modeled).
    #[display("Ping({_0}, {_1})")]
    Ping(Flags, u32),
    /// GoAway frame (error payload not yet modeled).
    GoAway(u32),
}

// === Frame ===

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
            FrameType::Ping => Ok(Some(Frame::Session(FrameSessionEvent::Ping(header.flags, header.length)))),
            FrameType::GoAway => Ok(Some(Frame::Session(FrameSessionEvent::GoAway(header.length)))),
        }
    }

    /// Writes the frame into the provided buffer.
    ///
    /// This helper does not encode data payloads; prefer [`FrameWriter`] for
    /// data frames to ensure headers and chunks are emitted consistently.
    pub fn write(&self, buffer: &mut impl ChunkBufferWriter) {
        match self {
            Frame::Session(event) => match event {
                FrameSessionEvent::Ping(flags, code) => {
                    Header::new(FrameType::Ping, *flags, StreamID(0), *code).write(buffer);
                }
                FrameSessionEvent::GoAway(code) => {
                    Header::new(FrameType::GoAway, Flags::empty(), StreamID(0), *code).write(buffer);
                }
            },
            Frame::Stream(stream_id, event) => match event {
                FrameStreamEvent::Data(flags, size) => {
                    Header::new(FrameType::Data, *flags, *stream_id, *size).write(buffer);
                }
                FrameStreamEvent::DataChunk(chunk) => {
                    if !chunk.is_empty() {
                        buffer.write_slice(&chunk);
                    }
                }
                FrameStreamEvent::WindowUpdate(flags, delta) => {
                    Header::new(FrameType::WindowUpdate, *flags, *stream_id, *delta).write(buffer);
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::{ChainedChunkBufferReader, ChainedChunkBufferWriter};
    use crate::packet::StreamID;

    #[test]
    fn write_frame_then_parse() {
        let frame = Frame::Session(FrameSessionEvent::Ping(Flags::syn(), 123));

        let mut buf = ChainedChunkBufferWriter::new();
        frame.write(&mut buf);

        let mut view: crate::chunk::ChunkView = buf.pop_front().expect("encoded header").into();
        assert_eq!(Frame::read(&mut view), Ok(Some(frame)));
    }

    #[test]
    fn read_from_frame_bytes() {
        // version=0, type=WindowUpdate(1), flags=FIN(0x4), stream_id=7, delta=99
        let bytes = [
            0u8, 1u8, //
            0u8, 4u8, //
            0u8, 0u8, 0u8, 7u8, //
            0u8, 0u8, 0u8, 99u8,
        ];
        let mut view: crate::chunk::ChunkView = bytes.to_vec().into();
        assert_eq!(Frame::read(&mut view), Ok(Some(Frame::Stream(StreamID(7), FrameStreamEvent::WindowUpdate(Flags::fin(), 99)))));
        assert_eq!(view.len(), 0);
    }

    #[test]
    fn pipe_writer_to_reader_frames() {
        let stream_id = StreamID(1);
        let data: Vec<u8> = (0..(crate::chunk::DEFAULT_CHUNK_CAPACITY * 2 + 17)).map(|i| (i % 251) as u8).collect();

        let mut writer = FrameWriter::new(ChainedChunkBufferWriter::new());
        writer.write(Frame::Stream(stream_id, FrameStreamEvent::Data(Flags::empty(), data.len() as u32))).expect("should write");
        for chunk in data.chunks(777) {
            writer.write(Frame::Stream(stream_id, FrameStreamEvent::DataChunk(chunk.to_vec().into()))).expect("should write");
        }
        writer.write(Frame::Session(FrameSessionEvent::GoAway(0))).expect("should write");

        let buffer = writer.take();
        let mut reader = FrameReader::new(ChainedChunkBufferReader::from(buffer));

        let mut got_header = false;
        let mut got = Vec::new();
        let mut got_goaway = false;

        loop {
            match reader.next_frame().expect("should not fail") {
                None => break,
                Some(Frame::Stream(id, FrameStreamEvent::Data(_, size))) => {
                    assert_eq!(id, stream_id);
                    assert_eq!(size as usize, data.len());
                    got_header = true;
                }
                Some(Frame::Stream(id, FrameStreamEvent::DataChunk(chunk))) => {
                    assert_eq!(id, stream_id);
                    got.extend_from_slice(&chunk);
                }
                Some(Frame::Session(FrameSessionEvent::GoAway(0))) => {
                    got_goaway = true;
                }
                other => panic!("unexpected frame: {other:?}"),
            }
        }

        assert!(got_header);
        assert!(got_goaway);
        assert_eq!(got, data);
    }

    #[test]
    fn small_data_frame() {
        let stream_id = StreamID(9);
        let data = b"hello".to_vec();
        let mut writer = FrameWriter::new(ChainedChunkBufferWriter::new());
        writer.write(Frame::Stream(stream_id, FrameStreamEvent::Data(Flags::empty(), data.len() as u32))).expect("should write");
        writer.write(Frame::Stream(stream_id, FrameStreamEvent::DataChunk(data.clone().into()))).expect("should write");

        let mut reader = FrameReader::new(ChainedChunkBufferReader::from(writer.take()));
        assert!(matches!(reader.next_frame().expect("should not fail"), Some(Frame::Stream(StreamID(9), FrameStreamEvent::Data(_, 5)))));
        let chunk = match reader.next_frame().expect("should not fail").expect("should not fail") {
            Frame::Stream(StreamID(9), FrameStreamEvent::DataChunk(chunk)) => chunk,
            other => panic!("unexpected frame: {other:?}"),
        };
        assert_eq!(&*chunk, &data);
        assert_eq!(reader.next_frame().expect("should not fail"), None);
    }

    #[test]
    fn large_data_frame() {
        let stream_id = StreamID(11);
        let data: Vec<u8> = (0..(crate::chunk::DEFAULT_CHUNK_CAPACITY * 3 + 1)).map(|i| (i % 251) as u8).collect();
        let mut writer = FrameWriter::new(ChainedChunkBufferWriter::new());
        writer.write(Frame::Stream(stream_id, FrameStreamEvent::Data(Flags::empty(), data.len() as u32))).expect("should write");
        for chunk in data.chunks(crate::chunk::DEFAULT_CHUNK_CAPACITY) {
            writer.write(Frame::Stream(stream_id, FrameStreamEvent::DataChunk(chunk.to_vec().into()))).expect("should write");
        }

        let mut reader = FrameReader::new(ChainedChunkBufferReader::from(writer.take()));
        let header = reader.next_frame().expect("should not fail");
        assert_eq!(header, Some(Frame::Stream(stream_id, FrameStreamEvent::Data(Flags::empty(), data.len() as u32))));

        let mut got = Vec::new();
        let mut chunks = 0usize;
        loop {
            match reader.next_frame().expect("should not fail") {
                None => break,
                Some(Frame::Stream(StreamID(11), FrameStreamEvent::DataChunk(chunk))) => {
                    chunks += 1;
                    got.extend_from_slice(&chunk);
                }
                other => panic!("unexpected frame: {other:?}"),
            }
        }
        assert!(chunks > 1);
        assert_eq!(got, data);
    }

    #[test]
    fn reader_wrong_frame_type() {
        let stream_id = StreamID(1);
        let other_stream = StreamID(3);
        let mut writer = FrameWriter::new(ChainedChunkBufferWriter::new());

        writer.write(Frame::Stream(stream_id, FrameStreamEvent::Data(Flags::empty(), 3))).expect("should write");

        let err = writer.write(Frame::Session(FrameSessionEvent::Ping(Flags::syn(), 0))).unwrap_err();
        assert_eq!(err, FrameWriterError::UnexpectedFrameType);

        // Reset writer state.
        let mut writer = FrameWriter::new(ChainedChunkBufferWriter::new());
        writer.write(Frame::Stream(stream_id, FrameStreamEvent::Data(Flags::empty(), 3))).expect("should write");
        let err = writer.write(Frame::Stream(other_stream, FrameStreamEvent::DataChunk(vec![1, 2, 3].into()))).unwrap_err();
        assert_eq!(err, FrameWriterError::InvalidStreamId);

        // Oversized chunk should error.
        let mut writer = FrameWriter::new(ChainedChunkBufferWriter::new());
        writer.write(Frame::Stream(stream_id, FrameStreamEvent::Data(Flags::empty(), 2))).expect("should write");
        let err = writer.write(Frame::Stream(stream_id, FrameStreamEvent::DataChunk(vec![1, 2, 3].into()))).unwrap_err();
        assert_eq!(err, FrameWriterError::ChunkTooLarge);
    }
}
