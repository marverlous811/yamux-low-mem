//! Yamux wire-level packet definitions.
//!
//! This module contains the byte-level representation of Yamux frames as described in `SPEC.md`.
//! Higher-level parsing and chunked streaming are implemented in [`crate::frame`].

use derive_more::Display;
use thiserror::Error;
use typesafe_builder::*;

use crate::chunk::{ChunkBufferReader, ChunkBufferWriter};

// === Constants ===

/// Number of bytes in a Yamux header.
pub const HEADER_LEN: usize = 12;

// === Errors ===

/// Errors emitted while parsing or encoding frames.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ParserError {
    /// Frame type is not recognized.
    #[error("unknown frame type: {0}")]
    UnknownType(u8),
    /// Length or delta field exceeded `u32`.
    #[error("length overflow")]
    LengthOverflow,
    /// A data chunk arrived before a data frame header.
    #[error("unexpected data chunk")]
    UnexpectedDataChunk,
    /// A data frame started before the previous payload completed.
    #[error("pending data: {remaining}")]
    PendingData { remaining: u32 },
    /// The provided stream identifier did not match the expected stream.
    #[error("stream mismatch: expected {expected}, got {got}")]
    StreamMismatch { expected: StreamID, got: StreamID },
    /// A chunk length or `remain` field disagreed with the data frame size.
    #[error("data size mismatch: expected {expected}, got {got}")]
    DataSizeMismatch { expected: u32, got: u32 },
    /// A zero-length chunk would stall progress.
    #[error("empty chunk")]
    EmptyChunk,
    /// Invalid stream ID for pending data.
    #[error("invalid stream ID for pending data")]
    InvalidStreamId,
    /// Unexpected frame type while waiting for data chunk.
    #[error("unexpected frame type")]
    UnexpectedFrameType,
}

/// Errors emitted while serializing frames.
///
/// Note: this type is currently unused; callers generally use [`ParserError`]
/// because parsing and encoding share the same invariants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SerializeError {
    /// Frame type is not recognized.
    UnknownType(u8),
    /// Length or delta field exceeded `u32`.
    LengthOverflow,
    /// A data chunk arrived before a data frame header.
    UnexpectedDataChunk,
    /// A data frame started before the previous payload completed.
    PendingData { remaining: u32 },
    /// The provided stream identifier did not match the expected stream.
    StreamMismatch { expected: StreamID, got: StreamID },
    /// A chunk length or `remain` field disagreed with the data frame size.
    DataSizeMismatch { expected: u32, got: u32 },
    /// A zero-length chunk would stall progress.
    EmptyChunk,
    /// Invalid stream ID for pending data.
    InvalidStreamId,
    /// Unexpected frame type while waiting for data chunk.
    UnexpectedFrameType,
}

// === Identifiers ===

/// Yamux stream identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display)]
pub struct StreamID(pub u32);

impl StreamID {
    /// True if the stream was opened by the client (odd IDs).
    pub fn is_client(&self) -> bool {
        self.0 & 1 == 1
    }

    /// True if the stream was opened by the server (even IDs).
    pub fn is_server(&self) -> bool {
        self.0 & 1 == 0
    }

    /// True when the ID refers to the session control stream (0).
    pub fn is_session(&self) -> bool {
        self.0 == 0
    }
}

// === Flags ===

/// Yamux control flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, Builder)]
#[display("Flags(syn: {syn}, ack: {ack}, fin: {fin}, rst: {rst})")]
pub struct Flags {
    #[builder(default = "false")]
    /// SYN (open stream).
    pub syn: bool,
    #[builder(default = "false")]
    /// ACK (accept stream).
    pub ack: bool,
    #[builder(default = "false")]
    /// FIN (half-close).
    pub fin: bool,
    #[builder(default = "false")]
    /// RST (hard reset).
    pub rst: bool,
}

impl Flags {
    /// Creates a new set of flags.
    pub fn new(syn: bool, ack: bool, fin: bool, rst: bool) -> Self {
        Self { syn, ack, fin, rst }
    }

    /// Creates an empty set of flags.
    pub fn empty() -> Self {
        Self::new(false, false, false, false)
    }

    /// Converts the bitfield into flags.
    pub fn from_bits(flags: u16) -> Self {
        Self {
            syn: (flags & 0x1) != 0,
            ack: (flags & 0x2) != 0,
            fin: (flags & 0x4) != 0,
            rst: (flags & 0x8) != 0,
        }
    }

    /// Returns the flag bitfield.
    pub fn bits(&self) -> u16 {
        (self.syn as u16) | ((self.ack as u16) << 1) | ((self.fin as u16) << 2) | ((self.rst as u16) << 3)
    }
}

// === Frame metadata ===

/// Yamux frame type discriminator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameType {
    Data = 0,
    WindowUpdate = 1,
    Ping = 2,
    GoAway = 3,
}

impl FrameType {
    /// Parses the on-the-wire discriminant.
    ///
    /// # Errors
    ///
    /// Returns [`ParserError::UnknownType`] if `value` is not a known frame type.
    fn from_u8(value: u8) -> Result<Self, ParserError> {
        match value {
            0 => Ok(FrameType::Data),
            1 => Ok(FrameType::WindowUpdate),
            2 => Ok(FrameType::Ping),
            3 => Ok(FrameType::GoAway),
            other => Err(ParserError::UnknownType(other)),
        }
    }

    /// Returns the on-the-wire discriminant.
    fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Wire header for Yamux frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    /// Yamux protocol version (currently always 0).
    pub version: u8,
    /// Frame type discriminant.
    pub type_: FrameType,
    /// Frame flags (SYN/ACK/FIN/RST).
    pub flags: Flags,
    /// Destination stream identifier.
    pub stream_id: StreamID,
    /// Payload length or control value (depends on frame type).
    pub length: u32,
}

impl Header {
    /// Constructs a new header for Yamux protocol version 0.
    pub fn new(type_: FrameType, flags: Flags, stream_id: StreamID, length: u32) -> Self {
        Self {
            version: 0,
            type_,
            flags,
            stream_id,
            length,
        }
    }

    /// Attempts to parse a header.
    ///
    /// Returns `Ok(None)` when fewer than [`HEADER_LEN`] bytes are available.
    pub fn read(buffer: &mut impl ChunkBufferReader) -> Result<Option<Self>, ParserError> {
        if buffer.len() < HEADER_LEN {
            return Ok(None);
        }

        let version = buffer.next_u8().expect("checked length");
        let type_byte = buffer.next_u8().expect("checked length");
        let flags_bits = buffer.next_u16().expect("checked length");
        let stream_id = buffer.next_u32().expect("checked length");
        let length = buffer.next_u32().expect("checked length");

        Ok(Some(Self {
            version,
            type_: FrameType::from_u8(type_byte)?,
            flags: Flags::from_bits(flags_bits),
            stream_id: StreamID(stream_id),
            length,
        }))
    }

    /// Writes the header into the provided buffer.
    pub fn write(&self, buffer: &mut impl ChunkBufferWriter) {
        buffer.write_u8(self.version);
        buffer.write_u8(self.type_.as_u8());
        buffer.write_u16(self.flags.bits());
        buffer.write_u32(self.stream_id.0);
        buffer.write_u32(self.length);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::ChunkOwned;

    #[test]
    fn stream_build_test() {
        assert!(StreamID(0).is_session());
        assert!(!StreamID(0).is_client());
        assert!(StreamID(0).is_server());

        assert!(StreamID(1).is_client());
        assert!(!StreamID(1).is_server());
        assert!(!StreamID(1).is_session());

        assert!(StreamID(2).is_server());
        assert!(!StreamID(2).is_client());
    }

    #[test]
    fn flags_build_test() {
        let flags = FlagsBuilder::new().with_syn(true).with_ack(true).with_fin(true).with_rst(true).build();
        assert_eq!(flags.bits() & 0xF, 0xF);

        for bits in 0u16..16u16 {
            let decoded = Flags::from_bits(bits);
            assert_eq!(decoded.bits(), bits);
        }
    }

    #[test]
    fn frame_type_vs_u8_test() {
        assert_eq!(FrameType::from_u8(0), Ok(FrameType::Data));
        assert_eq!(FrameType::from_u8(1), Ok(FrameType::WindowUpdate));
        assert_eq!(FrameType::from_u8(2), Ok(FrameType::Ping));
        assert_eq!(FrameType::from_u8(3), Ok(FrameType::GoAway));

        assert_eq!(FrameType::from_u8(250), Err(ParserError::UnknownType(250)));
    }

    #[test]
    fn header_build_parse_test() {
        let header = Header::new(FrameType::Data, FlagsBuilder::new().with_syn(true).build(), StreamID(3), 42);
        assert_eq!(header.version, 0);

        let mut out = ChunkOwned::default();
        header.write(&mut out);
        let mut view: crate::chunk::ChunkView = out.into();
        assert_eq!(Header::read(&mut view), Ok(Some(header)));
        assert_eq!(view.len(), 0);

        let mut short: crate::chunk::ChunkView = vec![0u8; HEADER_LEN - 1].into();
        assert_eq!(Header::read(&mut short), Ok(None));

        let mut bad_type_bytes = vec![0u8; HEADER_LEN];
        bad_type_bytes[0] = 0;
        bad_type_bytes[1] = 99;
        let mut bad: crate::chunk::ChunkView = bad_type_bytes.into();
        assert_eq!(Header::read(&mut bad), Err(ParserError::UnknownType(99)));
    }
}
