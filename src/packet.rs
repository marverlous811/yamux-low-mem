use crate::chunk::{ChunkBufferReader, ChunkBufferWriter};

/// Number of bytes in a Yamux header.
pub const HEADER_LEN: usize = 12;

/// Errors emitted while parsing or encoding frames.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParserError {
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

/// Errors emitted while serializing frames.
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

/// Yamux stream identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

/// Yamux control flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Flags {
    pub syn: bool,
    pub ack: bool,
    pub fin: bool,
    pub rst: bool,
}

impl Flags {
    /// Creates a new set of flags.
    pub fn new(syn: bool, ack: bool, fin: bool, rst: bool) -> Self {
        Self { syn, ack, fin, rst }
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

/// Yamux frame type discriminator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameType {
    Data = 0,
    WindowUpdate = 1,
    Ping = 2,
    GoAway = 3,
}

impl FrameType {
    fn from_u8(value: u8) -> Result<Self, ParserError> {
        match value {
            0 => Ok(FrameType::Data),
            1 => Ok(FrameType::WindowUpdate),
            2 => Ok(FrameType::Ping),
            3 => Ok(FrameType::GoAway),
            other => Err(ParserError::UnknownType(other)),
        }
    }

    fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Wire header for Yamux frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub version: u8,
    pub type_: FrameType,
    pub flags: Flags,
    pub stream_id: StreamID,
    pub length: u32,
}

impl Header {
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
        if buffer.remaining() < HEADER_LEN {
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
