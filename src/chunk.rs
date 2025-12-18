//! Chunked buffer primitives used to keep allocations small while parsing frames.
//!
//! The chunk types intentionally work on fixed-size blocks so we can reuse
//! allocations when slicing data into views for parsing and encoding.

use std::{collections::VecDeque, ops::Range, sync::Arc};

use derive_more::Display;
use thiserror::Error;

/// Number of bytes a [`ChunkOwned`] can store.
pub const CHUNK_CAPACITY: usize = 4090;

/// Trait for sequentially consuming bytes from chained chunks without copying.
pub trait ChunkBufferReader {
    /// Returns how many bytes are buffered so far.
    fn len(&self) -> usize;

    /// Pulls the next byte if available.
    fn next_u8(&mut self) -> Option<u8>;
    /// Pulls two bytes in network order.
    fn next_u16(&mut self) -> Option<u16>;
    /// Pulls a `u32` encoded as a 4-byte network-order integer.
    fn next_u32(&mut self) -> Option<u32>;
    /// Returns a view limited by `max_len`.
    fn next_chunk(&mut self, max_len: usize) -> Option<ChunkView>;
}

/// Writer for populating chained chunks.
pub trait ChunkBufferWriter {
    /// Returns how many bytes are buffered so far.
    fn len(&self) -> usize;

    /// True when no bytes are buffered.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Appends a single byte.
    fn write_u8(&mut self, byte: u8);
    /// Appends two bytes in network order.
    fn write_u16(&mut self, value: u16);
    /// Appends a `u32` encoded as a 4-byte network-order integer.
    fn write_u32(&mut self, value: u32);
    /// Appends the contents of an existing [`ChunkView`].
    fn write_chunk(&mut self, chunk: &ChunkView);
}

pub trait ChunkBufferSource {
    /// Pops the next chunk from the front of the queue.
    fn pop_front(&mut self) -> Option<ChunkView>;
}

pub trait ChunkBufferSink {
    /// Pushes a new chunk to the front of the queue.
    fn push_back(&mut self, chunk: ChunkView);
}

/// Immutable window into a [`ChunkOwned`].
#[derive(Debug, Clone, Display)]
#[display("ChunkView({start}, {end})")]
pub struct ChunkView {
    data: Arc<ChunkOwned>,
    start: usize,
    end: usize,
}

/// Fixed-capacity buffer for ingesting bytes before slicing into views.
#[derive(Debug)]
pub struct ChunkOwned {
    data: Vec<u8>,
    len: usize,
}

/// Errors emitted by chunk helpers.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ChunkError {
    /// The provided input is larger than [`CHUNK_CAPACITY`].
    #[error("overflow {attempted}")]
    Overflow { attempted: usize },
}

impl From<Vec<u8>> for ChunkOwned {
    fn from(value: Vec<u8>) -> Self {
        Self { len: value.len(), data: value }
    }
}

impl From<ChunkOwned> for ChunkView {
    fn from(chunk: ChunkOwned) -> ChunkView {
        let end = chunk.len;
        ChunkView { data: Arc::new(chunk), start: 0, end }
    }
}

impl From<Vec<u8>> for ChunkView {
    fn from(value: Vec<u8>) -> Self {
        ChunkView::from(ChunkOwned::from(value))
    }
}

impl ChunkView {
    /// Returns the number of visible bytes.
    pub fn len(&self) -> usize {
        self.end - self.start
    }

    /// Returns true when no data is visible.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Borrows a narrower view of this chunk.
    ///
    /// # Panics
    ///
    /// Panics if `range` falls outside of the current view.
    pub fn view(&self, range: Range<usize>) -> Result<ChunkView, ChunkError> {
        if range.end > self.len() {
            return Err(ChunkError::Overflow { attempted: self.start + range.end });
        }
        let start = self.start + range.start;
        let end = self.start + range.end;
        Ok(ChunkView {
            data: Arc::clone(&self.data),
            start,
            end,
        })
    }

    /// Exposes the visible bytes as a slice.
    pub fn as_slice(&self) -> &[u8] {
        &self.data.data[self.start..self.end]
    }
}

impl ChunkOwned {
    /// Creates an empty chunk.
    pub fn new() -> Self {
        Self {
            data: Vec::with_capacity(CHUNK_CAPACITY),
            len: 0,
        }
    }

    /// Returns true when the chunk holds no bytes.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the number of bytes stored in the chunk.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns how many bytes can still be written.
    pub fn remaining(&self) -> usize {
        CHUNK_CAPACITY - self.len
    }

    /// Appends as many bytes as possible from `bytes`.
    ///
    /// Returns the number of bytes written.
    pub fn extend_from_slice(&mut self, bytes: &[u8]) -> usize {
        let write_len = bytes.len().min(self.remaining());
        self.data.extend_from_slice(&bytes[..write_len]);
        self.len += write_len;
        write_len
    }

    /// Appends a single byte if capacity allows.
    pub fn push(&mut self, byte: u8) -> bool {
        if self.len == CHUNK_CAPACITY {
            return false;
        }
        self.data.push(byte);
        self.len += 1;
        true
    }

    /// Exposes the written bytes as a slice.
    pub fn as_slice(&self) -> &[u8] {
        &self.data[..self.len]
    }
}

impl Default for ChunkOwned {
    fn default() -> Self {
        Self::new()
    }
}

impl ChunkBufferWriter for Vec<u8> {
    fn len(&self) -> usize {
        Vec::len(self)
    }

    fn write_u8(&mut self, byte: u8) {
        self.push(byte);
    }

    fn write_u16(&mut self, value: u16) {
        self.extend_from_slice(&value.to_be_bytes());
    }

    fn write_u32(&mut self, value: u32) {
        self.extend_from_slice(&value.to_be_bytes());
    }

    fn write_chunk(&mut self, chunk: &ChunkView) {
        self.extend_from_slice(chunk.as_slice());
    }
}

impl PartialEq for ChunkView {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for ChunkView {}

/// Writer that appends encoded bytes into chained [`ChunkOwned`] blocks.
///
/// This adapter keeps chunk logic centralized so callers can grow a queue
/// without inlining capacity checks.
pub struct ChainedChunkBufferWriter {
    queue: VecDeque<ChunkOwned>,
    len: usize,
    front_offset: usize,
}

impl ChainedChunkBufferWriter {
    /// Creates an empty chunk queue.
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            len: 0,
            front_offset: 0,
        }
    }

    /// Borrows the next available bytes for flushing to an I/O sink.
    pub fn front_slice(&self) -> Option<&[u8]> {
        let front = self.queue.front()?;
        Some(&front.as_slice()[self.front_offset..])
    }

    /// Advances the current chunk by `len` bytes, dropping it when fully consumed.
    ///
    /// # Panics
    ///
    /// Panics if `len` exceeds the remaining bytes in the front chunk.
    pub fn consume_front(&mut self, len: usize) {
        if let Some(front) = self.queue.front() {
            assert!(self.front_offset + len <= front.len(), "consume beyond chunk length");
            self.front_offset += len;
            if self.front_offset == front.len() {
                self.queue.pop_front();
                self.front_offset = 0;
            }
        } else {
            assert_eq!(len, 0, "consume on empty queue");
        }
    }

    /// Exposes buffered data as a contiguous vector (primarily for tests).
    #[cfg(test)]
    pub fn into_bytes(self) -> Vec<u8> {
        self.queue.into_iter().flat_map(|c| c.as_slice().to_vec()).collect()
    }

    fn ensure_tail(&mut self) {
        if !matches!(self.queue.back(), Some(chunk) if chunk.remaining() > 0) {
            self.queue.push_back(ChunkOwned::new());
        }
    }
}

impl Default for ChainedChunkBufferWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl ChunkBufferSource for ChainedChunkBufferWriter {
    fn pop_front(&mut self) -> Option<ChunkView> {
        self.queue.pop_front().map(|c| {
            self.len -= c.len();
            c.into()
        })
    }
}

impl ChunkBufferWriter for ChainedChunkBufferWriter {
    fn len(&self) -> usize {
        self.len
    }

    fn write_u8(&mut self, byte: u8) {
        self.len += 1;
        self.ensure_tail();
        let tail = self.queue.back_mut().expect("tail exists");
        if tail.push(byte) {
            return;
        }
        self.ensure_tail();
        self.queue.back_mut().expect("tail exists").push(byte);
    }

    fn write_u16(&mut self, value: u16) {
        for b in value.to_be_bytes() {
            self.write_u8(b);
        }
    }

    fn write_u32(&mut self, value: u32) {
        for b in value.to_be_bytes() {
            self.write_u8(b);
        }
    }

    fn write_chunk(&mut self, chunk: &ChunkView) {
        let mut offset = 0;
        while offset < chunk.len() {
            self.ensure_tail();
            let tail = self.queue.back_mut().expect("tail exists");
            let wrote = tail.extend_from_slice(&chunk.as_slice()[offset..]);
            offset += wrote;
        }
    }
}

/// Reader that walks across chained [`ChunkView`] blocks.
pub struct ChainedChunkBufferReader {
    chunks: VecDeque<ChunkView>,
    front_offset: usize,
    len: usize,
}

impl ChainedChunkBufferReader {
    pub fn new() -> Self {
        Self {
            chunks: VecDeque::new(),
            front_offset: 0,
            len: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }
}

impl From<Vec<u8>> for ChainedChunkBufferReader {
    fn from(value: Vec<u8>) -> Self {
        Self {
            len: value.len(),
            chunks: VecDeque::from_iter([ChunkOwned::from(value).into()]),
            front_offset: 0,
        }
    }
}

impl From<ChainedChunkBufferWriter> for ChainedChunkBufferReader {
    fn from(value: ChainedChunkBufferWriter) -> Self {
        Self {
            len: value.len(),
            chunks: VecDeque::from_iter(value.queue.into_iter().map(|c| c.into())),
            front_offset: 0,
        }
    }
}

impl Default for ChainedChunkBufferReader {
    fn default() -> Self {
        Self::new()
    }
}

impl ChunkBufferSink for ChainedChunkBufferReader {
    fn push_back(&mut self, chunk: ChunkView) {
        self.len += chunk.len();
        self.chunks.push_back(chunk);
    }
}

impl ChunkBufferReader for ChainedChunkBufferReader {
    fn len(&self) -> usize {
        self.len
    }

    fn next_u8(&mut self) -> Option<u8> {
        let front = self.chunks.front()?;
        let value = front.as_slice()[self.front_offset];
        self.front_offset += 1;
        self.len -= 1;

        if self.front_offset == front.len() {
            self.chunks.pop_front();
            self.front_offset = 0;
        }

        Some(value)
    }

    fn next_u16(&mut self) -> Option<u16> {
        if self.len() < 2 {
            return None;
        }
        let hi = self.next_u8()? as u16;
        let lo = self.next_u8()? as u16;
        Some((hi << 8) | lo)
    }

    fn next_u32(&mut self) -> Option<u32> {
        if self.len() < 4 {
            return None;
        }
        let mut buf = [0u8; 4];
        for byte in buf.iter_mut() {
            *byte = self.next_u8()?;
        }
        Some(u32::from_be_bytes(buf))
    }

    fn next_chunk(&mut self, max_len: usize) -> Option<ChunkView> {
        let front = self.chunks.front()?;
        if front.len() - self.front_offset > max_len {
            let out = front.view(self.front_offset..self.front_offset + max_len).expect("should got child view");
            self.front_offset += max_len;
            self.len -= max_len;
            Some(out)
        } else {
            let out: ChunkView = front.view(self.front_offset..front.len()).expect("should got child view");
            self.chunks.pop_front();
            self.front_offset = 0;
            self.len -= out.len();
            Some(out)
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn chunk_view_build_from_vec() {
        //TODO
    }

    #[test]
    fn chunk_view_subview() {
        //TODO
    }

    #[test]
    fn chunk_owned_build_from_vec() {
        //TODO
    }

    #[test]
    fn chunk_owned_push() {
        //TODO
    }

    #[test]
    fn chunk_owned_to_view() {
        //TODO
    }

    #[test]
    fn chunk_owned_extend_slice() {
        //TODO
    }

    #[test]
    fn chain_chunk_buffer_writer_write_datas() {
        //TODO
    }

    #[test]
    fn chain_chunk_buffer_writer_pop() {
        //TODO
    }
}
