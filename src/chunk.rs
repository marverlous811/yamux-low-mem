//! Chunked buffer primitives used to keep allocations small while parsing frames.
//!
//! The chunk types intentionally work on fixed-size blocks so we can reuse
//! allocations when slicing data into views for parsing and encoding.

use std::{
    collections::VecDeque,
    ops::{Deref, Range},
    sync::Arc,
};

use derive_more::Display;
use thiserror::Error;

// === Constants ===

/// Number of bytes a [`ChunkOwned`] can store.
pub const DEFAULT_CHUNK_CAPACITY: usize = 4090;

// === Traits ===

/// Trait for sequentially consuming bytes from chained chunks without copying.
pub trait ChunkBufferReader {
    /// Returns how many bytes are buffered so far.
    fn len(&self) -> usize;

    /// True when no bytes are buffered.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Pulls the next byte if available.
    fn next_u8(&mut self) -> Option<u8>;

    /// Pulls two bytes in network order.
    fn next_u16(&mut self) -> Option<u16> {
        if self.len() < 2 {
            return None;
        }
        let v1 = self.next_u8().expect("chunk is empty");
        let v2 = self.next_u8().expect("chunk is empty");
        let value = u16::from_be_bytes([v1, v2]);
        Some(value)
    }

    /// Pulls a `u32` encoded as a 4-byte network-order integer.
    fn next_u32(&mut self) -> Option<u32> {
        if self.len() < 4 {
            return None;
        }
        let v1 = self.next_u8().expect("chunk is empty");
        let v2 = self.next_u8().expect("chunk is empty");
        let v3 = self.next_u8().expect("chunk is empty");
        let v4 = self.next_u8().expect("chunk is empty");
        let value = u32::from_be_bytes([v1, v2, v3, v4]);
        Some(value)
    }

    /// Returns a view limited by `max_len`.
    fn next_chunk(&mut self, max_len: usize) -> Option<ChunkView>;
}

/// Trait for appending bytes into a chunked buffer.
pub trait ChunkBufferWriter {
    /// Returns how many bytes are buffered so far.
    fn filled_len(&self) -> usize;

    /// Return available size, which is the number of bytes that can be written
    fn available_len(&self) -> usize;

    /// Appends a single byte.
    fn write_u8(&mut self, byte: u8);

    /// Appends two bytes in network order.
    fn write_u16(&mut self, value: u16) {
        self.write_u8((value >> 8) as u8);
        self.write_u8((value & 0xFF) as u8);
    }

    /// Appends a `u32` encoded as a 4-byte network-order integer.
    fn write_u32(&mut self, value: u32) {
        self.write_u8((value >> 24) as u8);
        self.write_u8((value >> 16) as u8);
        self.write_u8((value >> 8) as u8);
        self.write_u8((value & 0xFF) as u8);
    }

    /// Appends the contents of a slice.
    fn write_slice(&mut self, chunk: &[u8]);
}

/// Trait for treating a chunk queue as a readable source.
pub trait ChunkBufferSource {
    /// Returns the front slice of the queue.
    fn front_slice(&self) -> Option<&[u8]>;

    /// Pops the next chunk from the front of the queue.
    fn pop_front(&mut self) -> Option<ChunkView>;

    /// Consumes `len` bytes from the front of the queue.
    fn consume_front(&mut self, len: usize);
}

/// Trait for treating a chunk queue as a sink of additional chunks.
pub trait ChunkBufferSink {
    /// Pushes a new chunk to the back of the queue.
    fn push_back(&mut self, chunk: ChunkView);
}

// === Errors ===

/// Errors emitted by chunk helpers.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ChunkError {
    /// The requested range exceeded the current view.
    #[error("overflow {attempted}")]
    Overflow { attempted: usize },
}

// === Chunk view (shared) ===

/// Immutable window into a [`ChunkOwned`].
#[derive(Debug, Clone, Display)]
#[display("ChunkView({start}, {end})")]
pub struct ChunkView {
    data: Arc<ChunkOwned>,
    start: usize,
    end: usize,
}

// === Chunk storage (owned) ===

/// Fixed-capacity buffer for ingesting bytes before slicing into views.
#[derive(Debug)]
pub struct ChunkOwned {
    data: Vec<u8>,
    len: usize,
    consumed: usize,
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

impl From<&[u8]> for ChunkView {
    fn from(value: &[u8]) -> Self {
        ChunkView::from(ChunkOwned::from(value.to_vec()))
    }
}

impl ChunkView {
    /// Borrows a narrower view of this chunk.
    ///
    /// # Errors
    ///
    /// Returns [`ChunkError::Overflow`] if `range` falls outside of the current view.
    pub fn view(&self, range: Range<usize>) -> Result<ChunkView, ChunkError> {
        if range.end > self.len() {
            return Err(ChunkError::Overflow { attempted: self.start + range.end });
        }
        let start = self.start + range.start;
        let end = self.start + range.end;
        Ok(ChunkView { data: Arc::clone(&self.data), start, end })
    }
}

impl ChunkBufferReader for ChunkView {
    /// Returns the number of visible bytes.
    fn len(&self) -> usize {
        self.end - self.start
    }

    fn next_u8(&mut self) -> Option<u8> {
        if self.is_empty() {
            return None;
        }
        let value = self.data.data[self.start];
        self.start += 1;
        Some(value)
    }

    fn next_chunk(&mut self, max_len: usize) -> Option<ChunkView> {
        if self.is_empty() {
            return None;
        }

        let chunk_len = max_len.min(self.len());
        let chunk = self.view(0..chunk_len).expect("should ok with above min");
        self.start += chunk_len;
        Some(chunk)
    }
}

impl Deref for ChunkView {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.data.data[self.start..self.end]
    }
}

impl PartialEq for ChunkView {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}

impl Eq for ChunkView {}

impl From<Vec<u8>> for ChunkOwned {
    fn from(value: Vec<u8>) -> Self {
        Self { len: value.len(), consumed: 0, data: value }
    }
}

impl Default for ChunkOwned {
    fn default() -> Self {
        Self {
            data: vec![0; DEFAULT_CHUNK_CAPACITY],
            len: 0,
            consumed: 0,
        }
    }
}

impl ChunkOwned {
    /// Returns the filled slice (excluding bytes already consumed).
    pub fn filled_slice(&self) -> &[u8] {
        &self.data[self.consumed..self.len]
    }

    /// Marks `len` bytes as consumed from the front of this chunk.
    pub fn consume_front(&mut self, len: usize) {
        assert!(self.consumed + len <= self.len);
        self.consumed += len;
    }
}

impl ChunkBufferWriter for ChunkOwned {
    fn filled_len(&self) -> usize {
        self.len - self.consumed
    }

    fn available_len(&self) -> usize {
        self.data.capacity() - self.len
    }

    fn write_u8(&mut self, byte: u8) {
        self.data[self.len] = byte;
        self.len += 1;
    }

    fn write_slice(&mut self, chunk: &[u8]) {
        self.data[self.len..self.len + chunk.len()].copy_from_slice(chunk);
        self.len += chunk.len();
    }
}

// === Chained writer ===

/// Writer that appends encoded bytes into chained [`ChunkOwned`] blocks.
///
/// This adapter keeps chunk logic centralized so callers can grow a queue
/// without inlining capacity checks.
pub struct ChainedChunkBufferWriter {
    queue: VecDeque<ChunkOwned>,
    filled_len: usize,
}

impl ChainedChunkBufferWriter {
    /// Creates an empty chunk queue.
    pub fn new() -> Self {
        Self { queue: VecDeque::new(), filled_len: 0 }
    }
}

impl Default for ChainedChunkBufferWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl ChunkBufferSource for ChainedChunkBufferWriter {
    fn front_slice(&self) -> Option<&[u8]> {
        self.queue.front().map(|c| c.filled_slice())
    }

    fn pop_front(&mut self) -> Option<ChunkView> {
        self.queue.pop_front().map(|c| {
            self.filled_len -= c.filled_len();
            c.into()
        })
    }

    fn consume_front(&mut self, len: usize) {
        // Callers ensure `len` does not exceed `front_slice().len()`.
        let front = self.queue.front_mut().expect("should have front");
        front.consume_front(len);
        self.filled_len -= len;
        if front.filled_len() == 0 {
            self.queue.pop_front();
        }
    }
}

impl ChunkBufferWriter for ChainedChunkBufferWriter {
    fn filled_len(&self) -> usize {
        self.filled_len
    }

    fn available_len(&self) -> usize {
        if let Some(last) = self.queue.back() {
            last.available_len()
        } else {
            0
        }
    }

    fn write_u8(&mut self, byte: u8) {
        if let Some(last) = self.queue.back_mut()
            && last.available_len() >= 1
        {
            last.write_u8(byte);
        } else {
            let mut chunk = ChunkOwned::default();
            chunk.write_u8(byte);
            self.queue.push_back(chunk);
        }
        self.filled_len += 1;
    }

    fn write_slice(&mut self, data: &[u8]) {
        if data.is_empty() {
            return;
        }
        if self.queue.is_empty() {
            self.queue.push_back(ChunkOwned::default());
        }
        let mut offset = 0;
        while offset < data.len() {
            if let Some(last) = self.queue.back_mut()
                && last.available_len() >= 1
            {
                let write_len = last.available_len().min(data.len() - offset);
                last.write_slice(&data[offset..offset + write_len]);
                offset += write_len;
                self.filled_len += write_len;
            } else {
                let mut chunk = ChunkOwned::default();
                let write_len = chunk.available_len().min(data.len() - offset);
                chunk.write_slice(&data[offset..offset + write_len]);
                offset += write_len;
                self.queue.push_back(chunk);
                self.filled_len += write_len;
            }
        }
    }
}

// === Chained reader ===

/// Reader that walks across chained [`ChunkView`] blocks.
pub struct ChainedChunkBufferReader {
    chunks: VecDeque<ChunkView>,
    len: usize,
}

impl ChainedChunkBufferReader {
    /// Creates an empty reader.
    pub fn new() -> Self {
        Self { chunks: VecDeque::new(), len: 0 }
    }

    /// True when no bytes are currently buffered.
    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }
}

impl From<ChainedChunkBufferWriter> for ChainedChunkBufferReader {
    fn from(value: ChainedChunkBufferWriter) -> Self {
        Self {
            len: value.filled_len(),
            chunks: VecDeque::from_iter(value.queue.into_iter().map(|c| c.into())),
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
        let front = self.chunks.front_mut()?;
        let out = front.next_u8().expect("front chunk must have at least one bytes");
        self.len -= 1;
        if front.is_empty() {
            self.chunks.pop_front();
        }
        Some(out)
    }

    fn next_chunk(&mut self, max_len: usize) -> Option<ChunkView> {
        let front = self.chunks.front_mut()?;
        let out = front.next_chunk(max_len).expect("front chunk must have at least one bytes");
        self.len -= out.len();
        if front.is_empty() {
            self.chunks.pop_front();
        }
        Some(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_view_build_from_vec() {
        let mut view: ChunkView = vec![1u8, 2, 3].into();
        assert_eq!(view.len(), 3);
        assert_eq!(&*view, &[1, 2, 3]);

        assert_eq!(view.next_u8(), Some(1));
        assert_eq!(view.next_u8(), Some(2));
        assert_eq!(view.next_u8(), Some(3));
        assert_eq!(view.next_u8(), None);
    }

    #[test]
    fn chunk_view_subview() {
        let view: ChunkView = vec![10u8, 11, 12, 13].into();
        let sub = view.view(1..3);
        assert_eq!(sub.as_deref(), Ok([11u8, 12].as_slice()));

        assert_eq!(view.view(0..99), Err(ChunkError::Overflow { attempted: 99 }));
    }

    #[test]
    fn chunk_owned_build_from_vec() {
        let owned: ChunkOwned = vec![1u8, 2, 3, 4].into();
        assert_eq!(owned.filled_len(), 4);
        assert_eq!(owned.filled_slice(), &[1, 2, 3, 4]);
    }

    #[test]
    fn chunk_owned_push() {
        let mut owned = ChunkOwned::default();
        assert_eq!(owned.filled_len(), 0);
        assert_eq!(owned.available_len(), DEFAULT_CHUNK_CAPACITY);

        owned.write_u8(7);
        owned.write_u8(8);
        assert_eq!(owned.filled_len(), 2);
        assert_eq!(owned.filled_slice(), &[7, 8]);
        assert_eq!(owned.available_len(), DEFAULT_CHUNK_CAPACITY - 2);
    }

    #[test]
    fn chunk_owned_to_view() {
        let mut owned = ChunkOwned::default();
        owned.write_slice(&[1, 2, 3]);
        let view: ChunkView = owned.into();
        assert_eq!(view.len(), 3);
        assert_eq!(&*view, &[1, 2, 3]);
    }

    #[test]
    fn chunk_owned_extend_slice() {
        let mut owned = ChunkOwned::default();
        owned.write_slice(&[1, 2]);
        owned.write_slice(&[3, 4, 5]);
        assert_eq!(owned.filled_len(), 5);
        assert_eq!(owned.filled_slice(), &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn chain_chunk_buffer_writer_write_datas() {
        let mut writer = ChainedChunkBufferWriter::new();
        let data: Vec<u8> = (0..(DEFAULT_CHUNK_CAPACITY * 2 + 13)).map(|i| (i % 251) as u8).collect();

        writer.write_slice(&data);
        assert_eq!(writer.filled_len(), data.len());
        assert!(writer.filled_len() > DEFAULT_CHUNK_CAPACITY);

        let mut reader: ChainedChunkBufferReader = writer.into();
        let mut roundtrip = Vec::new();
        while let Some(chunk) = reader.next_chunk(usize::MAX) {
            roundtrip.extend_from_slice(&chunk);
        }
        assert_eq!(roundtrip, data);
    }

    #[test]
    fn chain_chunk_buffer_writer_pop() {
        let mut writer = ChainedChunkBufferWriter::new();
        let data = vec![9u8; DEFAULT_CHUNK_CAPACITY + 5];
        writer.write_slice(&data);

        let first = writer.pop_front().expect("expected first chunk");
        assert_eq!(first.len(), DEFAULT_CHUNK_CAPACITY);
        assert_eq!(&*first, &data[..DEFAULT_CHUNK_CAPACITY]);
        assert_eq!(writer.filled_len(), 5);

        let second = writer.pop_front().expect("expected second chunk");
        assert_eq!(second.len(), 5);
        assert_eq!(&*second, &data[DEFAULT_CHUNK_CAPACITY..]);
        assert_eq!(writer.filled_len(), 0);
    }
}
