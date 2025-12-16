use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::{
    AsyncRead, AsyncWrite, Stream,
    channel::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender, channel, unbounded},
};

use crate::{chunk::ChunkView, frame::FrameStreamEvent};

/// Initial per-stream flow-control window in bytes.
pub const INITIAL_WINDOW: u32 = 256 * 1024;

/// Internal state machine that turns stream I/O into [`FrameStreamEvent`] values.
///
/// The session drives this type by:
/// - Feeding inbound events via [`YamuxStreamHead::on_input`].
/// - Polling it as a [`Stream`] to obtain outbound events.
pub struct YamuxStreamHead {
    tx: Option<UnboundedSender<ChunkView>>,
    rx: Option<Receiver<ChunkView>>,
}

impl YamuxStreamHead {
    fn new(tx: UnboundedSender<ChunkView>, rx: Receiver<ChunkView>) -> Self {
        Self { tx: Some(tx), rx: Some(rx) }
    }

    /// Handles an incoming frame for this stream.
    pub fn on_input(&mut self, _event: FrameStreamEvent) {
        todo!()
    }
}

impl Stream for YamuxStreamHead {
    type Item = FrameStreamEvent;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}

/// User-facing half of a logical Yamux stream.
pub struct YamuxStream {
    tx: Sender<ChunkView>,
    rx: UnboundedReceiver<ChunkView>,
    recv_chunk: Option<(ChunkView, usize)>,
}

/// Creates a paired head/stream used by the session and user-facing API.
pub(crate) fn build_stream() -> (YamuxStreamHead, YamuxStream) {
    let (tx, rx) = channel(1);
    let (tx2, rx2) = unbounded();
    (YamuxStreamHead::new(tx2, rx), YamuxStream { tx, rx: rx2, recv_chunk: None })
}

impl AsyncRead for YamuxStream {
    fn poll_read(self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &mut [u8]) -> Poll<std::io::Result<usize>> {
        todo!()
    }
}

impl AsyncWrite for YamuxStream {
    fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &[u8]) -> Poll<std::io::Result<usize>> {
        todo!()
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        todo!()
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        todo!()
    }
}
