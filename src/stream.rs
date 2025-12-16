use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{
    AsyncRead, AsyncWrite, SinkExt, Stream, StreamExt,
    channel::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender, channel, unbounded},
};

use crate::{
    chunk::{CHUNK_CAPACITY, ChunkView},
    frame::FrameStreamEvent,
    packet::{Flags, FlagsBuilder},
};

/// Initial per-stream flow-control window in bytes.
pub const INITIAL_WINDOW: u32 = 256 * 1024;

struct State {
    local: bool,
    remote: bool,
}

struct Window {
    // store how many byte we can send
    send: usize,
    // store how many byte we already received
    recv: usize,
}

impl State {
    fn is_closed(&self) -> bool {
        !self.local && !self.remote
    }
}

/// Internal state machine that turns stream I/O into [`FrameStreamEvent`] values.
///
/// The session drives this type by:
/// - Feeding inbound events via [`YamuxStreamHead::on_input`].
/// - Polling it as a [`Stream`] to obtain outbound events.
pub struct YamuxStreamHead {
    tx: UnboundedSender<ChunkView>,
    rx: Receiver<ChunkView>,
    state: State,
    window: Window,
    outs: VecDeque<FrameStreamEvent>,
    recv_state: Option<usize>,
}

impl YamuxStreamHead {
    fn open(tx: UnboundedSender<ChunkView>, rx: Receiver<ChunkView>) -> Self {
        let init_pkt = FrameStreamEvent::WindowUpdate(FlagsBuilder::default().syn(true).build().expect("should build ok"), INITIAL_WINDOW);
        Self {
            tx,
            rx,
            state: State { local: true, remote: false },
            window: Window {
                send: INITIAL_WINDOW as usize,
                recv: INITIAL_WINDOW as usize,
            },
            outs: VecDeque::from_iter([init_pkt]),
            recv_state: None,
        }
    }

    fn accept(tx: UnboundedSender<ChunkView>, rx: Receiver<ChunkView>) -> Self {
        let init_pkt = FrameStreamEvent::WindowUpdate(FlagsBuilder::default().ack(true).build().expect("should build ok"), INITIAL_WINDOW);
        Self {
            tx,
            rx,
            state: State { local: true, remote: true },
            window: Window {
                send: INITIAL_WINDOW as usize,
                recv: INITIAL_WINDOW as usize,
            },
            outs: VecDeque::from_iter([init_pkt]),
            recv_state: None,
        }
    }

    /// Handles an incoming frame for this stream.
    pub fn on_input(&mut self, event: FrameStreamEvent) -> std::io::Result<()> {
        match event {
            FrameStreamEvent::Data(flags, size) => {
                if self.recv_state.is_some() {
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "received data while already receiving data"));
                }

                self.handle_flag(flags);
                self.recv_state = Some(size as usize);

                Ok(())
            }
            FrameStreamEvent::DataChunk(chunk_view) => {
                if let Some(recv_state) = &mut self.recv_state {
                    if *recv_state < chunk_view.len() {
                        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "received data chunk size is larger than expected"));
                    }

                    *recv_state -= chunk_view.len();
                    let received_len = chunk_view.len();
                    self.tx.unbounded_send(chunk_view).expect("should send ok");

                    if *recv_state == 0 {
                        self.recv_state = None;
                    }

                    self.mark_received_bytes(received_len);
                    Ok(())
                } else {
                    Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "received data chunk while not receiving data"))
                }
            }
            FrameStreamEvent::WindowUpdate(flags, delta) => {
                self.handle_flag(flags);
                self.window.send += delta as usize;

                Ok(())
            }
        }
    }

    fn handle_flag(&mut self, flag: Flags) {
        if flag.ack {
            log::info!("[YamuxStreamHead] received ack => remote opened");
            self.state.remote = true;
        }

        if flag.rst {
            log::info!("[YamuxStreamHead] received rst => both local and remote disabled");
            self.state.local = false;
            self.state.remote = false;
        }

        if flag.fin {
            log::warn!("[YamuxStreamHead] received fin => remote closed, send empty chunk");
            self.state.remote = false;
            self.tx.unbounded_send(vec![].into()).expect("should send ok");
        }
    }

    fn mark_received_bytes(&mut self, received: usize) {
        self.window.recv += received;
        // auto send WindowUpdate when we received INITIAL_WINDOW
        if self.window.recv + CHUNK_CAPACITY >= INITIAL_WINDOW as usize / 2 {
            self.outs.push_back(FrameStreamEvent::WindowUpdate(
                FlagsBuilder::default().ack(true).build().expect("should build ok"),
                self.window.recv as u32,
            ));
            self.window.recv = 0;
        }
    }
}

impl Stream for YamuxStreamHead {
    type Item = FrameStreamEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.state.is_closed() {
            //both local and remote are closed => stream is closed
            return Poll::Ready(None);
        }

        // we need to wait for more data to be available
        // this hard limit is for simpler implementation. I am avoid complex flow-control window management
        // other option is try to send as mush as possible with condition this.window.send == 0, but it lead to complex logic
        if this.window.send < CHUNK_CAPACITY {
            return Poll::Pending;
        }

        while let Poll::Ready(event) = this.rx.poll_next_unpin(cx) {
            match event {
                Some(chunk) => {
                    this.outs.push_back(FrameStreamEvent::Data(Flags::empty(), chunk.len() as u32));
                    this.outs.push_back(FrameStreamEvent::DataChunk(chunk));
                }
                None => {
                    log::warn!("[YamuxStreamHead] rx unexpected None");
                    return Poll::Ready(None);
                }
            }
        }

        if let Some(out) = this.outs.pop_front() {
            Poll::Ready(Some(out))
        } else {
            Poll::Pending
        }
    }
}

/// User-facing half of a logical Yamux stream.
pub struct YamuxStream {
    tx: Sender<ChunkView>,
    rx: UnboundedReceiver<ChunkView>,
    recv_chunk: Option<(ChunkView, usize)>,
}

impl AsyncRead for YamuxStream {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();

        if this.recv_chunk.is_none() {
            if let Poll::Ready(event) = this.rx.poll_next_unpin(cx) {
                if let Some(chunk) = event {
                    this.recv_chunk = Some((chunk, 0));
                } else {
                    return Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "internal channel closed")));
                }
            }
        }

        if let Some((chunk, offset)) = &mut this.recv_chunk {
            let read_len = (chunk.len() - *offset).min(buf.len());
            buf[..read_len].copy_from_slice(&chunk.as_slice()[*offset..*offset + read_len]);
            *offset += read_len;
            if *offset == chunk.len() {
                this.recv_chunk = None;
            }
            return Poll::Ready(Ok(read_len));
        }

        Poll::Pending
    }
}

impl AsyncWrite for YamuxStream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();

        if let Poll::Ready(event) = this.tx.poll_ready(cx) {
            if let Err(e) = event {
                return Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe, e)));
            }
            let send_len = buf.len().min(CHUNK_CAPACITY);
            if let Err(e) = this.tx.start_send(buf[..send_len].to_vec().into()) {
                return Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe, e)));
            }
            return Poll::Ready(Ok(send_len));
        }

        Poll::Pending
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        this.tx.poll_flush_unpin(cx).map_err(|e| std::io::Error::new(std::io::ErrorKind::BrokenPipe, e))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        this.tx.poll_close_unpin(cx).map_err(|e| std::io::Error::new(std::io::ErrorKind::BrokenPipe, e))
    }
}

/// Creates a paired head/stream used by the session and user-facing API.
pub(crate) fn open_stream() -> (YamuxStreamHead, YamuxStream) {
    let (tx, rx) = channel(1);
    let (tx2, rx2) = unbounded();
    (YamuxStreamHead::open(tx2, rx), YamuxStream { tx, rx: rx2, recv_chunk: None })
}

/// Creates a paired head/stream used by the session and user-facing API.
pub(crate) fn accept_stream() -> (YamuxStreamHead, YamuxStream) {
    let (tx, rx) = channel(1);
    let (tx2, rx2) = unbounded();
    (YamuxStreamHead::accept(tx2, rx), YamuxStream { tx, rx: rx2, recv_chunk: None })
}
