use std::time::Duration;

use futures::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use tokio_util::compat::TokioAsyncReadCompatExt as _;

use yamux_low_mem::session::YamuxSession;

enum SessionCmd {
    OpenStream { reply: oneshot::Sender<yamux_low_mem::stream::YamuxStream> },
    Shutdown,
}

enum YamuxCmd {
    OpenStream { reply: oneshot::Sender<yamux::Stream> },
    Shutdown,
}

#[tokio::test]
async fn interoperates_with_yamux_over_duplex() {
    let (ours_io, theirs_io) = tokio::io::duplex(64 * 1024);

    let (session_cmd_tx, mut session_cmd_rx) = mpsc::unbounded_channel::<SessionCmd>();
    let (ours_incoming_tx, mut ours_incoming_rx) = mpsc::unbounded_channel::<yamux_low_mem::stream::YamuxStream>();

    let session_task = tokio::spawn(async move {
        let mut session = YamuxSession::server(ours_io.compat());

        loop {
            tokio::select! {
                cmd = session_cmd_rx.recv() => {
                    match cmd {
                        Some(SessionCmd::OpenStream { reply }) => {
                            let stream = session.open_stream();
                            let _ = reply.send(stream);
                        }
                        Some(SessionCmd::Shutdown) | None => break,
                    }
                }
                incoming = futures::StreamExt::next(&mut session) => {
                    match incoming {
                        Some(stream) => { let _ = ours_incoming_tx.send(stream); }
                        None => break,
                    }
                }
            }
        }
    });

    let (yamux_cmd_tx, mut yamux_cmd_rx) = mpsc::unbounded_channel::<YamuxCmd>();
    let (yamux_incoming_tx, mut yamux_incoming_rx) = mpsc::unbounded_channel::<yamux::Stream>();

    let yamux_task = tokio::spawn(async move {
        let mut conn = yamux::Connection::new(theirs_io.compat(), yamux::Config::default(), yamux::Mode::Client);

        loop {
            tokio::select! {
                cmd = yamux_cmd_rx.recv() => {
                    match cmd {
                        Some(YamuxCmd::OpenStream { reply }) => {
                            let stream = futures::future::poll_fn(|cx| conn.poll_new_outbound(cx)).await.unwrap();
                            let _ = reply.send(stream);
                        }
                        Some(YamuxCmd::Shutdown) | None => break,
                    }
                }
                inbound = futures::future::poll_fn(|cx| conn.poll_next_inbound(cx)) => {
                    match inbound {
                        Some(Ok(stream)) => { let _ = yamux_incoming_tx.send(stream); }
                        Some(Err(_)) => break,
                        None => break,
                    }
                }
            }
        }
    });

    let test_timeout = Duration::from_secs(2);

    // Our implementation opens a stream -> yamux accepts it.
    let (reply_tx, reply_rx) = oneshot::channel();
    session_cmd_tx.send(SessionCmd::OpenStream { reply: reply_tx }).expect("session task still alive");
    let mut ours_stream = timeout(test_timeout, reply_rx).await.expect("open stream timed out").unwrap();

    ours_stream.write_all(b"hello").await.unwrap();

    let mut yamux_stream = timeout(test_timeout, yamux_incoming_rx.recv()).await.expect("yamux accept timed out").expect("yamux channel closed");

    let mut buf = [0u8; 5];
    yamux_stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"hello");
    yamux_stream.write_all(b"world").await.unwrap();

    let mut buf = [0u8; 5];
    ours_stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"world");

    // yamux opens a stream -> our implementation accepts it.
    let (reply_tx, reply_rx) = oneshot::channel();
    yamux_cmd_tx.send(YamuxCmd::OpenStream { reply: reply_tx }).expect("yamux task still alive");
    let mut yamux_outgoing = timeout(test_timeout, reply_rx).await.expect("yamux open stream timed out").unwrap();
    yamux_outgoing.write_all(b"ping").await.unwrap();

    let mut ours_incoming = timeout(test_timeout, ours_incoming_rx.recv())
        .await
        .expect("our accept timed out")
        .expect("our incoming channel closed");

    let mut buf = [0u8; 4];
    ours_incoming.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"ping");
    ours_incoming.write_all(b"pong").await.unwrap();

    let mut buf = [0u8; 4];
    yamux_outgoing.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"pong");

    let _ = session_cmd_tx.send(SessionCmd::Shutdown);
    let _ = yamux_cmd_tx.send(YamuxCmd::Shutdown);
    let _ = timeout(test_timeout, session_task).await;
    let _ = timeout(test_timeout, yamux_task).await;
}
