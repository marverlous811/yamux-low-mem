//! Minimal Tokio-based Yamux server example.
//!
//! Listens on `127.0.0.1:8080`, accepts inbound Yamux streams, reads a single message,
//! and echoes it back.

use futures::{AsyncReadExt, AsyncWriteExt, StreamExt};
use tokio_util::compat::TokioAsyncReadCompatExt;
use yamux_low_mem::session::YamuxSession;

#[tokio::main]
/// Runs the example server.
async fn main() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await.unwrap();
    println!("Server listening on 127.0.0.1:8080");
    loop {
        let (stream, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            let mut session = YamuxSession::server(stream.compat(), 8096);
            while let Some(mut stream) = session.next().await {
                println!("Received new stream");
                tokio::spawn(async move {
                    let mut buf = [0u8; 1024];
                    let len = stream.read(&mut buf).await.unwrap();
                    println!("Read {} bytes from stream", len);

                    // Echo the data back
                    stream.write_all(&buf[..len]).await.unwrap();
                    println!("Echoed {} bytes back to stream", len);
                });
            }
        });
    }
}
