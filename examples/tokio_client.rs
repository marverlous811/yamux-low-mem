//! Minimal Tokio-based Yamux client example.
//!
//! Connects to `127.0.0.1:8080`, opens a Yamux stream, writes a message, and reads the echo.

use futures::{AsyncReadExt, AsyncWriteExt, StreamExt};
use tokio_util::compat::TokioAsyncReadCompatExt;
use yamux_low_mem::session::YamuxSession;

#[tokio::main]
/// Runs the example client.
async fn main() {
    //TODO: implement client here

    let stream = tokio::net::TcpStream::connect("127.0.0.1:8080").await.unwrap();
    println!("Connected to server");

    let mut session = YamuxSession::client(stream.compat(), 8096);
    let mut stream2 = session.open_stream();

    tokio::spawn(async move {
        let send_data = b"Hello from client!";
        stream2.write_all(&send_data[..]).await.unwrap();

        let mut recv_data = [0u8; 1024];
        let len = stream2.read(&mut recv_data).await.unwrap();

        println!("Received {} bytes: {:?}", len, &recv_data[..len]);
        assert_eq!(len, send_data.len());
        assert_eq!(&recv_data[..len], send_data);
    });

    while let Some(_new_stream) = session.next().await {
        println!("Received stream from server");
        // TODO: read from the stream
    }
}
