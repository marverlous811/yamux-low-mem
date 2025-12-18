//! Reverse proxy client example using `tokio-yamux` (reference implementation).
//!
//! - Connects a Yamux session to the server at `--yamux-server` (default `127.0.0.1:3000`).
//! - For each inbound Yamux stream, dials `--target-addr` (default `127.0.0.1:8080`)
//!   and pipes bytes in both directions.

use std::net::SocketAddr;

use clap::Parser;
use futures::StreamExt;
use tokio::{io::AsyncWriteExt, net::TcpStream};

#[derive(Debug, Parser)]
struct Args {
    /// Yamux server address (control connection).
    #[arg(long, default_value = "127.0.0.1:3000", value_name = "IP:PORT")]
    yamux_server: SocketAddr,

    /// Where each inbound Yamux stream is forwarded to.
    #[arg(long, default_value = "127.0.0.1:8080", value_name = "IP:PORT")]
    target_addr: SocketAddr,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt().init();

    let transport = TcpStream::connect(args.yamux_server).await?;
    log::info!("connected to yamux server at {}", args.yamux_server);

    let mut session = tokio_yamux::Session::new(transport, tokio_yamux::Config::default(), tokio_yamux::session::SessionType::Client);
    while let Some(next) = session.next().await {
        let stream = next?;
        let target_addr = args.target_addr;
        tokio::spawn(async move {
            log::info!("pipe opened");
            if let Err(err) = pipe_yamux_to_target(stream, target_addr).await {
                log::error!("pipe closed with error: {err:#}");
            }
            log::info!("pipe closed");
        });
    }

    Ok(())
}

async fn pipe_yamux_to_target(mut stream: tokio_yamux::StreamHandle, target_addr: SocketAddr) -> anyhow::Result<()> {
    let mut outbound = TcpStream::connect(target_addr).await?;
    let _ = tokio::io::copy_bidirectional(&mut stream, &mut outbound).await?;
    let _ = outbound.shutdown().await;
    let _ = stream.shutdown().await;
    Ok(())
}
