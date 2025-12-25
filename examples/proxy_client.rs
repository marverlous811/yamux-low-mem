//! Reverse proxy client example using `yamux-low-mem`.
//!
//! - Connects a Yamux session to the server at `--yamux-server` (default `127.0.0.1:3000`).
//! - For each inbound Yamux stream, dials `--target-addr` (default `127.0.0.1:8080`)
//!   and pipes bytes in both directions.

use std::net::SocketAddr;

use clap::Parser;
use futures::StreamExt;
use tokio::{io::AsyncWriteExt, net::TcpStream};
use tokio_util::compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};

use yamux_low_mem::session::YamuxSession;

const MAX_WRITE_BUFFER: usize = 64 * 1024;

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
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let transport = TcpStream::connect(args.yamux_server).await?;
    log::info!("connected to yamux server at {}", args.yamux_server);

    let cfg = yamux_low_mem::session::YamuxSessionConfig {
        max_write_buffer: MAX_WRITE_BUFFER,
        keep_alive_config: None,
    };
    let mut session = YamuxSession::client(transport.compat(), cfg);
    while let Some(stream) = session.next().await {
        let id = stream.stream_id();
        let target_addr = args.target_addr;
        tokio::spawn(async move {
            log::info!("pipe {id} opened");
            if let Err(err) = pipe_yamux_to_target(stream, target_addr).await {
                log::error!("pipe {id} closed with error: {err:#}");
            }
            log::info!("pipe {id} closed");
        });
    }

    Ok(())
}

async fn pipe_yamux_to_target(stream: yamux_low_mem::stream::YamuxStream, target_addr: SocketAddr) -> anyhow::Result<()> {
    let mut inbound = stream.compat();
    let mut outbound = TcpStream::connect(target_addr).await?;
    let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await?;
    let _ = outbound.shutdown().await;
    let _ = inbound.shutdown().await;
    Ok(())
}
