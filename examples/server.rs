use std::{net::SocketAddr, time::Duration};

use clap::Parser;
use futures::StreamExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::compat::TokioAsyncReadCompatExt;
use yamux_low_mem::session::KeepAliveConfig;

#[derive(Debug, Parser)]
pub struct Args {
    #[arg(long, default_value = "0.0.0.0:3000")]
    pub listen_addr: std::net::SocketAddr,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt::init();

    let listener = TcpListener::bind(args.listen_addr).await.expect("must binded");

    log::info!("listening on {}", args.listen_addr);

    while let Ok((stream, peer_addr)) = listener.accept().await {
        log::info!("accepted connection from {}", peer_addr);
        tokio::spawn(async move {
            if let Err(e) = serve_connection(stream, peer_addr).await {
                log::error!("connection {} error: {:?}", peer_addr, e);
            }
        });
    }

    Ok(())
}

async fn serve_connection(stream: TcpStream, addr: SocketAddr) -> anyhow::Result<()> {
    let cfg = yamux_low_mem::session::YamuxSessionConfig {
        max_write_buffer: 65536,
        keep_alive_config: Some(KeepAliveConfig {
            interval: Duration::from_secs(10),
            timeout: Duration::from_secs(30),
        }),
    };
    let mut session = yamux_low_mem::YamuxSession::client(stream.compat(), cfg);
    log::info!("Yamux session established {addr}");

    while let Some(_) = session.next().await {
        log::info!("new yamux stream from {addr}");
    }

    log::info!("Yamux session closed {addr}");

    Ok(())
}
