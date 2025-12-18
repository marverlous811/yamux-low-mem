//! Reverse proxy server example using `yamux-low-mem`.
//!
//! - Listens for incoming TCP (e.g. HTTP) on `--http-bind` (default `0.0.0.0:8080`).
//! - Listens for incoming Yamux sessions on `--yamux-bind` (default `0.0.0.0:3000`).
//! - Each inbound TCP connection is tunneled over a new Yamux stream to the connected client.

use std::net::SocketAddr;

use clap::Parser;
use futures::StreamExt;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};
use tokio_util::compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};

use yamux_low_mem::session::YamuxSession;

const MAX_WRITE_BUFFER: usize = 64 * 1024;

#[derive(Debug, Parser)]
struct Args {
    /// Bind address for incoming TCP connections to tunnel.
    #[arg(long, default_value = "0.0.0.0:18080", value_name = "IP:PORT")]
    http_bind: SocketAddr,

    /// Bind address for incoming Yamux sessions.
    #[arg(long, default_value = "0.0.0.0:3000", value_name = "IP:PORT")]
    yamux_bind: SocketAddr,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();
    let args = Args::parse();

    let http_listener = TcpListener::bind(args.http_bind).await?;
    log::info!("http tcp listening on {}", args.http_bind);

    let yamux_listener = TcpListener::bind(args.yamux_bind).await?;
    log::info!("yamux listening on {}", args.yamux_bind);

    loop {
        let (transport, peer_addr) = yamux_listener.accept().await?;
        log::info!("yamux client connected from {peer_addr}");

        if let Err(err) = serve_client(transport, &http_listener).await {
            log::error!("yamux client {peer_addr} disconnected: {err:#}");
        }
    }
}

async fn serve_client(transport: TcpStream, http_listener: &TcpListener) -> anyhow::Result<()> {
    let mut session = YamuxSession::server(transport.compat(), MAX_WRITE_BUFFER);

    loop {
        tokio::select! {
            accept_res = http_listener.accept() => {
                let (tcp, peer) = accept_res?;
                log::info!("http client connected from {peer}");
                let stream = session.open_stream();
                tokio::spawn(async move {
                    log::info!("pipe {peer} opened");
                    if let Err(err) = pipe_tcp_over_yamux(tcp, stream).await {
                        log::error!("pipe {peer} closed with error: {err:#}");
                    }
                    log::info!("pipe {peer} closed");
                });
            }
            incoming = session.next() => {
                match incoming {
                    Some(_stream) => {
                        // Client-initiated streams aren't used in this example; drop them.
                    }
                    None => break,
                }
            }
        }
    }

    Ok(())
}

async fn pipe_tcp_over_yamux(tcp: TcpStream, stream: yamux_low_mem::stream::YamuxStream) -> anyhow::Result<()> {
    let mut local = tcp;
    let mut remote = stream.compat();
    let _ = tokio::io::copy_bidirectional(&mut local, &mut remote).await?;
    let _ = local.shutdown().await;
    let _ = remote.shutdown().await;
    Ok(())
}
