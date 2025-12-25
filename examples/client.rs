use std::{net::SocketAddr, time::Duration};

use clap::Parser;

use futures::StreamExt;
use tokio_util::compat::TokioAsyncReadCompatExt;
// use tokio_yamux::Session;
use yamux_low_mem::session::{KeepAliveConfig, YamuxSession};

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = "0.0.0.0:3000")]
    server_addr: SocketAddr,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    tracing_subscriber::fmt::init();

    let stream = tokio::net::TcpStream::connect(args.server_addr).await.expect("Must connect to server");
    log::info!("Connected to server at {}", args.server_addr);

    let mut session = YamuxSession::client(
        stream.compat(),
        yamux_low_mem::session::YamuxSessionConfig {
            max_write_buffer: 65536,
            keep_alive_config: Some(KeepAliveConfig {
                interval: Duration::from_secs(10),
                timeout: Duration::from_secs(30),
            }),
        },
    );
    log::info!("Yamux session established");

    while let Some(_) = session.next().await {
        log::info!("New yamux stream from server");
    }
}
