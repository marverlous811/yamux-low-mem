//! Benchmark client using `yamux-low-mem`.
//!
//! Creates `--connections` TCP connections to the server; each connection is one Yamux session.
//! For each session:
//! - Opens `--streams` outbound streams (default 5) and reads them to EOF.
//! - Accepts `--streams` inbound streams and writes `--bytes` to each, flushing every chunk.

use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Context;
use clap::Parser;
use futures::StreamExt;
use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    task::{JoinHandle, JoinSet},
};
use tokio_util::compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};

use yamux_low_mem::YamuxSession;

#[path = "bench/common.rs"]
mod bench_common;

#[derive(Debug, Parser, Clone)]
struct Args {
    /// Server address to connect to.
    #[arg(long, default_value = "127.0.0.1:8080", value_name = "IP:PORT")]
    server: SocketAddr,

    /// Number of sessions (TCP connections) to create.
    #[arg(short = 'n', long, default_value_t = 100)]
    connections: usize,

    /// Number of streams each side opens per session.
    #[arg(long, default_value_t = bench_common::DEFAULT_STREAMS)]
    streams: usize,

    /// Total bytes written per accepted stream.
    #[arg(long, default_value_t = bench_common::DEFAULT_TOTAL_BYTES)]
    bytes: usize,

    /// Chunk size per write() call (flushed each iteration).
    #[arg(long, default_value_t = bench_common::DEFAULT_CHUNK_SIZE)]
    chunk: usize,

    /// Per-stream egress rate limit (kilobits per second). Set to 0 for unlimited.
    #[arg(long, default_value_t = bench_common::DEFAULT_PER_STREAM_KBPS)]
    kbps: u64,

    /// Max buffered bytes queued for the transport.
    #[arg(long, default_value_t = 64 * 1024)]
    max_write_buffer: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let counters = Arc::new(bench_common::Counters::default());
    let _reporter = bench_common::spawn_bandwidth_reporter("client/low-mem", counters.clone());

    let start = Instant::now();
    let mut joins: Vec<JoinHandle<anyhow::Result<()>>> = Vec::with_capacity(args.connections);

    for idx in 0..args.connections {
        let args = args.clone();
        let counters = counters.clone();
        joins.push(tokio::spawn(async move { run_one(idx, args, counters).await }));
    }

    for j in joins {
        j.await.context("connection task panicked")??;
    }

    let elapsed = start.elapsed();
    log::info!("all connections done: n={} elapsed={elapsed:?}", args.connections);
    Ok(())
}

async fn run_one(idx: usize, args: Args, counters: Arc<bench_common::Counters>) -> anyhow::Result<()> {
    let tcp = TcpStream::connect(args.server).await?;
    tcp.set_nodelay(true).ok();
    counters.inc_sessions();

    let mut session = YamuxSession::client(tcp.compat(), args.max_write_buffer);
    let mut tasks: JoinSet<anyhow::Result<()>> = JoinSet::new();

    for _ in 0..args.streams {
        let counters = counters.clone();
        counters.inc_streams();
        let mut stream = session.open_stream().compat();
        tasks.spawn(async move {
            let _ = bench_common::discard_to_eof(&mut stream, &counters).await?;
            counters.dec_streams();
            Ok(())
        });
    }

    let expected_tasks = args.streams * 2;
    let mut accepted = 0usize;
    let mut completed = 0usize;
    let start = Instant::now();

    while accepted < args.streams || completed < expected_tasks {
        tokio::select! {
            incoming = session.next() => {
                let Some(stream) = incoming else {
                    break;
                };
                if accepted < args.streams {
                    accepted += 1;
                    let bytes = args.bytes;
                    let chunk = args.chunk;
                    let kbps = args.kbps;
                    let counters = counters.clone();
                    counters.inc_streams();
                    let mut stream = stream.compat();
                    tasks.spawn(async move {
                        bench_common::write_zeros(&mut stream, bytes, chunk, kbps, &counters).await?;
                        let _ = stream.flush().await;
                        tokio::time::sleep(Duration::from_secs(30)).await;
                        let _ = stream.shutdown().await;
                        counters.dec_streams();
                        Ok(())
                    });
                }
            }
            joined = tasks.join_next(), if completed < expected_tasks => {
                match joined {
                    Some(res) => {
                        completed += 1;
                        res.context("task join failed")??;
                    }
                    None => break,
                }
            }
        }
    }

    let elapsed = start.elapsed();
    let per_direction = (args.streams * args.bytes) as u64;
    let total = per_direction.saturating_mul(2);
    log::info!("conn[{idx}] done: streams={} bytes/stream={} total_bytes={} elapsed={elapsed:?}", args.streams, args.bytes, total);
    counters.dec_sessions();
    Ok(())
}
