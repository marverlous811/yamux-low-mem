use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::{Duration, interval};

pub const DEFAULT_STREAMS: usize = 5;
pub const DEFAULT_TOTAL_BYTES: usize = 2 * 1024 * 1024;
pub const DEFAULT_CHUNK_SIZE: usize = 4 * 1024;
/// Default per-stream egress rate limit in kilobits per second.
pub const DEFAULT_PER_STREAM_KBPS: u64 = 2000;

#[derive(Debug, Default)]
pub struct Counters {
    in_bytes: AtomicU64,
    out_bytes: AtomicU64,
    active_sessions: AtomicU64,
    active_streams: AtomicU64,
}

impl Counters {
    pub fn add_in(&self, n: usize) {
        self.in_bytes.fetch_add(n as u64, Ordering::Relaxed);
    }

    pub fn add_out(&self, n: usize) {
        self.out_bytes.fetch_add(n as u64, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> (u64, u64) {
        (self.in_bytes.load(Ordering::Relaxed), self.out_bytes.load(Ordering::Relaxed))
    }

    pub fn inc_sessions(&self) {
        self.active_sessions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_sessions(&self) {
        self.active_sessions.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn inc_streams(&self) {
        self.active_streams.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_streams(&self) {
        self.active_streams.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn gauges(&self) -> (u64, u64) {
        (self.active_sessions.load(Ordering::Relaxed), self.active_streams.load(Ordering::Relaxed))
    }
}

pub fn spawn_bandwidth_reporter(label: impl Into<String>, counters: Arc<Counters>) -> tokio::task::JoinHandle<()> {
    let label = label.into();
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(1));
        let mut last = counters.snapshot();
        let mut last_at = Instant::now();

        loop {
            ticker.tick().await;
            let now = Instant::now();
            let dt = now.duration_since(last_at).as_secs_f64().max(1e-9);

            let cur = counters.snapshot();
            let din = cur.0.saturating_sub(last.0);
            let dout = cur.1.saturating_sub(last.1);

            let in_mib_s = (din as f64) / (1024.0 * 1024.0) / dt;
            let out_mib_s = (dout as f64) / (1024.0 * 1024.0) / dt;
            let total_in_mib = (cur.0 as f64) / (1024.0 * 1024.0);
            let total_out_mib = (cur.1 as f64) / (1024.0 * 1024.0);
            let (sessions, streams) = counters.gauges();

            log::info!("[{label}] active: sessions={sessions} streams={streams} bw: in={in_mib_s:.2} MiB/s out={out_mib_s:.2} MiB/s totals: in={total_in_mib:.1} MiB out={total_out_mib:.1} MiB");

            last = cur;
            last_at = now;
        }
    })
}

pub async fn write_zeros<W: AsyncWrite + Unpin>(writer: &mut W, total_bytes: usize, chunk_size: usize, per_stream_kbps: u64, counters: &Counters) -> io::Result<()> {
    let mut remaining = total_bytes;
    let buf = vec![0u8; chunk_size];
    let start = Instant::now();
    let mut sent_total = 0usize;

    let rate_bytes_per_sec = if per_stream_kbps == 0 {
        None
    } else {
        Some((per_stream_kbps.saturating_mul(1000) / 8).max(1))
    };

    while remaining > 0 {
        let to_write = remaining.min(buf.len());
        writer.write_all(&buf[..to_write]).await?;
        writer.flush().await?;
        counters.add_out(to_write);
        sent_total += to_write;
        remaining -= to_write;

        if let Some(rate_bytes_per_sec) = rate_bytes_per_sec {
            let target = Duration::from_secs_f64((sent_total as f64) / (rate_bytes_per_sec as f64));
            let elapsed = start.elapsed();
            if target > elapsed {
                tokio::time::sleep(target - elapsed).await;
            }
        }
    }

    Ok(())
}

pub async fn discard_to_eof<R: AsyncRead + Unpin>(reader: &mut R, counters: &Counters) -> io::Result<u64> {
    let mut buf = vec![0u8; 16 * 1024];
    let mut total = 0u64;

    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            return Ok(total);
        }
        counters.add_in(n);
        total += n as u64;
    }
}
