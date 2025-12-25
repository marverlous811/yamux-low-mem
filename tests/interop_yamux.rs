//! Interoperability tests against the reference `yamux` crate.
//!
//! These tests are placeholders meant to validate that `yamux-low-mem` can interoperate with
//! `yamux` for both client and server roles.

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::StreamExt;
    use tokio_util::compat::TokioAsyncReadCompatExt;

    pub trait TimeoutResultExtend<T> {
        fn timeout(self, timeout: Duration) -> impl Future<Output = anyhow::Result<T>>;
        fn timeout_1s(self) -> impl Future<Output = anyhow::Result<T>>;
    }

    pub trait TimeoutOptionExtend<T> {
        fn timeout(self, timeout: Duration) -> impl Future<Output = anyhow::Result<T>>;
        fn timeout_1s(self) -> impl Future<Output = anyhow::Result<T>>;
    }

    impl<T, O, E> TimeoutResultExtend<O> for T
    where
        T: Future<Output = Result<O, E>>,
        E: std::error::Error + Send + Sync + 'static,
    {
        async fn timeout(self, timeout: Duration) -> anyhow::Result<O> {
            let out = tokio::time::timeout(timeout, self).await??;
            Ok(out)
        }

        async fn timeout_1s(self) -> anyhow::Result<O> {
            self.timeout(Duration::from_secs(1)).await
        }
    }

    impl<T, O> TimeoutOptionExtend<O> for T
    where
        T: Future<Output = Option<O>>,
    {
        async fn timeout(self, timeout: Duration) -> anyhow::Result<O> {
            let out = tokio::time::timeout(timeout, self).await?;
            out.ok_or(anyhow::anyhow!("none"))
        }

        async fn timeout_1s(self) -> anyhow::Result<O> {
            self.timeout(Duration::from_secs(1)).await
        }
    }

    #[tokio::test]
    #[test_log::test]
    /// Exercises opening an outbound stream against a `yamux` server.
    async fn should_work_with_yamux_server_accept_stream() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (left, right) = tokio::io::duplex(64 * 1024);
        let mut server = tokio_yamux::Session::new(left, tokio_yamux::Config::default(), tokio_yamux::session::SessionType::Server);

        let cfg = yamux_low_mem::session::YamuxSessionConfig {
            max_write_buffer: 8096,
            keep_alive_config: None,
        };
        let mut client = yamux_low_mem::YamuxSession::client(right.compat(), cfg);

        tokio::spawn(async move {
            use futures::{AsyncReadExt, AsyncWriteExt};
            let mut stream = client.open_stream();

            tokio::spawn(async move { while let Some(_) = client.next().await {} });

            let mut buf = [0u8; 1024];
            log::info!("wait read");
            let buf_len = stream.read(&mut buf).await.expect("should read");
            log::info!("on read {} bytes", buf_len);
            stream.write_all(&buf[..buf_len]).await.expect("should write all");
            stream.flush().await.expect("should flush");
            log::info!("on write {} bytes", buf_len);
        });

        let mut stream = server.next().timeout_1s().await.expect("should get stream").expect("should get stream");

        tokio::spawn(async move { while let Some(_) = server.next().await {} });

        let data = b"hello";
        stream.write_all(data).timeout_1s().await.expect("should write all");
        stream.flush().timeout_1s().await.expect("should flush");

        let mut echoed_buf = [0u8; 1024];
        let echoed_size = stream.read(&mut echoed_buf).timeout_1s().await.expect("should read");
        assert_eq!(&echoed_buf[..echoed_size], data);
    }

    #[tokio::test]
    #[test_log::test]
    /// Exercises accepting an inbound stream created by a `yamux` client.
    async fn should_work_with_yamux_server_open_stream() {
        use futures::{AsyncReadExt, AsyncWriteExt};

        let (left, right) = tokio::io::duplex(64 * 1024);
        let mut server = tokio_yamux::Session::new(left, tokio_yamux::Config::default(), tokio_yamux::session::SessionType::Server);

        let cfg = yamux_low_mem::session::YamuxSessionConfig {
            max_write_buffer: 8096,
            keep_alive_config: None,
        };
        let mut client = yamux_low_mem::YamuxSession::client(right.compat(), cfg);

        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};

            let mut stream = server.open_stream().expect("should open stream");

            tokio::spawn(async move { while let Some(_) = server.next().await {} });

            let mut buf = [0u8; 1024];
            log::info!("wait read");
            let buf_len = stream.read(&mut buf).await.expect("should read first pkt");
            log::info!("on read {} bytes", buf_len);
            stream.write_all(&buf[..buf_len]).await.expect("should write all");
            stream.flush().await.expect("should flush");
            log::info!("on write {} bytes", buf_len);
        });

        let mut stream = client.next().timeout_1s().await.expect("should get stream");

        tokio::spawn(async move { while let Some(_) = client.next().await {} });

        let data = b"hello";
        stream.write_all(data).timeout_1s().await.expect("should write all");
        stream.flush().timeout_1s().await.expect("should flush");

        let mut echoed_buf = [0u8; 1024];
        let echoed_size = stream.read(&mut echoed_buf).timeout_1s().await.expect("should read");
        assert_eq!(&echoed_buf[..echoed_size], data);
    }

    #[tokio::test]
    #[test_log::test]
    /// Exercises opening an outbound stream against a `yamux` server when we act as a client.
    async fn should_work_with_yamux_client_open_stream() {
        use futures::{AsyncReadExt, AsyncWriteExt};

        let (left, right) = tokio::io::duplex(64 * 1024);
        let mut server = tokio_yamux::Session::new(left, tokio_yamux::Config::default(), tokio_yamux::session::SessionType::Client);
        let cfg = yamux_low_mem::session::YamuxSessionConfig {
            max_write_buffer: 8096,
            keep_alive_config: None,
        };

        let mut client = yamux_low_mem::YamuxSession::server(right.compat(), cfg);

        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};

            let mut stream = server.open_stream().expect("should open stream");

            tokio::spawn(async move { while let Some(_) = server.next().await {} });

            let mut buf = [0u8; 1024];
            log::info!("wait read");
            let buf_len = stream.read(&mut buf).await.expect("should read first pkt");
            log::info!("on read {} bytes", buf_len);
            stream.write_all(&buf[..buf_len]).await.expect("should write all");
            stream.flush().await.expect("should flush");
            log::info!("on write {} bytes", buf_len);
        });

        let mut stream = client.next().timeout_1s().await.expect("should get stream");

        tokio::spawn(async move { while let Some(_) = client.next().await {} });

        let data = b"hello";
        stream.write_all(data).timeout_1s().await.expect("should write all");
        stream.flush().timeout_1s().await.expect("should flush");

        let mut echoed_buf = [0u8; 1024];
        let echoed_size = stream.read(&mut echoed_buf).timeout_1s().await.expect("should read");
        assert_eq!(&echoed_buf[..echoed_size], data);
    }

    #[tokio::test]
    #[test_log::test]
    /// Exercises accepting an inbound stream created by a `yamux` server when we act as a client.
    async fn should_work_with_yamux_client_accept_stream() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (left, right) = tokio::io::duplex(64 * 1024);
        let mut server = tokio_yamux::Session::new(left, tokio_yamux::Config::default(), tokio_yamux::session::SessionType::Client);
        let cfg = yamux_low_mem::session::YamuxSessionConfig {
            max_write_buffer: 8096,
            keep_alive_config: None,
        };
        let mut client = yamux_low_mem::YamuxSession::server(right.compat(), cfg);

        tokio::spawn(async move {
            use futures::{AsyncReadExt, AsyncWriteExt};
            let mut stream = client.open_stream();

            tokio::spawn(async move { while let Some(_) = client.next().await {} });

            let mut buf = [0u8; 1024];
            log::info!("wait read");
            let buf_len = stream.read(&mut buf).await.expect("should read");
            log::info!("on read {} bytes", buf_len);
            stream.write_all(&buf[..buf_len]).await.expect("should write all");
            stream.flush().await.expect("should flush");
            log::info!("on write {} bytes", buf_len);
        });

        let mut stream = server.next().timeout_1s().await.expect("should get stream").expect("should get stream");

        tokio::spawn(async move { while let Some(_) = server.next().await {} });

        let data = b"hello";
        stream.write_all(data).timeout_1s().await.expect("should write all");
        stream.flush().timeout_1s().await.expect("should flush");

        let mut echoed_buf = [0u8; 1024];
        let echoed_size = stream.read(&mut echoed_buf).timeout_1s().await.expect("should read");
        assert_eq!(&echoed_buf[..echoed_size], data);
    }
}
