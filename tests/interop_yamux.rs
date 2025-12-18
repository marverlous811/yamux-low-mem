//! Interoperability tests against the reference `yamux` crate.
//!
//! These tests are placeholders meant to validate that `yamux-low-mem` can interoperate with
//! `yamux` for both client and server roles.

#[cfg(test)]
mod tests {
    #[tokio::test]
    /// Exercises opening an outbound stream against a `yamux` server.
    async fn should_work_with_yamux_server_open_stream() {
        //TODO test open_stream, write_data, close_stream
    }

    #[tokio::test]
    /// Exercises accepting an inbound stream created by a `yamux` client.
    async fn should_work_with_yamux_server_accept_stream() {
        //TODO test open_stream, write_data, close_stream
    }

    #[tokio::test]
    /// Exercises opening an outbound stream against a `yamux` server when we act as a client.
    async fn should_work_with_yamux_client_open_stream() {
        //TODO test open_stream, write_data, close_stream
    }

    #[tokio::test]
    /// Exercises accepting an inbound stream created by a `yamux` server when we act as a client.
    async fn should_work_with_yamux_client_accept_stream() {
        //TODO test open_stream, write_data, close_stream
    }
}
