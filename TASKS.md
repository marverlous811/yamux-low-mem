## Session gaps vs SPEC.md / IMPLEMENT.md

- [x] Implement flow control windows: initialize per-stream window (256 KB default), decrement on outbound data, send/handle `WindowUpdate` deltas, and block sends when window is exhausted.
- [ ] Handle stream lifecycle flags: react to `FIN`/`RST`, propagate half-close to `YamuxStream`, and remove closed streams instead of leaking them.
- [ ] Support session control frames: send/echo `Ping` with SYN/ACK, emit `GoAway` on shutdown with proper error codes, and handle incoming control frames.
- [ ] Enforce StreamID rules: reject or reset streams with wrong parity or duplicates, and prevent using reserved stream ID 0 for data.
- [ ] Surface protocol errors to callers: expose errors when invalid frames arrive (unexpected data, size mismatches) and close the session cleanly.

## E2E interop tests vs `yamux` crate (SPEC.md coverage)

- [x] Basic interop over `tokio::io::duplex`: our `YamuxSession` (server) <-> `yamux::Connection` (client), open streams in both directions and exchange data (`tests/interop_yamux.rs`).
- [ ] Stream open/accept semantics: validate SYN then ACK path (ACK can be delayed; data may arrive before ACK).
- [ ] Stream reject semantics: remote sends RST after SYN (including after some data); local write/read surfaces an error.
- [ ] FIN half-close: local FIN -> remote reads EOF but can still write back until it FINs; verify both directions.
- [ ] RST hard-close: either side resets a stream; subsequent reads/writes fail and stream is removed.
- [ ] Flow control interop: enforce initial 256KB window, stall sender when window exhausted, resume after `WindowUpdate` (both directions).
- [ ] SYN via `WindowUpdate`: open/accept stream using a `WindowUpdate` frame with SYN/ACK (zero-length data).
- [ ] Ping RTT/keepalive: send `Ping` with SYN + opaque value and verify peer echoes ACK with same value.
- [ ] GoAway session termination: verify normal termination (0x0) and protocol/internal errors (0x1/0x2) stop new streams and drain existing ones.
- [ ] StreamID rules: wrong parity / duplicate IDs rejected (RST or protocol error) and stream ID 0 never used for data frames.
