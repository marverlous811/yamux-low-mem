use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::Mutex;

#[derive(Debug, Clone)]
pub struct KeepAliveConfig {
    pub interval: Duration,
    pub timeout: Duration,
}

impl Default for KeepAliveConfig {
    fn default() -> Self {
        KeepAliveConfig {
            interval: Duration::from_secs(10),
            timeout: Duration::from_secs(30),
        }
    }
}

pub(crate) enum RttEvent {
    KeepAlive(u32),
    Close,
}

#[derive(Debug, Clone)]
pub(crate) struct Rtt {
    inner: Arc<Mutex<RttInner>>,
    ping_seq: u32,
    cfg: KeepAliveConfig,
}

impl Rtt {
    pub(crate) fn new(cfg: KeepAliveConfig) -> Self {
        let inner = Arc::new(Mutex::new(RttInner {
            state: RttState::Waiting { next: Instant::now() },
        }));
        Rtt { inner, ping_seq: 0, cfg }
    }

    pub(crate) fn next_ping(&mut self) -> Option<RttEvent> {
        let state = &mut self.inner.lock().state;

        match state {
            RttState::Waiting { next } => {
                if *next > Instant::now() {
                    return None;
                }
            }
            RttState::AwaitingPong { sent_at, nonce } => {
                let rtt = sent_at.elapsed();
                if rtt < self.cfg.timeout {
                    log::debug!("still awaiting pong for nonce {}, elapsed {:?}, timeout {:?}", nonce, rtt, self.cfg.timeout);
                    return None;
                }
                return Some(RttEvent::Close);
            }
        };

        self.ping_seq += 1;
        let nonce = self.ping_seq;

        log::debug!("sending ping with nonce {}", nonce);
        *state = RttState::AwaitingPong { sent_at: Instant::now(), nonce };
        Some(RttEvent::KeepAlive(nonce))
    }

    pub(crate) fn handle_pong(&mut self, received_nonce: u32) -> bool {
        let mut inner = self.inner.lock();
        let (sent_at, expected_nonce) = match &inner.state {
            RttState::Waiting { .. } => {
                log::error!("received unexpected pong with nonce {}", received_nonce);
                return false;
            }
            RttState::AwaitingPong { sent_at, nonce } => (*sent_at, *nonce),
        };

        if received_nonce != expected_nonce {
            log::error!("received pong with unexpected nonce {}, expected {}", received_nonce, expected_nonce);
            return false;
        }

        let rtt = sent_at.elapsed();
        log::debug!("received pong with nonce {}, rtt = {:?}", received_nonce, rtt);

        let next = Instant::now() + self.cfg.interval;
        inner.state = RttState::Waiting { next };

        true
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(Clone))]
struct RttInner {
    state: RttState,
}

#[derive(Debug)]
#[cfg_attr(test, derive(Clone))]
enum RttState {
    AwaitingPong { sent_at: Instant, nonce: u32 },
    Waiting { next: Instant },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn test_rtt_ping_pong() {
        let mut rtt = Rtt::new(KeepAliveConfig {
            interval: Duration::from_millis(100),
            timeout: Duration::from_secs(1),
        });

        // First ping
        let event = rtt.next_ping();
        assert!(matches!(event, Some(RttEvent::KeepAlive(nonce)) if nonce == 1));

        // Simulate receiving pong
        let pong_handled = rtt.handle_pong(1);
        assert!(pong_handled);

        // Wait for interval to pass
        sleep(Duration::from_millis(150));
        let event = rtt.next_ping();
        assert!(matches!(event, Some(RttEvent::KeepAlive(nonce)) if nonce == 2));
    }

    #[test]
    fn test_rtt_timeout() {
        let mut rtt = Rtt::new(KeepAliveConfig {
            interval: Duration::from_millis(100),
            timeout: Duration::from_millis(200),
        });

        // First ping
        let event = rtt.next_ping();
        assert!(matches!(event, Some(RttEvent::KeepAlive(nonce)) if nonce == 1));

        // Wait for timeout to pass
        sleep(Duration::from_millis(250));
        let event = rtt.next_ping();
        assert!(matches!(event, Some(RttEvent::Close)));
    }
}
