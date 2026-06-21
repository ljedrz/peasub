use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rand::Rng;
use tokio::sync::Notify;
use tokio::time;
use tracing::debug;

use crate::config::CoverStrategy;
use crate::gossip::GossipState;
use crate::node::Node;
use pea2pea::{protocols::Writing, Pea2Pea};

/// The background task that turns a `Node` into a metadata-private one.
///
/// On every "tick" (whose schedule is dictated by the chosen
/// [`CoverStrategy`]) it pulls the next message to transmit from the
/// shared outbox — or, if the outbox is empty, generates a fresh cover
/// message of the same on-the-wire size — and ships it to a single
/// randomly-chosen connected peer.
///
/// Because real and cover messages share the same path, and because the
/// *timing* of the ticks is independent of application activity, the
/// resulting outbound traffic is constant-rate (or Poisson-distributed)
/// and observationally indistinguishable from a stream with no real
/// content at all.
///
/// [`CoverStrategy`]: crate::config::CoverStrategy
pub struct CoverScheduler {
    state: Arc<GossipState>,
    strategy: CoverStrategy,
    wake: Notify,
    /// Round-robin cursor into the peer list. Picking peers with a
    /// per-node cursor (rather than uniformly at random) avoids the
    /// pathological case of every node in a small ring happening to
    /// select the same peer on the same tick.
    cursor: Mutex<usize>,
    /// The last peer we shipped a frame to; used to bias the next
    /// selection away from it, which prevents the "ping-pong" failure
    /// mode where two adjacent nodes bounce a single message back and
    /// forth indefinitely without ever reaching the rest of the ring.
    last_peer: parking_lot::Mutex<Option<std::net::SocketAddr>>,
}

impl CoverScheduler {
    pub fn new(state: Arc<GossipState>, strategy: CoverStrategy) -> Self {
        Self {
            state,
            strategy,
            wake: Notify::new(),
            cursor: Mutex::new(0),
            last_peer: parking_lot::Mutex::new(None),
        }
    }

    /// Wake the scheduler. Called by `Node::shutdown` so a `Poisson` task
    /// that is currently sleeping for a long interval can be torn down
    /// promptly.
    pub fn wake(&self) {
        self.wake.notify_waiters();
    }

    /// Drives the cover-traffic generator for the lifetime of the node.
    pub async fn run(&self, node: Node) {
        match self.strategy {
            CoverStrategy::Constant { interval } => self.run_constant(node, interval).await,
            CoverStrategy::Poisson { rate } => self.run_poisson(node, rate).await,
        }
    }

    async fn run_constant(&self, node: Node, interval: Duration) {
        let mut ticker = time::interval(interval);
        ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        // consume the immediate first tick that `time::interval` would
        // otherwise fire at `t=0`; we don't want to send a message
        // immediately on startup, only after the first full interval
        ticker.tick().await;

        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = self.wake.notified() => break,
            }
            if self.is_shutting_down() {
                break;
            }
            self.send_one(&node).await;
        }
    }

    async fn run_poisson(&self, node: Node, rate: f64) {
        loop {
            // Compute the next inter-arrival *before* the await point so
            // that the (non-`Send`) thread-local RNG does not have to be
            // held across it.
            let dur = {
                let mut rng = rand::rng();
                // Inter-arrival time for a Poisson process of rate `rate`
                // is Exp(rate). Sample via inverse-CDF: -ln(U) / rate,
                // where U is uniform in (0, 1). We clamp the upper end
                // so a single astronomically-large draw cannot stall
                // shutdown.
                let u: f64 = rng.random::<f64>().clamp(f64::MIN_POSITIVE, 1.0);
                let secs = (-u.ln() / rate).min(60.0);
                Duration::from_secs_f64(secs)
            };

            tokio::select! {
                _ = time::sleep(dur) => {}
                _ = self.wake.notified() => break,
            }
            if self.is_shutting_down() {
                break;
            }
            self.send_one(&node).await;
        }
    }

    fn is_shutting_down(&self) -> bool {
        self.state.shutting_down().load(Ordering::SeqCst)
    }

    async fn send_one(&self, node: &Node) {
        let peers = node.connected_peers();
        if peers.is_empty() {
            return;
        }

        let peer = {
            let mut cursor = self.cursor.lock();
            let mut last_peer = self.last_peer.lock();
            let mut rng = rand::rng();

            // Build a candidate set that excludes the peer we sent the
            // previous frame to. With only one peer, of course, we have
            // no choice but to send to it again.
            let candidates: Vec<usize> = if peers.len() > 1 && last_peer.is_some() {
                let last = last_peer.unwrap();
                peers
                    .iter()
                    .enumerate()
                    .filter_map(|(i, &p)| if p == last { None } else { Some(i) })
                    .collect()
            } else {
                (0..peers.len()).collect()
            };

            // Pick among the candidates using the round-robin cursor plus
            // a random offset, so two adjacent nodes do not select the
            // same "next" peer in lockstep.
            let offset = rng.random_range(0..candidates.len());
            let idx = candidates[(*cursor + offset) % candidates.len()];
            *cursor = cursor.wrapping_add(1);
            *last_peer = Some(peers[idx]);
            peers[idx]
        };

        let payload = self.state.next_outbound();
        if let Err(e) = node.unicast_fast(peer, payload) {
            debug!(parent: node.node().span(), "cover send to {peer} failed: {e}");
        }
    }
}
