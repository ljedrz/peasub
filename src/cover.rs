//! The cover-traffic scheduler — the background task that turns a
//! `Node` into a metadata-private one.

use std::net::SocketAddr;
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

/// The background task that turns a [`Node`] into a
/// metadata-private one.
///
/// On every "tick" (whose schedule is dictated by the chosen
/// [`CoverStrategy`]) it pulls the next message to transmit from
/// the shared queues — application messages first, then relays,
/// and finally a freshly-generated cover message of the same
/// on-the-wire size — and ships it to a single randomly-chosen
/// connected peer.
///
/// Because real and cover messages share the same code path, and
/// because the *timing* of the ticks is independent of
/// application activity, the resulting outbound traffic is
/// constant-rate (or Poisson-distributed) and observationally
/// indistinguishable from a stream with no real content at all.
///
/// [`Node`]: crate::Node
pub struct CoverScheduler {
    state: Arc<GossipState>,
    strategy: CoverStrategy,
    /// Used to break the scheduler out of its long `Poisson` sleeps
    /// promptly when [`Node::shutdown`] is called.
    ///
    /// [`Node::shutdown`]: crate::Node::shutdown
    wake: Notify,
    /// Round-robin cursor into the peer list. Mixing the cursor
    /// with a per-pick random offset (rather than picking peers
    /// uniformly at random) avoids the pathological case of
    /// every node in a small ring happening to select the same
    /// peer on the same tick.
    cursor: Mutex<usize>,
    /// The peers we shipped frames to on the previous tick; used
    /// to bias the next selection away from them. This prevents
    /// the "ping-pong" failure mode in which two adjacent nodes
    /// bounce a single message back and forth indefinitely
    /// without ever reaching the rest of the overlay. With
    /// `fanout > 1` the set covers every peer sent to in the
    /// previous tick.
    last_peers: Mutex<Vec<SocketAddr>>,
}

impl CoverScheduler {
    /// Creates a new scheduler that drains `state` according to
    /// `strategy`.
    pub fn new(state: Arc<GossipState>, strategy: CoverStrategy) -> Self {
        Self {
            state,
            strategy,
            wake: Notify::new(),
            cursor: Mutex::new(0),
            last_peers: Mutex::new(Vec::new()),
        }
    }

    /// Wakes the scheduler. Called by [`Node::shutdown`] so a
    /// `Poisson` task that is currently sleeping for a long
    /// interval can be torn down promptly.
    ///
    /// [`Node::shutdown`]: crate::Node::shutdown
    pub fn wake(&self) {
        self.wake.notify_waiters();
    }

    /// Drives the cover-traffic generator for the lifetime of
    /// the node. Returns when the node is shutting down.
    pub async fn run(&self, node: Node) {
        match self.strategy {
            CoverStrategy::Constant { interval } => self.run_constant(node, interval).await,
            CoverStrategy::Poisson { rate } => self.run_poisson(node, rate).await,
        }
    }

    async fn run_constant(&self, node: Node, interval: Duration) {
        let mut ticker = time::interval(interval);
        ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        // Consume the immediate first tick that `time::interval`
        // would otherwise fire at `t=0`; we don't want to send a
        // message immediately on startup, only after the first
        // full interval has elapsed.
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
            // Compute the next inter-arrival *before* the await
            // point so that the (non-`Send`) thread-local RNG does
            // not have to be held across it.
            let dur = {
                let mut rng = rand::rng();
                // Inter-arrival time for a Poisson process of rate
                // `rate` is Exp(rate). Sample via inverse-CDF:
                // -ln(U) / rate, where U is uniform in (0, 1).
                // We clamp the upper end so a single
                // astronomically-large draw cannot stall shutdown.
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

    /// Pull the next message and ship it to `fanout` distinct
    /// peers. Every tick emits exactly `fanout` frames of the same
    /// on-the-wire size, whether the message is real, relayed, or
    /// cover, so the metadata-privacy property is preserved: an
    /// observer sees a constant (or Poisson-distributed) stream
    /// of same-sized frames regardless of application activity.
    async fn send_one(&self, node: &Node) {
        let peers = node.connected_peers();
        if peers.is_empty() {
            return;
        }

        let fanout = self.state.config().fanout.min(peers.len());
        let payload = self.state.next_outbound();

        let targets = self.pick_targets(&peers, fanout);
        for peer in targets {
            if let Err(e) = node.unicast_fast(peer, payload.clone()) {
                debug!(parent: node.node().span(), "cover send to {peer} failed: {e}");
            }
        }
    }

    /// Pick `fanout` distinct peer addresses from `peers`, biasing
    /// the selection away from the peers we sent to on the previous
    /// tick.
    ///
    /// The selection proceeds in two phases:
    ///
    /// 1. Prefer "fresh" peers (not in `last_peers`). This avoids
    ///    the ping-pong failure mode where two adjacent nodes
    ///    bounce a message between themselves.
    /// 2. If the fresh pool is exhausted before `fanout` peers are
    ///    chosen, fall back to the recently-used pool.
    ///
    /// Within each pool, the cursor + per-pick random offset
    /// ensures two adjacent nodes don't select the same "next"
    /// peer in lockstep. The cursor advances on every pick, so
    /// over time the selection rotates through the whole peer set
    /// even when `fanout` is small.
    fn pick_targets(&self, peers: &[SocketAddr], fanout: usize) -> Vec<SocketAddr> {
        let mut rng = rand::rng();
        let mut cursor = self.cursor.lock();
        let mut last_peers = self.last_peers.lock();

        let mut fresh: Vec<usize> = Vec::new();
        let mut used: Vec<usize> = Vec::new();
        for (i, p) in peers.iter().enumerate() {
            if last_peers.contains(p) {
                used.push(i);
            } else {
                fresh.push(i);
            }
        }

        let mut chosen: Vec<usize> = Vec::with_capacity(fanout);
        for _ in 0..fanout {
            let pool = if !fresh.is_empty() {
                &mut fresh
            } else if !used.is_empty() {
                &mut used
            } else {
                break;
            };
            let remaining = pool.len();
            let offset = rng.random_range(0..remaining);
            let pick = (*cursor + offset) % remaining;
            *cursor = cursor.wrapping_add(1);
            chosen.push(pool.remove(pick));
        }

        last_peers.clear();
        let result: Vec<SocketAddr> = chosen.iter().map(|&i| peers[i]).collect();
        last_peers.extend(result.iter().copied());

        result
    }
}
