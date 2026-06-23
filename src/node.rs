//! The [`Node`] type — a thin wrapper around a [`peashape::Node`]
//! that adds the gossip-specific bookkeeping (LRU dedup, relay
//! forwarding).

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::BytesMut;
use parking_lot::Mutex;
use pea2pea::{Node as P2pNode, Pea2Pea};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tracing::debug;

use crate::config::NodeConfig;
use crate::error::Error;
use crate::gossip::GossipState;

/// A single peer in a `peasub` gossip network.
///
/// Internally, a `Node` is a [`peashape::Node`] (which provides
/// the wire-level shaping, cover traffic, priority lanes, and
/// length-delimited codec) plus a small amount of
/// gossip-specific bookkeeping:
///
/// - an LRU of recently-seen message identifiers, used to
///   suppress re-broadcast of frames that have already
///   circulated through this node;
/// - a background task that subscribes to the `peashape`
///   incoming broadcast, dedups each frame against the LRU,
///   and re-enqueues novel frames into the `peashape`
///   low-priority lane for fanout. The LIFO discipline of
///   `peashape`'s low-priority lane is what gives fresh
///   relays priority over older ones.
///
/// `Node` is cheap to `clone`: clones share the same
/// peashape shaper, gossip state, and forwarding task.
#[derive(Clone)]
pub struct Node {
    config: NodeConfig,
    peashape: peashape::Node,
    gossip: Arc<GossipState>,
    incoming: broadcast::Sender<BytesMut>,
    forwarding_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl Node {
    /// Constructs a new, unstarted `Node`. Call [`Node::spawn`]
    /// to enable the `pea2pea` protocols, bring up the
    /// listener, launch the peashape shaping scheduler, and
    /// start the dedup-and-relay task.
    ///
    /// # Panics
    ///
    /// Panics if the configuration is internally inconsistent
    /// (see [`peashape::ShapeConfig`] for the full list). In
    /// addition, `dedup_capacity` must be non-zero.
    pub fn new(config: NodeConfig) -> Self {
        assert!(config.dedup_capacity > 0, "dedup_capacity must be non-zero",);
        let peashape = peashape::Node::new(config.to_shape_config());
        let gossip = Arc::new(GossipState::new(config.dedup_capacity));
        let (incoming, _) = broadcast::channel(peashape::SUBSCRIBER_CAPACITY);
        Self {
            config,
            peashape,
            gossip,
            incoming,
            forwarding_handle: Arc::new(Mutex::new(None)),
        }
    }

    /// Starts the node.
    ///
    /// Spawns the `peashape` node (which enables the `pea2pea`
    /// protocols, brings up the listener, and starts the
    /// shaping scheduler) and starts the dedup-and-relay
    /// background task. Returns the bound listening address,
    /// or `None` if the node was configured without a
    /// listener.
    pub async fn spawn(&self) -> io::Result<Option<SocketAddr>> {
        let addr = self.peashape.spawn().await?;

        // Start the dedup-and-relay task: subscribe to
        // peashape's incoming broadcast, dedup each frame, and
        // re-enqueue novel frames into peashape's low-priority
        // lane for fanout.
        let mut rx = self.peashape.subscribe();
        let shaper = self.peashape.shaper();
        let gossip = self.gossip.clone();
        let incoming = self.incoming.clone();
        let handle = tokio::spawn(async move {
            while let Ok(frame) = rx.recv().await {
                if frame.len() < peashape::ID_SIZE {
                    continue;
                }
                let mut id = [0u8; peashape::ID_SIZE];
                id.copy_from_slice(&frame[..peashape::ID_SIZE]);
                if gossip.check_and_record(&id) {
                    // Re-broadcast the original frame
                    // byte-for-byte (so the receiving peer's
                    // dedup cache recognizes the same ID).
                    if let Err(e) = shaper.enqueue_raw(
                        peashape::Lane::Low,
                        peashape::Target::Broadcast,
                        frame.clone(),
                    ) {
                        debug!("could not enqueue a relay frame: {e}");
                    }
                    // Deliver to peasub subscribers (the
                    // application).
                    let _ = incoming.send(frame);
                }
            }
        });
        *self.forwarding_handle.lock() = Some(handle);

        Ok(addr)
    }

    /// Submits a real (application-originated) message to be
    /// gossiped through the network.
    ///
    /// The payload is automatically padded to the wire-level
    /// message size with random bytes; the resulting frame is
    /// byte-for-byte indistinguishable from a cover frame. The
    /// returned 32-byte array is the random identifier that
    /// has been assigned to the message — useful for
    /// correlating publishes with their downstream delivery,
    /// but it has no significance on the wire beyond dedup.
    ///
    /// The next cover tick will transmit the message, so the
    /// *time* at which it appears on the wire is governed by
    /// the cover schedule, not by the call to `publish`.
    ///
    /// # Errors
    ///
    /// - [`Error::PayloadTooLarge`] if the payload is too big
    ///   to fit in a single frame.
    /// - [`Error::AppOutboxFull`] if the application outbox is
    ///   saturated.
    pub fn publish(&self, payload: &[u8]) -> Result<[u8; peashape::ID_SIZE], Error> {
        self.peashape.broadcast_shaped(payload).map_err(Error::from)
    }

    /// Returns a broadcast receiver that yields every
    /// *novel* frame received from a peer — whether it
    /// originated as a "real" gossip message or as a peer's
    /// cover traffic is **not** observable on the wire (or by
    /// this method). Frames that the local dedup cache has
    /// already seen are filtered out.
    ///
    /// The application is responsible for filtering cover
    /// frames; the conventional way to do so is to use a
    /// payload format with a recognizable structure (e.g. a
    /// version byte or a magic header) that random cover bytes
    /// will not match by chance.
    pub fn subscribe(&self) -> broadcast::Receiver<BytesMut> {
        self.incoming.subscribe()
    }

    /// Initiates an outbound connection to a peer. If the node
    /// is already connected to that address, the call succeeds
    /// without taking further action.
    ///
    /// # Errors
    ///
    /// Returns any I/O error reported by `pea2pea` (e.g. the
    /// listener is not bound, the address is unreachable, the
    /// connection limits have been reached).
    pub async fn connect(&self, addr: SocketAddr) -> io::Result<()> {
        self.peashape.connect(addr).await
    }

    /// Closes the connection to a peer, if one is currently
    /// open. Returns `true` if a connection was actually torn
    /// down.
    pub async fn disconnect(&self, addr: SocketAddr) -> bool {
        self.peashape.disconnect(addr).await
    }

    /// Returns the addresses of currently-connected peers.
    pub fn connected_peers(&self) -> Vec<SocketAddr> {
        self.peashape.connected_peers()
    }

    /// Returns the bound listening address, or an error if the
    /// node was not configured with a listener.
    pub async fn local_addr(&self) -> io::Result<SocketAddr> {
        self.peashape.local_addr().await
    }

    /// Returns a reference to the underlying `peashape::Node`.
    ///
    /// Exposed so that callers can layer additional `peashape`
    /// functionality on top of `peasub` (e.g. a custom
    /// `peashape` `Handshake`).
    pub fn peashape(&self) -> &peashape::Node {
        &self.peashape
    }

    /// Returns a reference to the underlying `pea2pea::Node`.
    ///
    /// Exposed so that callers can layer additional `pea2pea`
    /// protocols on top of `peasub` (e.g. a custom
    /// `Handshake`) before calling [`Node::spawn`].
    pub fn p2p(&self) -> &P2pNode {
        self.peashape.p2p()
    }

    /// Returns the [`NodeConfig`] this node was built from.
    pub fn config(&self) -> &NodeConfig {
        &self.config
    }

    /// Gracefully shuts the node down.
    ///
    /// Sets the shutdown flag (which the peashape scheduler
    /// polls), wakes the peashape scheduler out of any pending
    /// sleep, closes all connections, aborts the `pea2pea`
    /// background tasks, and aborts the dedup-and-relay task.
    /// Waits for the peashape shaping task to finish. After
    /// `shutdown` returns the node is unusable; callers should
    /// drop it.
    ///
    /// `shutdown` is idempotent: calling it on an
    /// already-shut-down node is a no-op.
    pub async fn shutdown(&self) {
        // Stop the dedup-and-relay task first so it doesn't
        // try to enqueue frames into a shaper that is being
        // shut down.
        let handle = self.forwarding_handle.lock().take();
        if let Some(handle) = handle {
            handle.abort();
        }
        self.peashape.shutdown().await;
    }
}

impl Pea2Pea for Node {
    fn node(&self) -> &P2pNode {
        self.peashape.node()
    }
}
