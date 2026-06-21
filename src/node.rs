//! The [`Node`] type and its `pea2pea` protocol implementations.

use std::io;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::BytesMut;
use parking_lot::Mutex;
use pea2pea::{
    protocols::{Reading, Writing},
    Config, ConnectionSide, Node as P2pNode, Pea2Pea,
};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::codec::Codec;
use crate::config::NodeConfig;
use crate::cover::CoverScheduler;
use crate::error::Error;
use crate::gossip::GossipState;

/// A single peer in a `peasub` gossip network.
///
/// Internally, a `Node` is a `pea2pea::Node` plus a small layer
/// of metadata-private bookkeeping: two shared queues (an
/// application-message queue and a LIFO relay queue), an LRU of
/// recently-seen message IDs, and a background cover-scheduler
/// task that drains the queues at a constant (or
/// Poisson-distributed) rate, generating cover traffic when
/// neither queue has anything to send.
///
/// `Node` is cheap to `clone`: clones share the same queues,
/// dedup cache, and cover-traffic task.
#[derive(Clone)]
pub struct Node {
    p2p: P2pNode,
    state: Arc<GossipState>,
    cover: Arc<CoverScheduler>,
    cover_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl Node {
    /// Constructs a new, unstarted `Node`. Call [`Node::spawn`] to
    /// enable the `pea2pea` protocols, bring up the listener, and
    /// launch the cover-traffic scheduler.
    ///
    /// # Panics
    ///
    /// Panics if the configuration is internally inconsistent:
    ///
    /// - `message_size <= gossip::ID_SIZE` (no room for payload);
    /// - `message_size > max_frame_size` (would be rejected by the
    ///   decoder);
    /// - `app_outbox_capacity == 0` or `relay_outbox_capacity == 0`
    ///   (no buffering is possible);
    /// - `fanout == 0` (no peers would ever be selected).
    pub fn new(config: NodeConfig) -> Self {
        let p2p_config = Config {
            name: config.name.clone(),
            listener_addr: config.listener_addr,
            max_connections: config.max_connections,
            max_connections_per_ip: config.max_connections_per_ip,
            reuse_listener_port: config.reuse_listener_port,
            ..Config::default()
        };

        assert!(
            config.message_size > crate::gossip::ID_SIZE,
            "message_size ({} bytes) must exceed ID_SIZE ({} bytes) to leave room for payload",
            config.message_size,
            crate::gossip::ID_SIZE,
        );
        assert!(
            config.message_size <= config.max_frame_size,
            "message_size ({} bytes) must not exceed max_frame_size ({} bytes)",
            config.message_size,
            config.max_frame_size,
        );
        assert!(
            config.app_outbox_capacity > 0,
            "app_outbox_capacity must be non-zero",
        );
        assert!(
            config.relay_outbox_capacity > 0,
            "relay_outbox_capacity must be non-zero",
        );
        assert!(config.fanout > 0, "fanout must be non-zero",);

        let p2p = P2pNode::new(p2p_config);
        let state = Arc::new(GossipState::new(&config));
        let cover = Arc::new(CoverScheduler::new(state.clone(), config.cover));
        Self {
            p2p,
            state,
            cover,
            cover_handle: Arc::new(Mutex::new(None)),
        }
    }

    /// Starts the node.
    ///
    /// Enables the `pea2pea` `Reading` and `Writing` protocols,
    /// brings up the listener (if one is configured), and spawns
    /// the cover-traffic scheduler. Returns the bound listening
    /// address, or `None` if the node was configured without a
    /// listener.
    pub async fn spawn(&self) -> io::Result<Option<SocketAddr>> {
        self.enable_reading().await;
        self.enable_writing().await;
        let addr = self.p2p.toggle_listener().await?;

        let cover = self.cover.clone();
        let node = self.clone();
        let handle = tokio::spawn(async move {
            cover.run(node).await;
        });
        *self.cover_handle.lock() = Some(handle);

        Ok(addr)
    }

    /// Submits a real (application-originated) message to be
    /// gossiped through the network.
    ///
    /// The payload is automatically padded to the wire-level
    /// message size with random bytes; the resulting frame is
    /// byte-for-byte indistinguishable from a cover frame. The
    /// returned 32-byte array is the random identifier that has
    /// been assigned to the message — useful for correlating
    /// publishes with their downstream delivery, but it has no
    /// significance on the wire beyond dedup.
    ///
    /// The next cover tick will transmit the message, so the
    /// *time* at which it appears on the wire is governed by the
    /// cover schedule, not by the call to `publish`.
    ///
    /// # Errors
    ///
    /// - [`Error::PayloadTooLarge`] if the payload is too big to
    ///   fit in a single frame (raise `message_size` or shrink the
    ///   payload).
    /// - [`Error::AppOutboxFull`] if the application outbox is
    ///   saturated (slow down, raise the cover rate, or raise
    ///   `app_outbox_capacity`).
    pub fn publish(&self, payload: &[u8]) -> Result<[u8; 32], Error> {
        self.state.enqueue_real(payload)
    }

    /// Returns a broadcast receiver that yields every frame
    /// received from a peer — whether it originated as a "real"
    /// gossip message or as a peer's cover traffic is **not**
    /// observable on the wire (or by this method).
    ///
    /// The application is responsible for filtering cover frames;
    /// the conventional way to do so is to use a payload format
    /// with a recognizable structure (e.g. a version byte or a
    /// magic header) that random cover bytes will not match by
    /// chance.
    pub fn subscribe(&self) -> broadcast::Receiver<BytesMut> {
        self.state.incoming().subscribe()
    }

    /// Initiates an outbound connection to a peer. If the node is
    /// already connected to that address, the call succeeds
    /// without taking further action.
    ///
    /// # Errors
    ///
    /// Returns any I/O error reported by `pea2pea` (e.g. the
    /// listener is not bound, the address is unreachable, the
    /// connection limits have been reached).
    pub async fn connect(&self, addr: SocketAddr) -> io::Result<()> {
        self.p2p.connect(addr).await
    }

    /// Closes the connection to a peer, if one is currently open.
    /// Returns `true` if a connection was actually torn down.
    pub async fn disconnect(&self, addr: SocketAddr) -> bool {
        self.p2p.disconnect(addr).await
    }

    /// Returns the addresses of currently-connected peers.
    pub fn connected_peers(&self) -> Vec<SocketAddr> {
        self.p2p.connected_addrs()
    }

    /// Returns the bound listening address, or an error if the
    /// node was not configured with a listener.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::AddrNotAvailable`] if no listener
    /// is configured or if the listener has been toggled off.
    pub async fn local_addr(&self) -> io::Result<SocketAddr> {
        self.p2p.listening_addr().await
    }

    /// Returns a reference to the underlying `pea2pea::Node`.
    ///
    /// Exposed so that callers can layer additional `pea2pea`
    /// protocols on top of `peasub` (e.g. a custom `Handshake`)
    /// before calling [`Node::spawn`].
    pub fn p2p(&self) -> &P2pNode {
        &self.p2p
    }

    /// Returns the [`NodeConfig`] this node was built from.
    pub fn config(&self) -> &NodeConfig {
        self.state.config()
    }

    /// Gracefully shuts the node down.
    ///
    /// Sets the shutdown flag (which the cover scheduler polls),
    /// wakes the cover scheduler out of any pending sleep, closes
    /// all connections, and aborts the `pea2pea` background tasks.
    /// Waits for the cover-traffic task to finish. After
    /// `shutdown` returns the node is unusable; callers should
    /// drop it.
    ///
    /// `shutdown` is idempotent: calling it on an already-shut-down
    /// node is a no-op (the second call's `JoinHandle.take()`
    /// finds `None` and skips the await).
    pub async fn shutdown(&self) {
        self.state.shutting_down().store(true, Ordering::SeqCst);
        self.cover.wake();
        self.p2p.shut_down().await;
        // Drop the mutex guard before awaiting the handle to
        // satisfy `clippy::await_holding_lock`.
        let handle = self.cover_handle.lock().take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }
    }
}

impl Pea2Pea for Node {
    fn node(&self) -> &P2pNode {
        &self.p2p
    }
}

impl Reading for Node {
    type Message = BytesMut;
    type Codec = Codec;

    fn codec(&self, _addr: SocketAddr, _side: ConnectionSide) -> Self::Codec {
        Codec::new(
            self.state.config().message_size,
            self.state.config().max_frame_size,
        )
    }

    async fn process_message(&self, _source: SocketAddr, message: Self::Message) {
        self.state.handle_incoming(message);
    }
}

impl Writing for Node {
    type Message = BytesMut;
    type Codec = Codec;

    fn codec(&self, _addr: SocketAddr, _side: ConnectionSide) -> Self::Codec {
        Codec::new(
            self.state.config().message_size,
            self.state.config().max_frame_size,
        )
    }
}
