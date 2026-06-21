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
/// Internally, a `Node` is a `pea2pea::Node` plus a small layer of
/// metadata-private bookkeeping: a shared outbox, an LRU of recently
/// seen message IDs, and a background task that drains the outbox at a
/// constant (or Poisson-distributed) rate, generating cover traffic when
/// the application has nothing to send.
///
/// `Node` is cheap to clone — clones share the same outbox, dedup
/// cache, and cover-traffic task.
#[derive(Clone)]
pub struct Node {
    p2p: P2pNode,
    state: Arc<GossipState>,
    cover: Arc<CoverScheduler>,
    cover_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl Node {
    /// Constructs a new, unstarted `Node`. Call [`Node::spawn`] to
    /// start the listener, enable the reading/writing protocols, and
    /// launch the cover-traffic scheduler.
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
            "message_size must exceed ID_SIZE ({} bytes)",
            crate::gossip::ID_SIZE,
        );
        assert!(
            config.message_size <= config.max_frame_size,
            "message_size must not exceed max_frame_size",
        );
        assert!(
            config.app_outbox_capacity > 0,
            "app_outbox_capacity must be non-zero"
        );
        assert!(
            config.relay_outbox_capacity > 0,
            "relay_outbox_capacity must be non-zero"
        );

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
    /// Enables the `Reading` and `Writing` protocols, brings up the
    /// listener (if one is configured), and spawns the cover-traffic
    /// scheduler. Returns the bound listening address, or `None` if the
    /// node was configured without a listener.
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

    /// Submits a real (application-originated) message to be gossiped
    /// through the network.
    ///
    /// The payload is automatically padded to the wire-level message
    /// size with random bytes; an external observer cannot distinguish
    /// the resulting frame from a cover frame. Returns the random
    /// 32-byte identifier that has been assigned to the message.
    ///
    /// If the local outbox is saturated (the application is publishing
    /// faster than the cover rate for an extended period), this returns
    /// `Error::OutboxFull` and the message is dropped.
    pub fn publish(&self, payload: &[u8]) -> Result<[u8; 32], Error> {
        self.state.enqueue_real(payload)
    }

    /// Returns a broadcast receiver that yields every frame received
    /// from a peer — whether it originated as a "real" gossip message
    /// or as a peer's cover traffic is **not** observable on the wire.
    ///
    /// The application is responsible for filtering cover frames; the
    /// conventional way to do so is to use a payload format with a
    /// recognizable structure (e.g. a version byte or a magic header)
    /// that random cover bytes will not match by chance.
    pub fn subscribe(&self) -> broadcast::Receiver<BytesMut> {
        self.state.incoming().subscribe()
    }

    /// Initiates an outbound connection to a peer. If the node is
    /// already connected to that address, the call succeeds without
    /// taking further action.
    pub async fn connect(&self, addr: SocketAddr) -> io::Result<()> {
        self.p2p.connect(addr).await
    }

    /// Closes the connection to a peer, if one is currently open.
    pub async fn disconnect(&self, addr: SocketAddr) -> bool {
        self.p2p.disconnect(addr).await
    }

    /// Returns the addresses of currently-connected peers.
    pub fn connected_peers(&self) -> Vec<SocketAddr> {
        self.p2p.connected_addrs()
    }

    /// Returns the bound listening address, or an error if the node was
    /// not configured with a listener.
    pub async fn local_addr(&self) -> io::Result<SocketAddr> {
        self.p2p.listening_addr().await
    }

    /// Returns a reference to the underlying `pea2pea::Node`. Exposed
    /// so that callers can hook in additional `pea2pea` protocols
    /// (e.g. a custom `Handshake`) before calling [`Node::spawn`].
    pub fn p2p(&self) -> &P2pNode {
        &self.p2p
    }

    /// Returns the `NodeConfig` this node was built from.
    pub fn config(&self) -> &NodeConfig {
        self.state.config()
    }

    /// Gracefully shuts the node down.
    ///
    /// Stops the cover-traffic scheduler, closes all connections, and
    /// aborts the `pea2pea` background tasks. After `shutdown` returns
    /// the node is unusable; callers should drop it.
    pub async fn shutdown(&self) {
        self.state.shutting_down().store(true, Ordering::SeqCst);
        self.cover.wake();
        self.p2p.shut_down().await;
        // Drop the mutex guard before awaiting the handle to satisfy
        // `clippy::await_holding_lock`.
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
