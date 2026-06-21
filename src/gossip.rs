//! Shared, clone-able state backing every [`Node`].
//!
//! [`Node`]: crate::Node

use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;

use bytes::{BufMut, BytesMut};
use lru::LruCache;
use parking_lot::Mutex;
use rand::Rng;
use tokio::sync::broadcast;

use crate::config::NodeConfig;
use crate::error::Error;

/// Size, in bytes, of the message-identifier field that prefixes
/// every gossip frame. Chosen to be wide enough to make accidental
/// collisions between two unrelated messages astronomically
/// unlikely (~10^-77).
pub const ID_SIZE: usize = 32;

/// Capacity of the broadcast channel used to deliver received
/// messages to application subscribers. When the channel is full,
/// the oldest message is dropped and receivers observe
/// `RecvError::Lagged`.
const SUBSCRIBER_CAPACITY: usize = 1024;

/// The shared, node-wide state that backs every clone of a [`Node`].
///
/// Two queues are maintained:
///
/// - `app_outbox` holds messages submitted via [`Node::publish`].
///   The cover scheduler always drains this queue *first*, so
///   application data is never delayed by relay traffic. The queue
///   is bounded; [`Error::AppOutboxFull`] is returned once it
///   saturates.
/// - `relay_outbox` holds messages received from peers for
///   re-broadcast. It is LIFO with drop-oldest eviction: a
///   freshly-received frame is pushed to the *front* and the very
///   next cover tick will pop and forward it. Under sustained
///   inflow, the oldest queued relay is discarded from the *back*
///   to make room, so fresh messages are preserved at the expense
///   of stale ones. This is appropriate because gossip is a
///   redundant protocol and losing the occasional relay only
///   slows convergence without sacrificing correctness.
///
/// In addition, an LRU of recently-seen identifiers suppresses
/// re-broadcast of frames that have already circulated through
/// this node.
///
/// [`Node`]: crate::Node
/// [`Node::publish`]: crate::Node::publish
/// [`Error::AppOutboxFull`]: crate::Error::AppOutboxFull
pub struct GossipState {
    config: NodeConfig,
    incoming: broadcast::Sender<BytesMut>,
    app_outbox: Mutex<VecDeque<BytesMut>>,
    relay_outbox: Mutex<VecDeque<BytesMut>>,
    seen: Mutex<LruCache<[u8; ID_SIZE], ()>>,
    shutting_down: AtomicBool,
}

impl GossipState {
    /// Builds a fresh `GossipState` from the given configuration.
    ///
    /// # Panics
    ///
    /// Panics if `config.dedup_capacity` is `0` (the LRU constructor
    /// requires a non-zero capacity).
    pub fn new(config: &NodeConfig) -> Self {
        let (incoming, _) = broadcast::channel(SUBSCRIBER_CAPACITY);
        Self {
            config: config.clone(),
            incoming,
            app_outbox: Mutex::new(VecDeque::with_capacity(config.app_outbox_capacity)),
            relay_outbox: Mutex::new(VecDeque::with_capacity(config.relay_outbox_capacity)),
            seen: Mutex::new(LruCache::new(
                std::num::NonZeroUsize::new(config.dedup_capacity)
                    .expect("dedup_capacity must be non-zero"),
            )),
            shutting_down: AtomicBool::new(false),
        }
    }

    /// Returns a reference to the configuration this state was
    /// built from.
    pub fn config(&self) -> &NodeConfig {
        &self.config
    }

    /// Returns a clone of the broadcast sender used to deliver
    /// received messages to application subscribers.
    pub fn incoming(&self) -> broadcast::Sender<BytesMut> {
        self.incoming.clone()
    }

    /// Returns a shared reference to the shutdown flag. The cover
    /// scheduler polls this on every iteration of its loop.
    pub fn shutting_down(&self) -> &AtomicBool {
        &self.shutting_down
    }

    /// Enqueue a real (application-originated) message for
    /// gossiping.
    ///
    /// The payload is padded to `message_size` with random bytes;
    /// on the wire it is therefore indistinguishable from a
    /// cover message. The returned identifier is the random
    /// 32-byte ID that the message has been assigned; the
    /// application can use it (e.g. correlated with its own
    /// bookkeeping) but it has no on-the-wire significance
    /// beyond dedup at intermediate nodes.
    ///
    /// Application messages are placed in a dedicated queue that
    /// the cover scheduler always drains first, so the next cover
    /// tick transmits them regardless of how much relay traffic
    /// has piled up. This preserves the metadata-privacy property
    /// (timing is still locked to the cover schedule; content is
    /// still indistinguishable from cover) while ensuring
    /// application data is delivered promptly.
    ///
    /// # Errors
    ///
    /// - [`Error::PayloadTooLarge`] if `payload.len()` exceeds
    ///   `message_size - ID_SIZE`.
    /// - [`Error::AppOutboxFull`] if the application outbox is at
    ///   capacity.
    pub fn enqueue_real(&self, payload: &[u8]) -> Result<[u8; ID_SIZE], Error> {
        let max_payload = self.config.message_size - ID_SIZE;
        if payload.len() > max_payload {
            return Err(Error::PayloadTooLarge {
                size: payload.len(),
                max: max_payload,
            });
        }

        let id: [u8; ID_SIZE] = rand::rng().random();
        let mut msg = BytesMut::with_capacity(self.config.message_size);
        msg.extend_from_slice(&id);
        msg.extend_from_slice(payload);
        let pad = self.config.message_size - ID_SIZE - payload.len();
        if pad > 0 {
            let mut rng = rand::rng();
            for _ in 0..pad {
                msg.put_u8(rng.random());
            }
        }

        let mut app = self.app_outbox.lock();
        if app.len() >= self.config.app_outbox_capacity {
            return Err(Error::AppOutboxFull);
        }
        app.push_back(msg);
        Ok(id)
    }

    /// Returns the next message to put on the wire.
    ///
    /// Drains the application outbox first, then the relay outbox,
    /// and finally falls back to a freshly-generated cover message.
    /// All three paths produce a frame of the same on-the-wire
    /// size, so an observer cannot distinguish them by length.
    pub fn next_outbound(&self) -> BytesMut {
        if let Some(msg) = self.app_outbox.lock().pop_front() {
            return msg;
        }
        if let Some(msg) = self.relay_outbox.lock().pop_front() {
            return msg;
        }
        self.random_cover()
    }

    /// Returns a freshly-generated cover message: a random
    /// 32-byte ID followed by `message_size - ID_SIZE` random
    /// bytes. Indistinguishable on the wire from a relayed or
    /// application message.
    fn random_cover(&self) -> BytesMut {
        let mut msg = BytesMut::with_capacity(self.config.message_size);
        let id: [u8; ID_SIZE] = rand::rng().random();
        msg.extend_from_slice(&id);
        let mut rng = rand::rng();
        for _ in 0..(self.config.message_size - ID_SIZE) {
            msg.put_u8(rng.random());
        }
        msg
    }

    /// Process a frame received from a peer.
    ///
    /// Silently drops frames of the wrong size (a misbehaving or
    /// non-`peasub` peer). Otherwise, deduplicates the frame
    /// against the LRU of recently-seen identifiers: a frame whose
    /// ID has been seen before is dropped without further
    /// processing. A novel frame is delivered to all current
    /// subscribers via the broadcast channel and re-queued at
    /// the *front* of the relay outbox so that the very next
    /// cover tick will forward it.
    ///
    /// The LIFO discipline of the relay outbox (combined with
    /// drop-oldest eviction) is what makes fanout-1 gossip
    /// converge quickly: a fresh relay is forwarded immediately
    /// rather than waiting in line behind older ones.
    pub fn handle_incoming(&self, message: BytesMut) {
        if message.len() != self.config.message_size {
            return;
        }
        let mut id = [0u8; ID_SIZE];
        id.copy_from_slice(&message[..ID_SIZE]);

        {
            let mut seen = self.seen.lock();
            if seen.contains(&id) {
                return;
            }
            seen.put(id, ());
        }

        // Subscribers see the message even if it does not make it
        // into the outbox for re-broadcast; gossip is best-effort.
        let _ = self.incoming.send(message.clone());

        // Insert the freshly-received frame at the *front* of the
        // relay outbox so that the very next cover tick forwards
        // it (pop_front). Without this, a message that arrives
        // behind a backlog of older relays would have to wait its
        // turn in FIFO and the gossip chain would stall. The
        // drop-oldest eviction operates on the back, so fresh
        // messages are preserved while stale ones are discarded
        // first.
        let mut relay = self.relay_outbox.lock();
        while relay.len() >= self.config.relay_outbox_capacity {
            relay.pop_back();
        }
        relay.push_front(message);
    }
}
