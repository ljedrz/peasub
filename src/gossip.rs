use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;

use bytes::{BufMut, BytesMut};
use lru::LruCache;
use parking_lot::Mutex;
use rand::Rng;
use tokio::sync::broadcast;

use crate::config::NodeConfig;
use crate::error::Error;

/// Size, in bytes, of the message-identifier field that prefixes every
/// gossip frame. Chosen to be wide enough to make accidental collisions
/// between two unrelated messages astronomically unlikely.
pub const ID_SIZE: usize = 32;

/// Capacity of the broadcast channel used to deliver received messages to
/// application subscribers. When the channel is full, the oldest message
/// is dropped and receivers observe a `RecvError::Lagged`.
const SUBSCRIBER_CAPACITY: usize = 1024;

/// The shared, node-wide state that backs every clone of a [`Node`].
///
/// Hides the cover-traffic bookkeeping (outbox, dedup, subscribers) from
/// the pea2pea-facing layer.
///
/// Two queues are maintained:
///
/// - `app_outbox` holds messages submitted via [`Node::publish`]. The
///   cover scheduler always drains this queue first, so application
///   data is never delayed by relay traffic. The queue is bounded;
///   [`Error::AppOutboxFull`] is returned once it saturates.
/// - `relay_outbox` holds messages received from peers for re-broadcast.
///   It is drop-oldest: under sustained inflow, the oldest relayed
///   message is discarded to make room. This is appropriate because
///   gossip is a redundant protocol and losing the occasional relay
///   only slows convergence.
///
/// [`Node::publish`]: crate::Node::publish
pub struct GossipState {
    config: NodeConfig,
    incoming: broadcast::Sender<BytesMut>,
    app_outbox: Mutex<VecDeque<BytesMut>>,
    relay_outbox: Mutex<VecDeque<BytesMut>>,
    seen: Mutex<LruCache<[u8; ID_SIZE], ()>>,
    shutting_down: AtomicBool,
}

impl GossipState {
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

    pub fn config(&self) -> &NodeConfig {
        &self.config
    }

    pub fn incoming(&self) -> broadcast::Sender<BytesMut> {
        self.incoming.clone()
    }

    pub fn shutting_down(&self) -> &AtomicBool {
        &self.shutting_down
    }

    /// Enqueue a real (application-originated) message for gossiping.
    ///
    /// The payload is padded to `message_size` with random bytes; on the
    /// wire it is therefore indistinguishable from a cover message. The
    /// returned identifier is the random 32-byte ID that the message has
    /// been assigned; the application can use it (e.g. correlated with its
    /// own bookkeeping) but it has no on-the-wire significance beyond
    /// dedup at intermediate nodes.
    ///
    /// Application messages are placed in a dedicated queue that the
    /// cover scheduler always drains first, so the next cover tick
    /// transmits them regardless of how much relay traffic has piled
    /// up. This preserves the metadata-privacy property (timing is
    /// still locked to the cover schedule; content is still
    /// indistinguishable from cover) while ensuring application
    /// data is delivered promptly.
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

    /// The next message to put on the wire.
    ///
    /// Drains the application outbox first, then the relay outbox,
    /// and finally falls back to a freshly-generated cover message.
    /// All three paths produce a frame of the same on-the-wire size,
    /// so an observer cannot distinguish them by length.
    pub fn next_outbound(&self) -> BytesMut {
        if let Some(msg) = self.app_outbox.lock().pop_front() {
            return msg;
        }
        if let Some(msg) = self.relay_outbox.lock().pop_front() {
            return msg;
        }
        self.random_cover()
    }

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
    /// Drops frames with the wrong size, deduplicates against the LRU of
    /// recently-seen identifiers, and otherwise fans the message out to
    /// application subscribers and re-queues it for forwarding.
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

        // Subscribers see the message even if it does not make it into
        // the outbox for re-broadcast; gossip is best-effort.
        let _ = self.incoming.send(message.clone());

        // Insert the freshly-received frame at the **front** of the
        // relay outbox so that the very next cover tick forwards it
        // (pop_front). Without this, a message that arrives behind a
        // backlog of older relays has to wait its turn in the FIFO
        // and the gossip chain stalls. The drop-oldest eviction
        // operates on the back of the deque, so fresh messages are
        // preserved while stale ones are discarded first.
        let mut relay = self.relay_outbox.lock();
        while relay.len() >= self.config.relay_outbox_capacity {
            relay.pop_back();
        }
        relay.push_front(message);
    }
}
