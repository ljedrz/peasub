use std::time::Duration;

/// The set of parameters that govern a `Node`.
///
/// Reasonable defaults are provided via `Default`; the only field most callers
/// will need to touch is `cover`, which selects the cover-traffic schedule.
#[derive(Clone, Debug)]
pub struct NodeConfig {
    /// A friendly identifier of the node; appears in `tracing` output.
    pub name: Option<String>,

    /// The local socket address to bind the listener to. If `None`, the node
    /// will not accept inbound connections (it can still initiate outbound
    /// ones).
    pub listener_addr: Option<std::net::SocketAddr>,

    /// The strategy used to schedule outgoing traffic. This is the central
    /// knob controlling the metadata-privacy properties of the node.
    pub cover: CoverStrategy,

    /// The on-the-wire size, in bytes, of every gossip frame.
    ///
    /// All frames (real or cover) are padded to this size, so an observer
    /// cannot distinguish the two by length. The first [`gossip::ID_SIZE`]
    /// bytes of every frame are a message identifier used for dedup; the
    /// remainder is payload.
    ///
    /// [`gossip::ID_SIZE`]: crate::gossip::ID_SIZE
    pub message_size: usize,

    /// Maximum number of application-submitted messages that the node
    /// will hold at any given time. The cover scheduler always drains
    /// this queue before generating cover traffic, so messages
    /// submitted via [`Node::publish`] are transmitted on the very
    /// next cover tick regardless of how much relay traffic has piled
    /// up. Once this queue is full, further `publish` calls return
    /// [`Error::AppOutboxFull`].
    ///
    /// [`Node::publish`]: crate::Node::publish
    /// [`Error::AppOutboxFull`]: crate::Error::AppOutboxFull
    pub app_outbox_capacity: usize,

    /// Maximum number of relayed (peer-received) messages that the
    /// node will hold for re-broadcast. The relay outbox is
    /// drop-oldest: when it is full, the oldest entry is discarded to
    /// make room for a new one. With `n` peers per node and a cover
    /// rate of `r` messages per second, the steady-state inflow is
    /// `(n - 1) * r`; a capacity of `(n - 1) * r * drain_time_seconds`
    /// keeps eviction rare.
    pub relay_outbox_capacity: usize,

    /// Capacity of the LRU cache used to suppress re-broadcast of recently
    /// seen message identifiers. Larger values reduce redundant forwarding
    /// at the cost of memory.
    pub dedup_capacity: usize,

    /// Upper bound, in bytes, on a single frame the decoder will accept.
    /// Frames larger than this are rejected (the connection is torn down
    /// by `pea2pea`). The configured `message_size` must not exceed this.
    pub max_frame_size: usize,

    /// Maximum number of simultaneously-active connections.
    pub max_connections: u16,

    /// Maximum number of connections to a single IP address.
    pub max_connections_per_ip: u16,

    /// Whether newly accepted connections should immediately be torn down
    /// if they cannot immediately produce a valid frame. Disabling this is
    /// a small DoS mitigation; left enabled for compatibility with the
    /// length-delimited framing.
    pub reuse_listener_port: bool,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            name: None,
            listener_addr: None,
            cover: CoverStrategy::Constant {
                interval: Duration::from_secs(1),
            },
            message_size: 256,
            app_outbox_capacity: 256,
            relay_outbox_capacity: 1024,
            dedup_capacity: 4096,
            max_frame_size: 1024 * 1024,
            max_connections: 64,
            max_connections_per_ip: 8,
            reuse_listener_port: false,
        }
    }
}

/// How the node generates outbound traffic.
///
/// The two strategies differ only in the inter-arrival timing of cover
/// messages; the *total* outgoing rate (and the indistinguishability of
/// real vs. cover frames on the wire) is preserved.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum CoverStrategy {
    /// Emit one cover frame exactly every `interval`.
    ///
    /// The simplest and most predictable schedule. The outgoing bandwidth
    /// is `message_size / interval` per peer-pair; suitable when peers
    /// have a loose real-time sync and the cover rate is not too high.
    Constant { interval: Duration },

    /// Emit cover frames with inter-arrival times drawn from
    /// `Exp(rate)`, i.e. a Poisson process with mean inter-arrival
    /// time `1 / rate` seconds.
    ///
    /// Because the schedule itself is randomized, an observer who can
    /// only see *the node's* traffic cannot statistically distinguish
    /// a Poisson-scheduled cover stream from a stream whose inter-arrival
    /// times are influenced by user activity. This is the
    /// "metadata-private" choice in the strictest sense.
    Poisson { rate: f64 },
}
