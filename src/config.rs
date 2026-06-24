//! Configuration types for a [`Node`].
//!
//! [`Node`]: crate::Node

use std::time::Duration;

use peashape::{ShapeConfig, ShapingStrategy};

/// The set of parameters that govern a [`Node`].
///
/// Most callers only need to set [`NodeConfig::cover`]; every
/// other field has a sensible default. To see what the defaults
/// are, use [`NodeConfig::default`] and read the resulting
/// struct.
///
/// Internally a `NodeConfig` is translated into a
/// [`peashape::ShapeConfig`] (and the gossip-specific bits that
/// `peasub` adds on top of `peashape`).
///
/// [`Node`]: crate::Node
#[derive(Clone, Debug)]
pub struct NodeConfig {
    /// A friendly identifier of the node, surfaced in `tracing`
    /// output. If `None`, `pea2pea` assigns a numeric ID.
    pub name: Option<String>,

    /// The local socket address to bind the listener to. If
    /// `None`, the node will not accept inbound connections (it
    /// can still initiate outbound ones via [`Node::connect`]).
    ///
    /// [`Node::connect`]: crate::Node::connect
    pub listener_addr: Option<std::net::SocketAddr>,

    /// The strategy used to schedule outgoing traffic. This is
    /// the central knob controlling the metadata-privacy
    /// properties of the node; see [`CoverStrategy`] for the
    /// two options.
    pub cover: CoverStrategy,

    /// Number of distinct peers each outbound frame is forwarded
    /// to on every cover tick. Higher fanout → faster gossip
    /// convergence at the cost of proportionally more
    /// bandwidth.
    ///
    /// `fanout = 1` preserves the original fanout-1 random-walk
    /// behavior; production deployments should typically use
    /// 3–6 (the standard gossip-sub range) so that a single
    /// message reaches the whole overlay in `O(log N)` hops
    /// rather than `O(N)` hops.
    ///
    /// The effective fanout is clamped to the number of
    /// connected peers on every tick, so a node with fewer
    /// peers than `fanout` simply sends to all of them.
    ///
    /// # Bandwidth
    ///
    /// Total outbound bandwidth is
    /// `fanout * message_size * cover_rate`. Raising `fanout`
    /// raises bandwidth linearly but does **not** change the
    /// timing distribution of outbound traffic (every tick
    /// still emits exactly `fanout` frames, real or cover), so
    /// the metadata-privacy property is preserved.
    pub fanout: usize,

    /// The on-the-wire size, in bytes, of every gossip frame.
    ///
    /// All frames (real and cover) are padded to exactly this
    /// size, so an observer cannot distinguish the two by
    /// length. The first [`ID_SIZE`](crate::ID_SIZE) bytes of
    /// every frame are a random message identifier used for
    /// dedup; the remainder is the application payload (for
    /// real messages) or random bytes (for cover).
    ///
    /// Must be greater than [`ID_SIZE`](crate::ID_SIZE) and at
    /// most `max_frame_size`.
    pub message_size: usize,

    /// Maximum number of application-submitted messages that
    /// the node will hold at any given time. The cover
    /// scheduler always drains this queue before relaying or
    /// generating cover, so messages submitted via
    /// [`Node::publish`] are transmitted on the very next
    /// cover tick regardless of how much relay traffic has
    /// piled up. Once this queue is full, further `publish`
    /// calls return [`Error::AppOutboxFull`].
    ///
    /// [`Node::publish`]: crate::Node::publish
    /// [`Error::AppOutboxFull`]: crate::Error::AppOutboxFull
    pub app_outbox_capacity: usize,

    /// Maximum number of relayed (peer-received) messages that
    /// the node will hold for re-broadcast. The relay outbox
    /// is LIFO with drop-oldest eviction: each
    /// freshly-received frame is pushed to the front, and the
    /// next cover tick pops the front. Under sustained
    /// inflow, the oldest queued relay is discarded first.
    ///
    /// With fanout `f`, a cover rate of `r` ticks/s, and `n`
    /// connected peers, the average relay inflow is
    /// `f * r` frames/s (worst-case burst is `n * f * r` if
    /// every peer happens to forward to this node on the same
    /// tick). A capacity of `f * r * drain_time_seconds` keeps
    /// eviction rare under average load; size for the burst if
    /// propagation latency is critical.
    pub relay_outbox_capacity: usize,

    /// Capacity of the LRU cache used to suppress re-broadcast
    /// of recently-seen message identifiers. Larger values
    /// reduce redundant forwarding at the cost of memory. The
    /// cache evicts the least-recently-seen ID when full.
    pub dedup_capacity: usize,

    /// Upper bound, in bytes, on a single frame the decoder
    /// will accept. Frames larger than this are rejected and
    /// the connection is torn down by `pea2pea`. The
    /// configured `message_size` must not exceed this.
    pub max_frame_size: usize,

    /// Maximum number of simultaneously-active connections.
    pub max_connections: u16,

    /// Maximum number of connections to a single IP address.
    /// The `pea2pea` default of `1` is too restrictive for
    /// gossip (every node typically has at least one
    /// connection to each of its peers, which all share the
    /// same IP in loopback tests), so this defaults to `8`.
    pub max_connections_per_ip: u16,

    /// Whether to set `SO_REUSEPORT` on the listener socket,
    /// which allows multiple `peasub` nodes to bind the same
    /// address simultaneously and have the kernel
    /// load-balance inbound connections across them. Useful
    /// for zero-downtime upgrades and sharded listeners. Has
    /// no effect on platforms that do not support
    /// `SO_REUSEPORT` (in which case the listener fails to
    /// bind).
    pub reuse_listener_port: bool,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            name: None,
            listener_addr: None,
            // 1 message/second is the most conservative default;
            // the operator should raise this in line with their
            // expected publish rate.
            cover: CoverStrategy::Constant {
                interval: Duration::from_secs(1),
            },
            // 3 is the standard gossip-sub fanout: enough for
            // O(log N) convergence in typical overlays, modest
            // enough that the default 1 s cover interval stays
            // cheap. Raise it for denser overlays or tighter
            // convergence requirements.
            fanout: 3,
            // 256 bytes = 32-byte ID + 224 bytes of payload. Big
            // enough for a short application message after the
            // ID, small enough to keep per-connection memory
            // low.
            message_size: 256,
            // 256 application messages ≈ 4 minutes of slack at
            // 1 msg/s, which is enough for any reasonable burst.
            app_outbox_capacity: 256,
            // 1024 relay slots — with the LIFO discipline only
            // the most recent frames survive, so this is mostly
            // relevant under very high relay inflow.
            relay_outbox_capacity: 1024,
            // 4096 seen IDs ≈ 1.1 hours of unique IDs at
            // 1 msg/s before the LRU starts evicting.
            dedup_capacity: 4096,
            max_frame_size: 1024 * 1024,
            max_connections: 64,
            max_connections_per_ip: 8,
            reuse_listener_port: false,
        }
    }
}

impl NodeConfig {
    /// Translates this `NodeConfig` into the underlying
    /// [`peashape::ShapeConfig`] that drives wire-level
    /// shaping.
    pub(crate) fn to_shape_config(&self) -> ShapeConfig {
        ShapeConfig {
            name: self.name.clone(),
            listener_addr: self.listener_addr,
            strategy: match self.cover {
                CoverStrategy::Constant { interval } => ShapingStrategy::Constant { interval },
                CoverStrategy::Poisson { rate } => ShapingStrategy::Poisson { rate },
            },
            scope: peashape::ShapingScope::Global,
            fanout: self.fanout,
            frame_size: self.message_size,
            high_lane_capacity: self.app_outbox_capacity,
            low_lane_capacity: self.relay_outbox_capacity,
            max_frame_size: self.max_frame_size,
            max_connections: self.max_connections,
            max_connections_per_ip: self.max_connections_per_ip,
            reuse_listener_port: self.reuse_listener_port,
            // Fill any remaining `ShapeConfig` fields (e.g. a custom
            // cover generator) with their defaults. Spreading the
            // default rather than listing every field keeps this
            // conversion compiling across peashape versions that add
            // new fields.
            ..ShapeConfig::default()
        }
    }
}

/// How the node generates outbound traffic.
///
/// The two strategies differ only in the inter-arrival timing
/// of cover messages; the *total* outgoing rate (and the
/// indistinguishability of real vs. cover frames on the wire)
/// is preserved.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum CoverStrategy {
    /// Emit one cover frame exactly every `interval`.
    ///
    /// The simplest and most predictable schedule. The outgoing
    /// bandwidth is `message_size / interval` per peer-pair;
    /// suitable when peers have a loose real-time sync and the
    /// cover rate is not too high.
    Constant {
        /// The fixed delay between consecutive cover frames.
        interval: Duration,
    },

    /// Emit cover frames with inter-arrival times drawn from
    /// `Exp(rate)`, i.e. a Poisson process with mean
    /// inter-arrival time `1 / rate` seconds.
    ///
    /// Because the schedule itself is randomized, an observer
    /// who can only see *the node's* traffic cannot
    /// statistically distinguish a Poisson-scheduled cover
    /// stream from a stream whose inter-arrival times are
    /// influenced by user activity. This is the
    /// "metadata-private" choice in the strictest sense: the
    /// observed process is itself a Poisson process regardless
    /// of what the application does.
    Poisson {
        /// The Poisson-process rate, in frames per second.
        rate: f64,
    },
}
