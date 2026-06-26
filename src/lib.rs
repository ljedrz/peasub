//! `peasub` — a metadata-private gossip protocol built on top
//! of [`peashape`], which in turn is built on [`pea2pea`].
//!
//! # What it does
//!
//! `peasub` provides a *gossip* substrate in which every node
//! emits outbound traffic at a constant rate (or, optionally,
//! with Poisson-distributed inter-arrival times). Real
//! application messages are interleaved with cover traffic
//! drawn from the same code path, so a passive observer of
//! the wire cannot distinguish a "real" frame from a "cover"
//! frame, nor infer when a node has user activity.
//!
//! The privacy property is **not** free: real messages are
//! paced by the cover rate, so a node that wants to send at
//! 10 msg/s must configure a cover rate of at least 10 msg/s,
//! and the bandwidth cost is the same as if it were sending
//! cover frames 100% of the time. The cover rate is the only
//! knob that matters; the application should size its publish
//! rate to the cover rate and not exceed it.
//!
//! # Quick start
//!
//! ```no_run
//! use std::time::Duration;
//! use peasub::{CoverStrategy, Node, NodeConfig};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let node = Node::new(NodeConfig {
//!     name: Some("alice".into()),
//!     listener_addr: Some("127.0.0.1:0".parse()?),
//!     cover: CoverStrategy::Constant {
//!         interval: Duration::from_millis(100),
//!     },
//!     ..Default::default()
//! });
//!
//! let _local_addr = node.spawn().await?;
//!
//! // subscribe to incoming frames
//! let mut rx = node.subscribe();
//!
//! // submit a real message
//! node.publish(b"hello, peasub")?;
//!
//! // connect to a peer (any peer running a compatible `Node`)
//! // node.connect(remote_addr).await?;
//!
//! // ... process received frames ...
//! # drop(rx);
//! node.shutdown().await;
//! # Ok(()) }
//! ```
//!
//! # Threat model
//!
//! `peasub` is designed to defeat a *passive global network
//! observer* who can:
//!
//! - observe every byte sent between every pair of nodes;
//! - observe the timing of every byte;
//! - but cannot break the cryptographic primitives
//!   protecting the link (e.g. TLS via a `pea2pea`
//!   `Handshake`).
//!
//! Against such an observer, the cover-traffic schedule
//! ensures that the *timing distribution* and *size
//! distribution* of a node's outbound traffic are independent
//! of whether the application is publishing messages or not.
//! The observer learns nothing about the existence, frequency,
//! or destination of user activity beyond the rate the node
//! has been configured for.
//!
//! `peasub` does **not** attempt to defeat:
//!
//! - an observer that can compromise the node itself;
//! - side channels outside the network (e.g. a
//!   screen-snooping adversary, or a process that visibly
//!   burns CPU only when the user is active);
//! - an observer that controls a non-trivial fraction of the
//!   network's nodes and can correlate across them;
//! - traffic *content* analysis: cover hides *when* messages
//!   are sent, not *what* they say. End-to-end payload
//!   confidentiality is the application's responsibility (or
//!   can be layered on via a `pea2pea` `Handshake`).
//!
//! # Architecture
//!
//! - [`Node`] wraps a [`peashape::Node`] (which in turn wraps
//!   a [`pea2pea::Node`]) and adds two pieces of bookkeeping:
//!   an LRU of recently-seen message IDs (for dedup), and a
//!   background task that subscribes to the peashape incoming
//!   broadcast and re-enqueues novel frames into peashape's
//!   low-priority lane for fanout. The high-priority lane is
//!   used for application-submitted messages
//!   ([`Node::publish`]); the low-priority lane is used for
//!   relays.
//! - The length-delimited codec forces every frame on the
//!   wire to a single fixed size (configurable via
//!   [`NodeConfig::message_size`]), so the *length* of a
//!   frame is never a tell.
//! - The first [`ID_SIZE`] bytes of every frame are a random
//!   message identifier; real messages receive a random ID
//!   at publication time, cover messages receive a fresh
//!   random ID per emission. The receiver's LRU dedups on
//!   this field, which is the only piece of the frame the
//!   application could meaningfully leak through.
//! - When a peer receives a frame, it is deduplicated
//!   against the LRU, delivered to local subscribers, and
//!   re-queued into the peashape low-priority lane. The LIFO
//!   discipline of the low-priority lane forwards a fresh
//!   relay on the very next cover tick.
//! - When a tick fires, the peashape scheduler either pulls
//!   from the high-priority lane, then the low-priority lane,
//!   or generates a cover frame — and ships it to `fanout`
//!   randomly-chosen connected peers. Every tick emits
//!   exactly `fanout` frames of the same on-the-wire size, so
//!   the metadata-privacy property is preserved regardless of
//!   `fanout`.
//!
//! # Choosing the cover rate
//!
//! The cover rate is the *only* knob that controls the
//! privacy / bandwidth trade-off. As a rule of thumb:
//!
//! - the application should publish no faster than the cover
//!   rate, otherwise its messages accumulate in the
//!   high-priority lane;
//! - `relay_outbox_capacity` (the low-priority lane capacity)
//!   should be sized to
//!   `fanout * cover_rate * drain_seconds` to keep relay
//!   backlog from crowding out user data.
//!
//! # Composition with `peashape`
//!
//! Every byte that hits the wire goes through
//! [`peashape`]'s scheduler and codec, so `peasub` inherits
//! the metadata-privacy property for free. Conversely, the
//! `peashape` library is a generic, application-agnostic
//! substrate; `peasub` is one specific application (gossip).
//! If you want to build a different application on the same
//! property — private RPC, private pub/sub, private file
//! chunking, private membership — use `peashape` directly.
//!
//! [`pea2pea`]: pea2pea

#![deny(missing_docs)]
#![deny(unsafe_code)]
#![deny(rustdoc::broken_intra_doc_links)]

mod config;
mod error;
mod gossip;
mod node;

pub use crate::config::{CoverStrategy, NodeConfig};
pub use crate::error::Error;
pub use crate::node::Node;

/// Re-exported for backward compatibility: the random message
/// identifier is sized by the peashape convention.
pub use peashape::ID_SIZE;

/// Re-exported so that callers can wire up a topology in
/// tests without adding `pea2pea` as a direct dependency.
pub use pea2pea::{self, Topology, connect_nodes};
