//! `peasub` — a metadata-private gossip protocol built on top of
//! [`pea2pea`].
//!
//! # Overview
//!
//! `peasub` provides a *gossip* substrate in which every node emits
//! outbound traffic at a constant rate (or, optionally, with
//! Poisson-distributed inter-arrival times). Real application messages
//! are interleaved with cover traffic drawn from the same code path,
//! so an external observer — and even the receiving peer — cannot
//! distinguish a "real" frame from a "cover" frame, nor can they infer
//! when a node has user activity.
//!
//! The privacy property is **not** free: real messages are paced by
//! the cover rate, so a node that wants to send at 10 msg/s must
//! configure a cover rate of at least 10 msg/s, and the bandwidth
//! cost is the same as if it were sending cover frames 100% of the
//! time. The cover rate is the only knob that matters.
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
//! let local_addr = node.spawn().await?;
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
//! `peasub` is designed to defeat a *passive global network observer*
//! who can:
//!
//! - observe every byte sent between every pair of nodes;
//! - observe the timing of every byte;
//! - but cannot break the cryptographic primitives protecting the
//!   link (e.g. TLS via a `pea2pea` `Handshake`).
//!
//! Against such an observer, the cover-traffic schedule ensures that
//! the *timing distribution* and *size distribution* of a node's
//! outbound traffic are independent of whether the application is
//! publishing messages or not. The observer learns nothing about
//! the existence, frequency, or destination of user activity beyond
//! the rate the node has been configured for.
//!
//! `peasub` does **not** attempt to defeat an observer that can
//! compromise the node itself, that can correlate application-level
//! events with coarse traffic features (e.g. "the user is online
//! between 9am and 5pm"), or that controls a non-trivial fraction of
//! the network's nodes. A complete anonymity system needs more than
//! just cover traffic.
//!
//! # Architecture
//!
//! - [`Node`] wraps a [`pea2pea::Node`] and adds three pieces of
//!   bookkeeping: a bounded outbox of pending real messages, an LRU
//!   cache of recently-seen message IDs used for dedup, and a
//!   background [`CoverScheduler`] that drives outbound traffic.
//! - The [`codec::Codec`] forces every frame on the wire to a single
//!   fixed size (configurable via [`NodeConfig::message_size`]),
//!   so the *length* of a frame is never a tell.
//! - The first `32` bytes of every frame are a message identifier;
//!   real messages receive a random ID at publication time, cover
//!   messages receive a fresh random ID per emission.
//! - When a peer receives a frame, it is deduplicated against the LRU,
//!   delivered to local subscribers, and re-queued for forwarding.
//! - When a tick fires, the scheduler either pops the oldest entry
//!   from the outbox (a real or relayed message) or generates a cover
//!   frame, and ships it to one randomly-chosen connected peer.
//!
//! [`pea2pea`]: pea2pea

mod codec;
mod config;
mod cover;
mod error;
mod gossip;
mod node;

pub use crate::config::{CoverStrategy, NodeConfig};
pub use crate::error::Error;
pub use crate::node::Node;

/// Re-exported for convenience so that callers can wire up a topology
/// in tests without adding `pea2pea` as a direct dependency.
pub use pea2pea::{self, connect_nodes, Topology};
