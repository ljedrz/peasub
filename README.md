# peasub

A metadata-private gossip protocol built on top of [`pea2pea`]. It disseminates
messages through a peer-to-peer network while resisting traffic analysis via
constant-rate or Poisson-distributed cover traffic.

## Threat model

`peasub` is designed to defeat a *passive global network observer* who can:

- observe every byte sent between every pair of nodes;
- observe the timing of every byte;
- but cannot break the cryptographic primitives protecting the link
  (e.g. TLS via a `pea2pea` `Handshake`).

Against such an observer, the cover-traffic schedule ensures that the
*timing distribution* and *size distribution* of a node's outbound
traffic are independent of whether the application is publishing
messages or not. The observer learns nothing about the existence,
frequency, or destination of user activity beyond the rate the node
has been configured for.

`peasub` does **not** attempt to defeat an observer that can compromise
the node itself, that can correlate application-level events with
coarse traffic features, or that controls a non-trivial fraction of
the network's nodes.

## How it works

Every gossip frame on the wire is a fixed-size, length-prefixed
buffer. The first 32 bytes are a random message identifier; the
remainder is the application payload padded with random bytes. Real
(application-submitted) messages and cover (dummy) messages are
indistinguishable on the wire.
A background task ticks at the configured cover rate. On every
tick, it pulls the next message to send from one of two internal
queues:

1. The **application outbox** — messages submitted via `Node::publish`.
   Always drained first, so user data is never delayed by relay
   traffic. Bounded; `publish` returns `Error::AppOutboxFull` if it
   saturates.
2. The **relay outbox** — messages received from peers for
   re-broadcast. A drop-oldest LIFO: freshly received frames are
   placed at the front, so the next cover tick forwards them
   immediately. Under sustained inflow, the oldest queued relay is
   discarded first.

If both queues are empty, a fresh cover message is generated. All
three paths produce a frame of the same on-the-wire size, so an
observer cannot tell them apart by length or by when they arrive.

The message is then sent to `fanout` distinct randomly-chosen
peers (default 3). Because every tick emits exactly `fanout` frames
of the same size — real or cover — the outbound traffic's timing
and size distributions remain independent of application activity.

A peer receiving a frame deduplicates it by ID (LRU cache), delivers
it to local subscribers via a `tokio::sync::broadcast` channel, and
re-queues it for forwarding.

## Receiving messages

`Node::subscribe` yields *every* frame the node sees on the wire —
real and cover alike. This is intentional: the library cannot tell
them apart (that's the privacy property), so the responsibility for
distinguishing real application data from random cover bytes falls
on the application.

The conventional pattern is to frame your payloads with a
recognizable structure that random cover bytes will not match by
chance:

- a **magic header** (4–8 bytes) makes accidental collisions
  negligible (`1/2^32` per frame for a 4-byte header);
- a **length field** after the header lets the receiver strip the
  random padding `peasub` appends to fill the fixed `message_size`;
- for stronger guarantees, use a **MAC** or **authenticated
  encryption** over the payload — this also defeats cover-frame
  forgery by a malicious peer.

See `examples/two_nodes.rs` for a complete worked example of this
pattern.
## Quick start

Two nodes, one real message, end to end. Alice publishes, Bob
receives and extracts the payload from the cover stream:

```rust
use std::time::Duration;
use peasub::{CoverStrategy, Node, NodeConfig, ID_SIZE};

const MAGIC: &[u8; 4] = b"PESU";

fn frame_payload(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(MAGIC.len() + 1 + data.len());
    out.extend_from_slice(MAGIC);
    out.push(data.len() as u8);
    out.extend_from_slice(data);
    out
}

fn extract_payload(frame: &[u8]) -> Option<&[u8]> {
    let payload = frame.get(ID_SIZE..)?;
    let rest = payload.strip_prefix(MAGIC)?;
    let len = *rest.first()? as usize;
    rest.get(1..1 + len)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mk = || Node::new(NodeConfig {
        listener_addr: Some("127.0.0.1:0".parse()?),
        cover: CoverStrategy::Constant { interval: Duration::from_millis(100) },
        ..Default::default()
    });

    let alice = mk();
    let bob = mk();
    alice.spawn().await?;
    bob.spawn().await?;

    let bob_addr = bob.local_addr().await?;
    alice.connect(bob_addr).await?;

    let mut bob_rx = bob.subscribe();
    alice.publish(&frame_payload(b"hello, peasub"))?;

    // Bob drains frames until the real one arrives; cover frames
    // are skipped by extract_payload.
    loop {
        if let Ok(frame) = bob_rx.recv().await {
            if let Some(payload) = extract_payload(&frame) {
                println!("Bob got: {:?}", String::from_utf8_lossy(payload));
                break;
            }
        }
    }

    alice.shutdown().await;
    bob.shutdown().await;
    Ok(())
}
```

Run `cargo run --example two_nodes` to see this in action, or
`cargo run --example demo` to see the traffic-analysis-resistance
property visualized.

## Choosing the cover rate

The cover rate is the *only* knob that controls the privacy /
bandwidth trade-off:

- **Higher** cover rate → tighter privacy, more bandwidth.
- **Lower** cover rate → looser privacy, less bandwidth.

The application must publish no faster than the cover rate; otherwise
its messages accumulate in the application outbox. The
`relay_outbox_capacity` should be sized to `fanout * cover_rate *
drain_seconds` to keep relay backlog from crowding out user data.

## Fanout

`fanout` (default 3) controls how many distinct peers each outbound
frame is forwarded to on every cover tick. It trades convergence
speed against bandwidth:

- **Higher** fanout → faster gossip convergence (`O(log N)` hops),
  proportionally more bandwidth.
- **Lower** fanout → slower convergence (`O(N)` hops at fanout 1),
  less bandwidth.

Total outbound bandwidth is `fanout * message_size * cover_rate`.
Raising `fanout` does **not** change the timing distribution of
outbound traffic (every tick still emits exactly `fanout` frames,
real or cover), so the metadata-privacy property is preserved
regardless of the fanout value.

The effective fanout is clamped to the number of connected peers on
every tick, so a node with fewer peers than `fanout` simply sends to
all of them.

## Cover strategies

```rust
pub enum CoverStrategy {
    /// One message per fixed `interval`.
    Constant { interval: Duration },
    /// Inter-arrival times drawn from Exp(rate) - Poisson process.
    Poisson { rate: f64 },
}
```

`Constant` is the simplest and most predictable. `Poisson` makes the
inter-arrival times themselves random, so an observer who can only
see *the node's* traffic cannot statistically distinguish the
Poisson-scheduled cover stream from one whose timing is influenced
by user activity — the strictest form of metadata privacy.

## Limitations

- **No application-level encryption.** Cover hides the *timing* of
  messages, not their *content*. The application is responsible for
  encrypting payloads if end-to-end confidentiality is needed; a
  `pea2pea` `Handshake` (e.g. Noise) can be layered on top for
  transport confidentiality.
- **TCP only.** Built on `pea2pea`, which is TCP-based. A timing
  adversary that observes TCP-level behavior (Nagle, delayed ACKs,
  etc.) may be able to extract additional features; consider
  disabling Nagle via a `pea2pea` `Handshake` that sets
  `TCP_NODELAY`, or layering constant-rate shaping at the
  application layer if that is in scope.

## License

Dual-licensed under MIT or CC0-1.0, at your option.
