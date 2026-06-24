# peasub

A metadata-private gossip protocol built on top of [`peashape`],
which in turn is built on [`pea2pea`]. It disseminates messages
through a peer-to-peer network while resisting traffic analysis
via constant-rate or Poisson-distributed cover traffic.

`peasub` is one specific application of `peashape`'s general
traffic-shaping primitive. If you want to build a different
private protocol (private RPC, private file chunks, private
membership, …) you can use `peashape` directly.

## Threat model

`peasub` is designed to defeat a *passive global network
observer* who can:

- observe every byte sent between every pair of nodes;
- observe the timing of every byte;
- but cannot break the cryptographic primitives protecting
  the link (e.g. TLS via a `pea2pea` `Handshake`).

Against such an observer, the cover-traffic schedule ensures
that the *timing distribution* and *size distribution* of a
node's outbound traffic are independent of whether the
application is publishing messages or not. The observer learns
nothing about the existence, frequency, or destination of user
activity beyond the rate the node has been configured for.

`peasub` does **not** attempt to defeat an observer that can
compromise the node itself, that can correlate
application-level events with coarse traffic features, or that
controls a non-trivial fraction of the network's nodes.

## How it works

Every gossip frame on the wire is a fixed-size, length-prefixed
buffer. The first 32 bytes are a random message identifier; the
remainder is the application payload padded with random bytes.
Real (application-submitted) messages and cover (dummy) messages
are indistinguishable on the wire **as long as the payload bytes
are themselves indistinguishable from random** — i.e. encrypted.
A plaintext payload (anything with recognizable structure)
defeats the cover property; see *Receiving messages* for the
recommended pattern.

`peashape` runs a background task that ticks at the configured
cover rate. On every tick, it pulls the next message to send
from one of two priority lanes:

1. The **high-priority lane** — messages submitted via
   `Node::publish`. Always drained first, so user data is never
   delayed by relay traffic. Bounded; `publish` returns
   `Error::AppOutboxFull` if it saturates.
2. The **low-priority lane** — messages received from peers for
   re-broadcast. A drop-oldest LIFO: freshly received frames
   are placed at the front, so the next cover tick forwards
   them immediately. Under sustained inflow, the oldest queued
   relay is discarded first.

If both lanes are empty, a fresh cover message is generated. All
three paths produce a frame of the same on-the-wire size, so an
observer cannot tell them apart by length or by when they
arrive.

The message is then sent to `fanout` distinct randomly-chosen
peers (default 3). Because every tick emits exactly `fanout`
frames of the same size — real or cover — the outbound
traffic's timing and size distributions remain independent of
application activity.

A peer receiving a frame deduplicates it by ID (LRU cache),
delivers it to local subscribers via a
`tokio::sync::broadcast` channel, and re-queues it for
forwarding into the low-priority lane.

## Receiving messages

`Node::subscribe` yields every *novel* frame the node sees on
the wire — real and cover alike. This is intentional: the
library cannot tell them apart (that's the privacy property),
so the responsibility for distinguishing real application data
from random cover bytes falls on the application.

The recommended pattern is **authenticated encryption** over
the payload. This does double duty:

- the AEAD tag is the "recognizable structure" — a frame that
  fails to authenticate is cover (or addressed to another
  key), so you need no plaintext magic header, and there is no
  plaintext tell on the wire;
- ciphertext is indistinguishable from the random cover bytes,
  which is what makes the metadata-privacy claim hold against
  an observer that can read raw frames;
- relaying nodes forward frames they cannot decrypt, so
  content is hidden from intermediate peers too.

Put the true length *inside* the encrypted block and size the
sealed payload to exactly `message_size - ID_SIZE` so `peasub`
adds no padding and the boundaries are deterministic. The Quick
start below is a complete worked example; `cargo run --example
encrypted` runs it end to end.

## Quick start

Two nodes, one real message, end to end. Alice publishes, Bob
receives and extracts the payload from the cover stream:

```rust
use std::time::Duration;
use peasub::{CoverStrategy, Node, NodeConfig, ID_SIZE};
use chacha20poly1305::{aead::{Aead, AeadCore, KeyInit, OsRng}, ChaCha20Poly1305, Key, Nonce};

// With the 256-byte default frame, the app owns message_size - ID_SIZE = 224
// bytes. Minus a 12-byte nonce and 16-byte Poly1305 tag, that leaves a
// fixed 196-byte encrypted block (2 bytes length + up to 194 bytes data).
const PAYLOAD: usize = 256 - ID_SIZE;       // 224
const NONCE: usize = 12;
const TAG: usize = 16;
const INNER: usize = PAYLOAD - NONCE - TAG; // 196
const LEN_HDR: usize = 2;
const MAX_DATA: usize = INNER - LEN_HDR;    // 194

// Encrypt `data` into a full-width frame payload. The contents peasub puts
// on the wire are ciphertext, so a real frame is byte-for-byte
// indistinguishable from a (random) cover frame.
fn seal(cipher: &ChaCha20Poly1305, data: &[u8]) -> Vec<u8> {
    assert!(data.len() <= MAX_DATA, "payload exceeds {MAX_DATA} bytes");
    let mut inner = vec![0u8; INNER];          // zero pad is hidden by encryption
    inner[..LEN_HDR].copy_from_slice(&(data.len() as u16).to_be_bytes());
    inner[LEN_HDR..LEN_HDR + data.len()].copy_from_slice(data);

    let nonce = ChaCha20Poly1305::generate_nonce(&mut OsRng);
    let ct = cipher.encrypt(&nonce, inner.as_ref()).expect("encrypt");
    let mut out = Vec::with_capacity(PAYLOAD); // 12 + 196 + 16 == 224 == PAYLOAD
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&ct);
    out
}

// Returns the plaintext if the frame authenticates, or None for cover frames
// (and frames addressed to a different key) — decryption failure is the filter.
fn open(cipher: &ChaCha20Poly1305, frame: &[u8]) -> Option<Vec<u8>> {
    let payload = frame.get(ID_SIZE..)?;       // strip peasub's 32-byte ID
    let nonce = Nonce::from_slice(payload.get(..NONCE)?);
    let inner = cipher.decrypt(nonce, payload.get(NONCE..)?).ok()?;
    let len = u16::from_be_bytes(inner.get(..LEN_HDR)?.try_into().ok()?) as usize;
    inner.get(LEN_HDR..LEN_HDR + len).map(<[u8]>::to_vec)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // DEMO ONLY: a hard-coded shared key. A real deployment does key
    // agreement (per-group keys, or a pea2pea Noise handshake) — peasub
    // deliberately stays out of the crypto business.
    let cipher = ChaCha20Poly1305::new(Key::from_slice(&[0x42; 32]));

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
    alice.publish(&seal(&cipher, b"hello, peasub"))?;

    // Bob drains frames until one authenticates; cover frames fail to
    // decrypt and are skipped.
    loop {
        if let Ok(frame) = bob_rx.recv().await {
            if let Some(payload) = open(&cipher, &frame) {
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

The crate ships four runnable examples:

- `cargo run --example encrypted` — the AEAD pattern above, end to end.
- `cargo run --example two_nodes` — the minimal subscribe/filter
  mechanics (with a *plaintext* marker — see the note in the file;
  not privacy-preserving, use `encrypted` for that).
- `cargo run --example demo` — visualizes the traffic-analysis
  resistance: constant rate, one frame size, no signal at publish time.
- `cargo run --example poisson_demo` — the same, with a Poisson schedule.

## Choosing the cover rate

The cover rate is the *only* knob that controls the privacy /
bandwidth trade-off:

- **Higher** cover rate → tighter privacy, more bandwidth.
- **Lower** cover rate → looser privacy, less bandwidth.

The application must publish no faster than the cover rate;
otherwise its messages accumulate in the application outbox.
The `relay_outbox_capacity` should be sized to
`fanout * cover_rate * drain_seconds` to keep relay backlog
from crowding out user data.

## Fanout

`fanout` (default 3) controls how many distinct peers each
outbound frame is forwarded to on every cover tick. It trades
convergence speed against bandwidth:

- **Higher** fanout → faster gossip convergence (`O(log N)`
  hops), proportionally more bandwidth.
- **Lower** fanout → slower convergence (`O(N)` hops at fanout
  1), less bandwidth.

Total outbound bandwidth is
`fanout * message_size * cover_rate`. Raising `fanout` does
**not** change the timing distribution of outbound traffic
(every tick still emits exactly `fanout` frames, real or
cover), so the metadata-privacy property is preserved
regardless of the fanout value.

The effective fanout is clamped to the number of connected
peers on every tick, so a node with fewer peers than `fanout`
simply sends to all of them.

## Cover strategies

```rust
pub enum CoverStrategy {
    /// One message per fixed `interval`.
    Constant { interval: Duration },
    /// Inter-arrival times drawn from Exp(rate) - Poisson process.
    Poisson { rate: f64 },
}
```

`Constant` is the simplest and most predictable. `Poisson`
makes the inter-arrival times themselves random, so an
observer who can only see *the node's* traffic cannot
statistically distinguish the Poisson-scheduled cover stream
from one whose timing is influenced by user activity — the
strictest form of metadata privacy.

## Limitations

- **No application-level encryption.** Cover hides the *timing*
  of messages, not their *content*. The application is
  responsible for encrypting payloads if end-to-end
  confidentiality is needed; a `pea2pea` `Handshake` (e.g.
  Noise) can be layered on top for transport confidentiality.
- **TCP only.** Built on `pea2pea`, which is TCP-based. A
  timing adversary that observes TCP-level behavior (Nagle,
  delayed ACKs, etc.) may be able to extract additional
  features; consider disabling Nagle via a `pea2pea`
  `Handshake` that sets `TCP_NODELAY`, or layering
  constant-rate shaping at the application layer if that is
  in scope.

## Building on top of `peasub`

`peasub`'s `Node` exposes the underlying `peashape::Node` via
`node.peashape()`, so applications that want more granular
control over the shaping layer (e.g. a different
`ShapingScope` or a custom Lane) can do so by calling
`peashape::Node` methods directly.

## License

Dual-licensed under MIT or CC0-1.0, at your option.

[`pea2pea`]: https://docs.rs/pea2pea
[`peashape`]: https://docs.rs/peashape
