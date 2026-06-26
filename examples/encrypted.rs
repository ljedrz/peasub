//! The **recommended** end-to-end pattern: authenticated encryption
//! over the payload.
//!
//! This is the privacy-correct way to use `peasub`. Unlike the
//! plaintext magic-header approach in `two_nodes.rs` (a teaching
//! simplification that puts a recognizable marker on the wire), here
//! every real frame is ciphertext — byte-for-byte indistinguishable
//! from a random cover frame to a passive observer. The receiver
//! distinguishes real from cover by *decryption*: a frame that fails
//! to authenticate is cover (or addressed to another key), so no
//! plaintext tell is ever exposed on the wire.
//!
//! The AEAD tag does triple duty:
//!   - it is the "recognizable structure" the receiver filters on, so
//!     no plaintext magic header is needed;
//!   - ciphertext is indistinguishable from random cover bytes; and
//!   - relaying peers forward frames they cannot decrypt, so content
//!     is hidden from intermediate hops too.
//!
//! Run with: cargo run --example encrypted

use std::time::Duration;

use chacha20poly1305::{
    ChaCha20Poly1305, Key, Nonce,
    aead::{Aead, AeadCore, KeyInit, OsRng},
};
use peasub::{CoverStrategy, ID_SIZE, Node, NodeConfig};

// With the 256-byte default frame, the app owns message_size - ID_SIZE
// = 224 bytes. Minus a 12-byte nonce and 16-byte Poly1305 tag, that
// leaves a fixed 196-byte encrypted block (2 bytes length + up to 194
// bytes of data).
const PAYLOAD: usize = 256 - ID_SIZE; // 224
const NONCE: usize = 12;
const TAG: usize = 16;
const INNER: usize = PAYLOAD - NONCE - TAG; // 196
const LEN_HDR: usize = 2;
const MAX_DATA: usize = INNER - LEN_HDR; // 194

/// Encrypts `data` into a full-width frame payload. What `peasub` puts
/// on the wire is ciphertext, so a real frame is byte-for-byte
/// indistinguishable from a (random) cover frame.
fn seal(cipher: &ChaCha20Poly1305, data: &[u8]) -> Vec<u8> {
    assert!(data.len() <= MAX_DATA, "payload exceeds {MAX_DATA} bytes");
    let mut inner = vec![0u8; INNER]; // zero pad is hidden by encryption
    inner[..LEN_HDR].copy_from_slice(&(data.len() as u16).to_be_bytes());
    inner[LEN_HDR..LEN_HDR + data.len()].copy_from_slice(data);

    let nonce = ChaCha20Poly1305::generate_nonce(&mut OsRng);
    let ct = cipher.encrypt(&nonce, inner.as_ref()).expect("encrypt");
    let mut out = Vec::with_capacity(PAYLOAD); // 12 + 196 + 16 == 224 == PAYLOAD
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&ct);
    out
}

/// Returns the plaintext if the frame authenticates, or `None` for
/// cover frames (and frames addressed to a different key) —
/// decryption failure is the filter.
fn open(cipher: &ChaCha20Poly1305, frame: &[u8]) -> Option<Vec<u8>> {
    let payload = frame.get(ID_SIZE..)?; // strip peasub's 32-byte ID
    let nonce = Nonce::from_slice(payload.get(..NONCE)?);
    let inner = cipher.decrypt(nonce, payload.get(NONCE..)?).ok()?;
    let len = u16::from_be_bytes(inner.get(..LEN_HDR)?.try_into().ok()?) as usize;
    inner.get(LEN_HDR..LEN_HDR + len).map(<[u8]>::to_vec)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // DEMO ONLY: a hard-coded shared key. A real deployment does key
    // agreement (per-group keys, or a pea2pea Noise handshake) —
    // `peasub` deliberately stays out of the crypto business.
    let cipher = ChaCha20Poly1305::new(Key::from_slice(&[0x42; 32]));

    let mk = |name: &str| -> Result<Node, Box<dyn std::error::Error>> {
        Ok(Node::new(NodeConfig {
            name: Some(name.into()),
            listener_addr: Some("127.0.0.1:0".parse()?),
            cover: CoverStrategy::Constant {
                interval: Duration::from_millis(100),
            },
            ..Default::default()
        }))
    };

    let alice = mk("alice")?;
    let bob = mk("bob")?;
    alice.spawn().await?;
    bob.spawn().await?;

    let bob_addr = bob.local_addr().await?;
    alice.connect(bob_addr).await?;
    for _ in 0..50 {
        if alice.connected_peers().contains(&bob_addr) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Subscribe before publishing so Bob doesn't miss the frame.
    let mut bob_rx = bob.subscribe();
    alice.publish(&seal(&cipher, b"hello, peasub"))?;
    println!("Alice published an encrypted message.");

    // Bob drains frames until one authenticates; cover frames fail to
    // decrypt and are silently skipped.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut got = None;
    while tokio::time::Instant::now() < deadline {
        if let Ok(Ok(frame)) = tokio::time::timeout(Duration::from_millis(200), bob_rx.recv()).await
            && let Some(plaintext) = open(&cipher, &frame)
        {
            got = Some(plaintext);
            break;
        }
    }

    match got {
        Some(p) => println!("Bob decrypted: {:?}", String::from_utf8_lossy(&p)),
        None => println!("Bob did not receive the message within the deadline."),
    }

    alice.shutdown().await;
    bob.shutdown().await;
    Ok(())
}
