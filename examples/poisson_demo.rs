//! Same scenario as `demo.rs`, but with a *Poisson*-distributed
//! cover schedule. Demonstrates that even an observer who knows
//! the *configuration* of the cover process (mean rate) cannot
//! tell, from the inter-arrival time distribution alone, when the
//! application was actually sending real messages.
//!
//! Run with: cargo run --example poisson_demo

use std::time::{Duration, Instant};

use peasub::{CoverStrategy, Node, NodeConfig};

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔═════════════════════════════════════════════════════════════╗");
    println!("║  peasub — Poisson cover-traffic demo                        ║");
    println!("╚═════════════════════════════════════════════════════════════╝");
    println!();
    println!("Cover rate: Poisson with mean 10 messages/second (mean");
    println!("inter-arrival = 100 ms, but the actual times are exponentially");
    println!("distributed). The application is \"idle\" until t = 1.0 s,");
    println!("when a real message is published. Bob records every frame he");
    println!("receives, but cannot tell — the inter-arrival times follow the");
    println!("same exponential distribution before and after the publish.\n");

    fn mk_node(name: &str, cover: CoverStrategy) -> Result<Node, Box<dyn std::error::Error>> {
        Ok(Node::new(NodeConfig {
            name: Some(name.into()),
            listener_addr: Some("127.0.0.1:0".parse()?),
            cover,
            message_size: 64,
            ..Default::default()
        }))
    }

    let alice = mk_node("alice", CoverStrategy::Poisson { rate: 10.0 })?;
    let bob = mk_node(
        "bob",
        CoverStrategy::Constant {
            interval: Duration::from_secs(10),
        },
    )?;
    alice.spawn().await?;
    bob.spawn().await?;
    alice.connect(bob.local_addr().await?).await?;

    let mut bob_rx = bob.subscribe();

    let start = Instant::now();
    let publish_at = start + Duration::from_secs(1);
    let end = start + Duration::from_millis(3_000);

    let alice_pub = alice.clone();
    tokio::spawn(async move {
        tokio::time::sleep_until(publish_at.into()).await;
        alice_pub.publish(b"<<< REAL MESSAGE >>>").unwrap();
    });

    println!(
        "{:<8}  {:<5}  {:<18}",
        "t (ms)", "size", "Δt from prev (ms)"
    );
    println!("{}", "-".repeat(40));

    let mut last_t = start;
    let mut frames: Vec<(Duration, usize)> = Vec::new();
    while Instant::now() < end {
        let timeout = end.saturating_duration_since(Instant::now());
        match tokio::time::timeout(timeout, bob_rx.recv()).await {
            Ok(Ok(buf)) => {
                let t = Instant::now();
                let dt = t.duration_since(last_t);
                frames.push((t.duration_since(start), buf.len()));
                println!(
                    "{:<8}  {:<5}  {:<18.1}",
                    t.duration_since(start).as_millis(),
                    buf.len(),
                    dt.as_secs_f64() * 1000.0
                );
                last_t = t;
            }
            _ => break,
        }
    }

    println!();
    println!("=== Bob's view of Alice's traffic ===");
    println!("  frames observed : {}", frames.len());
    if frames.len() < 2 {
        return Ok(());
    }
    let iats: Vec<f64> = frames
        .windows(2)
        .map(|w| w[1].0.as_secs_f64() * 1000.0 - w[0].0.as_secs_f64() * 1000.0)
        .collect();

    let mean = iats.iter().sum::<f64>() / iats.len() as f64;
    let var = iats.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / iats.len() as f64;
    let std = var.sqrt();

    println!();
    println!("Inter-arrival time statistics (over all frames):");
    println!(
        "  mean   : {:>6.2} ms  (Poisson with rate 10 => E[Δt] = 100 ms)",
        mean
    );
    println!(
        "  stddev : {:>6.2} ms  (E[Δt] for Exp(10) = 100 ms; stddev = mean)",
        std
    );
    println!(
        "  min    : {:>6.2} ms",
        iats.iter().cloned().fold(f64::INFINITY, f64::min)
    );
    println!(
        "  max    : {:>6.2} ms",
        iats.iter().cloned().fold(f64::NEG_INFINITY, f64::max)
    );
    println!();
    println!("Even though Alice published a real message at t = 1.0 s, the");
    println!("inter-arrival-time distribution is statistically");
    println!("indistinguishable from the unmodified Poisson process. A");
    println!("passive observer cannot distinguish \"the application is");
    println!("sending\" from \"the application is idle\".");

    alice.shutdown().await;
    bob.shutdown().await;
    Ok(())
}
